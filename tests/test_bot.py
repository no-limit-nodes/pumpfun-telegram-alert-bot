"""Offline tests: alert engine, Telegram notifier, and the NLN gRPC client.

Run with:  python -m unittest discover -s tests -v
The gRPC test starts a local fake StreamService, so no NLN API key is needed.
"""

import json
import os
import sys
import threading
import time
import unittest
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pumpfun_bot import (AlertEngine, BotConfig, TelegramNotifier, market_cap_sol,  # noqa: E402
                         TOPIC_CREATE, TOPIC_GRADUATE, TOPIC_TRADE)
from run_bot import replay_events  # noqa: E402

SAMPLE = ROOT / "examples" / "sample-events.jsonl"
WATCHED = "97jv9pjCf5D7VUZEyj6XnXBC5JF23Vsagc3bvSF6GRmL"


def run(cfg):
    engine = AlertEngine(cfg)
    alerts = []
    for topic, event in replay_events(SAMPLE):
        alerts += engine.handle(topic, event)
    return alerts


class EngineTests(unittest.TestCase):
    def test_market_cap_from_initial_curve(self):
        ev = {"virtual_sol_reserves": "30000000000", "virtual_token_reserves": "1073000000000000",
              "token_total_supply": "1000000000000000"}
        self.assertAlmostEqual(market_cap_sol(ev), 27.959, places=2)

    def test_default_config_alerts_launches_and_graduations(self):
        kinds = [a.kind for a in run(BotConfig())]
        self.assertEqual(kinds, ["launch", "launch", "graduation"])

    def test_whale_momentum_and_wallet(self):
        cfg = BotConfig(launches=False, graduations=False, whale_sol=5,
                        momentum_buys=3, watch_wallets={WATCHED})
        kinds = [a.kind for a in run(cfg)]
        self.assertEqual(kinds, ["whale", "momentum", "wallet"])

    def test_sells_do_not_count_toward_momentum(self):
        cfg = BotConfig(launches=False, graduations=False, momentum_buys=4)
        self.assertEqual(run(cfg), [])

    def test_keyword_filter(self):
        alerts = run(BotConfig(graduations=False, keywords=["dog"]))
        self.assertEqual([a.data["symbol"] for a in alerts], ["MDOG"])

    def test_html_is_escaped_for_telegram(self):
        alerts = run(BotConfig(graduations=False, keywords=["dog"]))
        self.assertIn("Moon &lt;b&gt;Dog&lt;/b&gt;", alerts[0].as_html())
        self.assertNotIn("<b>Dog</b>", alerts[0].as_html())

    def test_topics_follow_config(self):
        self.assertEqual(BotConfig(whale_sol=0).topics(), [TOPIC_CREATE, TOPIC_GRADUATE])
        self.assertEqual(BotConfig(launches=False, graduations=False, whale_sol=1).topics(),
                         [TOPIC_TRADE])


class TelegramTests(unittest.TestCase):
    def test_sends_html_and_retries_after_429(self):
        received = []

        class Handler(BaseHTTPRequestHandler):
            calls = 0

            def do_POST(self):
                Handler.calls += 1
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if Handler.calls == 1:
                    self.send_response(429)
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": False, "parameters": {"retry_after": 0.1}}).encode())
                    return
                received.append(body)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

            def log_message(self, *a):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        notifier = TelegramNotifier("TOKEN", "123")
        notifier.url = f"http://127.0.0.1:{server.server_port}/botTOKEN/sendMessage"
        notifier.send(run(BotConfig())[0])
        notifier.close()
        server.shutdown()
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["chat_id"], "123")
        self.assertEqual(received[0]["parse_mode"], "HTML")
        self.assertIn("New PumpFun launch", received[0]["text"])


class StreamClientTests(unittest.TestCase):
    def test_merge_reads_events_from_grpc_server(self):
        import grpc
        import nln_stream

        pb, pb_grpc = nln_stream.load_stubs()
        events = {t: [e for tt, e in replay_events(SAMPLE) if tt == t]
                  for t in (TOPIC_CREATE, TOPIC_GRADUATE)}
        seen_keys = []

        class FakeNLN(pb_grpc.StreamServiceServicer):
            def Subscribe(self, request, context):
                seen_keys.append(dict(context.invocation_metadata()).get("x-api-key"))
                assert request.format == pb.JSON
                for i, ev in enumerate(events[request.topic]):
                    yield pb.SubscribeResponse(topic=request.topic, offset=i,
                                               payload=json.dumps(ev).encode())
                time.sleep(5)  # keep the stream open like the real service

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb_grpc.add_StreamServiceServicer_to_server(FakeNLN(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        os.environ["NLN_STREAM_INSECURE"] = "1"
        stop = threading.Event()
        try:
            got = []
            for topic, ev in nln_stream.merge([TOPIC_CREATE, TOPIC_GRADUATE], "sk_test",
                                              endpoint=f"127.0.0.1:{port}", stop=stop):
                got.append((topic, ev["signature"]))
                if len(got) == 3:
                    stop.set()
        finally:
            os.environ.pop("NLN_STREAM_INSECURE", None)
            server.stop(0)
        self.assertEqual(sorted(t for t, _ in got), sorted([TOPIC_CREATE, TOPIC_CREATE, TOPIC_GRADUATE]))
        self.assertEqual(set(seen_keys), {"sk_test"})

    def test_bad_key_raises_auth_error(self):
        import grpc
        import nln_stream

        pb, pb_grpc = nln_stream.load_stubs()

        class Deny(pb_grpc.StreamServiceServicer):
            def Subscribe(self, request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        pb_grpc.add_StreamServiceServicer_to_server(Deny(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        os.environ["NLN_STREAM_INSECURE"] = "1"
        try:
            with self.assertRaises(nln_stream.StreamAuthError):
                next(nln_stream.subscribe(TOPIC_CREATE, "bad", endpoint=f"127.0.0.1:{port}"))
        finally:
            os.environ.pop("NLN_STREAM_INSECURE", None)
            server.stop(0)


if __name__ == "__main__":
    unittest.main()
