#!/usr/bin/env python3
"""Run the PumpFun alert bot on the NoLimitNodes decoded event stream.

Examples:
    # console only, every launch + graduation
    python run_bot.py

    # Telegram alerts for whale buys >= 5 SOL and launches with 15 buyers in 60s
    python run_bot.py --no-launches --whale-sol 5 --momentum-buys 15 --telegram

    # offline: replay recorded events (no API key needed)
    python run_bot.py --replay examples/sample-events.jsonl --whale-sol 2 --momentum-buys 3
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time

from pumpfun_bot import (AlertEngine, BotConfig, ConsoleNotifier, DiscordNotifier,
                         TelegramNotifier, broadcast)


def csv(value: str):
    return [v.strip() for v in value.split(",") if v.strip()]


def parse_args(argv=None):
    env = os.environ.get
    p = argparse.ArgumentParser(description="PumpFun launch, graduation and whale alerts via NLN.")
    p.add_argument("--no-launches", action="store_true", help="disable new-launch alerts")
    p.add_argument("--no-graduations", action="store_true", help="disable graduation alerts")
    p.add_argument("--whale-sol", type=float, default=float(env("WHALE_SOL", 0)),
                   help="alert on single buys of at least this many SOL (0 = off)")
    p.add_argument("--momentum-buys", type=int, default=int(env("MOMENTUM_BUYS", 0)),
                   help="alert when a new launch reaches this many unique buyers (0 = off)")
    p.add_argument("--momentum-window", type=int, default=int(env("MOMENTUM_WINDOW_S", 60)),
                   help="seconds after launch in which momentum buyers are counted")
    p.add_argument("--keywords", type=csv, default=csv(env("KEYWORDS", "")),
                   help="comma-separated words; only launches whose name/symbol match are alerted")
    p.add_argument("--watch", type=csv, default=csv(env("WATCH_WALLETS", "")),
                   help="comma-separated wallets whose launches and trades are always alerted")
    p.add_argument("--telegram", action="store_true",
                   help="send to Telegram (needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)")
    p.add_argument("--discord", action="store_true", help="send to Discord (needs DISCORD_WEBHOOK_URL)")
    p.add_argument("--quiet", action="store_true", help="do not print alerts to the console")
    p.add_argument("--json", action="store_true", help="print alerts as JSON lines")
    p.add_argument("--replay", metavar="FILE",
                   help="JSONL file of {\"topic\":..., \"event\":...} records to replay offline")
    p.add_argument("--stats-every", type=int, default=60, help="log stream stats every N seconds")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def build_notifiers(args):
    notifiers = []
    if not args.quiet:
        notifiers.append(ConsoleNotifier(as_json=args.json))
    if args.telegram:
        token, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
        if not token or not chat:
            sys.exit("--telegram needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        notifiers.append(TelegramNotifier(token, chat))
    if args.discord:
        url = os.environ.get("DISCORD_WEBHOOK_URL")
        if not url:
            sys.exit("--discord needs DISCORD_WEBHOOK_URL")
        notifiers.append(DiscordNotifier(url))
    return notifiers


def replay_events(path):
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                rec = json.loads(line)
                yield rec["topic"], rec["event"]


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    log = logging.getLogger("run_bot")

    cfg = BotConfig(
        launches=not args.no_launches,
        graduations=not args.no_graduations,
        whale_sol=args.whale_sol,
        momentum_buys=args.momentum_buys,
        momentum_window_s=args.momentum_window,
        keywords=args.keywords,
        watch_wallets=set(args.watch),
    )
    topics = cfg.topics()
    if not topics:
        sys.exit("Nothing to do: every alert type is disabled.")

    engine = AlertEngine(cfg)
    notifiers = build_notifiers(args)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    if args.replay:
        source = replay_events(args.replay)
    else:
        from nln_stream import merge, require_api_key

        source = merge(topics, require_api_key(), stop=stop)
    log.info("topics: %s", ", ".join(topics))

    last_stats = time.monotonic()
    try:
        for topic, event in source:
            broadcast(notifiers, engine.handle(topic, event))
            if stop.is_set():
                break
            if args.stats_every and time.monotonic() - last_stats >= args.stats_every:
                log.info("events/alerts so far: %s", engine.stats)
                last_stats = time.monotonic()
    except Exception as exc:  # StreamAuthError and unexpected failures
        if type(exc).__name__ == "StreamAuthError":
            log.error("NLN rejected the API key: %s", exc)
            return 2
        raise
    finally:
        for n in notifiers:
            n.close()
        log.info("final stats: %s", engine.stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
