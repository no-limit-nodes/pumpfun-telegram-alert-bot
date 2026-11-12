"""PumpFun alert engine and notifiers.

The engine is pure Python and does no network I/O, so it can be unit-tested
with recorded events. Notifiers deliver the resulting alerts to the console,
Telegram, or a Discord webhook.
"""

from __future__ import annotations

import html
import json
import logging
import queue
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

log = logging.getLogger("pumpfun_bot")

LAMPORTS_PER_SOL = 1_000_000_000
PUMPFUN_TOKEN_DECIMALS = 6  # PumpFun mints use 6 decimals (1B supply = 1e15 raw units)
DEFAULT_SUPPLY_RAW = 1_000_000_000 * 10**PUMPFUN_TOKEN_DECIMALS

TOPIC_CREATE = "prod.rpc.solana.pumpfun.create"
TOPIC_TRADE = "prod.rpc.solana.pumpfun.trade"
TOPIC_GRADUATE = "prod.rpc.solana.pumpfun.graduate"


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def price_sol(virtual_sol_reserves, virtual_token_reserves) -> Optional[float]:
    """Bonding-curve spot price in SOL per whole token, from virtual reserves."""
    vsr, vtr = _int(virtual_sol_reserves), _int(virtual_token_reserves)
    if vsr <= 0 or vtr <= 0:
        return None
    return (vsr / LAMPORTS_PER_SOL) / (vtr / 10**PUMPFUN_TOKEN_DECIMALS)


def market_cap_sol(event: dict) -> Optional[float]:
    """Fully diluted market cap in SOL implied by the bonding-curve price."""
    p = price_sol(event.get("virtual_sol_reserves"), event.get("virtual_token_reserves"))
    if p is None:
        return None
    supply = _int(event.get("token_total_supply"), DEFAULT_SUPPLY_RAW) or DEFAULT_SUPPLY_RAW
    return p * supply / 10**PUMPFUN_TOKEN_DECIMALS


def is_buy(trade: dict) -> bool:
    if "is_buy" in trade:
        return bool(trade["is_buy"])
    return str(trade.get("ix_name", "")).lower().startswith("buy")


@dataclass
class Alert:
    kind: str  # launch | graduation | whale | momentum | wallet
    title: str
    lines: List[str]
    mint: str = ""
    signature: str = ""
    data: dict = field(default_factory=dict)

    def links(self) -> List[str]:
        out = []
        if self.mint:
            out.append(f"https://pump.fun/coin/{self.mint}")
        if self.signature:
            out.append(f"https://solscan.io/tx/{self.signature}")
        return out

    def as_text(self) -> str:
        return "\n".join([self.title, *self.lines, *self.links()])

    def as_html(self) -> str:
        body = [f"<b>{html.escape(self.title)}</b>"]
        body += [html.escape(line) for line in self.lines]
        anchors = [f'<a href="{html.escape(url, quote=True)}">{label}</a>'
                   for url, label in zip(self.links(), ["pump.fun", "tx"] if self.mint else ["tx"])]
        if anchors:
            body.append(" · ".join(anchors))
        return "\n".join(body)

    def as_json(self) -> str:
        return json.dumps({"kind": self.kind, "title": self.title, "lines": self.lines,
                           "mint": self.mint, "signature": self.signature,
                           "links": self.links(), "data": self.data})


@dataclass
class BotConfig:
    launches: bool = True
    graduations: bool = True
    whale_sol: float = 0.0  # alert on single buys >= this many SOL (0 = off)
    momentum_buys: int = 0  # alert when a new launch gets this many unique buyers... (0 = off)
    momentum_window_s: int = 60  # ...within this many seconds of its creation
    keywords: List[str] = field(default_factory=list)  # filter launch alerts by name/symbol
    watch_wallets: Set[str] = field(default_factory=set)  # creators/traders to always report
    max_tracked_launches: int = 5000

    def needs_trades(self) -> bool:
        return self.whale_sol > 0 or self.momentum_buys > 0 or bool(self.watch_wallets)

    def topics(self) -> List[str]:
        topics = []
        if self.launches or self.momentum_buys > 0 or self.watch_wallets:
            topics.append(TOPIC_CREATE)
        if self.needs_trades():
            topics.append(TOPIC_TRADE)
        if self.graduations:
            topics.append(TOPIC_GRADUATE)
        return topics


@dataclass
class _Launch:
    name: str
    symbol: str
    created_at: int
    buyers: Set[str] = field(default_factory=set)
    momentum_alerted: bool = False


def _fmt_sol(lamports) -> str:
    return f"{_int(lamports) / LAMPORTS_PER_SOL:,.3f} SOL"


def _short(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}" if len(addr) > 10 else addr


class AlertEngine:
    """Turns decoded NLN PumpFun events into alerts according to ``BotConfig``."""

    def __init__(self, config: BotConfig):
        self.cfg = config
        self.keywords = [k.lower() for k in config.keywords if k.strip()]
        self.launches: "OrderedDict[str, _Launch]" = OrderedDict()
        self.stats = {"create": 0, "trade": 0, "graduate": 0, "alerts": 0}

    # -- dispatch -----------------------------------------------------------
    def handle(self, topic: str, event: dict) -> List[Alert]:
        if topic.endswith(".create"):
            self.stats["create"] += 1
            alerts = self._on_create(event)
        elif topic.endswith(".trade"):
            self.stats["trade"] += 1
            alerts = self._on_trade(event)
        elif topic.endswith(".graduate"):
            self.stats["graduate"] += 1
            alerts = self._on_graduate(event)
        else:
            alerts = []
        self.stats["alerts"] += len(alerts)
        return alerts

    # -- handlers -----------------------------------------------------------
    def _matches_keywords(self, name: str, symbol: str) -> bool:
        if not self.keywords:
            return True
        hay = f"{name} {symbol}".lower()
        return any(k in hay for k in self.keywords)

    def _on_create(self, ev: dict) -> List[Alert]:
        mint = ev.get("mint", "")
        name, symbol = str(ev.get("name", "")), str(ev.get("symbol", ""))
        creator = ev.get("creator") or ev.get("user") or ""
        created_at = _int(ev.get("timestamp") or ev.get("block_time"), int(time.time()))
        if mint:
            self.launches[mint] = _Launch(name=name, symbol=symbol, created_at=created_at)
            while len(self.launches) > self.cfg.max_tracked_launches:
                self.launches.popitem(last=False)

        mcap = market_cap_sol(ev)
        lines = [
            f"Mint: {mint}",
            f"Creator: {creator}",
            f"Start mcap: {mcap:,.1f} SOL" if mcap is not None else "Start mcap: unknown",
        ]
        if ev.get("uri"):
            lines.append(f"Metadata: {ev['uri']}")
        common = dict(mint=mint, signature=ev.get("signature", ""), data=ev)

        if creator in self.cfg.watch_wallets:
            return [Alert("wallet", f"👀 Watched wallet launched {symbol} ({name})", lines, **common)]
        if self.cfg.launches and self._matches_keywords(name, symbol):
            return [Alert("launch", f"🚀 New PumpFun launch: {symbol} ({name})", lines, **common)]
        return []

    def _on_trade(self, ev: dict) -> List[Alert]:
        alerts: List[Alert] = []
        mint, user = ev.get("mint", ""), ev.get("user", "")
        buy = is_buy(ev)
        sol = _int(ev.get("sol_amount"))
        launch = self.launches.get(mint)
        label = f"{launch.symbol} ({launch.name})" if launch else _short(mint)
        mcap = market_cap_sol(ev)
        mcap_line = f"Mcap after trade: {mcap:,.1f} SOL" if mcap is not None else None
        common = dict(mint=mint, signature=ev.get("signature", ""), data=ev)

        if user and user in self.cfg.watch_wallets:
            side = "bought" if buy else "sold"
            lines = [f"Wallet: {user}", f"Amount: {_fmt_sol(sol)}", f"Mint: {mint}"]
            if mcap_line:
                lines.append(mcap_line)
            alerts.append(Alert("wallet", f"👀 Watched wallet {side} {label}", lines, **common))

        if buy and self.cfg.whale_sol > 0 and sol >= self.cfg.whale_sol * LAMPORTS_PER_SOL:
            lines = [f"Buyer: {user}", f"Size: {_fmt_sol(sol)}", f"Mint: {mint}"]
            if mcap_line:
                lines.append(mcap_line)
            alerts.append(Alert("whale", f"🐋 Whale buy on {label}: {_fmt_sol(sol)}", lines, **common))

        if buy and launch and self.cfg.momentum_buys > 0 and not launch.momentum_alerted:
            ts = _int(ev.get("timestamp") or ev.get("block_time"), launch.created_at)
            age = ts - launch.created_at
            if age <= self.cfg.momentum_window_s:
                if user:
                    launch.buyers.add(user)
                if len(launch.buyers) >= self.cfg.momentum_buys:
                    launch.momentum_alerted = True
                    lines = [f"{len(launch.buyers)} unique buyers in {max(age, 0)}s after launch",
                             f"Mint: {mint}"]
                    if mcap_line:
                        lines.append(mcap_line)
                    alerts.append(Alert("momentum", f"🔥 Early momentum: {label}", lines, **common))
        return alerts

    def _on_graduate(self, ev: dict) -> List[Alert]:
        if not self.cfg.graduations:
            return []
        mint = ev.get("mint", "")
        launch = self.launches.get(mint)
        label = f"{launch.symbol} ({launch.name})" if launch else _short(mint)
        lines = [
            f"Mint: {mint}",
            f"Liquidity migrated: {_fmt_sol(ev.get('sol_amount'))}",
            f"New AMM pool: {ev.get('pool', 'unknown')}",
        ]
        return [Alert("graduation", f"🎓 Graduated from the bonding curve: {label}", lines,
                      mint=mint, signature=ev.get("signature", ""), data=ev)]


# -- notifiers --------------------------------------------------------------
class Notifier:
    def send(self, alert: Alert) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:
        pass


class ConsoleNotifier(Notifier):
    def __init__(self, as_json: bool = False):
        self.as_json = as_json

    def send(self, alert: Alert) -> None:
        print(alert.as_json() if self.as_json else alert.as_text() + "\n", flush=True)


class _HttpQueueNotifier(Notifier):
    """Sends alerts from a background thread so slow HTTP never stalls the stream."""

    min_interval = 1.0  # seconds between messages (Telegram: ~1 msg/s per chat)

    def __init__(self, backlog: int = 500):
        self.q: "queue.Queue[Optional[Alert]]" = queue.Queue(maxsize=backlog)
        self.dropped = 0
        self.thread = threading.Thread(target=self._run, daemon=True, name=type(self).__name__)
        self.thread.start()

    def send(self, alert: Alert) -> None:
        try:
            self.q.put_nowait(alert)
        except queue.Full:
            self.dropped += 1
            if self.dropped % 50 == 1:
                log.warning("%s backlog full; dropped %d alerts", type(self).__name__, self.dropped)

    def close(self) -> None:
        self.q.put(None)
        self.thread.join(timeout=10)

    def _run(self) -> None:
        last = 0.0
        while True:
            alert = self.q.get()
            if alert is None:
                return
            wait = self.min_interval - (time.monotonic() - last)
            if wait > 0:
                time.sleep(wait)
            for _ in range(3):
                retry_after = self._post(alert)
                if retry_after is None:
                    break
                time.sleep(retry_after)
            last = time.monotonic()

    def _request(self, url: str, payload: dict) -> Optional[float]:
        """POST JSON; return seconds to wait before retrying, or None when done."""
        req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10):
                return None
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                try:
                    body = json.loads(exc.read() or b"{}")
                except ValueError:
                    body = {}
                retry = (body.get("parameters", {}).get("retry_after")
                         or body.get("retry_after") or exc.headers.get("Retry-After") or 5)
                return float(retry)
            log.error("%s HTTP %s: %s", type(self).__name__, exc.code, exc.read()[:300])
        except (urllib.error.URLError, TimeoutError) as exc:
            log.error("%s request failed: %s", type(self).__name__, exc)
        return None

    def _post(self, alert: Alert) -> Optional[float]:  # pragma: no cover - interface
        raise NotImplementedError


class TelegramNotifier(_HttpQueueNotifier):
    def __init__(self, bot_token: str, chat_id: str):
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.chat_id = chat_id
        super().__init__()

    def _post(self, alert: Alert) -> Optional[float]:
        return self._request(self.url, {"chat_id": self.chat_id, "text": alert.as_html(),
                                        "parse_mode": "HTML", "disable_web_page_preview": True})


class DiscordNotifier(_HttpQueueNotifier):
    min_interval = 0.5

    def __init__(self, webhook_url: str):
        self.url = webhook_url
        super().__init__()

    def _post(self, alert: Alert) -> Optional[float]:
        # allowed_mentions stops token names like "@everyone" from pinging the server.
        return self._request(self.url, {"content": alert.as_text()[:1990],
                                        "allowed_mentions": {"parse": []}})


def broadcast(notifiers: Iterable[Notifier], alerts: Iterable[Alert]) -> None:
    for alert in alerts:
        for n in notifiers:
            n.send(alert)
