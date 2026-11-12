# PumpFun Alert Bot Guide: Events, Formulas and Deployment

This guide explains how the PumpFun alert bot turns NoLimitNodes (NLN) decoded events into Telegram and Discord alerts, and how to run it 24/7.

## Which NLN topics does the bot use?

| Topic | Event | Used for |
|---|---|---|
| `prod.rpc.solana.pumpfun.create` | `CreateEntry` | Launch alerts, momentum tracking, watched creators |
| `prod.rpc.solana.pumpfun.trade` | `TradeEntry` | Whale buys, momentum buyers, watched traders |
| `prod.rpc.solana.pumpfun.graduate` | `GraduateEvent` | Graduation alerts |

Full field references are in the [NLN schema docs](https://github.com/no-limit-nodes/solana-programs-data-streaming-reference/blob/main/docs/schemas.md). The bot reads these fields:

- **create:** `mint`, `name`, `symbol`, `creator` (falls back to `user`), `uri`, `timestamp`, `virtual_sol_reserves`, `virtual_token_reserves`, `token_total_supply`, `signature`
- **trade:** `mint`, `user`, `sol_amount` (lamports), `ix_name` (`buy` or `sell`), `timestamp`, `virtual_*_reserves`, `signature`
- **graduate:** `mint`, `sol_amount`, `pool`, `signature`

Every numeric field arrives as a string. The bot parses it defensively, so a missing optional field never crashes the stream.

## How are the alerts decided?

- **Launch:** sent for each `create` event when launches are on and the name or symbol contains one of `--keywords` (if any are set).
- **Graduation:** sent for each `graduate` event. The token label comes from the earlier `create` event when the bot saw it, and otherwise from a shortened mint.
- **Whale buy:** sent when `ix_name` is a buy and `sol_amount ≥ --whale-sol × 1e9` lamports.
- **Early momentum:** the bot remembers the most recent 5,000 launches. Each buy within `--momentum-window` seconds of a token's creation adds the buyer to a set. When that set reaches `--momentum-buys` unique wallets, one alert is sent for the token. Sells and repeat buyers do not count.
- **Wallet watch:** any `create` whose creator, or any `trade` whose `user`, is in `--watch` is always alerted, regardless of keyword filters.

## What formula gives the bonding-curve price and market cap?

The PumpFun bonding curve prices tokens from its virtual reserves:

```text
price_sol  = (virtual_sol_reserves / 1e9) / (virtual_token_reserves / 1e6)
market_cap = price_sol × token_total_supply / 1e6
```

SOL has 9 decimals and PumpFun tokens have 6. With the launch reserves in the schema example (30 SOL virtual and 1.073 billion virtual tokens) and a supply of 1 billion tokens, the starting market cap is about 28 SOL.

## How do I run the bot 24/7?

Any always-on Linux host works. A minimal systemd unit:

```ini
[Unit]
Description=PumpFun alert bot
After=network-online.target

[Service]
WorkingDirectory=/opt/pumpfun-telegram-alert-bot
EnvironmentFile=/opt/pumpfun-telegram-alert-bot/.env
ExecStart=/opt/pumpfun-telegram-alert-bot/.venv/bin/python run_bot.py --telegram --quiet
Restart=always
RestartSec=5
RestartPreventExitStatus=2

[Install]
WantedBy=multi-user.target
```

The stream client reconnects with exponential backoff (1 s up to 30 s) on network errors. It exits with code 2 when NLN rejects the API key, and `RestartPreventExitStatus=2` stops systemd from restarting it in a loop on a bad key.

## How do I avoid Telegram rate limits?

Telegram limits how fast a bot can post to one chat. The bot sends at most one message per second from a background queue and waits for Telegram's `retry_after` on HTTP 429. If more than 500 alerts are queued, new ones are dropped and a warning is logged. If you hit that, tighten your filters: raise `--whale-sol`, set `--keywords`, or turn off plain launch alerts with `--no-launches` and rely on momentum alerts.

## FAQ

### Why use a decoded stream instead of `logsSubscribe` on an RPC node?

`logsSubscribe` returns raw program logs. You then have to decode Anchor events, track PumpFun program upgrades and fetch transactions for missing fields. NLN delivers typed `create`, `trade` and `graduate` events with consistent field names, so the bot holds only alert logic.

### Can I reuse the stream client in my own project?

Yes. `nln_stream.subscribe(topic, api_key)` yields decoded events for any NLN topic, and `nln_stream.merge([...], api_key)` combines several topics into one iterator. The client has no dependencies beyond `grpcio` and `grpcio-tools`.

### How do I test filter changes safely?

Run `python run_bot.py --replay examples/sample-events.jsonl` with your flags, or record your own JSONL file with lines shaped like `{"topic": "...", "event": {...}}`. Then run `python -m unittest discover -s tests` before deploying.
