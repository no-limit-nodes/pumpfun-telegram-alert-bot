# PumpFun Telegram Alert Bot: New Token, Graduation and Whale Alerts

A working PumpFun alert bot that sends **new pump.fun token launches, bonding-curve graduations, whale buys, early-momentum launches and watched-wallet trades** to Telegram, Discord or your terminal in real time. It runs on the [NoLimitNodes](https://nolimitnodes.com) decoded Solana event stream, so there is no transaction parsing, no IDL maintenance and no RPC polling.

```text
🚀 New PumpFun launch: CAT (the nick Pirate)
Mint: 9FYVRJAFa1DwtjZao34Cyfd9GYHwgDzRiQTC864ppump
Creator: Ggv9eQTn2v41TLUdW2wYsqYJuEgf5HEjxnMsqktJALLY
Start mcap: 28.0 SOL
https://pump.fun/coin/9FYVRJAFa1DwtjZao34Cyfd9GYHwgDzRiQTC864ppump
```

## What is a PumpFun alert bot?

A PumpFun alert bot watches the pump.fun program on Solana and notifies you when something you care about happens: a new token is created, a token's bonding curve completes and migrates to an AMM pool, a large buy lands, or a specific wallet trades. This bot gets those events already decoded from NoLimitNodes over gRPC and applies your filters before sending an alert.

## Features

| Alert | Trigger | Flag |
|---|---|---|
| 🚀 New launch | Every PumpFun `create` event, optionally filtered by keyword | on by default, `--keywords` |
| 🎓 Graduation | Bonding curve completes and liquidity migrates to an AMM pool | on by default |
| 🐋 Whale buy | A single buy of at least N SOL | `--whale-sol 5` |
| 🔥 Early momentum | A new launch reaches N unique buyers within T seconds | `--momentum-buys 15 --momentum-window 60` |
| 👀 Wallet watch | A watched wallet launches a token, buys or sells | `--watch WALLET1,WALLET2` |

The bot also:

- Sends to the console, **Telegram** (HTML messages, rate-limited, honors `retry_after`) and **Discord** webhooks (mentions disabled).
- Calculates market cap in SOL from bonding-curve virtual reserves.
- Reconnects automatically with exponential backoff and stops with a clear error on an invalid API key.
- Has a replay mode (`--replay`) to test filters offline without an API key.

## Quick start

```bash
git clone https://github.com/no-limit-nodes/pumpfun-telegram-alert-bot.git
cd pumpfun-telegram-alert-bot
python -m pip install -r requirements.txt

export NLN_API_KEY=sk_live_...        # https://app.nolimitnodes.com/register
python run_bot.py                      # launches + graduations in your terminal
```

### Send alerts to Telegram

1. Create a bot with [@BotFather](https://t.me/BotFather) and copy the token.
2. Add the bot to your channel or group, then get the chat ID (for a channel, `@yourchannel` works).
3. Run:

```bash
export TELEGRAM_BOT_TOKEN=123456:ABC...
export TELEGRAM_CHAT_ID=@your_channel
python run_bot.py --telegram --whale-sol 5 --momentum-buys 15
```

### Send alerts to Discord

```bash
export DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
python run_bot.py --discord --no-launches --whale-sol 10
```

### Try it offline

```bash
python run_bot.py --replay examples/sample-events.jsonl --whale-sol 5 --momentum-buys 3
```

## Configuration

Every flag can also be set as an environment variable (see [`.env.example`](.env.example)).

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--no-launches` | | off | Turn off new-launch alerts |
| `--no-graduations` | | off | Turn off graduation alerts |
| `--whale-sol` | `WHALE_SOL` | `0` (off) | Minimum single-buy size in SOL |
| `--momentum-buys` | `MOMENTUM_BUYS` | `0` (off) | Unique buyers needed for a momentum alert |
| `--momentum-window` | `MOMENTUM_WINDOW_S` | `60` | Seconds after launch in which buyers are counted |
| `--keywords` | `KEYWORDS` | none | Only alert launches whose name or symbol contains one of these words |
| `--watch` | `WATCH_WALLETS` | none | Wallets whose launches and trades are always alerted |
| `--telegram` / `--discord` | see above | off | Delivery channels |
| `--json` | | off | Print alerts as JSON lines (pipe into your own tools) |

## How it works

```text
NoLimitNodes gRPC stream (stream-1.nln.clr3.org:443)
  ├─ prod.rpc.solana.pumpfun.create     ─┐
  ├─ prod.rpc.solana.pumpfun.trade      ─┼─► AlertEngine (filters, momentum state) ─► Console / Telegram / Discord
  └─ prod.rpc.solana.pumpfun.graduate   ─┘
```

The bot subscribes only to the topics your configuration needs. The `trade` topic is high-volume (roughly 50 to 100 events per second according to the [NLN schema docs](https://github.com/no-limit-nodes/solana-programs-data-streaming-reference/blob/main/docs/schemas.md)), so it is opened only when whale, momentum or wallet alerts are on. See [`docs/guide.md`](docs/guide.md) for the event fields, the market-cap formula and deployment notes.

## Repository layout

- [`run_bot.py`](run_bot.py): command-line entry point.
- [`pumpfun_bot.py`](pumpfun_bot.py): alert engine, market-cap math and notifiers. Does no network I/O, so it is easy to unit-test.
- [`nln_stream.py`](nln_stream.py): NoLimitNodes gRPC client with reconnect and multi-topic fan-in. Reusable in other projects.
- [`examples/sample-events.jsonl`](examples/sample-events.jsonl): recorded-shape events for offline replay.
- [`tests/test_bot.py`](tests/test_bot.py): engine, Telegram retry and gRPC client tests (uses a local fake stream server).

## FAQ

### How do I get notified of new pump.fun tokens on Telegram?

Run `python run_bot.py --telegram` with `NLN_API_KEY`, `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` set. The bot subscribes to the NoLimitNodes `prod.rpc.solana.pumpfun.create` topic and posts each new launch, with its mint, creator, starting market cap and pump.fun link, to your chat.

### What does "graduated" mean on PumpFun?

A PumpFun token graduates when enough SOL has been bought into its bonding curve that the curve completes. Its liquidity then migrates to an AMM pool, typically PumpSwap. The graduation alert shows the SOL migrated and the new pool address from the NLN `graduate` event.

### How is market cap calculated?

Price in SOL per token is `(virtual_sol_reserves / 1e9) / (virtual_token_reserves / 1e6)`, using the bonding curve's virtual reserves from the event. Market cap is that price multiplied by total supply, which is usually 1 billion tokens. A fresh launch starts at about 28 SOL.

### Does this bot buy tokens or need a private key?

No. It only reads public on-chain events and sends notifications. It never signs transactions or holds keys. If you build a trading bot on top of it, keep keys in a secret manager and add your own risk controls.

### Can I use it without Telegram?

Yes. By default alerts print to the terminal. Add `--json` to get one JSON object per line for piping into scripts, databases or other bots, or use `--discord` for a Discord webhook.

## Related NoLimitNodes resources

- [Solana streaming reference (17 topics, 7 languages)](https://github.com/no-limit-nodes/solana-programs-data-streaming-reference)
- [Every Solana program decoded (1,074 events across 37 programs)](https://github.com/no-limit-nodes/every-solana-program-decoded)
- [Solana Priority Fee API](https://github.com/no-limit-nodes/solana-priority-fee-api): live compute-unit price recommendations from landed transactions

## Disclaimer

Alerts are informational and not financial advice. Memecoins are highly volatile, and many PumpFun tokens lose most of their value quickly.

## License

Released under the [MIT License](LICENSE).
