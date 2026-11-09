# Kotak Neo NIFTY F&O — Position Management Bot

Manages **manually placed** Kotak Neo positions only. The bot never opens, re-enters, or increases a position.

> **Trading risk:** Options trading can result in substantial losses. A stop-loss limit order is not guaranteed to execute, especially during fast markets or price gaps. Monitor broker orders yourself; this software is not financial advice and does not guarantee protection or profit.

## Required flow

1. You manually place an entry order in Kotak Neo.
2. The bot detects the executed position (WebSocket primary, REST backup).
3. The bot manages only that existing position.
4. **Half 1 (50%)** — square-off SL-Limit:
   - Trigger: entry ± ₹2
   - SL-Limit: entry ± ₹10
5. **Half 2 (50%)** — Target + companion SL (OCO):
   - Target: entry ± ₹2
   - SL trigger: entry ± ₹2
   - SL-Limit: entry ± ₹10
6. Orders are verified as accepted/active at the broker.
7. If any order fails → Telegram alert + persistent entry lock. Automated trading stops until manual intervention.

### LONG vs SHORT

Closing side and price direction are derived from the entry side:

| Entry | Close side | Target | SL trigger / limit |
|-------|------------|--------|--------------------|
| BUY (long) | SELL | entry + ₹2 | entry − ₹2 / entry − ₹10 |
| SELL (short) | BUY | entry − ₹2 | entry + ₹2 / entry + ₹10 |

### Strict rules

- No automatic BUY (or any) entry / re-entry orders.
- Never increase position quantity automatically.
- Never square off the entire position automatically after an order failure.
- Only manage quantities belonging to the existing position.
- If quantity cannot be split into equal whole lots (odd lot count), alert the user — never guess.

## Architecture

```text
Manual entry fill
      │
      ▼
WebSocket order feed (primary)  +  REST poller (backup / reconcile)
      │
      ▼
TradingEngine — half/half exit plan (LONG/SHORT aware)
      │
      ├── Half 1: SL-Limit (50%)
      └── Half 2: Target Limit + SL-Limit OCO (50%)
      │
      ▼
Verify on broker → PROTECTED
      or
Failure → Telegram + entry lock → wait for manual action
```

## Setup

Requires Python and valid Kotak Neo and (optionally) Telegram configuration. Start in dry-run mode; confirm every setting and broker response before considering live use.

```bash
# Copy the template, then add credentials locally. Never commit .env.
cp .env.example .env

python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
pip install -r fastapi_app/requirements.txt

# From the repository root
PYTHONPATH=. python -m fastapi_app.run
```

Keep `DRY_RUN=true` until credentials, product settings, lot sizes, and price offsets have been independently confirmed. Do not share broker credentials or tokens.

## Tests

```bash
PYTHONPATH=. pytest fastapi_app/tests/test_required_flow.py -v
```

## Operational notes

- Lot size is resolved from the broker scrip master (no silent fallback).
- Product type (`MIS` / `NRML`) is inherited from the entry order.
- SL-Limit orders can leave residual risk in fast markets if price gaps through the limit.
- An alert or entry lock is not a substitute for monitoring the broker account and verifying the final position.
