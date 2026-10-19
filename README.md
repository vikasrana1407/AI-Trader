# Kotak Neo NIFTY F&O — Position Management Bot

Manages **manually placed** Kotak Neo positions only. The bot never opens, re-enters, or increases a position.

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

```
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

```bash
cp .env.example .env
# Edit .env — keep DRY_RUN=true until credentials and offsets are confirmed

python -m venv venv
# Windows: .\venv\Scripts\activate
# Linux/macOS: source venv/bin/activate
pip install -r fastapi_app/requirements.txt

PYTHONPATH=. python -m fastapi_app.run
```

## Tests

```bash
PYTHONPATH=. pytest fastapi_app/tests/test_required_flow.py -v
```

## Notes

- Lot size is resolved from the broker scrip master (no silent fallback).
- Product type (`MIS` / `NRML`) is inherited from the entry order.
- SL-Limit orders can leave residual risk in fast markets if price gaps through the limit.
