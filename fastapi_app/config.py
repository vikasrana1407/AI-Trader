"""
FastAPI Bot Configuration.
Reads from environment variables (.env file) with safe defaults.
All settings are instance attributes so test fixtures can mutate them via monkeypatch.
"""

import os
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env if present (DO NOT fall back to .env.example at runtime)
try:
    from dotenv import load_dotenv
    _env = BASE_DIR / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass


def _bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).lower() in ("true", "1", "yes")

def _float(key: str, default: str) -> float:
    return float(os.getenv(key, default))

def _int(key: str, default: str) -> int:
    return int(os.getenv(key, default))

def _str(key: str, default: str = "") -> str:
    return os.getenv(key, default)


class BotSettings:
    """Mutable settings container — each attribute can be overridden at runtime."""

    def __init__(self):
        # ── Operation Mode ────────────────────────────────────────
        self.DRY_RUN: bool                     = _bool("DRY_RUN", "true")
        self.LOG_LEVEL: str                    = _str("LOG_LEVEL", "INFO").upper()
        # Hard safety: never auto-enter or re-enter
        self.AUTO_ENTRY_ENABLED: bool          = False
        self.AUTO_REENTRY_ENABLED: bool        = False
        self.ENABLE_SCHEMA_DIAGNOSTICS: bool   = _bool("ENABLE_SCHEMA_DIAGNOSTICS", "false")

        # ── Kotak Neo Credentials ─────────────────────────────────
        self.KOTAK_CONSUMER_KEY: str           = _str("KOTAK_CONSUMER_KEY")
        self.KOTAK_CONSUMER_SECRET: str        = _str("KOTAK_CONSUMER_SECRET")
        self.KOTAK_ENVIRONMENT: str            = _str("KOTAK_ENVIRONMENT", "PROD").upper()
        self.KOTAK_MOBILE_NUMBER: str          = _str("KOTAK_MOBILE_NUMBER")
        self.KOTAK_PASSWORD: str               = _str("KOTAK_PASSWORD")
        self.KOTAK_MPIN: str                   = _str("KOTAK_MPIN")
        self.KOTAK_TOTP_SECRET: str            = _str("KOTAK_TOTP_SECRET")
        self.KOTAK_UCC: str                    = _str("KOTAK_UCC")

        # ── Telegram Alerts ───────────────────────────────────────
        self.TELEGRAM_BOT_TOKEN: str           = _str("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID: str             = _str("TELEGRAM_CHAT_ID")
        self.TELEGRAM_ALERTS_ENABLED: bool     = _bool("TELEGRAM_ALERTS_ENABLED", "true")

        # ── Required flow parameters (fixed structure) ────────────
        # Even lots: Target 50% + SL 50% | Odd lots: SL 100%
        self.ORDER_STRUCTURE: str              = "HALF_TARGET_HALF_SL"
        self.HALF_B_SL_AFTER_TARGET: str       = "KEEP"
        # Odd lot counts → place SL on 100% qty (no target)
        self.SINGLE_LOT_BEHAVIOR: str          = "SL_100"
        self.PROFIT_OFFSET: float              = _float("PROFIT_OFFSET", os.getenv("TARGET_OFFSET", "2.0"))
        self.TARGET_OFFSET: float              = self.PROFIT_OFFSET
        self.SL_OFFSET: float                  = _float("SL_OFFSET", "2.0")
        self.SL_LIMIT_BUFFER: float            = _float("SL_LIMIT_BUFFER", "10.0")
        self.PARTIAL_FILL_HANDLING: str        = _str("PARTIAL_FILL_HANDLING", "AGGREGATED").upper()
        self.TICK_SIZE: float                  = _float("TICK_SIZE", "0.05")
        self.NIFTY_LOT_SIZE: Optional[int]     = _int("NIFTY_LOT_SIZE", "0") if os.getenv("NIFTY_LOT_SIZE") else None
        self.EXCHANGE_SEGMENT: str             = _str("EXCHANGE_SEGMENT", "nse_fo")
        self.PRODUCT_TYPE: str                 = _str("PRODUCT_TYPE", "MIS")
        # When entry is BO/CO (SDK cannot place_order with BO), place protective
        # exits using this product instead. Set empty to refuse + alert only.
        self.BO_PROTECT_AS_PRODUCT: str        = _str("BO_PROTECT_AS_PRODUCT", "MIS").upper()
        self.SYMBOL_FILTER_PREFIX: str         = _str("SYMBOL_FILTER_PREFIX", "NIFTY")

        # ── Protection Safety (ALERT only — never auto-flatten) ───
        self.SL_FAIL_ACTION: str               = "ALERT"
        self.SL_MAX_RETRIES: int               = _int("SL_MAX_RETRIES", "2")
        self.VERIFY_MAX_RETRIES: int           = _int("VERIFY_MAX_RETRIES", "3")
        self.VERIFY_INTERVAL_SEC: float        = _float("VERIFY_INTERVAL_SEC", "0.3")

        # Kept for call-site compatibility; unused by required-flow calc
        self.TARGET_ALLOCATION_PERCENT: float  = 50.0
        self.ODD_QTY_ROUNDING: str             = "FLOOR_TARGET"

        # ── Configuration Validation ──────────────────────────────
        valid_partial_fill = {"AGGREGATED", "IMMEDIATE"}
        if self.PARTIAL_FILL_HANDLING not in valid_partial_fill:
            raise ValueError(
                f"Unsupported PARTIAL_FILL_HANDLING: '{self.PARTIAL_FILL_HANDLING}'. "
                f"Valid values are: {', '.join(sorted(valid_partial_fill))}"
            )

        if self.SL_OFFSET <= 0:
            raise ValueError(f"SL_OFFSET must be > 0, got {self.SL_OFFSET}")

        if self.SL_LIMIT_BUFFER < 0:
            raise ValueError(f"SL_LIMIT_BUFFER must be >= 0, got {self.SL_LIMIT_BUFFER}")

        if not self.DRY_RUN:
            missing = []
            for req_key in ("KOTAK_CONSUMER_KEY", "KOTAK_MOBILE_NUMBER", "KOTAK_PASSWORD", "KOTAK_MPIN"):
                val = getattr(self, req_key, "")
                if not val or "your_" in val.lower() or val.strip() == "":
                    missing.append(req_key)
            if missing:
                raise ValueError(
                    f"LIVE mode enabled (DRY_RUN=false) but required Kotak credentials are missing or placeholders: "
                    f"{', '.join(missing)}"
                )

        # ── Timing & WebSocket ───────────────────────────────────
        self.POLL_INTERVAL_SEC: float          = _float("POLL_INTERVAL_MS", "1000") / 1000.0
        self.REST_BACKUP_POLL_INTERVAL_SEC: float = _float("REST_BACKUP_POLL_INTERVAL_SEC", "5.0")
        self.WS_RECONNECT_DELAY_SEC: float     = _float("WS_RECONNECT_DELAY_SEC", "2.0")
        self.WS_MAX_RECONNECT_ATTEMPTS: int    = _int("WS_MAX_RECONNECT_ATTEMPTS", "10")
        self.WS_SILENCE_ALERT_SEC: float       = _float("WS_SILENCE_ALERT_SEC", "300.0")
        self.RECONCILIATION_INTERVAL_SEC: float = _float("RECONCILIATION_INTERVAL_SEC", "15.0")
        self.RECONCILIATION_ALERT_COOLDOWN_SEC: float = _float("RECONCILIATION_ALERT_COOLDOWN_SEC", "1800.0")
        self.PERSISTENCE_FILE: str             = str(BASE_DIR / "data" / "fastapi_state.json")


# Module-level singleton — tests can reassign attributes directly
settings = BotSettings()
