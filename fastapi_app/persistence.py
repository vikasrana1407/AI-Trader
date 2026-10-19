"""
SQLite-backed persistence layer for the FastAPI Bot.
Uses Python's built-in sqlite3 — no ORM needed, zero extra dependencies.

Tables:
  positions   — all entry positions and their exit order references
  executions  — idempotency de-duplication fingerprints
  events      — rolling audit log (capped at 1000 rows)
  partial_fill_buffers — buffers for aggregated partial fills across restarts

Thread-safety: WAL mode + per-call connections (connection-per-thread).
"""

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import settings

logger = logging.getLogger("FastAPIBot.Persistence")


def make_order_tag(role: str, entry_id: str, attempt: int = 1) -> str:
    """
    Central order-tag generator producing deterministic, unique, collision-resistant tags.
    Kotak Neo tag constraints: alphanumeric and underscore, recommended max 20 chars.
    Format: BOT_{ROLE}_{clean_id}_{attempt}
    Examples:
      BOT_TARGET_045168_1 (19 chars)
      BOT_SL_0451685_1 (16 chars)
      BOT_SL_A_451685_1 (17 chars)
      BOT_SL_B_451685_1 (17 chars)
      BOT_TGT_A_51685_1 (17 chars)
      BOT_EMG_0451685_1 (17 chars)
    """
    clean_role = "".join(c for c in role.upper() if c.isalnum() or c == "_")
    if clean_role in ("TARGET_A", "TGT_A"):
        r_prefix = "TGT_A"
    elif clean_role in ("SL_A",):
        r_prefix = "SL_A"
    elif clean_role in ("SL_B",):
        r_prefix = "SL_B"
    elif "TARGET" in clean_role or clean_role == "TGT":
        r_prefix = "TARGET"
    elif "EMG" in clean_role or "EMERGENCY" in clean_role:
        r_prefix = "EMG"
    elif "FLAT" in clean_role:
        r_prefix = "FLAT"
    else:
        r_prefix = "SL"

    # Base prefix: BOT_{r_prefix}_
    prefix = f"BOT_{r_prefix}_"
    suffix = f"_{attempt}"
    # Max allowed for clean_id so total len <= 20
    max_id_len = max(1, 20 - len(prefix) - len(suffix))
    clean_id = "".join(c for c in str(entry_id) if c.isalnum() or c == "_")[-max_id_len:]
    if not clean_id:
        clean_id = "1"

    tag = f"{prefix}{clean_id}{suffix}"
    return tag[:20]


_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS positions (
    entry_order_id   TEXT PRIMARY KEY,
    symbol           TEXT NOT NULL,
    side             TEXT NOT NULL,
    total_qty        INTEGER NOT NULL,
    entry_price      REAL NOT NULL,
    order_structure  TEXT DEFAULT 'SPLIT_TARGET_SL',
    target_qty       INTEGER DEFAULT 0,
    target_price     REAL DEFAULT 0.0,
    target_order_id  TEXT,
    sl_qty           INTEGER DEFAULT 0,
    sl_trigger_price REAL DEFAULT 0.0,
    sl_limit_price   REAL DEFAULT 0.0,
    sl_order_id      TEXT,
    half_a_target_order_id TEXT,
    half_a_sl_order_id     TEXT,
    half_b_sl_order_id     TEXT,
    half_a_qty       INTEGER DEFAULT 0,
    half_b_qty       INTEGER DEFAULT 0,
    half_a_status    TEXT,
    half_b_status    TEXT,
    recovery_attempts INTEGER DEFAULT 0,
    is_simulated     INTEGER DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'ENTRY_DETECTED',
    latency_ms       REAL DEFAULT 0.0,
    processed_qty    INTEGER DEFAULT 0,
    cumulative_value REAL DEFAULT 0.0,
    remaining_qty    INTEGER DEFAULT 0,
    error_message    TEXT,
    intent           TEXT DEFAULT 'ENTRY',
    protection_state TEXT DEFAULT 'UNVERIFIED',
    verified_sl_qty  INTEGER DEFAULT 0,
    verified_target_qty INTEGER DEFAULT 0,
    last_reconciled_at REAL DEFAULT 0.0,
    last_broker_error TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    exec_key  TEXT PRIMARY KEY,
    meta      TEXT NOT NULL,
    seen_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    detail      TEXT,
    ts          REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS partial_fill_buffers (
    order_id     TEXT PRIMARY KEY,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL,
    fills_json   TEXT NOT NULL,
    updated_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS order_roles (
    order_id     TEXT PRIMARY KEY,
    role         TEXT NOT NULL,
    position_id  TEXT,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL,
    qty          INTEGER NOT NULL,
    status       TEXT NOT NULL,
    tag          TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS order_fills (
    order_id         TEXT PRIMARY KEY,
    processed_qty    INTEGER DEFAULT 0,
    cumulative_value REAL DEFAULT 0.0,
    last_status      TEXT,
    source           TEXT,
    processed_at     REAL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS order_placement_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_id           TEXT NOT NULL,
    role             TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    tag              TEXT NOT NULL UNIQUE,
    broker_order_id  TEXT,
    symbol           TEXT NOT NULL,
    side             TEXT NOT NULL,
    qty              INTEGER NOT NULL,
    price            REAL NOT NULL,
    trigger_price    REAL DEFAULT 0.0,
    status           TEXT NOT NULL, -- 'INITIATED', 'SUBMITTED', 'VERIFIED', 'REJECTED', 'UNKNOWN', 'RECOVERED'
    error_message    TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    UNIQUE(pos_id, role, attempt)
);

CREATE TABLE IF NOT EXISTS alert_dedup (
    alert_key        TEXT PRIMARY KEY,
    last_alerted_at  REAL NOT NULL,
    count            INTEGER DEFAULT 1,
    details          TEXT
);

CREATE TABLE IF NOT EXISTS system_locks (
    lock_key         TEXT PRIMARY KEY,
    is_locked        INTEGER NOT NULL DEFAULT 0,
    reason           TEXT,
    updated_at       REAL NOT NULL
);

-- Indexes for performance and uniqueness integrity
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_order_roles_pos ON order_roles(position_id);
CREATE INDEX IF NOT EXISTS idx_order_roles_tag ON order_roles(tag);
CREATE INDEX IF NOT EXISTS idx_attempts_pos_role ON order_placement_attempts(pos_id, role);
CREATE INDEX IF NOT EXISTS idx_attempts_tag ON order_placement_attempts(tag);
CREATE INDEX IF NOT EXISTS idx_attempts_broker_id ON order_placement_attempts(broker_order_id);
"""


class JSONStore:
    """
    SQLite-backed state store with the same public interface as the original
    JSON store, so the engine and routes need zero changes.
    """

    def __init__(self, filepath: Optional[str] = None):
        target_path = filepath if filepath is not None else settings.PERSISTENCE_FILE
        # Replace .json extension with .db if needed
        db_path = target_path.replace(".json", ".db") if target_path.endswith(".json") else target_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()
        logger.info(f"SQLite persistence initialised at: {db_path}")

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(_SCHEMA)
            # Safe migrations: ensure newly added columns exist in existing DBs
            migrations = [
                ("processed_qty", "INTEGER DEFAULT 0"),
                ("cumulative_value", "REAL DEFAULT 0.0"),
                ("remaining_qty", "INTEGER DEFAULT 0"),
                ("order_structure", "TEXT DEFAULT 'SPLIT_TARGET_SL'"),
                ("half_a_target_order_id", "TEXT"),
                ("half_a_sl_order_id", "TEXT"),
                ("half_b_sl_order_id", "TEXT"),
                ("half_a_qty", "INTEGER DEFAULT 0"),
                ("half_b_qty", "INTEGER DEFAULT 0"),
                ("half_a_status", "TEXT"),
                ("half_b_status", "TEXT"),
                ("recovery_attempts", "INTEGER DEFAULT 0"),
                ("is_simulated", "INTEGER DEFAULT 0"),
                ("intent", "TEXT DEFAULT 'ENTRY'"),
                ("protection_state", "TEXT DEFAULT 'UNVERIFIED'"),
                ("verified_sl_qty", "INTEGER DEFAULT 0"),
                ("verified_target_qty", "INTEGER DEFAULT 0"),
                ("last_reconciled_at", "REAL DEFAULT 0.0"),
                ("last_broker_error", "TEXT"),
            ]
            for col_name, col_type in migrations:
                try:
                    conn.execute(f"ALTER TABLE positions ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    pass  # already exists

            # Safe migrations for order_fills (never drop/reset existing rows)
            order_fill_migrations = [
                ("last_status", "TEXT"),
                ("source", "TEXT"),
                ("processed_at", "REAL"),
            ]
            for col_name, col_type in order_fill_migrations:
                try:
                    conn.execute(f"ALTER TABLE order_fills ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    pass

            # One-time migration: flag known legacy simulated test records
            try:
                conn.execute("""
                    UPDATE positions SET is_simulated = 1
                    WHERE entry_order_id = 'ENTRY-459A83'
                       OR target_order_id = 'FAST-594D0E'
                       OR sl_order_id = 'FAST-53FCFF'
                """)
            except Exception:
                pass

    def reset_db(self):
        """Wipes all positions, executions, events, buffers, order roles, fills, attempts, and alert dedup."""
        with self._conn() as conn:
            conn.execute("DELETE FROM positions")
            conn.execute("DELETE FROM executions")
            conn.execute("DELETE FROM events")
            conn.execute("DELETE FROM partial_fill_buffers")
            conn.execute("DELETE FROM order_roles")
            conn.execute("DELETE FROM order_fills")
            conn.execute("DELETE FROM order_placement_attempts")
            conn.execute("DELETE FROM alert_dedup")
            conn.commit()
        logger.info(f"Database at {self.db_path} has been reset.")

    # ──────────────────────────────────────
    # Positions
    # ──────────────────────────────────────

    def upsert_position(self, pos: Dict[str, Any]):
        now = time.time()
        has_remaining_qty = 1 if "remaining_qty" in pos else 0
        has_error_message = 1 if "error_message" in pos else 0
        has_status = 1 if "status" in pos and pos["status"] else 0
        has_intent = 1 if "intent" in pos and pos["intent"] else 0
        has_prot_state = 1 if "protection_state" in pos and pos["protection_state"] else 0
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO positions
                    (entry_order_id, symbol, side, total_qty, entry_price,
                     order_structure, target_qty, target_price, target_order_id,
                     sl_qty, sl_trigger_price, sl_limit_price, sl_order_id,
                     half_a_target_order_id, half_a_sl_order_id, half_b_sl_order_id,
                     half_a_qty, half_b_qty, half_a_status, half_b_status, recovery_attempts,
                     is_simulated, status, latency_ms, processed_qty, cumulative_value, remaining_qty,
                     error_message, intent, protection_state, verified_sl_qty, verified_target_qty,
                     last_reconciled_at, last_broker_error, created_at, updated_at)
                VALUES
                    (:entry_order_id, :symbol, :side, :total_qty, :entry_price,
                     :order_structure, :target_qty, :target_price, :target_order_id,
                     :sl_qty, :sl_trigger_price, :sl_limit_price, :sl_order_id,
                     :half_a_target_order_id, :half_a_sl_order_id, :half_b_sl_order_id,
                     :half_a_qty, :half_b_qty, :half_a_status, :half_b_status, :recovery_attempts,
                     :is_simulated, :status, :latency_ms, :processed_qty, :cumulative_value, :remaining_qty,
                     :error_message, :intent, :protection_state, :verified_sl_qty, :verified_target_qty,
                     :last_reconciled_at, :last_broker_error, :created_at, :updated_at)
                ON CONFLICT(entry_order_id) DO UPDATE SET
                    symbol                 = COALESCE(NULLIF(excluded.symbol, ''), positions.symbol),
                    side                   = COALESCE(NULLIF(excluded.side, ''), positions.side),
                    total_qty              = CASE WHEN excluded.total_qty > 0 THEN excluded.total_qty ELSE positions.total_qty END,
                    entry_price            = CASE WHEN excluded.entry_price > 0.0 THEN excluded.entry_price ELSE positions.entry_price END,
                    order_structure        = COALESCE(NULLIF(excluded.order_structure, ''), positions.order_structure),
                    target_qty             = CASE WHEN excluded.target_qty > 0 THEN excluded.target_qty ELSE positions.target_qty END,
                    target_price           = CASE WHEN excluded.target_price > 0.0 THEN excluded.target_price ELSE positions.target_price END,
                    target_order_id        = COALESCE(excluded.target_order_id, positions.target_order_id),
                    sl_qty                 = CASE WHEN excluded.sl_qty > 0 THEN excluded.sl_qty ELSE positions.sl_qty END,
                    sl_trigger_price       = CASE WHEN excluded.sl_trigger_price > 0.0 THEN excluded.sl_trigger_price ELSE positions.sl_trigger_price END,
                    sl_limit_price         = CASE WHEN excluded.sl_limit_price > 0.0 THEN excluded.sl_limit_price ELSE positions.sl_limit_price END,
                    sl_order_id            = COALESCE(excluded.sl_order_id, positions.sl_order_id),
                    half_a_target_order_id = COALESCE(excluded.half_a_target_order_id, positions.half_a_target_order_id),
                    half_a_sl_order_id     = COALESCE(excluded.half_a_sl_order_id, positions.half_a_sl_order_id),
                    half_b_sl_order_id     = COALESCE(excluded.half_b_sl_order_id, positions.half_b_sl_order_id),
                    half_a_qty             = CASE WHEN excluded.half_a_qty > 0 THEN excluded.half_a_qty ELSE positions.half_a_qty END,
                    half_b_qty             = CASE WHEN excluded.half_b_qty > 0 THEN excluded.half_b_qty ELSE positions.half_b_qty END,
                    half_a_status          = COALESCE(excluded.half_a_status, positions.half_a_status),
                    half_b_status          = COALESCE(excluded.half_b_status, positions.half_b_status),
                    recovery_attempts      = CASE WHEN excluded.recovery_attempts > 0 THEN excluded.recovery_attempts ELSE positions.recovery_attempts END,
                    is_simulated           = CASE WHEN excluded.is_simulated != 0 THEN excluded.is_simulated ELSE positions.is_simulated END,
                    status                 = CASE WHEN :has_status THEN excluded.status ELSE positions.status END,
                    latency_ms             = CASE WHEN excluded.latency_ms > 0.0 THEN excluded.latency_ms ELSE positions.latency_ms END,
                    processed_qty          = CASE WHEN excluded.processed_qty > 0 THEN excluded.processed_qty ELSE positions.processed_qty END,
                    cumulative_value       = CASE WHEN excluded.cumulative_value > 0.0 THEN excluded.cumulative_value ELSE positions.cumulative_value END,
                    remaining_qty          = CASE WHEN :has_remaining_qty THEN excluded.remaining_qty ELSE positions.remaining_qty END,
                    error_message          = CASE WHEN :has_error_message THEN excluded.error_message ELSE positions.error_message END,
                    intent                 = CASE WHEN :has_intent THEN excluded.intent ELSE positions.intent END,
                    protection_state       = CASE WHEN :has_prot_state THEN excluded.protection_state ELSE positions.protection_state END,
                    verified_sl_qty        = CASE WHEN excluded.verified_sl_qty > 0 THEN excluded.verified_sl_qty ELSE positions.verified_sl_qty END,
                    verified_target_qty    = CASE WHEN excluded.verified_target_qty > 0 THEN excluded.verified_target_qty ELSE positions.verified_target_qty END,
                    last_reconciled_at     = CASE WHEN excluded.last_reconciled_at > 0.0 THEN excluded.last_reconciled_at ELSE positions.last_reconciled_at END,
                    last_broker_error      = COALESCE(excluded.last_broker_error, positions.last_broker_error),
                    created_at             = CASE WHEN positions.created_at > 0.0 THEN positions.created_at ELSE excluded.created_at END,
                    updated_at             = excluded.updated_at
            """, {
                "entry_order_id":         pos.get("entry_order_id", ""),
                "symbol":                 pos.get("symbol", ""),
                "side":                   pos.get("side", ""),
                "total_qty":              pos.get("total_qty", 0),
                "entry_price":            pos.get("entry_price", 0.0),
                "order_structure":        pos.get("order_structure", "SPLIT_TARGET_SL"),
                "target_qty":             pos.get("target_qty", 0),
                "target_price":           pos.get("target_price", 0.0),
                "target_order_id":        pos.get("target_order_id"),
                "sl_qty":                 pos.get("sl_qty", 0),
                "sl_trigger_price":       pos.get("sl_trigger_price", 0.0),
                "sl_limit_price":         pos.get("sl_limit_price", 0.0),
                "sl_order_id":            pos.get("sl_order_id"),
                "half_a_target_order_id": pos.get("half_a_target_order_id") or pos.get("target_order_id"),
                "half_a_sl_order_id":     pos.get("half_a_sl_order_id"),
                "half_b_sl_order_id":     pos.get("half_b_sl_order_id") or pos.get("sl_order_id"),
                "half_a_qty":             pos.get("half_a_qty", pos.get("target_qty", 0)),
                "half_b_qty":             pos.get("half_b_qty", pos.get("sl_qty", 0)),
                "half_a_status":          pos.get("half_a_status"),
                "half_b_status":          pos.get("half_b_status"),
                "recovery_attempts":      pos.get("recovery_attempts", 0),
                "is_simulated":           int(pos.get("is_simulated", 0)),
                "status":                 pos.get("status", "ENTRY_DETECTED"),
                "latency_ms":             pos.get("latency_ms", 0.0),
                "processed_qty":          pos.get("processed_qty", pos.get("total_qty", 0)),
                "cumulative_value":       pos.get("cumulative_value") if pos.get("cumulative_value") is not None else self.get_cumulative_value(pos.get("entry_order_id", "")),
                "remaining_qty":          pos.get("remaining_qty", pos.get("total_qty", 0)),
                "error_message":          pos.get("error_message"),
                "intent":                 pos.get("intent", "ENTRY"),
                "protection_state":       pos.get("protection_state", "UNVERIFIED"),
                "verified_sl_qty":        pos.get("verified_sl_qty", 0),
                "verified_target_qty":    pos.get("verified_target_qty", 0),
                "last_reconciled_at":     pos.get("last_reconciled_at", 0.0),
                "last_broker_error":      pos.get("last_broker_error"),
                "created_at":             pos.get("created_at", now),
                "updated_at":             now,
                "has_remaining_qty":      has_remaining_qty,
                "has_error_message":      has_error_message,
                "has_status":             has_status,
                "has_intent":             has_intent,
                "has_prot_state":         has_prot_state,
            })

    def get_position(self, entry_order_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM positions WHERE entry_order_id = ?", (entry_order_id,)
            ).fetchone()
            return dict(row) if row else None

    # ──────────────────────────────────────
    # Placement Attempts (P0: Unique tags & retry tracking)
    # ──────────────────────────────────────

    def allocate_placement_attempt(
        self, pos_id: str, role: str, symbol: str, side: str,
        qty: int, price: float, trigger_price: float = 0.0
    ) -> Tuple[int, str]:
        """
        Allocates the next deterministic attempt counter and order tag for a position leg.
        Uses a transaction and unique constraint to guarantee no duplicate allocation.
        """
        now = time.time()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(attempt), 0) AS max_att FROM order_placement_attempts WHERE pos_id = ? AND role = ?",
                (pos_id, role)
            ).fetchone()
            next_attempt = (row["max_att"] if row else 0) + 1
            tag = make_order_tag(role, pos_id, next_attempt)

            conn.execute("""
                INSERT INTO order_placement_attempts
                    (pos_id, role, attempt, tag, symbol, side, qty, price, trigger_price, status, created_at, updated_at)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, 'INITIATED', ?, ?)
            """, (pos_id, role, next_attempt, tag, symbol, side, qty, price, trigger_price, now, now))
            return next_attempt, tag

    def update_placement_attempt(
        self, pos_id: str, role: str, attempt: int, status: str,
        broker_order_id: Optional[str] = None, error_message: Optional[str] = None
    ):
        now = time.time()
        with self._conn() as conn:
            conn.execute("""
                UPDATE order_placement_attempts
                SET status = ?,
                    broker_order_id = COALESCE(?, broker_order_id),
                    error_message = ?,
                    updated_at = ?
                WHERE pos_id = ? AND role = ? AND attempt = ?
            """, (status, broker_order_id, error_message, now, pos_id, role, attempt))

    def get_latest_placement_attempt(self, pos_id: str, role: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM order_placement_attempts WHERE pos_id = ? AND role = ? ORDER BY attempt DESC LIMIT 1",
                (pos_id, role)
            ).fetchone()
            return dict(row) if row else None

    def get_placement_attempt_by_tag(self, tag: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM order_placement_attempts WHERE tag = ?",
                (tag,)
            ).fetchone()
            return dict(row) if row else None

    def get_attempts_for_position(self, pos_id: str) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM order_placement_attempts WHERE pos_id = ? ORDER BY id ASC",
                (pos_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ──────────────────────────────────────
    # Alert Deduplication Ledger (P0: Survive restarts)
    # ──────────────────────────────────────

    def should_send_alert(self, alert_key: str, cooldown_sec: float = 1800.0, state_fingerprint: str = "") -> bool:
        """
        Durable deduplication for alerts stored in SQLite.
        Survives restarts and prevents alert storms.
        Returns True if alert should be sent, False if suppressed.
        """
        now = time.time()
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM alert_dedup WHERE alert_key = ?", (alert_key,)).fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO alert_dedup (alert_key, last_alerted_at, count, details) VALUES (?, ?, 1, ?)",
                    (alert_key, now, state_fingerprint)
                )
                return True

            last_ts = float(row["last_alerted_at"])
            last_fp = str(row["details"] or "")

            if (state_fingerprint and state_fingerprint != last_fp) or (now - last_ts >= cooldown_sec):
                conn.execute(
                    "UPDATE alert_dedup SET last_alerted_at = ?, count = count + 1, details = ? WHERE alert_key = ?",
                    (now, state_fingerprint, alert_key)
                )
                return True

            conn.execute("UPDATE alert_dedup SET count = count + 1 WHERE alert_key = ?", (alert_key,))
            return False

    def get_processed_qty(self, entry_order_id: str) -> int:
        """Returns the cumulative filled quantity already processed for this entry order."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT processed_qty FROM order_fills WHERE order_id = ?", (entry_order_id,)
            ).fetchone()
            if row and row["processed_qty"] is not None:
                return int(row["processed_qty"])
            row = conn.execute(
                "SELECT processed_qty FROM positions WHERE entry_order_id = ?", (entry_order_id,)
            ).fetchone()
            return int(row["processed_qty"]) if row and row["processed_qty"] is not None else 0

    def set_processed_qty(self, entry_order_id: str, qty: int):
        """Updates the cumulative filled quantity processed for this entry order."""
        now = time.time()
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO order_fills (order_id, processed_qty, cumulative_value, processed_at, updated_at)
                VALUES (?, ?, 0.0, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    processed_qty = excluded.processed_qty,
                    processed_at  = excluded.processed_at,
                    updated_at    = excluded.updated_at
            """, (entry_order_id, qty, now, now))
            conn.execute(
                "UPDATE positions SET processed_qty = ?, updated_at = ? WHERE entry_order_id = ?",
                (qty, now, entry_order_id)
            )

    def mark_broker_order_processed(
        self,
        order_id: str,
        filled_qty: int,
        cumulative_value: float = 0.0,
        status: str = "",
        source: str = "STARTUP",
    ):
        """
        Persist broker order id + cumulative fill so historical / already-seen
        orders are never treated as new executions across restarts.
        Does not delete or reset existing rows — only upserts forward.
        """
        if not order_id:
            return
        now = time.time()
        prev_qty = self.get_processed_qty(order_id)
        qty = max(int(filled_qty or 0), prev_qty)
        prev_val = self.get_cumulative_value(order_id)
        cum_val = max(float(cumulative_value or 0.0), prev_val)
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO order_fills (
                    order_id, processed_qty, cumulative_value, last_status, source, processed_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    processed_qty    = excluded.processed_qty,
                    cumulative_value = excluded.cumulative_value,
                    last_status      = COALESCE(excluded.last_status, order_fills.last_status),
                    source           = COALESCE(excluded.source, order_fills.source),
                    processed_at     = COALESCE(order_fills.processed_at, excluded.processed_at),
                    updated_at       = excluded.updated_at
            """, (order_id, qty, cum_val, status or None, source, now, now))
            conn.execute(
                "UPDATE positions SET processed_qty = ?, cumulative_value = ?, updated_at = ? WHERE entry_order_id = ?",
                (qty, cum_val, now, order_id),
            )

    def get_order_fill_record(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM order_fills WHERE order_id = ?", (order_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_cumulative_value(self, entry_order_id: str) -> float:
        """Returns the cumulative filled value (sum of qty * price) for this entry order."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT cumulative_value FROM order_fills WHERE order_id = ?", (entry_order_id,)
            ).fetchone()
            if row and row["cumulative_value"] is not None:
                return float(row["cumulative_value"])
            row = conn.execute(
                "SELECT cumulative_value FROM positions WHERE entry_order_id = ?", (entry_order_id,)
            ).fetchone()
            return float(row["cumulative_value"]) if row and row["cumulative_value"] is not None else 0.0

    def set_cumulative_value(self, entry_order_id: str, value: float):
        """Updates the cumulative filled value for this entry order."""
        now = time.time()
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO order_fills (order_id, processed_qty, cumulative_value, updated_at)
                VALUES (?, 0, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    cumulative_value = excluded.cumulative_value,
                    updated_at       = excluded.updated_at
            """, (entry_order_id, value, now))
            conn.execute(
                "UPDATE positions SET cumulative_value = ?, updated_at = ? WHERE entry_order_id = ?",
                (value, now, entry_order_id)
            )

    def all_positions(self) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM positions ORDER BY created_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def active_positions(self) -> List[Dict[str, Any]]:
        """Returns genuinely open positions that still need monitoring or protection.

        Active only when status is non-terminal AND qty is still open
        (remaining_qty > 0, or NULL for legacy rows). remaining_qty=0 must never
        stay active even if status was left as PROTECTED / NEEDS_RECONCILIATION.
        """
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT * FROM positions
                WHERE status NOT IN (
                    'CLOSED', 'CLOSED_EXTERNAL', 'SL_EXECUTED', 'TARGET_EXECUTED',
                    'MANUALLY_MODIFIED', 'CANCELLED', 'EXITED'
                )
                  AND (remaining_qty IS NULL OR remaining_qty > 0)
                  AND (is_simulated = 0 OR is_simulated IS NULL)
                ORDER BY created_at DESC
            """).fetchall()
            return [dict(r) for r in rows]

    def unprotected_positions(self) -> List[Dict[str, Any]]:
        """Returns active positions that lack verified protection."""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT * FROM positions 
                WHERE status IN ('UNPROTECTED', 'ENTRY_DETECTED', 'PLACEMENT_UNKNOWN', 'ERROR', 'NEEDS_MANUAL_ACTION', 'NEEDS_RECONCILIATION', 'FLAT_UNATTRIBUTED')
                  AND (remaining_qty IS NULL OR remaining_qty > 0)
                  AND (is_simulated = 0 OR is_simulated IS NULL)
                ORDER BY created_at DESC
            """).fetchall()
            return [dict(r) for r in rows]

    # ──────────────────────────────────────
    # Order Roles Registry
    # ──────────────────────────────────────

    def register_order_role(self, order_id: str, role: str, position_id: str,
                            symbol: str, side: str, qty: int, status: str = "OPEN",
                            tag: Optional[str] = None):
        """Registers an order with its designated role ('ENTRY', 'TARGET', 'SL', 'EMERGENCY_SL')."""
        now = time.time()
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO order_roles (order_id, role, position_id, symbol, side, qty, status, tag, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    role        = excluded.role,
                    position_id = excluded.position_id,
                    status      = excluded.status,
                    tag         = excluded.tag,
                    updated_at  = excluded.updated_at
            """, (order_id, role, position_id, symbol, side, qty, status, tag, now, now))

    def get_order_role(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Looks up the designated role and metadata of an order."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM order_roles WHERE order_id = ?", (order_id,)).fetchone()
            return dict(row) if row else None

    def get_orders_for_position(self, position_id: str) -> List[Dict[str, Any]]:
        """Returns all orders associated with a given position."""
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM order_roles WHERE position_id = ? ORDER BY created_at ASC", (position_id,)).fetchall()
            return [dict(r) for r in rows]

    def update_order_role_status(self, order_id: str, status: str):
        """Updates the status of an order in the role registry."""
        with self._conn() as conn:
            conn.execute("UPDATE order_roles SET status = ?, updated_at = ? WHERE order_id = ?", (status, time.time(), order_id))

    # ──────────────────────────────────────
    # Idempotency
    # ──────────────────────────────────────

    def is_execution_seen(self, exec_key: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM executions WHERE exec_key = ?", (exec_key,)
            ).fetchone()
            return row is not None

    def mark_execution_seen(self, exec_key: str, meta: Dict[str, Any]):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO executions (exec_key, meta, seen_at) VALUES (?, ?, ?)",
                (exec_key, json.dumps(meta), time.time())
            )

    def clear_broker_order_seed(self, order_id: str) -> None:
        """
        Remove fill-progress + startup execution fingerprints for an order so it
        can be processed for first-time protection after a bad historical seed.
        Does not delete position rows.
        """
        if not order_id:
            return
        with self._conn() as conn:
            conn.execute("DELETE FROM order_fills WHERE order_id = ?", (order_id,))
            conn.execute(
                "DELETE FROM executions WHERE exec_key LIKE ? OR exec_key LIKE ?",
                (f"{order_id}_%", f"HIST_{order_id}_%"),
            )

    # ──────────────────────────────────────
    # Audit Log
    # ──────────────────────────────────────

    def log_event(self, event_type: str, entity_id: str, detail: str):
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO events (event_type, entity_id, detail, ts) VALUES (?, ?, ?, ?)",
                (event_type, entity_id, detail, time.time())
            )
            # Keep rolling window of 1000 events
            conn.execute("""
                DELETE FROM events WHERE id NOT IN (
                    SELECT id FROM events ORDER BY id DESC LIMIT 1000
                )
            """)

    def recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT event_type, entity_id, detail, ts FROM events ORDER BY id DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_events(self, entity_id: str) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT event_type, entity_id, detail, ts FROM events WHERE entity_id = ? ORDER BY ts ASC",
                (entity_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    # ──────────────────────────────────────
    # Partial Fill Buffers (Persistence across restarts)
    # ──────────────────────────────────────

    def save_partial_fill_buffer(self, order_id: str, symbol: str, side: str, fills: List[Dict[str, Any]]):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO partial_fill_buffers (order_id, symbol, side, fills_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    symbol     = excluded.symbol,
                    side       = excluded.side,
                    fills_json = excluded.fills_json,
                    updated_at = excluded.updated_at
            """, (order_id, symbol, side, json.dumps(fills), time.time()))

    def get_partial_fill_buffer(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM partial_fill_buffers WHERE order_id = ?", (order_id,)
            ).fetchone()
            if not row:
                return None
            res = dict(row)
            res["fills"] = json.loads(res["fills_json"])
            return res

    def delete_partial_fill_buffer(self, order_id: str):
        with self._conn() as conn:
            conn.execute("DELETE FROM partial_fill_buffers WHERE order_id = ?", (order_id,))

    def all_partial_fill_buffers(self) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM partial_fill_buffers").fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["fills"] = json.loads(d["fills_json"])
                out.append(d)
            return out

    def set_entry_lock(self, is_locked: bool, reason: str = ""):
        """Persists emergency entry lock state and reason in SQLite (survives restarts)."""
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO system_locks (lock_key, is_locked, reason, updated_at)
                VALUES ('entry_lock', ?, ?, ?)
                ON CONFLICT(lock_key) DO UPDATE SET
                    is_locked=excluded.is_locked,
                    reason=excluded.reason,
                    updated_at=excluded.updated_at
                """,
                (1 if is_locked else 0, reason, time.time())
            )

    def is_entry_locked(self) -> Tuple[bool, str]:
        """Returns (is_locked, reason). Checks persistent entry lock in SQLite."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT is_locked, reason FROM system_locks WHERE lock_key = 'entry_lock'"
            ).fetchone()
            if row:
                return (bool(row[0]), str(row[1] or ""))
            return (False, "")

    def get_entry_lock(self) -> Dict[str, Any]:
        """Returns full entry lock metadata."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT lock_key, is_locked, reason, updated_at FROM system_locks WHERE lock_key = 'entry_lock'"
            ).fetchone()
            if row:
                return {
                    "lock_key": row[0],
                    "is_locked": bool(row[1]),
                    "reason": row[2],
                    "updated_at": row[3]
                }
            return {"lock_key": "entry_lock", "is_locked": False, "reason": "", "updated_at": 0.0}


if __name__ == "__main__":
    import sys
    if "--reset" in sys.argv:
        store = JSONStore()
        store.reset_db()
        print(f"✅ Successfully reset database at {store.db_path}")
