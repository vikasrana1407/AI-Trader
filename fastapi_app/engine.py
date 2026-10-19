"""
FastAPI Bot Execution Engine.
Manages manually placed Kotak Neo positions only — no auto entry/re-entry.

Required flow (100% SL-only protection):
  All fills: SL-Limit on 100% of confirmed filled quantity.
  No Target orders. No 50/50 split.
  LONG/SHORT SL direction derived from entry side.
  If SL placement fails or cannot be verified active → UNPROTECTED + entry lock + Telegram.
  No automatic retry. No auto square-off.
  Client manual modification → MANUALLY_MODIFIED terminal state, permanently excluded.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from dataclasses import dataclass
from .broker_client import BrokerClient
from .config import settings
from .models import OrderSide, PositionStatus
from .notifier import TelegramAlerts

TICK_SIZE = 0.05
STRUCTURE_SL_ONLY = "SL_ONLY"
REQUIRED_STRUCTURE = STRUCTURE_SL_ONLY


@dataclass(frozen=True)
class ExitPlan:
    entry_side: OrderSide
    entry_price: float
    total_qty: int
    target_qty: int
    sl_qty: int
    target_price: float
    sl_trigger_price: float
    sl_limit_price: float
    target_exit_side: OrderSide
    sl_exit_side: OrderSide
    order_structure: str = REQUIRED_STRUCTURE
    half_a_qty: int = 0
    half_a_sl_qty: int = 0
    half_b_qty: int = 0
    total_sl_qty: int = 0
    vwap_price: float = 0.0
    sl_valid: bool = True


def round_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round price to nearest valid NSE tick size (0.05). Floor at tick minimum."""
    if price <= 0:
        return tick
    val = round(round(price / tick) * tick, 2)
    return max(val, tick)


def calculate_sl_prices(
    entry_side: OrderSide,
    eff_price: float,
    sl_offset: float,
    sl_limit_buffer: float,
) -> Tuple[OrderSide, float, float, bool]:
    """Calculate SL exit side, trigger price, limit price, and validity."""
    if entry_side == OrderSide.BUY:
        sl_exit_side = OrderSide.SELL
        raw_sl_trigger = eff_price - sl_offset
        raw_sl_limit = eff_price - sl_limit_buffer
        sl_valid = raw_sl_trigger > 0 and raw_sl_limit > 0
        sl_trigger = round_tick(raw_sl_trigger) if raw_sl_trigger > 0 else 0.0
        sl_limit = round_tick(raw_sl_limit) if raw_sl_limit > 0 else 0.0
    elif entry_side == OrderSide.SELL:
        sl_exit_side = OrderSide.BUY
        raw_sl_trigger = eff_price + sl_offset
        raw_sl_limit = eff_price + sl_limit_buffer
        sl_valid = raw_sl_trigger > 0 and raw_sl_limit > 0
        sl_trigger = round_tick(raw_sl_trigger)
        sl_limit = round_tick(raw_sl_limit)
    else:
        raise ValueError(f"Unsupported side: {entry_side}")
    return sl_exit_side, sl_trigger, sl_limit, sl_valid


def calculate_exit_plan(
    entry_side: OrderSide,
    entry_price: float,
    total_qty: int,
    target_offset: float = 2.0,
    sl_offset: float = 2.0,
    sl_limit_buffer: float = 10.0,
    vwap_price: float = 0.0,
    lot_size: Optional[int] = None,
    **_ignored,
) -> ExitPlan:
    """
    Computes a 100% SL-only protection plan covering the full filled quantity.
    No Target order is placed.
    """
    if lot_size is None or lot_size <= 0:
        raise ValueError("lot_size is required and must be > 0 (no guessing).")
    if total_qty <= 0:
        raise ValueError(f"Invalid total_qty: {total_qty}")
    if total_qty % lot_size != 0:
        raise ValueError(
            f"Filled quantity {total_qty} is not a multiple of lot size {lot_size}."
        )

    eff_price = vwap_price if vwap_price > 0 else entry_price
    sl_exit_side, sl_trigger, sl_limit, sl_valid = calculate_sl_prices(
        entry_side, eff_price, sl_offset, sl_limit_buffer
    )
    target_exit_side = OrderSide.SELL if entry_side == OrderSide.BUY else OrderSide.BUY

    return ExitPlan(
        entry_side=entry_side,
        entry_price=entry_price,
        total_qty=total_qty,
        target_qty=0,
        sl_qty=total_qty,
        target_price=0.0,
        sl_trigger_price=sl_trigger,
        sl_limit_price=sl_limit,
        target_exit_side=target_exit_side,
        sl_exit_side=sl_exit_side,
        order_structure=STRUCTURE_SL_ONLY,
        half_a_qty=0,
        half_a_sl_qty=0,
        half_b_qty=total_qty,
        total_sl_qty=total_qty,
        vwap_price=vwap_price,
        sl_valid=sl_valid,
    )


def validate_exit_plan(plan: ExitPlan) -> Optional[str]:
    """Returns error string if plan is invalid, else None."""
    if plan.total_qty <= 0:
        return f"Invalid total_qty: {plan.total_qty}"

    if plan.sl_qty != plan.total_qty:
        return f"SL qty ({plan.sl_qty}) must cover 100% of filled qty ({plan.total_qty})"

    if not plan.sl_valid or plan.sl_trigger_price <= 0 or plan.sl_limit_price <= 0:
        return "SL trigger or limit price is <= 0 (cheap option); SL cannot be placed safely"

    if plan.entry_side == OrderSide.BUY:
        if plan.sl_limit_price > plan.sl_trigger_price:
            return f"LONG SL limit ({plan.sl_limit_price}) must be <= trigger ({plan.sl_trigger_price})"

    if plan.entry_side == OrderSide.SELL:
        if plan.sl_limit_price < plan.sl_trigger_price:
            return f"SHORT SL limit ({plan.sl_limit_price}) must be >= trigger ({plan.sl_trigger_price})"

    return None
from .order_feed import (
    UNSUPPORTED_PLACE_PRODUCTS,
    NormalizedOrderEvent,
    normalize_order_event,
    resolve_protective_product,
)
from .persistence import JSONStore, make_order_tag

logger = logging.getLogger("FastAPIBot.Engine")


class PartialFillBuffer:
    """Thread-safe buffer accumulating partial fills until order completes."""

    def __init__(self, order_id: str, symbol: str, side: OrderSide):
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.fills: List[Dict[str, Any]] = []
        self.is_complete = False

    def add(self, qty: int, price: float):
        self.fills.append({"qty": qty, "price": price, "time": time.time()})

    @property
    def total_qty(self) -> int:
        return sum(f["qty"] for f in self.fills)

    @property
    def vwap(self) -> float:
        total_val = sum(f["qty"] * f["price"] for f in self.fills)
        tq = self.total_qty
        return round(total_val / tq, 2) if tq else 0.0


class TradingEngine:
    """Main trading engine — all methods are thread-safe."""

    def __init__(self, persistence_file: Optional[str] = None):
        store_path = persistence_file or settings.PERSISTENCE_FILE
        self.store = JSONStore(filepath=store_path)
        self.broker = BrokerClient(dry_run=settings.DRY_RUN)
        self._positions: Dict[str, Dict[str, Any]] = {}  # in-memory shadow of JSON store
        self._fill_buffers: Dict[str, PartialFillBuffer] = {}
        self._processed_fills: set = set()
        self._lock = threading.Lock()
        self._processing_lock = threading.Lock()
        self._order_lock = threading.RLock()
        self._start_time = time.time()
        self._total_trades = 0
        self.last_order_event_ts: Optional[float] = None
        self.last_reconciliation_ts: Optional[float] = None
        self._reconciliation_alert_history: Dict[Tuple[str, str], float] = {}
        self._recovery_attempted: set = set()
        self._broker_net_cache: Optional[Dict[str, int]] = None
        self._broker_net_cache_ts: float = 0.0
        self._broker_net_cache_ttl: float = 10.0
        self._hist_ignore_logged: set = set()

    @staticmethod
    def _extract_position_symbol(bp: Dict[str, Any]) -> str:
        for key in (
            "trdSym", "tradingSymbol", "symbol", "pTrdSymbol",
            "pSymbol", "pSymbolName", "instrument_token",
        ):
            val = bp.get(key)
            if val is not None and str(val).strip():
                return str(val).strip().upper()
        return ""

    @staticmethod
    def _extract_position_net_qty(bp: Dict[str, Any]) -> int:
        for key in ("netQty", "net_qty", "NetQty", "netQuantity", "quantity"):
            raw = bp.get(key)
            if raw is None or str(raw).strip() == "":
                continue
            try:
                return int(float(raw))
            except (ValueError, TypeError):
                continue

        def _q(keys: Tuple[str, ...]) -> int:
            for k in keys:
                raw = bp.get(k)
                if raw is None or str(raw).strip() == "":
                    continue
                try:
                    return int(float(raw))
                except (ValueError, TypeError):
                    continue
            return 0

        buy = _q(("flBuyQty", "buyQty", "cfBuyQty", "buy_quantity", "totalBuyQty"))
        sell = _q(("flSellQty", "sellQty", "cfSellQty", "sell_quantity", "totalSellQty"))
        if buy or sell:
            return buy - sell
        return 0

    def _build_broker_net_map(self, broker_positions: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, int]]:
        if broker_positions is None:
            return None
        net_map: Dict[str, int] = {}
        for bp in broker_positions:
            if not isinstance(bp, dict):
                continue
            sym = self._extract_position_symbol(bp)
            if not sym:
                continue
            net_map[sym] = self._extract_position_net_qty(bp)
        return net_map

    @staticmethod
    def _is_broker_flat(net_qty_by_symbol: Optional[Dict[str, int]], symbol: str) -> bool:
        """True when positions API succeeded and the symbol has no open net qty.

        Missing symbol after a successful fetch counts as flat (broker dropped the row).
        """
        if net_qty_by_symbol is None:
            return False
        sym = (symbol or "").strip().upper()
        if not sym:
            return False
        if sym not in net_qty_by_symbol:
            return True
        return int(net_qty_by_symbol.get(sym) or 0) == 0

    def _cached_broker_net_map(self, force: bool = False) -> Optional[Dict[str, int]]:
        now = time.time()
        if (
            not force
            and self._broker_net_cache is not None
            and (now - self._broker_net_cache_ts) < self._broker_net_cache_ttl
        ):
            return self._broker_net_cache
        positions = self.broker.get_positions()
        net_map = self._build_broker_net_map(positions)
        if net_map is not None:
            self._broker_net_cache = net_map
            self._broker_net_cache_ts = now
            open_n = sum(1 for v in net_map.values() if v != 0)
            logger.info(
                f"BROKER_NET_MAP symbols={len(net_map)} open={open_n} "
                f"sample={ {k: net_map[k] for k in list(net_map)[:5]} }"
            )
        return net_map

    # ──────────────────────────────────────
    # Startup & Shutdown
    # ──────────────────────────────────────
    def startup(self):
        self.broker.initialize()
        # Fresh Kotak session after restart: clear persistent entry lock so new MIS
        # fills can be protected. Watchdog will re-lock if any position stays unprotected.
        locked, lock_reason = self.store.is_entry_locked()
        if locked:
            self.store.set_entry_lock(False, "")
            logger.warning(
                f"ENTRY_LOCK_CLEARED_ON_STARTUP Cleared entry lock after successful login. "
                f"Prior reason: {lock_reason}"
            )
            TelegramAlerts.notice(
                title="Entry lock cleared on startup",
                symbol="SYSTEM",
                reason=f"Fresh Kotak login — prior lock: {(lock_reason or 'n/a')[:200]}",
            )
        self._load_persisted_state()
        self.startup_reconciliation()
        TelegramAlerts.test_message()
        TelegramAlerts.bot_started()
        self._telegram_startup_trading_status()
        logger.info("✅ TradingEngine started and ready.")

    def _telegram_startup_trading_status(self) -> None:
        """Explain why trading alerts may be quiet until a new MIS/NRML fill arrives."""
        net_map = self._broker_net_cache
        if net_map is None:
            net_map = self._cached_broker_net_map() or {}
        open_syms = {k: v for k, v in net_map.items() if v != 0}
        active = len(self.store.active_positions())
        if open_syms:
            lines = "\n".join(f"• <code>{s}</code>: net={q}" for s, q in list(open_syms.items())[:8])
            TelegramAlerts.send(
                f"📡 <b>STARTUP: OPEN BROKER EXPOSURE</b>\n"
                f"{lines}\n"
                f"• Local active positions: {active}\n"
                f"Bot will attempt one-shot protection recovery for open MIS/NRML "
                f"positions that still have no SL/Target, then require manual action if that fails."
            )
            return

        TelegramAlerts.send(
            f"💤 <b>STARTUP: NO OPEN POSITIONS</b>\n"
            f"• Broker netQty=0 on watched symbols\n"
            f"• Local active positions: {active}\n"
            f"• Old COMPLETE/BO fills are ignored (already seeded)\n"
            f"📌 <b>Trading alerts appear only after a NEW fill</b> "
            f"while the bot is running — place <b>MIS or NRML</b> "
            f"(not Bracket/BO). Then you should get "
            f"PROTECTIVE ORDERS PLACED / 100% SL on Telegram."
        )

    def shutdown(self):
        """Clean shutdown of engine resources. Waits for active in-flight processing."""
        logger.info("TradingEngine shutting down...")
        with self._processing_lock:
            if hasattr(self.broker, "shutdown"):
                try:
                    self.broker.shutdown()
                except Exception as e:
                    logger.debug(f"Error during broker shutdown: {e}")
        logger.info("TradingEngine shutdown complete.")

    @staticmethod
    def _normalize_broker_ord_status(raw: Any) -> str:
        st = str(raw or "").strip().upper()
        if st in ("COMPLETE", "COMPLETED", "TRAD", "TRADED", "FILLED", "EXECUTED"):
            return "COMPLETE"
        if st in ("REJECTED", "REJ"):
            return "REJECTED"
        if st in ("CANCELLED", "CANCELED", "CAN"):
            return "CANCELLED"
        if st in ("PARTIAL_FILL", "PARTIALLY_FILLED", "PARTIAL"):
            return "PARTIAL_FILL"
        if st in ("OPEN", "TRIGGER_PENDING", "PENDING", "TRIGGER PENDING"):
            return "OPEN"
        return st

    def _maybe_alert_bo_entry(self, event: NormalizedOrderEvent) -> None:
        """
        Telegram once for Bracket/Cover fills.
        - Broker flat → info notice (nothing to protect).
        - Broker open + BO_PROTECT_AS set → info that exits will use MIS/NRML.
        - Broker open + no override → real NEEDS_MANUAL_ACTION.
        """
        raw = str(event.raw_product or "").strip().upper()
        if raw not in UNSUPPORTED_PLACE_PRODUCTS:
            return
        if event.status not in ("COMPLETE", "PARTIAL_FILL"):
            return
        if event.filled_qty <= 0:
            return
        from .order_feed import PLACE_ORDER_PRODUCTS
        override = str(getattr(settings, "BO_PROTECT_AS_PRODUCT", "") or "").strip().upper()
        sym = (event.symbol or "").strip().upper()
        net_map = self._cached_broker_net_map()
        net_qty = (net_map or {}).get(sym, 0) if net_map is not None else None

        if net_map is not None and net_qty == 0:
            # Leave flat Bracket history alone — no Telegram noise
            logger.debug(
                f"BO_ENTRY_FLAT_IGNORED {event.order_id} ({sym}) product={raw} qty={event.filled_qty}"
            )
            return

        if override in PLACE_ORDER_PRODUCTS:
            fp = f"protect_as_{override}_net_{net_qty}"
            if not self.store.should_send_alert(f"BO_ENTRY_{event.order_id}", cooldown_sec=3600.0, state_fingerprint=fp):
                logger.debug(f"BO_ENTRY_ALERT_SUPPRESSED {event.order_id} fingerprint={fp}")
                return
            reason = (
                f"Bracket/Cover entry (product={raw}, qty={event.filled_qty}"
                f"{f', netQty={net_qty}' if net_qty is not None else ''}). "
                f"Protective exits will be placed as {override} (BO_PROTECT_AS_PRODUCT). "
                f"Prefer MIS/NRML entries when possible."
            )
            logger.warning(f"BO_ENTRY_PROTECT_AS {event.order_id} ({sym}): {reason}")
            TelegramAlerts.notice(
                f"BRACKET ENTRY → EXITS AS {override}",
                symbol=sym,
                reason=reason,
                order_id=event.order_id,
            )
            return

        fp = "blocked"
        if not self.store.should_send_alert(f"BO_ENTRY_{event.order_id}", cooldown_sec=3600.0, state_fingerprint=fp):
            logger.debug(f"BO_ENTRY_ALERT_SUPPRESSED {event.order_id} fingerprint={fp}")
            return
        reason = (
            f"Bracket/Cover entry (product={raw}, qty={event.filled_qty}). "
            f"SDK cannot place BO/CO. Set BO_PROTECT_AS_PRODUCT=MIS|NRML or enter as MIS/NRML."
        )
        logger.warning(f"BO_ENTRY_BLOCKED {event.order_id} ({sym}): {reason}")
        TelegramAlerts.needs_manual_action(
            symbol=sym,
            reason=reason,
            order_id=event.order_id,
        )

    def _local_position_is_managed(self, order_id: str) -> bool:
        """True when we already track this entry (protected, closed, or failed/manual terminal)."""
        pos = self.store.get_position(order_id)
        if not pos:
            return False
        st = pos.get("status")
        if st in (
            PositionStatus.PROTECTED.value,
            PositionStatus.CLOSED.value,
            PositionStatus.CLOSED_EXTERNAL.value,
            PositionStatus.SL_EXECUTED.value,
            PositionStatus.TARGET_EXECUTED.value,
            PositionStatus.NEEDS_MANUAL_ACTION.value,
            PositionStatus.ERROR.value,
            # New terminal states — permanently excluded from auto-management
            PositionStatus.MANUALLY_MODIFIED.value,
            PositionStatus.CANCELLED.value,
            PositionStatus.EXITED.value,
        ):
            return True
        # SL already recorded → treat as managed (avoid double placement)
        legs = [
            pos.get("half_b_sl_order_id") or pos.get("sl_order_id"),
        ]
        return any(lid and not str(lid).startswith("SKIPPED_") for lid in legs)

    def _seed_historical_broker_orders(
        self,
        broker_orders: List[Dict[str, Any]],
        broker_net_by_sym: Dict[str, int],
    ) -> int:
        """
        Mark broker order-book rows as already processed for replay safety.
        Never places SL/target orders. Identity DB rows are upserted forward only.

        Open user COMPLETE fills (symbol still has broker net, no local protection)
        are NOT seeded — they remain eligible for first-time protective placement.
        """
        seeded = 0
        terminal = {"COMPLETE", "REJECTED", "CANCELLED"}
        for o in broker_orders or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("nOrdNo") or o.get("order_id") or "").strip()
            if not oid:
                continue

            status = self._normalize_broker_ord_status(o.get("ordSt") or o.get("status"))
            try:
                fld = int(o.get("fldQty") or o.get("filled_quantity") or 0)
            except (ValueError, TypeError):
                fld = 0
            try:
                avg_p = float(o.get("avgPrc") or o.get("average_price") or o.get("price") or 0.0)
            except (ValueError, TypeError):
                avg_p = 0.0
            o_sym = str(o.get("trdSym") or o.get("tradingSymbol") or o.get("symbol") or "").strip().upper()
            tag = str(o.get("tag") or o.get("GuiOrdId") or o.get("ordTag") or "").upper()
            is_bot = tag.startswith("BOT_") or tag.startswith("B_") or self.store.get_order_role(oid) is not None
            broker_flat = (not o_sym) or broker_net_by_sym.get(o_sym, 0) == 0
            locally_managed = self._local_position_is_managed(oid)

            # Always seed rejects/cancels and bot legs. Seed completes only when
            # the symbol is flat or we already manage/protect that entry.
            if status in ("REJECTED", "CANCELLED"):
                should_seed = True
            elif is_bot or broker_flat or locally_managed:
                should_seed = status in terminal or fld > 0
            elif status == "COMPLETE" or fld > 0:
                # Open exposure without local protection — leave unseeded
                logger.info(
                    f"STARTUP_SKIP_SEED Open unprotected fill {oid} ({o_sym}) "
                    f"net={broker_net_by_sym.get(o_sym, 0)} — eligible for protection"
                )
                should_seed = False
            else:
                should_seed = False

            if not should_seed:
                continue

            seed_qty = fld if fld > 0 else self.store.get_processed_qty(oid)
            if status in terminal and seed_qty <= 0:
                try:
                    seed_qty = int(o.get("qty") or o.get("quantity") or 0)
                except (ValueError, TypeError):
                    seed_qty = 0

            self.store.mark_broker_order_processed(
                order_id=oid,
                filled_qty=seed_qty,
                cumulative_value=round(seed_qty * avg_p, 2) if seed_qty and avg_p else 0.0,
                status=status or "HISTORICAL",
                source="STARTUP",
            )
            if seed_qty > 0 and avg_p > 0:
                exec_key = f"{oid}_{seed_qty}_{round(avg_p, 2)}"
                self.store.mark_execution_seen(
                    exec_key,
                    {"order_id": oid, "startup_history": True, "status": status, "symbol": o_sym},
                )
            # Also fingerprint terminal status-only keys so REJECTED/CANCELLED with 0 fill are sticky
            term_key = f"HIST_{oid}_{status or 'UNKNOWN'}"
            self.store.mark_execution_seen(
                term_key,
                {"order_id": oid, "startup_history": True, "status": status},
            )
            seeded += 1
        return seeded

    def _reopen_unprotected_seeded_fills(
        self,
        broker_orders: List[Dict[str, Any]],
        broker_net_by_sym: Dict[str, int],
    ) -> int:
        """
        Clear prior STARTUP seeds for open user COMPLETE fills that still have
        broker exposure and no local protection (fixes DB already poisoned by
        seed-all-complete).
        """
        cleared = 0
        for o in broker_orders or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("nOrdNo") or o.get("order_id") or "").strip()
            if not oid:
                continue
            status = self._normalize_broker_ord_status(o.get("ordSt") or o.get("status"))
            try:
                fld = int(o.get("fldQty") or o.get("filled_quantity") or 0)
            except (ValueError, TypeError):
                fld = 0
            if status != "COMPLETE" and fld <= 0:
                continue
            o_sym = str(o.get("trdSym") or o.get("tradingSymbol") or o.get("symbol") or "").strip().upper()
            if not o_sym or broker_net_by_sym.get(o_sym, 0) == 0:
                continue
            tag = str(o.get("tag") or o.get("GuiOrdId") or o.get("ordTag") or "").upper()
            is_bot = tag.startswith("BOT_") or tag.startswith("B_") or self.store.get_order_role(oid) is not None
            if is_bot:
                continue
            # Any local position row means lifecycle already started — never auto-retry
            if self.store.get_position(oid):
                continue
            fill_rec = self.store.get_order_fill_record(oid)
            if not fill_rec:
                continue
            self.store.clear_broker_order_seed(oid)
            cleared += 1
            logger.warning(
                f"STARTUP_REOPEN_FOR_PROTECTION Cleared seed for {oid} ({o_sym}) "
                f"net={broker_net_by_sym.get(o_sym, 0)} — will place protection"
            )
        return cleared

    def _protect_open_unseeded_entries(
        self,
        broker_orders: List[Dict[str, Any]],
        broker_net_by_sym: Dict[str, int],
    ) -> int:
        """
        First-time protection for open user fills left unseeded at startup.
        Does not retry failed/locked protection and never auto-enters.
        """
        placed = 0
        for o in broker_orders or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("nOrdNo") or o.get("order_id") or "").strip()
            if not oid:
                continue
            status = self._normalize_broker_ord_status(o.get("ordSt") or o.get("status"))
            try:
                fld = int(o.get("fldQty") or o.get("filled_quantity") or 0)
            except (ValueError, TypeError):
                fld = 0
            if status != "COMPLETE" or fld <= 0:
                continue
            o_sym = str(o.get("trdSym") or o.get("tradingSymbol") or o.get("symbol") or "").strip().upper()
            if not o_sym or broker_net_by_sym.get(o_sym, 0) == 0:
                continue
            tag = str(o.get("tag") or o.get("GuiOrdId") or o.get("ordTag") or "").upper()
            is_bot = tag.startswith("BOT_") or tag.startswith("B_") or self.store.get_order_role(oid) is not None
            if is_bot:
                continue
            if self._local_position_is_managed(oid):
                continue
            if self.store.get_processed_qty(oid) >= fld:
                continue
            locked, _ = self.store.is_entry_locked()
            if locked:
                logger.warning(
                    f"STARTUP_PROTECT_SKIPPED Entry lock active — cannot protect {oid} ({o_sym})"
                )
                if self.store.should_send_alert(f"STARTUP_PROTECT_LOCKED_{oid}"):
                    TelegramAlerts.needs_manual_action(
                        symbol=o_sym,
                        reason=f"Open fill {oid} needs protection but entry lock is active.",
                        order_id=oid,
                    )
                continue
            logger.info(f"STARTUP_PROTECT_OPEN_FILL Processing {oid} ({o_sym}) qty={fld} for protection")
            try:
                result = self.handle_order_event({"type": "order", "data": o}, source="STARTUP")
                if result:
                    placed += 1
            except Exception as exc:
                logger.error(f"STARTUP_PROTECT_FAILED {oid} ({o_sym}): {exc}")
                if self.store.should_send_alert(f"STARTUP_PROTECT_FAIL_{oid}"):
                    TelegramAlerts.needs_manual_action(
                        symbol=o_sym,
                        reason=f"Failed to place protection for open fill {oid}: {exc}",
                        order_id=oid,
                    )
        return placed

    def _position_has_real_protective_ids(self, pos: Dict[str, Any]) -> bool:
        """True if any non-SKIPPED SL order id is recorded (100% SL-only mode)."""
        for key in (
            "half_b_sl_order_id",
            "sl_order_id",
            # legacy / DB compat fields kept but not placed in new flow
            "half_a_target_order_id",
            "target_order_id",
            "half_a_sl_order_id",
        ):
            oid = pos.get(key)
            if oid and not str(oid).startswith("SKIPPED_"):
                return True
        return False

    def _recover_unprotected_without_legs(self, broker_net_by_sym: Dict[str, int]) -> int:
        """
        One-shot startup recovery: open broker net + local UNPROTECTED/NEEDS_MANUAL_ACTION/
        ERROR with no real SL/Target IDs → place protection once after fresh login.

        Does not replace cancelled/rejected legs that already have broker order ids.
        """
        attempted = 0
        for pos in list(self.store.all_positions()):
            if pos.get("is_simulated"):
                continue
            sym = str(pos.get("symbol") or "").strip().upper()
            if not sym:
                continue
            net = int(broker_net_by_sym.get(sym, 0) or 0)
            if net == 0:
                continue
            st = pos.get("status")
            if st not in (
                PositionStatus.UNPROTECTED.value,
                PositionStatus.NEEDS_MANUAL_ACTION.value,
                PositionStatus.ERROR.value,
                PositionStatus.ENTRY_DETECTED.value,
            ):
                continue
            if self._position_has_real_protective_ids(pos):
                continue

            order_id = str(pos.get("entry_order_id") or "")
            if not order_id or "_fill_" in order_id:
                # Composite keys are derived; recover via base entry id when possible
                order_id = order_id.split("_fill_")[0] if order_id else ""
            if not order_id:
                continue

            try:
                side = OrderSide.BUY if str(pos.get("side", "")).upper() in ("BUY", "B") else OrderSide.SELL
            except Exception:
                side = OrderSide.BUY
            try:
                qty = abs(int(pos.get("remaining_qty") or pos.get("total_qty") or net))
            except (TypeError, ValueError):
                qty = abs(net)
            try:
                price = float(pos.get("entry_price") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if qty <= 0 or price <= 0:
                logger.warning(
                    f"STARTUP_RECOVER_SKIP {order_id} ({sym}): invalid qty/price qty={qty} price={price}"
                )
                continue

            # Ensure place_order path is not blocked by the prior failure lock
            locked, _ = self.store.is_entry_locked()
            if locked:
                self.store.set_entry_lock(False, "")
                logger.warning(
                    f"STARTUP_RECOVER_UNLOCK Cleared entry lock to attempt protection for {order_id} ({sym})"
                )

            logger.warning(
                f"STARTUP_RECOVER_PROTECTION Placing protection for open unprotected "
                f"{order_id} ({sym}) qty={qty} net={net}"
            )
            TelegramAlerts.notice(
                title="Startup protection recovery",
                symbol=sym,
                reason=f"Retrying SL/Target once after fresh login for open qty={qty}",
                order_id=order_id,
            )
            attempted += 1
            try:
                self._place_protective_orders(
                    order_id=order_id,
                    symbol=sym,
                    side=side,
                    qty=qty,
                    price=price,
                    detection_ts=time.time(),
                    current_filled=0,
                    product_type=settings.PRODUCT_TYPE,
                )
            except Exception as exc:
                logger.error(f"STARTUP_RECOVER_FAILED {order_id} ({sym}): {exc}")
                if self.store.should_send_alert(f"STARTUP_RECOVER_FAIL_{order_id}"):
                    TelegramAlerts.needs_manual_action(
                        symbol=sym,
                        reason=f"Startup protection recovery failed: {exc}",
                        order_id=order_id,
                    )
        return attempted

    def _flag_unverified_open_exposure(
        self,
        broker_net_by_sym: Dict[str, int],
        broker_orders: List[Dict[str, Any]],
    ) -> None:
        """
        Open broker exposure without verifiable local protection -> NEEDS_MANUAL_ACTION + Telegram.
        Never auto-place replacement exits / retry / re-enter / square-off.
        """
        broker_map = {
            str(o.get("nOrdNo") or o.get("order_id", "")): self._normalize_broker_ord_status(
                o.get("ordSt") or o.get("status")
            )
            for o in (broker_orders or [])
            if isinstance(o, dict)
        }

        for sym, net_qty in broker_net_by_sym.items():
            if net_qty == 0:
                continue

            matching = None
            for pos in self.store.all_positions():
                if pos.get("symbol", "").strip().upper() != sym:
                    continue
                if pos.get("status") in (
                    PositionStatus.CLOSED.value,
                    PositionStatus.CLOSED_EXTERNAL.value,
                    PositionStatus.SL_EXECUTED.value,
                    PositionStatus.TARGET_EXECUTED.value,
                ):
                    continue
                if (pos.get("remaining_qty") is not None) and int(pos.get("remaining_qty") or 0) <= 0:
                    continue
                matching = pos
                break

            if matching:
                pos_id = matching["entry_order_id"]
                protected = matching.get("status") == PositionStatus.PROTECTED.value
                legs = [
                    matching.get("half_a_target_order_id") or matching.get("target_order_id"),
                    matching.get("half_a_sl_order_id"),
                    matching.get("half_b_sl_order_id") or matching.get("sl_order_id"),
                ]
                legs_ok = False
                if protected:
                    active = []
                    for lid in legs:
                        if not lid or str(lid).startswith("SKIPPED_"):
                            continue
                        st = broker_map.get(str(lid))
                        if st in ("OPEN", "COMPLETE"):
                            active.append(lid)
                    legs_ok = len(active) > 0

                if protected and legs_ok:
                    continue

                reason = (
                    f"Open broker exposure for {sym} (netQty={net_qty}) but protection cannot be verified "
                    f"for position {pos_id}. Manual intervention required."
                )
                matching["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                matching["protection_state"] = "UNVERIFIED"
                matching["error_message"] = reason
                matching["remaining_qty"] = abs(int(net_qty))
                self.store.upsert_position(matching)
                with self._lock:
                    self._positions[pos_id] = matching
                self.store.set_entry_lock(True, reason)
                if self.store.should_send_alert(f"STARTUP_UNVERIFIED_{pos_id}"):
                    TelegramAlerts.needs_manual_action(symbol=sym, reason=reason, order_id=pos_id)
                logger.warning(f"STARTUP_UNVERIFIED_PROTECTION {reason}")
            else:
                reason = (
                    f"Broker has open net position of {net_qty} for {sym} with no local managed position "
                    f"after startup protection pass. Manual check required."
                )
                logger.warning(f"STARTUP_UNPROTECTED_EXPOSURE {reason}")
                if self.store.should_send_alert(f"STARTUP_ORPHAN_EXPOSURE_{sym}"):
                    TelegramAlerts.needs_manual_action(symbol=sym, reason=reason, order_id="")

    def startup_reconciliation(self):
        """
        Startup reconciliation (replay-safe):
        - Fetch broker positions + order book.
        - Seed flat/bot/REJECTED/CANCELLED history (not open unprotected user completes).
        - Reopen + place first-time protection for open unprotected user fills.
        - Open exposure still unverified after that -> NEEDS_MANUAL_ACTION + Telegram.
        - Broker-flat local positions -> CLOSED_EXTERNAL.
        - Never auto-enter / re-enter / retry failed protection replacements.
        """
        logger.info("RECONCILIATION_STARTED Starting startup reconciliation audit...")
        try:
            broker_positions = self.broker.get_positions()
            broker_orders = self.broker.get_order_report()

            if broker_positions is None:
                err_msg = "STARTUP_RECONCILIATION_FAILED Unable to fetch broker positions during startup. API unavailable."
                logger.error(err_msg)
                TelegramAlerts.critical_error("Startup Reconciliation", err_msg)
                return

            if broker_orders is None:
                err_msg = "STARTUP_RECONCILIATION_FAILED Unable to fetch broker order book during startup. API unavailable."
                logger.error(err_msg)
                TelegramAlerts.critical_error("Startup Reconciliation", err_msg)
                return

            broker_net_by_sym = self._build_broker_net_map(broker_positions) or {}
            self._broker_net_cache = dict(broker_net_by_sym)
            self._broker_net_cache_ts = time.time()
            open_syms = {k: v for k, v in broker_net_by_sym.items() if v != 0}
            logger.info(
                f"STARTUP_BROKER_NET open_symbols={len(open_syms)} detail={open_syms or '{}'}"
            )

            seeded = self._seed_historical_broker_orders(broker_orders, broker_net_by_sym)
            logger.info(f"STARTUP_HISTORY_SEEDED Marked {seeded} broker order(s) as already processed (no new exits placed).")

            reopened = self._reopen_unprotected_seeded_fills(broker_orders, broker_net_by_sym)
            if reopened:
                logger.warning(
                    f"STARTUP_REOPENED Cleared {reopened} previously seeded open fill(s) for first-time protection."
                )

            # Resolve local open positions that broker confirms flat
            for pos in list(self.store.all_positions()):
                sym = str(pos.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                if broker_net_by_sym.get(sym, None) != 0:
                    continue
                curr_st = pos.get("status")
                if curr_st in (
                    PositionStatus.CLOSED.value,
                    PositionStatus.CLOSED_EXTERNAL.value,
                    PositionStatus.SL_EXECUTED.value,
                    PositionStatus.TARGET_EXECUTED.value,
                ):
                    if pos.get("remaining_qty", 0) != 0:
                        pos["remaining_qty"] = 0
                        self.store.upsert_position(pos)
                    continue
                if curr_st in (
                    PositionStatus.PROTECTED.value,
                    PositionStatus.UNPROTECTED.value,
                    PositionStatus.ENTRY_DETECTED.value,
                    PositionStatus.PARTIALLY_CLOSED.value,
                    PositionStatus.NEEDS_MANUAL_ACTION.value,
                    PositionStatus.ERROR.value,
                    PositionStatus.PARTIAL.value,
                ) or (pos.get("remaining_qty", 0) or 0) > 0:
                    pos_id = pos["entry_order_id"]
                    pos["status"] = PositionStatus.CLOSED_EXTERNAL.value
                    pos["remaining_qty"] = 0
                    pos["error_message"] = f"Broker net position for {sym} is 0 at startup; marked CLOSED_EXTERNAL."
                    self.store.upsert_position(pos)
                    with self._lock:
                        self._positions[pos_id] = pos
                    logger.info(f"STARTUP_CLOSED_EXTERNAL Position {pos_id} ({sym}) marked CLOSED_EXTERNAL.")

            # Scrub false "short entries" created from manual long exits (BUY SL wrongly placed)
            self._scrub_false_entries_from_exits(broker_net_by_sym)

            # First-time protection for open user fills (not retry of failed exits)
            protected_n = self._protect_open_unseeded_entries(broker_orders, broker_net_by_sym)
            if protected_n:
                logger.info(f"STARTUP_PROTECTION_PLACED Processed {protected_n} open fill(s) for protection.")

            # After fresh login: one-shot recover UNPROTECTED positions that never got
            # real SL/Target IDs (typical after stCode 100008). Does not replace cancelled legs.
            recovered_n = self._recover_unprotected_without_legs(broker_net_by_sym)
            if recovered_n:
                logger.info(
                    f"STARTUP_PROTECTION_RECOVERED Attempted recovery for {recovered_n} "
                    f"unprotected open position(s)."
                )

            self._flag_unverified_open_exposure(broker_net_by_sym, broker_orders)

            # Reconcile for ongoing audit only — must not place replacement exits
            self.reconcile(allow_auto_place=False)
            logger.info("RECONCILIATION_COMPLETED Startup reconciliation completed.")
        except Exception as e:
            logger.error(f"ERROR in startup reconciliation: {e}", exc_info=True)

    def _load_persisted_state(self):
        """On startup, load all positions and partial fill buffers into in-memory shadow."""
        all_pos = self.store.all_positions()
        for pos in all_pos:
            self._positions[pos["entry_order_id"]] = pos

        # Restore pending partial fill buffers from SQLite
        pending_bufs = self.store.all_partial_fill_buffers()
        for pb in pending_bufs:
            oid = pb["order_id"]
            buf = PartialFillBuffer(oid, pb["symbol"], OrderSide(pb["side"]))
            for f in pb["fills"]:
                buf.add(f["qty"], f["price"])
            self._fill_buffers[oid] = buf
            logger.info(f"Restored partial fill buffer for {oid}: {buf.total_qty} qty buffered.")

        unprotected = self.store.unprotected_positions()
        if unprotected:
            issues = [f"• {p['symbol']} ({p['entry_order_id']}) — state: {p['status']}" for p in unprotected]
            msg = "⚠️ <b>RESTART RECOVERY: Unprotected positions detected!</b>\n" + "\n".join(issues)
            TelegramAlerts.send(msg)
            logger.warning(f"Startup recovery: {len(unprotected)} unprotected position(s).")
        else:
            logger.info("Startup recovery clean — no unprotected positions.")

    # ──────────────────────────────────────
    # Entry Fill Handler (the core pipeline)
    # ──────────────────────────────────────
    def handle_order_event(self, order_data: Dict[str, Any], event_time: float = 0.0, source: str = "REST") -> Optional[str]:
        """
        Processes a raw Kotak Neo order event (from REST poll or simulation).
        Normalizes the event into NormalizedOrderEvent and routes to process_normalized_order_event.
        """
        event = normalize_order_event(order_data, source=source)
        if not event:
            return None
        if event_time > 0.0:
            event.event_timestamp = event_time
        return self.process_normalized_order_event(event)

    def process_normalized_order_event(self, event: NormalizedOrderEvent) -> Optional[str]:
        """
        PRIMARY execution entrypoint called by KotakOrderFeed WebSocket and REST poller.
        Thread-safe under _order_lock. Handles:
          - OCO lifecycle for target/SL exits
          - Self-trigger loop prevention
          - Symbol and order type filters
          - Incremental slice price calculation (from cumulative fldQty & avgPrc)
          - Idempotency & duplicate suppression
          - Prompt finalization of partial fills on completion or cancellation
          - Target + SL order placement & protection verification
        """
        order_id = event.order_id
        if not order_id:
            return None

        with self._order_lock:
            # Always check exit / OCO lifecycle for any exit leg that updated status
            # (only acts when order_id maps to an active local position / role)
            self._check_oco(order_id, event.status, filled_qty=event.filled_qty, avg_price=event.average_price or event.price)

            # Skip bot-generated orders to prevent infinite self-triggering loops
            tag_upper = event.tag.upper() if event.tag else ""
            if tag_upper.startswith("BOT_") or any(bt in tag_upper for bt in ("BOT_TARGET", "BOT_SL", "BOT_TGT", "BOT_EMG")):
                logger.info(f"ORDER_EVENT_IGNORED Bot order: {order_id} ({event.tag})")
                return None

            # Always notify once for Bracket/Cover fills (even if later ignored as historical)
            self._maybe_alert_bo_entry(event)

            # Ignore historical REJECTED / CANCELLED / already-seeded terminal history
            fill_rec = self.store.get_order_fill_record(order_id)
            if fill_rec and str(fill_rec.get("last_status") or "").upper() in ("REJECTED", "CANCELLED", "COMPLETE"):
                if event.status in ("REJECTED", "CANCELLED", "COMPLETE") and event.filled_qty <= int(fill_rec.get("processed_qty") or 0):
                    # Escape: STARTUP-seeded user fill while broker still shows open net
                    # and we never managed this entry — clear seed and protect (fires Telegram).
                    src = str(fill_rec.get("source") or "").upper()
                    if (
                        event.status == "COMPLETE"
                        and src == "STARTUP"
                        and not self._local_position_is_managed(order_id)
                        and event.symbol
                    ):
                        net_map = self._cached_broker_net_map()
                        sym_u = event.symbol.strip().upper()
                        net_qty = (net_map or {}).get(sym_u, 0) if net_map is not None else None
                        if net_map is None:
                            if order_id not in self._hist_ignore_logged:
                                self._hist_ignore_logged.add(order_id)
                                logger.warning(
                                    f"ORDER_SEED_BLOCKED {order_id} ({sym_u}): positions API unavailable; "
                                    f"cannot reopen for protection."
                                )
                                if self.store.should_send_alert(f"SEED_BLOCK_POS_API_{order_id}"):
                                    TelegramAlerts.needs_manual_action(
                                        symbol=sym_u,
                                        reason=(
                                            f"Fill {order_id} is seeded but broker positions API is unavailable. "
                                            f"Cannot auto-protect; check position manually."
                                        ),
                                        order_id=order_id,
                                    )
                        elif net_qty != 0:
                            self.store.clear_broker_order_seed(order_id)
                            logger.warning(
                                f"ORDER_REOPEN_LIVE Cleared STARTUP seed for {order_id} ({sym_u}) "
                                f"net={net_qty} — processing for protection"
                            )
                            fill_rec = None  # fall through to live protection path
                        else:
                            if order_id not in self._hist_ignore_logged:
                                self._hist_ignore_logged.add(order_id)
                                logger.info(
                                    f"ORDER_EVENT_IGNORED Historical/seeded {event.status} order {order_id} "
                                    f"(processed_qty={fill_rec.get('processed_qty')}, broker_net=0)."
                                )
                            return None
                        if fill_rec is not None:
                            return None
                    else:
                        if order_id not in self._hist_ignore_logged:
                            self._hist_ignore_logged.add(order_id)
                            logger.info(
                                f"ORDER_EVENT_IGNORED Historical/seeded {event.status} order {order_id} "
                                f"(processed_qty={fill_rec.get('processed_qty')})."
                            )
                        return None

            # Closed local position: never reopen protection from historical entry replays
            existing_pos = self.store.get_position(order_id)
            if existing_pos and existing_pos.get("status") in (
                PositionStatus.CLOSED.value,
                PositionStatus.CLOSED_EXTERNAL.value,
                PositionStatus.SL_EXECUTED.value,
                PositionStatus.TARGET_EXECUTED.value,
            ):
                logger.info(
                    f"ORDER_EVENT_IGNORED Order {order_id} belongs to closed position "
                    f"({existing_pos.get('status')}); historical fills ignored for new protection."
                )
                self.store.mark_broker_order_processed(
                    order_id=order_id,
                    filled_qty=event.filled_qty,
                    cumulative_value=round(event.filled_qty * (event.average_price or event.price or 0.0), 2),
                    status=event.status,
                    source="CLOSED_IGNORE",
                )
                return None

            # Only treat a fill as an entry if it is a manually placed order that opens a position.
            # Exits must never trigger protection (no bot buy/sell on your close/reduce).
            if self._is_exit_fill(event):
                side_str = event.side.value if event.side else "UNKNOWN"
                logger.info(
                    f"ORDER_EVENT_IGNORED Exit fill: {order_id} ({event.symbol} {side_str}) "
                    f"closes/reduces existing position — no new SL/buy/sell."
                )
                try:
                    self._handle_manual_exit_fill(event)
                except Exception as exit_ex:
                    logger.error(f"MANUAL_EXIT_HANDLE_FAILED {order_id}: {exit_ex}", exc_info=True)
                return None

            # Symbol prefix filter (e.g. only process NIFTY instruments)
            if settings.SYMBOL_FILTER_PREFIX and not event.symbol.upper().startswith(settings.SYMBOL_FILTER_PREFIX.upper()):
                logger.info(f"ORDER_EVENT_IGNORED Symbol {event.symbol} does not match prefix {settings.SYMBOL_FILTER_PREFIX}")
                return None

            # Only track regular Limit orders on BUY or SELL
            if event.order_type != "L":
                logger.info(f"ORDER_EVENT_IGNORED Order {order_id} type {event.order_type} is not Limit ('L')")
                return None

            side = event.side
            if not side:
                logger.info(f"ORDER_EVENT_IGNORED Order {order_id} side unknown: {event.side}")
                return None

            current_filled = event.filled_qty
            fill_price = event.average_price or event.price
            prev_processed = self.store.get_processed_qty(order_id)

            # Check if this is an order cancellation with an existing pending partial fill buffer
            if event.status in ("CANCELLED", "REJECTED") and order_id in self._fill_buffers:
                logger.info(f"PARTIAL_FILL_TERMINATED Order {order_id} was {event.status}. Finalizing buffered partial fills.")
                self._finalize_partial_fill_buffer(order_id, event.event_timestamp, product_type=event.product_type)
                return order_id

            # Only process fills (or terminal status with un-processed filled quantity)
            if event.status not in ("COMPLETE", "PARTIAL_FILL") and not (
                event.status in ("CANCELLED", "REJECTED") and current_filled > prev_processed
            ):
                logger.info(f"ORDER_EVENT_IGNORED Order {order_id} status {event.status} not fill")
                return None

            if current_filled <= 0 or fill_price <= 0:
                logger.info(f"ORDER_EVENT_IGNORED Order {order_id} invalid fill_qty={current_filled} or price={fill_price}")
                return None

            # Delta fill calculation (Kotak Neo fldQty is cumulative)
            if current_filled <= prev_processed:
                logger.info(f"DUPLICATE_EVENT Order {order_id}: current_filled={current_filled} <= prev_processed={prev_processed}")
                return None

            delta_qty = current_filled - prev_processed

            # Derive true incremental fill price for this delta slice
            prev_cum_val = self.store.get_cumulative_value(order_id)
            curr_cum_val = round(current_filled * fill_price, 2)
            if prev_processed > 0 and prev_cum_val > 0 and curr_cum_val > prev_cum_val:
                slice_price = round((curr_cum_val - prev_cum_val) / delta_qty, 2)
                if slice_price <= 0:
                    slice_price = fill_price
            else:
                slice_price = fill_price

            # Idempotency fingerprint (order_id + cumulative_filled + rounded price)
            exec_key = f"{order_id}_{current_filled}_{round(slice_price, 2)}"
            if self.store.is_execution_seen(exec_key):
                logger.info(f"DUPLICATE_EVENT Execution already seen: {exec_key}")
                return None
            self.store.mark_execution_seen(
                exec_key,
                {"order_id": order_id, "delta_qty": delta_qty, "current_filled": current_filled, "price": slice_price}
            )

            # Persist broker order id + cumulative fill + processing timestamp (restart-safe)
            self.store.mark_broker_order_processed(
                order_id=order_id,
                filled_qty=current_filled,
                cumulative_value=curr_cum_val,
                status=event.status,
                source="LIVE_EVENT",
            )

            if event.status == "PARTIAL_FILL":
                logger.info(f"PARTIAL_FILL Order {order_id}: new {delta_qty} (cumulative {current_filled}/{event.total_qty}) @ ₹{slice_price:.2f} (avg ₹{fill_price:.2f})")

            logger.info(f"NEW_FILL_DETECTED Order {order_id}: {delta_qty}x {event.symbol} ({side.value}) @ ₹{slice_price:.2f}")
            self.store.log_event("ENTRY_FILL", order_id, f"{event.symbol} {side.value} delta={delta_qty} cum={current_filled} slice_p={slice_price} avg_p={fill_price}")

            is_partial = (event.status == "PARTIAL_FILL" and event.unfilled_qty > 0)
            has_buffer = order_id in self._fill_buffers
            # Prefer raw broker product (e.g. BO) so we never silently place as MIS
            entry_product = event.raw_product or event.product_type
            if settings.PARTIAL_FILL_HANDLING == "AGGREGATED" and (is_partial or has_buffer):
                # AGGREGATED mode: buffer partial fills until complete
                self._buffer_partial_fill(
                    order_id, event.symbol, side, delta_qty, slice_price,
                    event.event_timestamp, is_complete=not is_partial,
                    product_type=entry_product
                )
                return order_id
            else:
                self._place_protective_orders(
                    order_id, event.symbol, side, delta_qty, slice_price,
                    event.event_timestamp, current_filled=current_filled,
                    product_type=entry_product
                )
                return order_id

    def _buffer_partial_fill(self, order_id: str, symbol: str, side: OrderSide,
                             qty: int, price: float, event_time: float, is_complete: bool,
                             product_type: Optional[str] = None):
        with self._lock:
            buf = self._fill_buffers.get(order_id)
            if not buf:
                buf = PartialFillBuffer(order_id, symbol, side)
                self._fill_buffers[order_id] = buf
            buf.add(qty, price)
            self.store.save_partial_fill_buffer(order_id, symbol, side.value, buf.fills)

            if is_complete:
                buf.is_complete = True
                total_qty = buf.total_qty
                vwap_price = buf.vwap
                del self._fill_buffers[order_id]
                self.store.delete_partial_fill_buffer(order_id)

        if is_complete:
            logger.info(f"Aggregated fill complete: {total_qty}x {symbol} @ VWAP ₹{vwap_price:.2f}")
            self._place_protective_orders(
                order_id, symbol, side, total_qty, vwap_price, event_time,
                product_type=product_type
            )

    def _finalize_partial_fill_buffer(self, order_id: str, event_time: float, product_type: Optional[str] = None):
        """Finalizes any pending partial fill buffer (e.g. after cancellation of remaining size)."""
        with self._lock:
            buf = self._fill_buffers.get(order_id)
            if not buf or buf.total_qty <= 0:
                return
            total_qty = buf.total_qty
            vwap_price = buf.vwap
            symbol = buf.symbol
            side = buf.side
            del self._fill_buffers[order_id]
            self.store.delete_partial_fill_buffer(order_id)

        logger.info(f"Finalizing partial fill buffer for {order_id}: {total_qty}x {symbol} @ VWAP ₹{vwap_price:.2f}")
        self._place_protective_orders(
            order_id, symbol, side, total_qty, vwap_price, event_time,
            product_type=product_type
        )

    def _scrub_false_entries_from_exits(self, broker_net_by_sym: Dict[str, int]) -> int:
        """
        Cancel bot SL and close local rows that were wrongly created from manual exits
        (e.g. SELL reduce of a long treated as short entry → BUY SL).
        """
        scrubbed = 0
        for pos in list(self.store.all_positions()):
            st = pos.get("status")
            if st in (
                PositionStatus.CLOSED.value,
                PositionStatus.CLOSED_EXTERNAL.value,
                PositionStatus.SL_EXECUTED.value,
                PositionStatus.TARGET_EXECUTED.value,
            ):
                continue
            sym = str(pos.get("symbol") or "").strip().upper()
            if not sym:
                continue
            pos_side = str(pos.get("side") or "").upper()
            net = int(broker_net_by_sym.get(sym, 0) or 0)
            # Short local row but broker is flat/long → was a long-exit misclassified as short entry
            false_short = pos_side in ("SELL", "S") and net >= 0
            # Long local row but broker is flat/short → was a short-exit misclassified as long entry
            false_long = pos_side in ("BUY", "B") and net <= 0
            if not (false_short or false_long):
                continue
            pos_id = pos["entry_order_id"]
            for sl_id in (
                pos.get("sl_order_id"),
                pos.get("half_b_sl_order_id"),
                pos.get("half_a_sl_order_id"),
            ):
                if sl_id and not str(sl_id).startswith("SKIPPED_"):
                    self._cancel_safe(str(sl_id), sym, f"scrub false entry {pos_id}")
            pos["status"] = PositionStatus.CLOSED_EXTERNAL.value
            pos["remaining_qty"] = 0
            pos["protection_state"] = "CLOSED_EXTERNAL"
            pos["error_message"] = (
                f"Scrubbed false entry (side={pos_side}, broker_net={net}); "
                f"was a manual exit, not a new position."
            )
            pos["sl_order_id"] = None
            pos["half_b_sl_order_id"] = None
            pos["half_a_sl_order_id"] = None
            self.store.upsert_position(pos)
            with self._lock:
                self._positions[pos_id] = pos
            scrubbed += 1
            logger.warning(
                f"FALSE_ENTRY_SCRUBBED_STARTUP {pos_id} ({sym}) side={pos_side} net={net}"
            )
        if scrubbed:
            logger.warning(f"STARTUP_SCRUBBED_FALSE_ENTRIES count={scrubbed}")
        return scrubbed

    def _active_positions_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        sym = (symbol or "").strip().upper()
        out: List[Dict[str, Any]] = []
        terminal = {
            PositionStatus.CLOSED.value,
            PositionStatus.CLOSED_EXTERNAL.value,
            PositionStatus.SL_EXECUTED.value,
            PositionStatus.TARGET_EXECUTED.value,
            PositionStatus.MANUALLY_MODIFIED.value,
            PositionStatus.CANCELLED.value,
            PositionStatus.EXITED.value,
        }
        with self._lock:
            positions = list(self._positions.values())
        for pos in positions:
            if str(pos.get("symbol") or "").strip().upper() != sym:
                continue
            if pos.get("status") in terminal:
                continue
            try:
                rem = int(pos.get("remaining_qty") if pos.get("remaining_qty") is not None else pos.get("total_qty") or 0)
            except (TypeError, ValueError):
                rem = 0
            if rem <= 0:
                continue
            out.append(pos)
        return out

    def _is_exit_fill(self, event: NormalizedOrderEvent) -> bool:
        """
        True when a fill closes/reduces an existing position (manual exit) — never protect those.
        Manual SELL of a long (or BUY of a short) must NOT be treated as a new entry.
        """
        tag_upper = event.tag.upper() if event.tag else ""
        if tag_upper.startswith("BOT_") or any(bt in tag_upper for bt in ("BOT_TARGET", "BOT_SL", "BOT_TGT", "BOT_EMG")):
            return True

        # Tag-only keywords (never match order_id substrings — e.g. "...EXIT..." in a client id)
        if any(kw in tag_upper for kw in ("EXIT", "SQUAREOFF", "SQOFF", "CLOSE")):
            return True

        # Check explicit order-role registry
        role_entry = self.store.get_order_role(event.order_id)
        if role_entry:
            role = str(role_entry.get("role", "")).upper()
            if role in ("EXIT", "TARGET", "SL", "EMERGENCY_SL", "TARGET_A", "SL_A", "SL_B"):
                return True
            if role == "ENTRY":
                return False

        # If order is registered as a target or SL in any active position
        with self._lock:
            for pos in list(self._positions.values()):
                exit_order_ids = {
                    pos.get("target_order_id"), pos.get("sl_order_id"),
                    pos.get("half_a_target_order_id"), pos.get("half_a_sl_order_id"),
                    pos.get("half_b_sl_order_id")
                }
                if event.order_id in exit_order_ids:
                    return True

        side = event.side
        sym = (event.symbol or "").strip().upper()
        if not side or not sym:
            return False

        # Opposite-side fill against an open local position = manual reduce/close
        for pos in self._active_positions_for_symbol(sym):
            pos_side = str(pos.get("side") or "").upper()
            if pos_side in ("BUY", "B") and side == OrderSide.SELL:
                return True
            if pos_side in ("SELL", "S") and side == OrderSide.BUY:
                return True

        # Broker net (only when this symbol is present): still-long after SELL / still-short after BUY
        # means the fill reduced an open position. Do NOT use net==0 alone — empty/stale books
        # would mis-classify brand-new entries as exits.
        net_map = self._cached_broker_net_map(force=True)
        if net_map is not None and sym in net_map:
            net = int(net_map.get(sym, 0) or 0)
            if side == OrderSide.SELL and net > 0:
                return True
            if side == OrderSide.BUY and net < 0:
                return True

        return False

    def _handle_manual_exit_fill(self, event: NormalizedOrderEvent) -> None:
        """
        Apply a manual exit fill: shrink/close matching local positions and cancel leftover bot SL.
        Never places a new buy/sell / never re-protects the exit leg.
        """
        side = event.side
        sym = (event.symbol or "").strip().upper()
        if not side or not sym:
            return
        try:
            curr = abs(int(event.filled_qty or 0))
            prev = abs(int(self.store.get_processed_qty(event.order_id) or 0))
            exit_qty = max(0, curr - prev) or curr
        except (TypeError, ValueError):
            exit_qty = 0
        if exit_qty <= 0:
            return

        # Mark exit order processed so it is never treated as an entry later
        try:
            px = float(event.average_price or event.price or 0.0)
            self.store.mark_broker_order_processed(
                order_id=event.order_id,
                filled_qty=int(event.filled_qty or exit_qty),
                cumulative_value=round(float(event.filled_qty or exit_qty) * px, 2),
                status=event.status,
                source="MANUAL_EXIT",
            )
        except Exception as mark_ex:
            logger.debug(f"exit fill mark_processed skipped: {mark_ex}")

        remaining_exit = exit_qty
        for pos in self._active_positions_for_symbol(sym):
            if remaining_exit <= 0:
                break
            pos_side = str(pos.get("side") or "").upper()
            is_opposite = (
                (pos_side in ("BUY", "B") and side == OrderSide.SELL)
                or (pos_side in ("SELL", "S") and side == OrderSide.BUY)
            )
            if not is_opposite:
                continue

            pos_id = pos["entry_order_id"]
            try:
                rem = int(pos.get("remaining_qty") if pos.get("remaining_qty") is not None else pos.get("total_qty") or 0)
            except (TypeError, ValueError):
                rem = 0
            applied = min(rem, remaining_exit)
            new_rem = max(0, rem - applied)
            remaining_exit -= applied

            sl_ids = [
                pos.get("sl_order_id"),
                pos.get("half_b_sl_order_id"),
                pos.get("half_a_sl_order_id"),
            ]
            # Always cancel bot SL on manual reduce/close — do not auto-replace (no new buy/sell).
            for sl_id in sl_ids:
                if sl_id and not str(sl_id).startswith("SKIPPED_"):
                    self._cancel_safe(str(sl_id), sym, f"manual exit fill {event.order_id} reduced/closed position")

            if new_rem <= 0:
                pos["remaining_qty"] = 0
                pos["status"] = PositionStatus.CLOSED_EXTERNAL.value
                pos["protection_state"] = "CLOSED_EXTERNAL"
                pos["error_message"] = f"Closed/reduced by manual exit order {event.order_id}"
                logger.info(
                    f"MANUAL_EXIT_CLOSED Position {pos_id} ({sym}) closed by manual "
                    f"{side.value} fill {event.order_id}"
                )
            else:
                pos["remaining_qty"] = new_rem
                pos["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                pos["protection_state"] = "UNPROTECTED"
                pos["error_message"] = (
                    f"Manual exit {event.order_id} reduced qty to {new_rem}. "
                    f"Bot SL cancelled — no auto re-place. Protect remaining manually if needed."
                )
                logger.warning(
                    f"MANUAL_EXIT_REDUCED Position {pos_id} ({sym}) remaining={new_rem} "
                    f"after manual {side.value} {event.order_id}; SL cancelled, no auto buy/sell."
                )
                if self.store.should_send_alert(f"MANUAL_EXIT_REDUCED_{pos_id}"):
                    TelegramAlerts.needs_manual_action(
                        symbol=sym,
                        reason=pos["error_message"],
                        order_id=pos_id,
                    )

            pos["sl_order_id"] = None
            pos["half_b_sl_order_id"] = None
            pos["half_a_sl_order_id"] = None
            self.store.upsert_position(pos)
            with self._lock:
                self._positions[pos_id] = pos

        # If this order was wrongly stored as a protected "entry", scrub it
        false_pos = self.store.get_position(event.order_id)
        if false_pos and str(false_pos.get("status")) not in (
            PositionStatus.CLOSED.value,
            PositionStatus.CLOSED_EXTERNAL.value,
        ):
            for sl_id in (
                false_pos.get("sl_order_id"),
                false_pos.get("half_b_sl_order_id"),
                false_pos.get("half_a_sl_order_id"),
            ):
                if sl_id and not str(sl_id).startswith("SKIPPED_"):
                    self._cancel_safe(str(sl_id), sym, f"scrub false entry protection on exit fill {event.order_id}")
            false_pos["status"] = PositionStatus.CLOSED_EXTERNAL.value
            false_pos["remaining_qty"] = 0
            false_pos["protection_state"] = "CLOSED_EXTERNAL"
            false_pos["error_message"] = "Manual exit fill — not an entry; protection scrubbed"
            false_pos["sl_order_id"] = None
            false_pos["half_b_sl_order_id"] = None
            self.store.upsert_position(false_pos)
            with self._lock:
                self._positions[event.order_id] = false_pos
            logger.warning(
                f"FALSE_ENTRY_SCRUBBED {event.order_id} ({sym}) was an exit fill; "
                f"cancelled bot SL and closed local row."
            )

    def _call_place_order(self, symbol: str, side: OrderSide, qty: int,
                          ord_type: str, price: float, trigger_price: float = 0.0,
                          tag: str = "", product_type: Optional[str] = None) -> str:
        import inspect
        kwargs = {
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "ord_type": ord_type,
            "price": price,
            "trigger_price": trigger_price,
            "tag": tag,
        }
        try:
            sig = inspect.signature(self.broker.place_order)
            if "product" in sig.parameters and product_type:
                kwargs["product"] = product_type
        except Exception:
            if product_type:
                kwargs["product"] = product_type
        return self.broker.place_order(**kwargs)

    def _place_protective_orders(self, order_id: str, symbol: str, side: OrderSide,
                                  qty: int, price: float, detection_ts: float, current_filled: int = 0,
                                  product_type: Optional[str] = None):
        """Core: validates, calculates, and places required half/half exit orders."""
        # Stop automated trading when a prior failure locked the bot
        is_locked, lock_reason = self.store.is_entry_locked()
        if is_locked:
            err = f"Automated trading locked — waiting for manual intervention: {lock_reason}"
            logger.error(f"ENTRY_LOCKED {err} Ignoring fill for {order_id} ({symbol}).")
            self._mark_error(order_id, symbol, side, qty, price, err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            if self.store.should_send_alert(f"LOCKED_SKIP_{order_id}"):
                TelegramAlerts.needs_manual_action(symbol=symbol, reason=err, order_id=order_id)
            return

        # 0. Resolve placeable product (never silent BO/CO → MIS)
        place_product, prod_err = resolve_protective_product(
            product_type, fallback=settings.PRODUCT_TYPE
        )
        if not place_product:
            err = prod_err or f"Unsupported product for protection: {product_type!r}"
            logger.error(f"PRODUCT_BLOCKED {err}")
            self._mark_error(order_id, symbol, side, qty, price, err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, err)
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=err, order_id=order_id)
            return
        product_type = place_product
        logger.info(
            f"PROTECTIVE_PRODUCT {order_id} ({symbol}): using placeable product={product_type}"
        )

        # 1. Lot size from scrip master (never guess)
        lot_size = self.broker.get_lot_size(symbol)
        if lot_size is None or lot_size <= 0:
            err = f"Scrip master did not return lot size for {symbol}."
            logger.error(f"LOT_SIZE_LOOKUP_FAILED {err}. Marking position NEEDS_MANUAL_ACTION.")
            self._mark_error(order_id, symbol, side, qty, price, err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, err)
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=err, order_id=order_id)
            return

        # 2. Validate qty is a whole-lot multiple (required by broker)
        if qty % lot_size != 0:
            err = (
                f"Filled quantity {qty} for {symbol} is not a multiple of lot size {lot_size}. "
                f"Cannot place SL safely. Manual intervention required."
            )
            logger.error(f"LOT_SIZE_VALIDATION_FAILED {err}")
            self._mark_error(order_id, symbol, side, qty, price, err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, err)
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=err, order_id=order_id)
            return

        # 3. Calculate exit plan (even lots -> 50/50 target+SL; odd lots -> 100% SL)
        try:
            plan: ExitPlan = calculate_exit_plan(
                entry_side=side,
                entry_price=price,
                total_qty=qty,
                target_offset=settings.TARGET_OFFSET,
                sl_offset=settings.SL_OFFSET,
                sl_limit_buffer=settings.SL_LIMIT_BUFFER,
                lot_size=lot_size,
            )
        except Exception as ex:
            logger.error(f"ERROR Exit plan calculation failed for {order_id}: {ex}")
            self._mark_error(order_id, symbol, side, qty, price, str(ex), status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, f"Exit plan calculation failed for {order_id} ({symbol}): {ex}")
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=str(ex), order_id=order_id)
            return

        # 4. Cheap option validation (trigger or limit <= 0)
        if not plan.sl_valid or plan.sl_trigger_price <= 0 or plan.sl_limit_price <= 0:
            cheap_err = (
                f"Cheap option detected: SL trigger (₹{plan.sl_trigger_price:.2f}) "
                f"or limit (₹{plan.sl_limit_price:.2f}) <= 0 for {symbol}. "
                f"SL cannot be placed safely."
            )
            logger.error(f"CHEAP_OPTION_REJECTED {cheap_err} Marking position NEEDS_MANUAL_ACTION.")
            self._mark_error(order_id, symbol, side, qty, price, cheap_err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, cheap_err)
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=cheap_err, order_id=order_id)
            return

        # 5. Risk validate
        err = validate_exit_plan(plan)
        if err:
            logger.error(f"ERROR Risk validation failed for {order_id}: {err}")
            self._mark_error(order_id, symbol, side, qty, price, err, status=PositionStatus.NEEDS_MANUAL_ACTION.value)
            self.store.set_entry_lock(True, err)
            TelegramAlerts.needs_manual_action(symbol=symbol, reason=err, order_id=order_id)
            return

        # Check latency warning (> 2.0s threshold)
        elapsed_sec = time.time() - detection_ts
        if elapsed_sec > 2.0:
            logger.warning(
                f"LATENCY_WARNING Time from fill detection to protective orders placed: "
                f"{elapsed_sec:.3f}s (> 2.0s threshold)"
            )

        # Unique position key
        pos_key = f"{order_id}_fill_{current_filled}" if current_filled > 0 and current_filled != qty else order_id

        # Create position record
        pos: Dict[str, Any] = {
            "entry_order_id": pos_key,
            "symbol": symbol,
            "side": side.value,
            "total_qty": qty,
            "entry_price": price,
            "order_structure": plan.order_structure,
            "target_qty": 0,
            "target_price": plan.target_price,
            "target_order_id": "SKIPPED_SL_ONLY",
            "sl_qty": plan.sl_qty,
            "sl_trigger_price": plan.sl_trigger_price,
            "sl_limit_price": plan.sl_limit_price,
            "sl_order_id": None,
            "half_a_target_order_id": "SKIPPED_SL_ONLY",
            "half_a_sl_order_id": "SKIPPED_SL_ONLY",
            "half_b_sl_order_id": None,
            "half_a_qty": 0,
            "half_b_qty": qty,
            "recovery_attempts": 0,
            "status": PositionStatus.ENTRY_DETECTED.value,
            "protection_state": "PLACEMENT_PENDING",
            "latency_ms": 0.0,
            "processed_qty": qty,
            "cumulative_value": self.store.get_cumulative_value(order_id),
            "remaining_qty": qty,
            "created_at": time.time(),
            "error_message": None
        }
        self.store.upsert_position(pos)
        self.store.register_order_role(
            order_id, role="ENTRY", position_id=pos_key,
            symbol=symbol, side=side.value, qty=qty, status="FILLED"
        )
        self.store.log_event(
            "ENTRY_DETECTED", pos_key,
            f"symbol={symbol} side={side.value} qty={qty} price={price:.2f} "
            f"product={product_type} source=100pct_SL_only"
        )
        with self._lock:
            self._positions[pos_key] = pos

        if settings.DRY_RUN:
            logger.info(
                f"[DRY RUN] SL-only plan for {symbol}: "
                f"entry {side.value} {qty}@{price:.2f} lot={lot_size}; "
                f"SL {qty} {plan.sl_exit_side.value} "
                f"trg={plan.sl_trigger_price:.2f} lim={plan.sl_limit_price:.2f}"
            )

        self._place_sl_only(
            pos, pos_key, order_id, symbol, side, qty, price, plan, detection_ts,
            product_type=product_type
        )

    def _place_sl_only(
        self,
        pos: Dict[str, Any],
        pos_key: str,
        order_id: str,
        symbol: str,
        side: OrderSide,
        qty: int,
        price: float,
        plan: ExitPlan,
        detection_ts: float,
        product_type: Optional[str] = None,
    ) -> None:
        """
        Places exactly one SL order covering 100% of the confirmed filled quantity.
        No Target order is ever placed. No fallback split logic.
        On failure: UNPROTECTED + entry lock + critical Telegram alert.
        No automatic retry. No auto square-off.
        """
        leg_tag = make_order_tag("SL", order_id, 1)
        sl_id: Optional[str] = None
        err_msg: Optional[str] = None

        try:
            sl_id = self._call_place_order(
                symbol=symbol,
                side=plan.sl_exit_side,
                qty=qty,
                ord_type="SL",
                price=plan.sl_limit_price,
                trigger_price=plan.sl_trigger_price,
                tag=leg_tag,
                product_type=product_type,
            )
        except Exception as ex:
            err_msg = f"SL placement exception: {ex}"
            logger.error(f"SL_PLACEMENT_FAILED {pos_key} ({symbol}): {ex}")

        if not sl_id:
            err_msg = err_msg or "SL placement returned empty order id"
            pos["status"] = PositionStatus.UNPROTECTED.value
            pos["protection_state"] = "UNPROTECTED"
            pos["error_message"] = err_msg
            pos["latency_ms"] = round((time.time() - detection_ts) * 1000, 2)
            self.store.upsert_position(pos)
            with self._lock:
                self._positions[pos_key] = pos
            self.store.log_event("SL_PLACEMENT_FAILED", pos_key, err_msg)
            lock_reason = f"SL placement failed for {pos_key} ({symbol}): {err_msg}"
            self.store.set_entry_lock(True, lock_reason)
            logger.error(f"PROTECTION_FAILED Position {pos_key} UNPROTECTED: {err_msg}")
            TelegramAlerts.protection_failed_idle_alert(
                symbol=symbol,
                entry_order_id=order_id,
                filled_qty=qty,
                failed_order_type="SL",
                broker_error=err_msg,
                failed_order_id=None,
                position_status="UNPROTECTED",
                protection_confirmed=False,
            )
            return

        # Soft-verify: a successful place_order nOrdNo is trusted. Extra status lookup
        # often adds 1-5s (and can briefly return None while Kotak indexes the order).
        verified_status = None
        try:
            verified_status = self.broker.get_order_status(sl_id)
        except Exception as ver_ex:
            logger.warning(f"SL_VERIFY_SKIP {sl_id}: status lookup failed ({ver_ex}); trusting place_order id")

        sl_active = (
            self.broker.dry_run
            or verified_status in ("OPEN", "TRIGGER_PENDING", "TRIGGER PENDING", "COMPLETE", "TRAD", "FILLED")
            or verified_status is None  # book lag right after place — treat as active
        )

        if not sl_active:
            err_msg = (
                f"SL {sl_id} placed but broker reports status={verified_status!r}; "
                f"position is NOT confirmed protected."
            )
            pos["status"] = PositionStatus.UNPROTECTED.value
            pos["protection_state"] = "UNVERIFIED"
            pos["error_message"] = err_msg
            pos["half_b_sl_order_id"] = sl_id
            pos["sl_order_id"] = sl_id
            pos["latency_ms"] = round((time.time() - detection_ts) * 1000, 2)
            self.store.register_order_role(
                sl_id, role="SL", position_id=pos_key,
                symbol=symbol, side=plan.sl_exit_side.value, qty=qty,
                status="UNVERIFIED", tag=leg_tag,
            )
            self.store.upsert_position(pos)
            with self._lock:
                self._positions[pos_key] = pos
            self.store.log_event("SL_UNVERIFIED", pos_key, err_msg)
            lock_reason = f"SL unverified for {pos_key} ({symbol}): {err_msg}"
            self.store.set_entry_lock(True, lock_reason)
            logger.error(f"SL_UNVERIFIED {err_msg}")
            TelegramAlerts.protection_failed_idle_alert(
                symbol=symbol,
                entry_order_id=order_id,
                filled_qty=qty,
                failed_order_type="SL",
                broker_error=err_msg,
                failed_order_id=sl_id,
                position_status="UNPROTECTED",
                protection_confirmed=False,
            )
            return

        # SL active and verified
        pos["order_structure"] = STRUCTURE_SL_ONLY
        pos["target_qty"] = 0
        pos["target_order_id"] = "SKIPPED_SL_ONLY"
        pos["half_a_target_order_id"] = "SKIPPED_SL_ONLY"
        pos["half_a_sl_order_id"] = "SKIPPED_SL_ONLY"
        pos["half_a_qty"] = 0
        pos["half_b_qty"] = qty
        pos["sl_qty"] = qty
        pos["sl_trigger_price"] = plan.sl_trigger_price
        pos["sl_limit_price"] = plan.sl_limit_price
        pos["half_b_sl_order_id"] = sl_id
        pos["sl_order_id"] = sl_id
        pos["verified_sl_qty"] = qty
        pos["latency_ms"] = round((time.time() - detection_ts) * 1000, 2)
        pos["status"] = PositionStatus.PROTECTED.value
        pos["protection_state"] = "PROTECTED"
        pos["error_message"] = None

        self.store.register_order_role(
            sl_id, role="SL", position_id=pos_key,
            symbol=symbol, side=plan.sl_exit_side.value, qty=qty,
            status="OPEN", tag=leg_tag,
        )
        self.store.upsert_position(pos)
        with self._lock:
            self._positions[pos_key] = pos
            self._total_trades += 1

        self.store.log_event(
            "PROTECTED", pos_key,
            f"sl={sl_id} qty={qty} trg={plan.sl_trigger_price:.2f} "
            f"lim={plan.sl_limit_price:.2f} structure={STRUCTURE_SL_ONLY} "
            f"latency_ms={pos['latency_ms']:.1f}",
        )
        logger.info(
            f"SL_ONLY_PROTECTED Position {pos_key} PROTECTED | "
            f"SL={sl_id} qty={qty} trg=₹{plan.sl_trigger_price:.2f} "
            f"lim=₹{plan.sl_limit_price:.2f} latency={pos['latency_ms']:.0f}ms"
        )
        TelegramAlerts.sl_only_placed(
            symbol=symbol,
            side=side.value,
            entry_price=price,
            sl_qty=qty,
            sl_trigger=plan.sl_trigger_price,
            sl_limit=plan.sl_limit_price,
            latency_ms=pos["latency_ms"],
            sl_side=plan.sl_exit_side.value,
        )

    def _mark_error(self, order_id: str, symbol: str, side: OrderSide, qty: int, price: float, msg: str, status: str = PositionStatus.ERROR.value):
        pos = self._positions.get(order_id) or {
            "entry_order_id": order_id, "symbol": symbol, "side": side.value,
            "total_qty": qty, "entry_price": price, "target_order_id": None,
            "sl_order_id": None, "created_at": time.time()
        }
        pos["status"] = status
        pos["error_message"] = msg
        self.store.upsert_position(pos)
        with self._lock:
            self._positions[order_id] = pos

    def _mark_manually_modified(self, pos_key: str, sl_order_id: str, reason: str) -> None:
        """
        Permanently mark a position as MANUALLY_MODIFIED.
        This is a terminal state: the entry_order_id is excluded from all future
        automated management (no SL recreation, no replacement, no reconciliation action).
        Sends a critical Telegram alert and logs to audit trail.
        """
        with self._lock:
            pos = self._positions.get(pos_key) or self.store.get_position(pos_key)
            if not pos:
                logger.warning(f"MANUALLY_MODIFIED_SKIP {pos_key}: no position record found")
                return

            already_terminal = pos.get("status") in (
                PositionStatus.MANUALLY_MODIFIED.value,
                PositionStatus.CANCELLED.value,
                PositionStatus.EXITED.value,
                PositionStatus.CLOSED.value,
                PositionStatus.CLOSED_EXTERNAL.value,
                PositionStatus.SL_EXECUTED.value,
            )
            if already_terminal:
                logger.info(
                    f"MANUALLY_MODIFIED_SKIP {pos_key}: already in terminal state {pos.get('status')}"
                )
                return

            pos["status"] = PositionStatus.MANUALLY_MODIFIED.value
            pos["protection_state"] = "MANUALLY_MODIFIED"
            pos["error_message"] = reason
            pos["remaining_qty"] = 0  # assume no remaining exposure after manual action
            self.store.upsert_position(pos)
            self._positions[pos_key] = pos

        self.store.update_order_role_status(sl_order_id, "MANUALLY_MODIFIED")
        self.store.log_event(
            "MANUALLY_MODIFIED", pos_key,
            f"sl_order_id={sl_order_id} reason={reason}"
        )
        logger.warning(f"MANUALLY_MODIFIED {pos_key} sl={sl_order_id}: {reason}")

        sym = pos.get("symbol", "UNKNOWN")
        # Alert only once per position (deduped by key)
        if self.store.should_send_alert(f"MANUALLY_MODIFIED_{pos_key}"):
            TelegramAlerts.critical_error(
                "Manual SL Change Detected",
                f"Entry {pos_key} ({sym}) marked MANUALLY_MODIFIED.\n"
                f"SL order {sl_order_id} was cancelled/modified by the user.\n"
                f"Reason: {reason}\n"
                f"Bot will NOT recreate or replace the SL. Manage position manually.",
            )

    # ──────────────────────────────────────
    # OCO & Exit Lifecycle
    # ──────────────────────────────────────
    def _check_oco(self, order_id: str, status: str, filled_qty: int = 0, avg_price: float = 0.0):
        """Hardened exit lifecycle: validates fills, partial fills, race conditions, and companion orders."""
        # 1. Check for cancellation / rejection of exit orders
        if status in ("CANCELLED", "REJECTED"):
            alert_key = f"ORDER_{order_id}_{status}"
            should_alert = self.store.should_send_alert(alert_key, cooldown_sec=1800.0, state_fingerprint=status)
            with self._lock:
                role_info = self.store.get_order_role(order_id)
                role_name = role_info.get("role") if role_info else None
                matched_pos_key = role_info.get("position_id") if role_info else None

                for pos in list(self._positions.values()):
                    pos_key = pos.get("entry_order_id")
                    if pos.get("status") in (
                        PositionStatus.CLOSED.value,
                        PositionStatus.CLOSED_EXTERNAL.value,
                        PositionStatus.MANUALLY_MODIFIED.value,
                        PositionStatus.CANCELLED.value,
                        PositionStatus.EXITED.value,
                        PositionStatus.SL_EXECUTED.value,
                        PositionStatus.TARGET_EXECUTED.value,
                    ):
                        continue

                    is_sl = (
                        pos.get("sl_order_id") == order_id
                        or pos.get("half_b_sl_order_id") == order_id
                        or pos.get("half_a_sl_order_id") == order_id
                        or (matched_pos_key == pos_key and role_name in ("SL", "SL_A", "SL_B"))
                    )
                    is_target = (
                        pos.get("target_order_id") == order_id
                        or pos.get("half_a_target_order_id") == order_id
                        or (matched_pos_key == pos_key and role_name in ("TARGET", "TARGET_EXIT", "TARGET_A"))
                    )

                    if is_sl:
                        rem_qty = pos.get("remaining_qty", pos.get("total_qty", 0))
                        sym = pos.get("symbol", "UNKNOWN")
                        self.store.update_order_role_status(order_id, status)

                        if status == "CANCELLED":
                            # --- Client manually cancelled the bot SL ---
                            # MANUALLY_MODIFIED is a terminal state. Bot never recreates SL.
                            reason = (
                                f"SL order {order_id} was CANCELLED on broker. "
                                f"Assumed manual cancellation by client. "
                                f"Bot will NOT recreate or replace the SL."
                            )
                            logger.warning(
                                f"SL_MANUALLY_CANCELLED {pos_key} ({sym}): "
                                f"SL {order_id} CANCELLED. Marking MANUALLY_MODIFIED."
                            )
                            already_terminal = pos.get("status") in (
                                PositionStatus.MANUALLY_MODIFIED.value,
                                PositionStatus.CANCELLED.value,
                                PositionStatus.EXITED.value,
                                PositionStatus.CLOSED.value,
                                PositionStatus.CLOSED_EXTERNAL.value,
                            )
                            if not already_terminal:
                                pos["status"] = PositionStatus.MANUALLY_MODIFIED.value
                                pos["protection_state"] = "MANUALLY_MODIFIED"
                                pos["error_message"] = reason
                                pos["remaining_qty"] = 0
                                self.store.upsert_position(pos)
                                self._positions[pos_key] = pos
                                self.store.log_event(
                                    "MANUALLY_MODIFIED", pos_key,
                                    f"sl_order_id={order_id} reason={reason}"
                                )
                            if should_alert:
                                TelegramAlerts.critical_error(
                                    "Manual SL Cancellation Detected",
                                    f"Entry {pos_key} ({sym}) \u2014 SL {order_id} was CANCELLED.\n"
                                    f"Assuming client manually cancelled. Bot will NOT recreate the SL.\n"
                                    f"If position is still open, please manage it manually.\n"
                                    f"Remaining qty at time of cancellation: {rem_qty}",
                                )
                        else:
                            # REJECTED: broker/margin issue — position now unprotected
                            if rem_qty > 0:
                                pos["status"] = PositionStatus.UNPROTECTED.value
                                pos["protection_state"] = "UNPROTECTED"
                                pos["error_message"] = f"SL order {order_id} was {status} on broker"
                                self.store.set_entry_lock(True, f"SL order {order_id} {status} on {sym}")
                                self.store.upsert_position(pos)
                                self._positions[pos_key] = pos
                                logger.error(
                                    f"SL_{status} SL order {order_id} was {status}! "
                                    f"Position {pos_key} marked UNPROTECTED."
                                )
                                if should_alert:
                                    TelegramAlerts.protection_failed_idle_alert(
                                        symbol=sym,
                                        entry_order_id=pos_key,
                                        filled_qty=rem_qty,
                                        failed_order_type="SL",
                                        broker_error=pos["error_message"],
                                        failed_order_id=order_id,
                                        position_status="UNPROTECTED",
                                        protection_confirmed=False,
                                    )
                        return

                    if is_target:
                        # Target orders are no longer placed in 100% SL-only mode.
                        # If a legacy target cancel/reject arrives, log and ignore.
                        self.store.update_order_role_status(order_id, status)
                        logger.info(
                            f"TARGET_{status} Target order {order_id} was {status}. "
                            f"No action taken (SL-only mode; target not managed)."
                        )
                        return
            return

        if status not in ("TRAD", "COMPLETE", "FILLED", "EXECUTED", "PARTIAL_FILL", "PARTIAL"):
            return

        # Idempotency fingerprint for exit order fill processing
        exec_key = f"EXIT_{order_id}_{filled_qty}_{status}"
        if self.store.is_execution_seen(exec_key):
            return

        with self._lock:
            for pos in list(self._positions.values()):
                pos_key = pos.get("entry_order_id")
                if pos.get("status") in (
                    PositionStatus.CLOSED.value,
                    PositionStatus.SL_EXECUTED.value,
                    PositionStatus.CLOSED_EXTERNAL.value,
                    PositionStatus.MANUALLY_MODIFIED.value,
                ):
                    continue

                sl_id = pos.get("sl_order_id") or pos.get("half_b_sl_order_id")
                if order_id == sl_id:
                    self.store.mark_execution_seen(exec_key, {"order_id": order_id, "status": status})
                    logger.info(
                        f"SL_FILLED 100% SL executed for {pos['symbol']}. Position fully closed."
                    )
                    self.store.update_order_role_status(order_id, "COMPLETE")
                    pos["status"] = PositionStatus.SL_EXECUTED.value
                    pos["remaining_qty"] = 0
                    pos["protection_state"] = "CLOSED"
                    self.store.upsert_position(pos)
                    self._positions[pos_key] = pos
                    TelegramAlerts.sl_hit(pos["symbol"], pos.get("sl_trigger_price", 0.0), "N/A")
                    return

    def _cancel_safe(self, order_id: Optional[str], symbol: str, reason: str) -> bool:
        """Hardened cancellation: sends cancel, verifies confirmation, logs structured events."""
        if not order_id or order_id == "SKIPPED_QTY_0" or str(order_id).startswith("SKIPPED_"):
            return True

        logger.info(f"OCO_CANCEL_SENT Cancelling order {order_id} ({reason})")
        try:
            self.broker.cancel_order(order_id)
            # Verify cancellation
            st = self.broker.get_order_status(order_id)
            if st in ("CANCELLED", "CAN") or self.broker.dry_run:
                logger.info(f"OCO_CANCEL_CONFIRMED Order {order_id} cancellation verified.")
                self.store.log_event("CANCELLED", order_id, reason)
                self.store.update_order_role_status(order_id, "CANCELLED")
                return True
            elif st in ("TRAD", "COMPLETE", "FILLED", "EXECUTED"):
                logger.warning(f"OCO_RACE Order {order_id} filled before cancellation took effect!")
                self.store.update_order_role_status(order_id, "COMPLETE")
                return False
            else:
                logger.warning(f"OCO_CANCEL_UNVERIFIED Order {order_id} status after cancel is {st}")
                self.store.log_event("CANCELLED_UNVERIFIED", order_id, f"{reason}; status={st}")
                return False
        except Exception as e:
            err = f"Failed to cancel {order_id} ({reason}): {e}"
            logger.error(f"ERROR {err}")
            TelegramAlerts.critical_error("OCO Cancellation", err)
            return False

    # ──────────────────────────────────────
    # Reconciliation (3-way audit)
    # ──────────────────────────────────────
    def _should_alert_issue(self, pos_id: str, issue_type: str, state_fingerprint: str = "") -> bool:
        """Sends Telegram reconciliation alert with cooldown per position per issue type and deduplication on state."""
        key = (pos_id, issue_type)
        now = time.time()
        last_info = self._reconciliation_alert_history.get(key)
        cooldown = getattr(settings, "RECONCILIATION_ALERT_COOLDOWN_SEC", 1800.0)
        if last_info is None:
            self._reconciliation_alert_history[key] = (now, state_fingerprint)
            return True
        last_ts, last_state = last_info
        if state_fingerprint and state_fingerprint != last_state:
            self._reconciliation_alert_history[key] = (now, state_fingerprint)
            return True
        if now - last_ts >= cooldown:
            self._reconciliation_alert_history[key] = (now, state_fingerprint)
            return True
        return False

    def reconcile(self, allow_auto_place: bool = False) -> List[str]:
        """
        Compares local positions vs broker order report and live broker positions.
        Returns list of detected issues.
        Rate-limits Telegram alert to once per position per issue type per cooldown.
        Never places replacement exits when allow_auto_place=False (startup / safety default).
        Open position with unverifiable protection -> NEEDS_MANUAL_ACTION + Telegram.
        Broker-flat positions -> CLOSED_EXTERNAL.
        """
        self.last_reconciliation_ts = time.time()
        logger.info("RECONCILIATION_STARTED Starting 3-way reconciliation audit...")
        issues = []
        alert_issues = []
        broker_orders = self.broker.get_order_report()
        if broker_orders is None:
            issue_msg = "Broker order report API unavailable: failed to fetch live orders from broker"
            logger.error(f"RECONCILIATION_ERROR {issue_msg}")
            issues.append(issue_msg)
            if self._should_alert_issue("GLOBAL", "BROKER_ORDERS_UNAVAILABLE"):
                alert_issues.append(issue_msg)
            broker_map = None
        else:
            broker_map = {
                str(o.get("nOrdNo") or o.get("order_id", "")): str(o.get("ordSt") or o.get("status", "")).upper()
                for o in broker_orders
            }

        # 0. Live broker positions check & verification
        broker_positions = self.broker.get_positions()
        if broker_positions is None:
            issue_msg = "Broker positions API unavailable: failed to fetch live positions from broker"
            logger.error(f"RECONCILIATION_ERROR {issue_msg}")
            issues.append(issue_msg)
            if self._should_alert_issue("GLOBAL", "BROKER_POSITIONS_UNAVAILABLE"):
                alert_issues.append(issue_msg)
            net_qty_by_symbol = None
        else:
            net_qty_by_symbol = self._build_broker_net_map(broker_positions) or {}
            self._broker_net_cache = dict(net_qty_by_symbol)
            self._broker_net_cache_ts = time.time()

        # If order book was unavailable, cannot safely audit order states without creating false alarms
        if broker_map is None:
            if alert_issues:
                logger.warning(f"Reconciliation alerting {len(alert_issues)} issue(s): {alert_issues}")
                TelegramAlerts.send(
                    "⚠️ <b>RECONCILIATION MISMATCH DETECTED</b>\n" +
                    "\n".join(f"• {i}" for i in alert_issues)
                )
            logger.warning("RECONCILIATION_INCOMPLETE Audit aborted because broker order report was unavailable.")
            return issues

        # Recover open user fills wrongly marked historical/seeded (first-time protect only)
        if net_qty_by_symbol is not None and broker_orders:
            reopened = self._reopen_unprotected_seeded_fills(broker_orders, net_qty_by_symbol)
            if reopened:
                protected_n = self._protect_open_unseeded_entries(broker_orders, net_qty_by_symbol)
                logger.warning(
                    f"RECON_PROTECT_RECOVERY Cleared {reopened} seeded fill(s); "
                    f"protection pass handled {protected_n}."
                )

        # Check existing positions against broker orders and fills
        for pos in list(self.store.all_positions()):
            if pos.get("is_simulated"):
                continue
            pos_id = pos["entry_order_id"]
            sym = pos["symbol"].strip().upper()
            curr_st = pos.get("status")
            order_struct = pos.get("order_structure", "SPLIT_TARGET_SL")
            sl_id = pos.get("sl_order_id") or pos.get("half_b_sl_order_id")
            tgt_id = pos.get("target_order_id") or pos.get("half_a_target_order_id")

            # A. Check if position's SL executed on broker
            if sl_id and broker_map.get(sl_id) in ("COMPLETE", "TRAD", "FILLED"):
                sl_det = self.broker.get_order_details(sl_id)
                fld = int(sl_det.get("fldQty") or sl_det.get("filled_quantity") or 0) if sl_det else 0
                total_q = pos.get("total_qty", 0)
                if fld >= total_q or fld == 0:
                    if curr_st != PositionStatus.SL_EXECUTED.value:
                        pos["status"] = PositionStatus.SL_EXECUTED.value
                        pos["remaining_qty"] = 0
                        pos["error_message"] = None
                        self.store.update_order_role_status(sl_id, "COMPLETE")
                        self.store.upsert_position(pos)
                        with self._lock:
                            self._positions[pos_id] = pos
                        logger.info(f"Position {pos_id} confirmed SL_EXECUTED via broker order {sl_id}.")
                        continue
                elif 0 < fld < total_q:
                    pos["status"] = PositionStatus.PARTIALLY_CLOSED.value
                    pos["remaining_qty"] = total_q - fld
                    self.store.update_order_role_status(sl_id, "PARTIAL_FILL")
                    self.store.upsert_position(pos)
                    with self._lock:
                        self._positions[pos_id] = pos
                    continue

            # B. Broker flat: cancel leftover bot exit orders; close stale local rows
            if self._is_broker_flat(net_qty_by_symbol, sym):
                # 1. Clean up any leftover orphan orders still pending on broker
                if sl_id and not str(sl_id).startswith("SKIPPED_") and broker_map.get(sl_id) in ("OPEN", "TRIGGER_PENDING"):
                    logger.warning(
                        f"ORPHAN_ORDER_CLEANUP Position {pos_id} ({sym}): broker flat, cancelling orphaned SL {sl_id}..."
                    )
                    self._cancel_safe(sl_id, sym, "orphaned SL on flat broker position")
                    issues.append(f"Position {pos_id} ({sym}): cancelled orphaned SL {sl_id} on flat broker position")

                if tgt_id and not str(tgt_id).startswith("SKIPPED_") and broker_map.get(tgt_id) in ("OPEN", "TRIGGER_PENDING"):
                    logger.warning(
                        f"ORPHAN_ORDER_CLEANUP Position {pos_id} ({sym}): broker flat, cancelling orphaned Target {tgt_id}..."
                    )
                    self._cancel_safe(tgt_id, sym, "orphaned Target on flat broker position")
                    issues.append(f"Position {pos_id} ({sym}): cancelled orphaned Target {tgt_id} on flat broker position")

                # 2. Already terminal — nothing to fix
                if curr_st in (
                    PositionStatus.CLOSED.value,
                    PositionStatus.CLOSED_EXTERNAL.value,
                    PositionStatus.SL_EXECUTED.value,
                    PositionStatus.TARGET_EXECUTED.value,
                    PositionStatus.MANUALLY_MODIFIED.value,
                    PositionStatus.CANCELLED.value,
                    PositionStatus.EXITED.value,
                ):
                    continue

                # 3. Any non-terminal local row while broker is flat → CLOSED_EXTERNAL
                # (covers remaining_qty already 0 but status left as PROTECTED/UNPROTECTED/etc.)
                pos["status"] = PositionStatus.CLOSED_EXTERNAL.value
                pos["remaining_qty"] = 0
                pos["error_message"] = None
                pos["protection_state"] = "CLOSED_EXTERNAL"
                self.store.upsert_position(pos)
                with self._lock:
                    self._positions[pos_id] = pos
                logger.info(
                    f"RECONCILIATION_RESOLVED Position {pos_id} ({sym}): broker net is flat. "
                    f"Resolved to CLOSED_EXTERNAL with remaining_qty=0 (was {curr_st})."
                )
                continue

            # C. Auto-resolve when all required protective legs are active on broker.
            # SKIPPED_QTY_0 alone must NEVER count as protection (false PROTECTED bug).
            if curr_st in (PositionStatus.UNPROTECTED.value, PositionStatus.ERROR.value):
                def _is_skipped(oid: Any) -> bool:
                    return (not oid) or str(oid).startswith("SKIPPED_")

                def _leg_active(oid: Any) -> bool:
                    if _is_skipped(oid):
                        return True  # not required
                    return broker_map.get(str(oid)) in ("OPEN", "TRIGGER_PENDING")

                tgt_a_id = pos.get("half_a_target_order_id") or tgt_id
                sl_a_id = pos.get("half_a_sl_order_id")
                sl_b_id = pos.get("half_b_sl_order_id") or sl_id
                real_ids = [x for x in (tgt_a_id, sl_a_id, sl_b_id) if not _is_skipped(x)]
                if real_ids and all(_leg_active(x) for x in (tgt_a_id, sl_a_id, sl_b_id)):
                    pos["status"] = PositionStatus.PROTECTED.value
                    pos["protection_state"] = "PROTECTED"
                    pos["remaining_qty"] = pos.get("total_qty")
                    pos["error_message"] = None
                    self.store.upsert_position(pos)
                    with self._lock:
                        self._positions[pos_id] = pos
                    logger.info(
                        f"AUTO_RESOLVED Position {pos_id} ({sym}) verified PROTECTED "
                        f"(active legs={real_ids})."
                    )

        # 1. Audit active protected positions (skip symbols already flat on broker)
        for pos in self.store.active_positions():
            pos_id = pos["entry_order_id"]
            sym_audit = str(pos.get("symbol", "")).strip().upper()
            if self._is_broker_flat(net_qty_by_symbol, sym_audit):
                continue
            sl_ids = [pos.get("sl_order_id"), pos.get("half_a_sl_order_id"), pos.get("half_b_sl_order_id")]
            checked_sls = set()
            for sl_id in sl_ids:
                if not sl_id or str(sl_id).startswith("SKIPPED_") or sl_id in checked_sls:
                    continue
                checked_sls.add(sl_id)
                broker_status = broker_map.get(sl_id)
                if not broker_status:
                    # Check live order status: only flag absent if truly absent
                    live_status = self.broker.get_order_status(sl_id)
                    if live_status is None:
                        issue_msg = f"Position {pos_id} ({pos['symbol']}): SL order {sl_id} not found in broker order book!"
                        issues.append(issue_msg)
                        pos["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                        pos["protection_state"] = "UNVERIFIED"
                        pos["error_message"] = f"SL order {sl_id} not found in broker order book"
                        self.store.set_entry_lock(True, pos["error_message"])
                        self.store.upsert_position(pos)
                        with self._lock:
                            self._positions[pos_id] = pos
                        if self._should_alert_issue(pos_id, f"SL_MISSING_{sl_id}"):
                            alert_issues.append(issue_msg)
                    elif live_status in ("CAN", "CANCELLED", "REJ", "REJECTED"):
                        issue_msg = f"Position {pos_id} ({pos['symbol']}): SL order {sl_id} is {live_status} on broker but ACTIVE in local state!"
                        issues.append(issue_msg)
                        pos["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                        pos["protection_state"] = "UNVERIFIED"
                        pos["error_message"] = f"SL order {sl_id} was {live_status} on broker"
                        self.store.set_entry_lock(True, pos["error_message"])
                        self.store.upsert_position(pos)
                        with self._lock:
                            self._positions[pos_id] = pos
                        if self._should_alert_issue(pos_id, f"SL_CANCELLED_{sl_id}"):
                            alert_issues.append(issue_msg)
                elif broker_status in ("CAN", "CANCELLED", "REJ", "REJECTED"):
                    issue_msg = f"Position {pos_id} ({pos['symbol']}): SL order {sl_id} is {broker_status} on broker but ACTIVE in local state!"
                    issues.append(issue_msg)
                    pos["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                    pos["protection_state"] = "UNVERIFIED"
                    pos["error_message"] = f"SL order {sl_id} was {broker_status} on broker"
                    self.store.set_entry_lock(True, pos["error_message"])
                    self.store.upsert_position(pos)
                    with self._lock:
                        self._positions[pos_id] = pos
                    if self._should_alert_issue(pos_id, f"SL_CANCELLED_{sl_id}"):
                        alert_issues.append(issue_msg)

        # 2. Unprotected / unverified open positions: alert only — never auto-place replacements
        for pos in self.store.unprotected_positions():
            pos_id = pos["entry_order_id"]
            sym = pos["symbol"].strip().upper()
            if self._is_broker_flat(net_qty_by_symbol, sym):
                continue

            # Bind already-active broker exit legs if present (no new orders)
            if pos.get("status") in (
                PositionStatus.ENTRY_DETECTED.value,
                PositionStatus.UNPROTECTED.value,
                PositionStatus.NEEDS_MANUAL_ACTION.value,
            ):
                existing_tgt = None
                existing_sl = None
                order_suffix = pos_id[-8:]
                order_prefix = pos_id[:6]
                for o in broker_orders or []:
                    if not isinstance(o, dict):
                        continue
                    o_sym = str(o.get("trdSym") or o.get("tradingSymbol") or o.get("symbol", "")).strip().upper()
                    o_tag = str(o.get("tag") or "").upper()
                    o_st = self._normalize_broker_ord_status(o.get("ordSt") or o.get("status"))
                    o_id = str(o.get("nOrdNo") or o.get("order_id", ""))
                    if o_sym == sym and o_st == "OPEN" and (order_suffix in o_tag or order_prefix in o_tag):
                        if "SL" in o_tag:
                            existing_sl = o_id
                        elif "TGT" in o_tag or "TARGET" in o_tag:
                            existing_tgt = o_id

                if existing_sl:
                    logger.info(
                        f"RECONCILIATION_BIND Found existing active broker SL order for {pos_id}: "
                        f"sl={existing_sl}. Binding without placing duplicates."
                    )
                    pos["target_order_id"] = "SKIPPED_SL_ONLY"
                    pos["half_a_target_order_id"] = "SKIPPED_SL_ONLY"
                    pos["half_a_sl_order_id"] = "SKIPPED_SL_ONLY"
                    pos["sl_order_id"] = existing_sl
                    pos["half_b_sl_order_id"] = existing_sl
                    pos["status"] = PositionStatus.PROTECTED.value
                    pos["protection_state"] = "PROTECTED"
                    pos["error_message"] = None
                    self.store.upsert_position(pos)
                    with self._lock:
                        self._positions[pos_id] = pos
                    continue

                # Protection cannot be verified — manual action only
                if allow_auto_place:
                    logger.warning(
                        f"RECONCILIATION: allow_auto_place=True ignored for safety on {pos_id}; "
                        f"no replacement exits will be submitted."
                    )
                reason = (
                    f"Open position {pos_id} ({sym}) protection cannot be verified. "
                    f"No automatic replacement / retry / re-entry / square-off will be placed."
                )
                pos["status"] = PositionStatus.NEEDS_MANUAL_ACTION.value
                pos["protection_state"] = "UNVERIFIED"
                pos["error_message"] = reason
                self.store.upsert_position(pos)
                with self._lock:
                    self._positions[pos_id] = pos
                self.store.set_entry_lock(True, reason)
                issue_msg = f"NEEDS_MANUAL_ACTION: {pos_id} ({sym}) — protection unverified"
                issues.append(issue_msg)
                if self._should_alert_issue(pos_id, "UNVERIFIED_PROTECTION", pos.get("status")):
                    alert_issues.append(issue_msg)
                    if self.store.should_send_alert(f"UNVERIFIED_PROTECTION_{pos_id}"):
                        TelegramAlerts.needs_manual_action(symbol=sym, reason=reason, order_id=pos_id)
            else:
                issue_msg = f"Unprotected position: {pos_id} ({pos['symbol']}) — state: {pos['status']}"
                issues.append(issue_msg)
                if self._should_alert_issue(pos_id, f"UNPROTECTED_{pos.get('status')}", pos.get("status")):
                    alert_issues.append(issue_msg)

        if alert_issues:
            logger.warning(f"Reconciliation alerting {len(alert_issues)} issue(s): {alert_issues}")
            TelegramAlerts.send(
                "⚠️ <b>RECONCILIATION MISMATCH DETECTED</b>\n" +
                "\n".join(f"• {i}" for i in alert_issues)
            )
        else:
            if issues:
                logger.debug(f"Reconciliation detected {len(issues)} issue(s) (Telegram alert suppressed by cooldown).")
            else:
                logger.debug("Reconciliation audit clean — all active positions protected.")

        # If book is flat and nothing left active, drop stale entry lock from earlier failures
        if net_qty_by_symbol is not None and not self.store.active_positions():
            locked, lock_reason = self.store.is_entry_locked()
            if locked:
                self.store.set_entry_lock(False, "")
                logger.warning(
                    f"ENTRY_LOCK_CLEARED_ON_FLAT_RECON Cleared entry lock (broker flat, no active positions). "
                    f"Prior reason: {lock_reason}"
                )

        if broker_positions is None or broker_orders is None:
            logger.warning(f"RECONCILIATION_INCOMPLETE Audit finished with broker API failures ({len(issues)} issue(s)).")
        else:
            logger.info(f"RECONCILIATION_COMPLETED Audit completed with {len(issues)} issue(s).")
        return issues

    # ──────────────────────────────────────
    # State Accessors (used by API routes)
    # ──────────────────────────────────────
    def get_all_positions(self) -> List[Dict[str, Any]]:
        return self.store.all_positions()

    def get_active_positions(self) -> List[Dict[str, Any]]:
        """Unified position source: genuinely active open positions."""
        return self.store.active_positions()

    def get_position(self, pos_id: str) -> Optional[Dict[str, Any]]:
        return self.store.get_position(pos_id)

    def get_health(self) -> Dict[str, Any]:
        import datetime
        from .workers import order_feed, coordinator

        now = time.time()
        uptime_sec = round(now - self._start_time, 2)
        total_sec = int(uptime_sec)
        h, rem = divmod(total_sec, 3600)
        m, s = divmod(rem, 60)
        uptime_str = f"{h}h {m}m {s}s"

        def _fmt(ts: Optional[float]) -> str:
            if not ts:
                return "none"
            return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()

        coord_status = coordinator.get_status()
        ws_status = coord_status["ws_state"].lower()

        broker_connected = True if (settings.DRY_RUN or (self.broker.sdk is not None)) else False
        broker_status = "connected" if broker_connected else "unauthenticated"

        # Safe health status determination: reflects overall system & broker connectivity
        status_val = coord_status["overall_health"].lower()
        if not broker_connected and not settings.DRY_RUN:
            status_val = "unhealthy"

        active = len(self.get_active_positions())

        last_ev_ts = self.last_order_event_ts or order_feed.last_event_ts
        last_rec_ts = coord_status["last_recon_success_ts"] or self.last_reconciliation_ts
        last_poll_ts = coord_status["last_poll_success_ts"]

        return {
            "status": status_val,
            "websocket": ws_status,
            "broker": broker_status,
            "last_order_event": _fmt(last_ev_ts),
            "last_reconciliation": _fmt(last_rec_ts),
            "last_rest_poll": _fmt(last_poll_ts),
            "poll_consecutive_errors": coord_status["poll_consecutive_errors"],
            "recon_consecutive_errors": coord_status["recon_consecutive_errors"],
            "uptime": uptime_str,
            "uptime_seconds": uptime_sec,
            "active_positions": active,
            "total_trades_managed": self._total_trades,
            "mode": "DRY_RUN" if settings.DRY_RUN else "LIVE",
        }

    # ──────────────────────────────────────
    # Test Injection (dry-run only)
    # ──────────────────────────────────────
    def inject_fill(self, symbol: str, side: OrderSide, qty: int, price: float) -> str:
        """Injects a simulated manual fill into the broker mock for testing."""
        return self.broker.simulate_entry_fill(symbol, side, qty, price) \
            if hasattr(self.broker, "simulate_entry_fill") else \
            self.broker._inject(symbol, side, qty, price)


# Singleton engine instance shared by routes and background workers
engine = TradingEngine()
