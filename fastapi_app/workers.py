"""
Async Background Workers & Lifecycle Coordinator for the FastAPI Bot:
  1. order_feed_loop        — PRIMARY: Kotak Neo WebSocket Order & Position Streaming
  2. order_monitor_loop     — BACKUP: REST order book polling fallback (every 5s)
  3. reconcile_loop         — AUDIT: 3-way position consistency (every 15s)
  4. heartbeat_loop         — DEAD-BOT DETECTION: periodic health logging & Telegram monitoring

WorkerCoordinator guarantees:
  - Exactly one instance of each worker task
  - Prevention of duplicate background workers
  - Clean, ordered graceful shutdown
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import time
from typing import Optional, Dict, Any

from .config import settings
from .engine import engine
from .notifier import TelegramAlerts
from .order_feed import KotakOrderFeed

logger = logging.getLogger("FastAPIBot.Workers")

# Primary WebSocket Order Feed instance
order_feed = KotakOrderFeed(
    engine=engine,
    reconcile_callback=engine.reconcile,
    reconnect_delay=settings.WS_RECONNECT_DELAY_SEC,
    max_reconnect_attempts=settings.WS_MAX_RECONNECT_ATTEMPTS
)


class WorkerCoordinator:
    """Coordinates and supervises all persistent background tasks."""

    def __init__(self):
        self._feed_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._recon_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        self._lock = asyncio.Lock()
        self._is_running = False

        # Metrics for health monitoring (separate attempt, success, error tracking)
        self.last_poll_attempt_ts: Optional[float] = None
        self.last_poll_attempt_mono: Optional[float] = None
        self.last_poll_success_ts: Optional[float] = None
        self.last_poll_success_mono: Optional[float] = None
        self.last_order_book_proc_ts: Optional[float] = None
        self.last_order_book_proc_mono: Optional[float] = None
        self.poll_consecutive_errors: int = 0
        self.last_poll_error_ts: Optional[float] = None
        self.last_poll_error_mono: Optional[float] = None
        self.last_poll_error_cat: Optional[str] = None

        self.last_recon_attempt_ts: Optional[float] = None
        self.last_recon_attempt_mono: Optional[float] = None
        self.last_recon_success_ts: Optional[float] = None
        self.last_recon_success_mono: Optional[float] = None
        self.recon_consecutive_errors: int = 0
        self.last_recon_error_ts: Optional[float] = None
        self.last_recon_error_mono: Optional[float] = None
        self.last_recon_error_cat: Optional[str] = None

        self._was_ws_disconnected: bool = False
        self._was_outage: bool = False

    @property
    def last_rest_poll_ts(self) -> Optional[float]:
        return self.last_poll_success_ts

    @last_rest_poll_ts.setter
    def last_rest_poll_ts(self, val: Optional[float]):
        self.last_poll_success_ts = val

    @property
    def last_recon_ts(self) -> Optional[float]:
        return self.last_recon_success_ts

    @last_recon_ts.setter
    def last_recon_ts(self, val: Optional[float]):
        self.last_recon_success_ts = val

    def record_poll_attempt(self, ts: float, mono: Optional[float] = None):
        self.last_poll_attempt_ts = ts
        self.last_poll_attempt_mono = mono if mono is not None else time.monotonic()

    def record_poll_success(self, ts: float, mono: Optional[float] = None):
        self.last_poll_success_ts = ts
        self.last_poll_success_mono = mono if mono is not None else time.monotonic()
        self.poll_consecutive_errors = 0

    def record_poll_failure(self, ts: float, error_cat: str, err_detail: str = "", mono: Optional[float] = None):
        self.poll_consecutive_errors += 1
        self.last_poll_error_ts = ts
        self.last_poll_error_mono = mono if mono is not None else time.monotonic()
        self.last_poll_error_cat = error_cat

    def record_order_book_proc(self, ts: float, mono: Optional[float] = None):
        self.last_order_book_proc_ts = ts
        self.last_order_book_proc_mono = mono if mono is not None else time.monotonic()

    def record_recon_attempt(self, ts: float, mono: Optional[float] = None):
        self.last_recon_attempt_ts = ts
        self.last_recon_attempt_mono = mono if mono is not None else time.monotonic()

    def record_recon_success(self, ts: float, mono: Optional[float] = None):
        self.last_recon_success_ts = ts
        self.last_recon_success_mono = mono if mono is not None else time.monotonic()
        self.recon_consecutive_errors = 0

    def record_recon_failure(self, ts: float, error_cat: str, err_detail: str = "", mono: Optional[float] = None):
        self.recon_consecutive_errors += 1
        self.last_recon_error_ts = ts
        self.last_recon_error_mono = mono if mono is not None else time.monotonic()
        self.last_recon_error_cat = error_cat

    def supervise_tasks(self):
        """Supervises running background worker tasks and safely restarts any terminated task."""
        if not self._is_running:
            return

        if self._feed_task and self._feed_task.done():
            exc = None
            try:
                exc = self._feed_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            logger.warning(f"SUPERVISOR: KotakOrderFeed terminated unexpectedly (exc={exc}). Restarting...")
            self._feed_task = asyncio.create_task(order_feed_loop(), name="KotakOrderFeed")

        if self._monitor_task and self._monitor_task.done():
            exc = None
            try:
                exc = self._monitor_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            logger.warning(f"SUPERVISOR: RESTBackupPoller terminated unexpectedly (exc={exc}). Restarting...")
            self._monitor_task = asyncio.create_task(order_monitor_loop(), name="RESTBackupPoller")

        if self._recon_task and self._recon_task.done():
            exc = None
            try:
                exc = self._recon_task.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass
            logger.warning(f"SUPERVISOR: ReconcileEngine terminated unexpectedly (exc={exc}). Restarting...")
            self._recon_task = asyncio.create_task(reconcile_loop(), name="ReconcileEngine")

    def get_status(self) -> Dict[str, Any]:
        now_wall = time.time()
        now_mono = time.monotonic()
        ws_conn = bool(order_feed.is_connected)

        if not ws_conn:
            ws_state = "disconnected"
        else:
            if getattr(order_feed, "last_event_mono", None) and (now_mono - order_feed.last_event_mono < 300.0):
                ws_state = "connected"
            elif order_feed.last_event_ts and (now_wall - order_feed.last_event_ts < 300.0):
                ws_state = "connected"
            else:
                ws_state = "idle"

        rest_failing = (self.poll_consecutive_errors >= 3)
        recon_failing = (self.recon_consecutive_errors >= 3)

        if not ws_conn and rest_failing:
            overall = "unhealthy"
        elif not ws_conn or rest_failing or recon_failing:
            overall = "degraded"
        else:
            overall = "healthy"

        return {
            "overall_health": overall,
            "ws_state": ws_state,
            "ws_connected": ws_conn,
            "ws_connected_at": order_feed.connected_at,
            "ws_connected_at_mono": getattr(order_feed, "connected_at_mono", None),
            "ws_disconnected_at": order_feed.disconnected_at,
            "ws_disconnected_at_mono": getattr(order_feed, "disconnected_at_mono", None),
            "ws_last_event_ts": order_feed.last_event_ts,
            "ws_last_event_mono": getattr(order_feed, "last_event_mono", None),
            "ws_last_msg_ts": getattr(order_feed, "last_msg_ts", None),
            "ws_last_msg_mono": getattr(order_feed, "last_msg_mono", None),
            "ws_last_frame_ts": getattr(order_feed, "last_frame_ts", None),
            "ws_last_frame_mono": getattr(order_feed, "last_frame_mono", None),
            "last_poll_attempt_ts": self.last_poll_attempt_ts,
            "last_poll_success_ts": self.last_poll_success_ts,
            "last_order_book_proc_ts": self.last_order_book_proc_ts,
            "poll_consecutive_errors": self.poll_consecutive_errors,
            "last_poll_error_ts": self.last_poll_error_ts,
            "last_poll_error_cat": self.last_poll_error_cat,
            "last_recon_attempt_ts": self.last_recon_attempt_ts,
            "last_recon_success_ts": self.last_recon_success_ts,
            "recon_consecutive_errors": self.recon_consecutive_errors,
            "last_recon_error_ts": self.last_recon_error_ts,
            "last_recon_error_cat": self.last_recon_error_cat,
        }

    @property
    def is_running(self) -> bool:
        return self._is_running

    async def start_all(self):
        """Starts all background workers idempotently. Prevents duplicate tasks."""
        async with self._lock:
            if self._is_running:
                logger.warning("WorkerCoordinator: Workers are already running. Skipping duplicate startup.")
                return

            self._is_running = True
            logger.info("🚀 Launching persistent background workers...")

            # 1. Primary: WebSocket Order Feed
            if self._feed_task is None or self._feed_task.done():
                self._feed_task = asyncio.create_task(order_feed_loop(), name="KotakOrderFeed")

            # 2. Backup: REST Order Monitor Poller
            if self._monitor_task is None or self._monitor_task.done():
                self._monitor_task = asyncio.create_task(order_monitor_loop(), name="RESTBackupPoller")

            # 3. Watchdog: 3-Way Reconciliation Audit
            if self._recon_task is None or self._recon_task.done():
                self._recon_task = asyncio.create_task(reconcile_loop(), name="ReconcileEngine")

            # 4. Heartbeat: Periodic status log & dead-bot detection
            if self._heartbeat_task is None or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(heartbeat_loop(), name="HeartbeatMonitor")

            logger.info("✅ All 4 background workers launched (WebSocket, REST poller, Reconciler, Heartbeat).")

    async def stop_all(self, timeout: float = 10.0):
        """Clean graceful shutdown of all background workers."""
        async with self._lock:
            if not self._is_running:
                return

            logger.info("🛑 Stopping background workers...")
            self._is_running = False

            # Stop WebSocket first
            try:
                await order_feed.stop()
            except Exception as e:
                logger.warning(f"Error stopping order feed: {e}")

            # Cancel running background tasks
            tasks = [t for t in (self._feed_task, self._monitor_task, self._recon_task, self._heartbeat_task) if t and not t.done()]
            for t in tasks:
                t.cancel()

            if tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning(f"Background tasks did not terminate within {timeout}s; proceeding with shutdown.")

            self._feed_task = None
            self._monitor_task = None
            self._recon_task = None
            self._heartbeat_task = None
            logger.info("✅ All background workers stopped cleanly.")


# Singleton coordinator
coordinator = WorkerCoordinator()


# ─────────────────────────────────────────────────────────────
# 1. Primary WebSocket Worker
# ─────────────────────────────────────────────────────────────
async def order_feed_loop():
    """
    PRIMARY execution detector task.
    Runs the Kotak Neo Order & Position Streaming WebSocket client.
    """
    logger.info("⚡ Order Feed Worker starting (PRIMARY real-time execution detector)...")
    try:
        await order_feed.start()
        # Keep worker loop alive until order_feed is cancelled or stopped
        if order_feed._task:
            await order_feed._task
    except asyncio.CancelledError:
        logger.info("Order feed loop cancelled.")
        await order_feed.stop()
    except Exception as e:
        logger.error(f"ERROR in order feed worker: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────
# 2. Backup REST Polling Worker
# ─────────────────────────────────────────────────────────────
async def order_monitor_loop():
    """
    BACKUP REST Polling Worker:
    Polls Kotak Neo order report every REST_BACKUP_POLL_INTERVAL_SEC (default 5.0s).
    Runs synchronous broker network calls off the event loop using asyncio.to_thread.
    Tracks separate attempt, success, and error timestamps with error categorization.
    """
    base_interval = float(settings.REST_BACKUP_POLL_INTERVAL_SEC)
    logger.info(f"🔁 REST Backup Poller started (interval: {base_interval:.1f}s)")
    # Minimal stagger only — do not wait a full poll interval before first scan
    await asyncio.sleep(0.05)

    while coordinator.is_running:
        now_wall = time.time()
        now_mono = time.monotonic()
        coordinator.record_poll_attempt(now_wall, mono=now_mono)
        interval = base_interval

        try:
            # Offload synchronous broker network call so event loop is never blocked
            orders = await asyncio.to_thread(engine.broker.get_order_report)
            if orders is None:
                # Distinguish broker API error/unauthorized/rate-limit from genuine empty []
                err_detail = "Broker returned error response or API unavailable"
                coordinator.record_poll_failure(now_wall, "BROKER_API_ERROR", err_detail, mono=now_mono)
                if coordinator.poll_consecutive_errors >= 3:
                    TelegramAlerts.rest_poller_failed(
                        coordinator.poll_consecutive_errors,
                        "BROKER_API_ERROR",
                        err_detail
                    )
                # Bounded backoff with jitter on repeated failure
                backoff = min(base_interval * (2 ** min(coordinator.poll_consecutive_errors, 4)), 30.0)
                interval = backoff + (time.time() % 1.0)
            else:
                if coordinator.poll_consecutive_errors >= 3:
                    TelegramAlerts.rest_poller_recovered()
                coordinator.record_poll_success(now_wall, mono=now_mono)
                for order_data in orders:
                    engine.handle_order_event(order_data, event_time=now_wall, source="REST")
                coordinator.record_order_book_proc(time.time(), mono=time.monotonic())

        except asyncio.CancelledError:
            logger.info("REST backup poller loop cancelled.")
            break
        except Exception as e:
            coordinator.record_poll_failure(now_wall, "UNCAUGHT_EXCEPTION", str(e), mono=now_mono)
            logger.error(f"ERROR in REST backup poller: {e}", exc_info=True)
            if coordinator.poll_consecutive_errors >= 3:
                TelegramAlerts.rest_poller_failed(
                    coordinator.poll_consecutive_errors,
                    "UNCAUGHT_EXCEPTION",
                    str(e)
                )

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break


# ─────────────────────────────────────────────────────────────
# 3. 3-Way Reconciliation Worker
# ─────────────────────────────────────────────────────────────
async def reconcile_loop():
    """
    Periodic 3-way reconciliation audit:
    Local DB  ↔  Broker Order Book  ↔  Broker Positions.
    Runs off the event loop via asyncio.to_thread every RECONCILIATION_INTERVAL_SEC seconds.
    """
    interval = float(settings.RECONCILIATION_INTERVAL_SEC)
    logger.info(f"🔍 Reconciliation audit loop started (interval: {interval:.0f}s)")
    await asyncio.sleep(interval)  # Initial delay on startup

    while coordinator.is_running:
        now_wall = time.time()
        now_mono = time.monotonic()
        coordinator.record_recon_attempt(now_wall, mono=now_mono)

        try:
            # Offload synchronous reconciliation to worker thread
            await asyncio.to_thread(engine.reconcile)
            coordinator.record_recon_success(now_wall, mono=now_mono)
        except asyncio.CancelledError:
            logger.info("Reconciliation loop cancelled.")
            break
        except Exception as e:
            coordinator.record_recon_failure(now_wall, "RECONCILE_EXCEPTION", str(e), mono=now_mono)
            logger.error(f"ERROR in reconciliation loop ({coordinator.recon_consecutive_errors} consecutive): {e}", exc_info=True)
            if coordinator.recon_consecutive_errors >= 3:
                TelegramAlerts.critical_error("Reconciliation Loop", f"Reconciliation failed {coordinator.recon_consecutive_errors} times consecutively: {e}")

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break


def is_market_hours_ist(now_ts: Optional[float] = None) -> bool:
    """Returns True if the current time is within NSE market hours (09:15-15:30 IST, Mon-Fri)."""
    tz_ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    dt = datetime.datetime.fromtimestamp(now_ts or time.time(), tz=tz_ist)
    if dt.weekday() >= 5:  # Saturday or Sunday
        return False
    market_open = dt.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = dt.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= dt <= market_close


# ─────────────────────────────────────────────────────────────
# 4. Heartbeat & Dead-Bot Detection Worker
# ─────────────────────────────────────────────────────────────
async def heartbeat_loop():
    """
    Periodically logs system health metrics.
    Monitors for dead bot, sustained WebSocket disconnects, and unprotected positions.
    Never declares WebSocket dead merely because no order event occurred.
    """
    interval = 30.0  # Run heartbeat audit every 30 seconds
    logger.info("💓 Heartbeat monitor started (interval: 30s)")

    while coordinator.is_running:
        try:
            now_wall = time.time()
            now_mono = time.monotonic()
            coordinator.supervise_tasks()

            ws_connected = order_feed.is_connected
            # Classify connection status accurately:
            if not ws_connected:
                ws_status = "RECONNECTING" if getattr(order_feed, "_reconnect_count", 0) > 0 else "DISCONNECTED"
            elif getattr(order_feed, "last_event_mono", None) and (now_mono - order_feed.last_event_mono < 300.0):
                ws_status = "CONNECTED"
            elif order_feed.last_event_ts and (now_wall - order_feed.last_event_ts < 300.0):
                ws_status = "CONNECTED"
            else:
                ws_status = "IDLE"

            # Compute relative times using monotonic clock with wall-clock fallback
            if getattr(order_feed, "last_event_mono", None):
                last_ev = f"{round(now_mono - order_feed.last_event_mono, 1)}s ago"
            elif order_feed.last_event_ts:
                last_ev = f"{round(now_wall - order_feed.last_event_ts, 1)}s ago"
            else:
                last_ev = "never"

            if getattr(coordinator, "last_recon_success_mono", None):
                last_rc = f"{round(now_mono - coordinator.last_recon_success_mono, 1)}s ago"
            elif coordinator.last_recon_success_ts:
                last_rc = f"{round(now_wall - coordinator.last_recon_success_ts, 1)}s ago"
            else:
                last_rc = "never"

            if getattr(coordinator, "last_poll_success_mono", None):
                last_pl = f"{round(now_mono - coordinator.last_poll_success_mono, 1)}s ago"
            elif coordinator.last_poll_success_ts:
                last_pl = f"{round(now_wall - coordinator.last_poll_success_ts, 1)}s ago"
            else:
                last_pl = "never"

            active_positions = engine.get_active_positions()
            active_pos_count = len(active_positions)
            unprotected_pos = [
                p for p in active_positions 
                if p.get("status") in ("UNPROTECTED", "ENTRY_DETECTED", "ERROR", "NEEDS_MANUAL_ACTION")
                and (p.get("remaining_qty") is None or p.get("remaining_qty") > 0)
            ]

            logger.info(
                f"HEARTBEAT WS={ws_status} LAST_EVENT={last_ev} "
                f"LAST_RECONCILIATION={last_rc} LAST_POLL={last_pl} "
                f"ACTIVE_POSITIONS={active_pos_count}"
            )

            # 1. WebSocket Disconnect Monitoring & Recovery
            if not ws_connected:
                if getattr(order_feed, "disconnected_at_mono", None):
                    down_sec = now_mono - order_feed.disconnected_at_mono
                elif order_feed.disconnected_at:
                    down_sec = now_wall - order_feed.disconnected_at
                else:
                    down_sec = 0.0

                coordinator._was_ws_disconnected = True
                if down_sec > 30.0 and not settings.DRY_RUN:
                    TelegramAlerts.websocket_disconnected(down_sec)
            else:
                if coordinator._was_ws_disconnected:
                    coordinator._was_ws_disconnected = False
                    TelegramAlerts.websocket_recovered()

            # 2. Critical Dual Monitoring Outage: Both WS and REST down
            rest_failing = (coordinator.poll_consecutive_errors >= 3)
            if not ws_connected and rest_failing and not settings.DRY_RUN:
                coordinator._was_outage = True
                TelegramAlerts.critical_monitoring_outage(
                    f"WebSocket disconnected and REST poller failed {coordinator.poll_consecutive_errors} times consecutively."
                )
            elif (ws_connected or not rest_failing) and coordinator._was_outage:
                coordinator._was_outage = False
                TelegramAlerts.critical_monitoring_recovered()

            # 3. Connected and Idle logging (Do NOT alert falsely!)
            if is_market_hours_ist(now_wall) and ws_connected:
                if ws_status == "IDLE":
                    logger.debug(
                        f"Order feed is IDLE during market hours. Last event {last_ev}, "
                        f"REST fallback health: {coordinator.poll_consecutive_errors} errors."
                    )

            # 4. Unprotected positions alert
            if unprotected_pos and not settings.DRY_RUN:
                # Query broker positions to avoid alerting on confirmed flat positions
                broker_positions = engine.broker.get_positions()
                broker_net_by_sym = None
                if broker_positions is not None:
                    broker_net_by_sym = engine._build_broker_net_map(broker_positions) or {}

                truly_unprotected = []
                for p in unprotected_pos:
                    sym = str(p.get("symbol", "")).strip().upper()
                    # Broker flat (net 0 or symbol absent) → close local row; do not alert
                    if engine._is_broker_flat(broker_net_by_sym, sym):
                        if p.get("status") != "CLOSED_EXTERNAL" or (p.get("remaining_qty") or 0) != 0:
                            p["status"] = "CLOSED_EXTERNAL"
                            p["remaining_qty"] = 0
                            p["protection_state"] = "CLOSED_EXTERNAL"
                            p["error_message"] = None
                            engine.store.upsert_position(p)
                            with engine._lock:
                                engine._positions[p.get("entry_order_id")] = p
                            logger.info(
                                f"WATCHDOG_FLAT_RESOLVED {p.get('entry_order_id')} ({sym}) → CLOSED_EXTERNAL"
                            )
                        continue

                    # Verify SL protection on broker: local ACTIVE record is insufficient
                    sl_id = p.get("sl_order_id") or p.get("half_b_sl_order_id")
                    sl_status = engine.broker.get_order_status(sl_id) if sl_id else None
                    if sl_status in ("OPEN", "TRIGGER_PENDING"):
                        # Active SL verified on broker
                        continue

                    truly_unprotected.append(p)

                for p in truly_unprotected:
                    pos_id = p.get("entry_order_id")
                    sym = p.get("symbol")
                    rem_qty = p.get("remaining_qty") or p.get("total_qty", 0)
                    cur_st = p.get("status")
                    dedup_key = f"WATCHDOG_UNPROTECTED_{pos_id}"
                    state_fp = f"{cur_st}_{rem_qty}"

                    # Persist UNPROTECTED status if not already
                    if cur_st != "UNPROTECTED":
                        p["status"] = "UNPROTECTED"
                        p["protection_state"] = "UNPROTECTED"
                        engine.store.upsert_position(p)
                        with engine._lock:
                            engine._positions[pos_id] = p

                    # Persist entry lock
                    engine.store.set_entry_lock(True, f"Watchdog alert: unprotected position {pos_id} ({sym})")

                    if engine.store.should_send_alert(dedup_key, cooldown_sec=1800.0, state_fingerprint=state_fp):
                        TelegramAlerts.protection_failed_idle_alert(
                            symbol=sym,
                            entry_order_id=pos_id,
                            filled_qty=rem_qty,
                            failed_order_type="NO_ACTIVE_SL_VERIFIED",
                            broker_error=p.get("error_message") or "Watchdog reports no active SL verified on broker",
                            failed_order_id=p.get("sl_order_id"),
                            position_status="UNPROTECTED",
                            protection_confirmed=False
                        )

        except asyncio.CancelledError:
            logger.info("Heartbeat loop cancelled.")
            break
        except Exception as e:
            logger.warning(f"Error in heartbeat loop: {e}", exc_info=True)

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
