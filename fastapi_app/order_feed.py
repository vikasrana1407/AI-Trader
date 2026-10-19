"""
Kotak Neo Order & Position Streaming WebSocket Service (PRIMARY Execution Detector).

Uses official Kotak Neo SDK (neo_api_client.websocket.orderfeed / create_order_feed)
to receive real-time order lifecycle events.

Key capabilities:
  - Connects to Kotak Neo order feed WebSocket.
  - Normalizes raw broker events into NormalizedOrderEvent.
  - Safe parsing & validation; resilient against missing or extra fields.
  - Bounded exponential backoff on disconnect.
  - Automatic REST reconciliation after reconnection.
  - Simulated feed support for DRY_RUN mode and automated testing.
  - Structured audit logging (WEBSOCKET_CONNECTED, ORDER_EVENT_RECEIVED, etc.).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
import time
from typing import Any, Callable, Dict, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field

from .config import settings
from .models import OrderSide

logger = logging.getLogger("FastAPIBot.OrderFeed")

# Kotak Neo place_order accepts ONLY these exact codes (SDK v3+).
# Order feed / order_report often sends aliases or legacy short codes.
PLACE_ORDER_PRODUCTS = frozenset({"CNC", "NRML", "MIS", "MTF"})
# Broker may report these on manual entries; SDK place_order rejects them.
UNSUPPORTED_PLACE_PRODUCTS = frozenset({
    "BO", "CO", "BRACKET", "COVER", "BRACKET ORDER", "COVER ORDER",
})
_PRODUCT_ALIASES = {
    "CNC": "CNC",
    "C": "CNC",
    "CASH": "CNC",
    "CASH AND CARRY": "CNC",
    "MIS": "MIS",
    "M": "MIS",
    "INTRADAY": "MIS",
    "NRML": "NRML",
    "I": "NRML",
    "NORMAL": "NRML",
    "MTF": "MTF",
}
_unsupported_logged: set = set()


def normalize_product_type(
    raw: Optional[Any],
    fallback: Optional[str] = None,
) -> Optional[str]:
    """
    Map broker product aliases to Kotak Neo place_order codes.

    Returns one of CNC/NRML/MIS/MTF, or None when the product cannot be placed.

    - Empty/missing raw → configured fallback (if valid).
    - BO/CO/unknown non-empty → None (never silently rewrite to MIS here).
    """
    fb = str(fallback or "").strip().upper() or None
    if fb and fb not in PLACE_ORDER_PRODUCTS:
        fb = None

    if raw is None:
        return fb

    key = str(raw).strip().upper()
    if not key:
        return fb
    if key in PLACE_ORDER_PRODUCTS:
        return key

    mapped = _PRODUCT_ALIASES.get(key)
    if mapped:
        if key != mapped:
            logger.info(f"PRODUCT_NORMALIZED '{raw}' -> '{mapped}'")
        return mapped

    if key in UNSUPPORTED_PLACE_PRODUCTS:
        if key not in _unsupported_logged:
            _unsupported_logged.add(key)
            logger.warning(
                f"PRODUCT_UNSUPPORTED '{raw}' cannot be used with place_order "
                f"(SDK v3 allows only {sorted(PLACE_ORDER_PRODUCTS)}). "
                f"Further repeats for '{key}' suppressed at this log level."
            )
        return None

    if key not in _unsupported_logged:
        _unsupported_logged.add(key)
        logger.error(
            f"PRODUCT_UNKNOWN '{raw}' not in {sorted(PLACE_ORDER_PRODUCTS)}; "
            f"refusing silent fallback (would have been {fb})."
        )
    return None


def resolve_protective_product(
    raw: Optional[Any],
    fallback: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve product for protective SL/target placement.

    Returns (product_code, error_message).
    BO/CO: if settings.BO_PROTECT_AS_PRODUCT is CNC/NRML/MIS/MTF, use that
    explicitly for exits (not a silent alias — caller must Telegram).
    Otherwise error_message is set and placement must not proceed.
    """
    key = str(raw or "").strip().upper()
    if key in UNSUPPORTED_PLACE_PRODUCTS:
        override = str(getattr(settings, "BO_PROTECT_AS_PRODUCT", "") or "").strip().upper()
        if override in PLACE_ORDER_PRODUCTS:
            logger.warning(
                f"BO_PROTECT_AS Entry product '{key}' → placing protective exits as '{override}' "
                f"(BO_PROTECT_AS_PRODUCT). SDK cannot place_order with BO/CO."
            )
            return override, None
        err = (
            f"Entry product '{key}' is Bracket/Cover Order. "
            f"Kotak Neo SDK place_order does not accept BO/CO — only "
            f"{sorted(PLACE_ORDER_PRODUCTS)}. Set BO_PROTECT_AS_PRODUCT=MIS|NRML "
            f"to place exits, or enter as MIS/NRML. Bot will not silently convert."
        )
        return None, err

    prod = normalize_product_type(raw, fallback=fallback)
    if not prod:
        err = (
            f"Cannot resolve placeable product from entry product '{raw or ''}' "
            f"(fallback '{fallback or ''}'). Allowed: {sorted(PLACE_ORDER_PRODUCTS)}."
        )
        return None, err
    return prod, None


# ─────────────────────────────────────────────────────────────
# Normalized Internal Model
# ─────────────────────────────────────────────────────────────

class NormalizedOrderEvent(BaseModel):
    """Internal standardized representation of an order update."""
    model_config = ConfigDict(extra="ignore")

    order_id: str
    status: str                         # Uppercase standardized status
    raw_status: str                     # Original raw status from broker
    symbol: str
    side: Optional[OrderSide] = None    # BUY or SELL
    total_qty: int = 0
    filled_qty: int = 0                 # Cumulative filled quantity
    unfilled_qty: int = 0
    average_price: float = 0.0
    price: float = 0.0
    order_type: str = "L"               # L, MKT, SL, etc.
    product_type: Optional[str] = None  # CNC/NRML/MIS/MTF for place_order, or None if unsupported
    raw_product: Optional[str] = None   # Broker wire value (may be BO/CO)
    exchange_order_id: Optional[str] = None
    tag: str = ""
    source: str = "WS"                  # WS or REST
    event_timestamp: float = Field(default_factory=time.time)
    raw_event: Dict[str, Any] = Field(default_factory=dict)


def normalize_order_event(raw: Any, source: str = "WS") -> Optional[NormalizedOrderEvent]:
    """
    Safely parses and normalizes an event from Kotak Neo WebSocket or REST API.
    Handles:
      - neo_api_client.websocket.orderfeed.OrderUpdate instances
      - Raw dicts with wire aliases (nOrdNo, ordSt, avgPrc, fldQty, etc.)
      - Raw dicts with full names (order_id, order_status, average_price, etc.)
      - JSON strings
    Returns NormalizedOrderEvent or None if unparseable/missing required order ID.
    """
    if raw is None:
        return None

    # Handle string JSON
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            logger.warning(f"ORDER_EVENT_IGNORED Malformed JSON string: {raw[:100]}")
            return None

    # Handle Pydantic OrderUpdate from SDK
    payload: Dict[str, Any] = {}
    if hasattr(raw, "data"):
        data_obj = getattr(raw, "data")
        if hasattr(data_obj, "model_dump"):
            payload = data_obj.model_dump(by_alias=True)
        elif hasattr(data_obj, "__dict__"):
            payload = dict(data_obj.__dict__)
        elif isinstance(data_obj, dict):
            payload = data_obj
    elif isinstance(raw, dict):
        # Could be {"type": "order", "data": {...}} or flat dict
        if raw.get("type") == "order" and isinstance(raw.get("data"), dict):
            payload = raw["data"]
        else:
            payload = raw
    else:
        logger.warning(f"ORDER_EVENT_IGNORED Unsupported event type: {type(raw).__name__}")
        return None

    # Extract order ID
    order_id = str(
        payload.get("nOrdNo") or payload.get("order_id") or ""
    ).strip()
    if not order_id:
        logger.info("ORDER_EVENT_IGNORED Missing order_id (nOrdNo)")
        return None

    # Status normalization
    raw_st = str(payload.get("ordSt") or payload.get("order_status") or "").strip()
    st_upper = raw_st.upper()
    if st_upper in ("COMPLETE", "TRAD", "FILLED", "EXECUTED"):
        status = "COMPLETE"
    elif st_upper in ("PARTIALLY_FILLED", "PART_FILLED", "PART FILLED"):
        status = "PARTIAL_FILL"
    elif st_upper in ("CANCELLED", "CAN"):
        status = "CANCELLED"
    elif st_upper in ("REJECTED", "REJ"):
        status = "REJECTED"
    elif st_upper in ("OPEN", "TRIGGER_PENDING"):
        status = "OPEN"
    else:
        status = st_upper or "UNKNOWN"

    # Transaction type / side
    trns = str(payload.get("trnsTp") or payload.get("transaction_type") or "").upper().strip()
    side: Optional[OrderSide] = None
    if trns in ("B", "BUY"):
        side = OrderSide.BUY
    elif trns in ("S", "SELL"):
        side = OrderSide.SELL

    # Quantities
    try:
        qty = int(payload.get("qty") or payload.get("quantity") or 0)
    except (ValueError, TypeError):
        qty = 0

    try:
        fld_qty = int(payload.get("fldQty") or payload.get("filled_quantity") or 0)
    except (ValueError, TypeError):
        fld_qty = 0

    try:
        unfld_sz = int(payload.get("unFldSz") or payload.get("unfilled_size") or 0)
    except (ValueError, TypeError):
        unfld_sz = 0

    # If fld_qty was not reported but status is complete, assume entire qty filled
    if fld_qty == 0 and status == "COMPLETE" and qty > 0:
        fld_qty = qty
    elif fld_qty > 0 and unfld_sz > 0 and status not in ("CANCELLED", "REJECTED"):
        status = "PARTIAL_FILL"
    elif unfld_sz == 0 and fld_qty >= qty and qty > 0 and status not in ("CANCELLED", "REJECTED"):
        status = "COMPLETE"

    # Prices
    try:
        avg_prc = float(payload.get("avgPrc") or payload.get("average_price") or 0.0)
    except (ValueError, TypeError):
        avg_prc = 0.0

    try:
        prc = float(payload.get("prc") or payload.get("price") or 0.0)
    except (ValueError, TypeError):
        prc = 0.0

    # Fallback: if average price is zero, use order price
    if avg_prc <= 0.0 and prc > 0.0:
        avg_prc = prc

    # Symbol
    symbol = str(
        payload.get("trdSym") or payload.get("trading_symbol") or
        payload.get("sym") or payload.get("symbol") or ""
    ).strip()

    # Order type (L, MKT, SL, etc.)
    ord_type = str(
        payload.get("ordTyp") or payload.get("prcTp") or
        payload.get("price_type") or "L"
    ).upper().strip()
    if ord_type in ("LIMIT", "LMT"):
        ord_type = "L"

    # Exchange order ID & tag
    ex_ord_id = payload.get("exOrdId") or payload.get("exchange_order_id")
    if ex_ord_id:
        ex_ord_id = str(ex_ord_id).strip()

    tag = str(
        payload.get("tag") or payload.get("ordTag") or payload.get("order_tag") or
        payload.get("GuiOrdId") or payload.get("ordModNo") or ""
    ).strip()

    # Product type — must be CNC/NRML/MIS/MTF for place_order (SDK rejects BO/CO/aliases)
    raw_prd = payload.get("prd") or payload.get("prod") or payload.get("product")
    raw_product = str(raw_prd).strip().upper() if raw_prd is not None and str(raw_prd).strip() else None
    product_type = normalize_product_type(raw_prd, fallback=settings.PRODUCT_TYPE)

    event = NormalizedOrderEvent(
        order_id=order_id,
        status=status,
        raw_status=raw_st,
        symbol=symbol,
        side=side,
        total_qty=qty,
        filled_qty=fld_qty,
        unfilled_qty=unfld_sz,
        average_price=avg_prc,
        price=prc,
        order_type=ord_type,
        product_type=product_type,
        raw_product=raw_product,
        exchange_order_id=ex_ord_id,
        tag=tag,
        source=source,
        event_timestamp=time.time(),
        raw_event=payload if isinstance(payload, dict) else {"raw": str(payload)}
    )

    logger.info(
        f"ORDER_EVENT_RECEIVED [{source}] Order: {event.order_id} | Status: {event.status} | "
        f"Symbol: {event.symbol} | Filled: {event.filled_qty}/{event.total_qty} @ ₹{event.average_price:.2f} | "
        f"Product: raw={event.raw_product or '-'} placeable={event.product_type or 'NONE'}"
    )
    return event


# ─────────────────────────────────────────────────────────────
# Kotak Order Feed WebSocket Service
# ─────────────────────────────────────────────────────────────

class KotakOrderFeed:
    """
    Manages the Kotak Neo Order & Position Streaming WebSocket connection.
    Acts as the PRIMARY real-time order fill detection engine.
    """

    def __init__(
        self,
        engine,
        reconcile_callback: Optional[Callable[[], Any]] = None,
        reconnect_delay: float = 2.0,
        max_reconnect_attempts: int = 10
    ):
        self.engine = engine
        self.reconcile_callback = reconcile_callback
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts

        self.is_connected = False
        self.connected_at: Optional[float] = None
        self.connected_at_mono: Optional[float] = None
        self.disconnected_at: Optional[float] = None
        self.disconnected_at_mono: Optional[float] = None
        self.last_event_ts: Optional[float] = None
        self.last_event_mono: Optional[float] = None
        self.last_msg_ts: Optional[float] = None
        self.last_msg_mono: Optional[float] = None
        self.last_frame_ts: Optional[float] = None
        self.last_frame_mono: Optional[float] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._reconnect_count = 0
        self._first_raw_msg_logged = False

        # For DRY_RUN / simulation & testing
        self._sim_queue: Optional[asyncio.Queue] = None

        # Reference to SDK WebSocket client if active
        self._sdk_feed = None

    @property
    def sim_queue(self) -> asyncio.Queue:
        if self._sim_queue is None:
            self._sim_queue = asyncio.Queue()
        return self._sim_queue

    async def start(self):
        """Starts the order feed background worker task."""
        async with self._lock:
            if self._running and self._task and not self._task.done():
                logger.debug("Order feed already running.")
                return
            self._sim_queue = asyncio.Queue()
            self._running = True
            self._task = asyncio.create_task(self._run_loop(), name="KotakOrderFeedLoop")
            logger.info("KotakOrderFeed service started.")

    async def stop(self):
        """Stops the order feed background worker cleanly."""
        async with self._lock:
            self._running = False
            self.is_connected = False
            self.connected_at = None

            if self._sdk_feed and hasattr(self._sdk_feed, "close"):
                try:
                    await self._sdk_feed.close()
                except Exception as e:
                    logger.debug(f"Error closing SDK feed: {e}")
                self._sdk_feed = None

            if self._task:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
                self._task = None

            logger.info("WEBSOCKET_SHUTDOWN KotakOrderFeed shut down cleanly.")

    def inject_event(self, raw_data: Any):
        """
        Injects an event into the order feed (synchronous or async-safe).
        Used by DRY_RUN mode, API simulate endpoints, and unit tests.
        """
        self.sim_queue.put_nowait(raw_data)

    def simulate_disconnect(self):
        """Simulate a WebSocket disconnection for testing and dry-run validation."""
        now_ts = time.time()
        now_mono = time.monotonic()
        self.is_connected = False
        self.connected_at = None
        self.connected_at_mono = None
        self.disconnected_at = now_ts
        self.disconnected_at_mono = now_mono
        logger.warning("WEBSOCKET_DISCONNECTED Order feed disconnected (simulated).")
        logger.info("WEBSOCKET_RECONNECTING Attempting reconnect...")

    def simulate_reconnect(self):
        """Simulate a successful WebSocket reconnection followed by REST reconciliation."""
        now_ts = time.time()
        now_mono = time.monotonic()
        self.is_connected = True
        self.connected_at = now_ts
        self.connected_at_mono = now_mono
        self.disconnected_at = None
        self.disconnected_at_mono = None
        logger.info("WEBSOCKET_CONNECTED Connected to Kotak Neo Order Feed WebSocket (simulated).")
        if self.reconcile_callback:
            logger.info("RECONCILIATION_STARTED Running reconciliation post-reconnect...")
            try:
                self.reconcile_callback()
                logger.info("RECONCILIATION_COMPLETED Post-reconnect reconciliation complete.")
            except Exception as err:
                logger.error(f"ERROR in post-reconnect reconciliation: {err}")

    async def _run_loop(self):
        """Main connection and message consumption loop with exponential backoff."""
        while self._running:
            if settings.DRY_RUN:
                # In DRY_RUN, consume simulated events from queue
                now_ts = time.time()
                now_mono = time.monotonic()
                self.is_connected = True
                self.connected_at = now_ts
                self.connected_at_mono = now_mono
                self.disconnected_at = None
                self.disconnected_at_mono = None
                logger.info("WEBSOCKET_CONNECTED [DRY_RUN] Simulated order feed active.")
                try:
                    while self._running:
                        raw_msg = await self.sim_queue.get()
                        self._handle_raw_message(raw_msg)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"ERROR in simulated feed loop: {e}", exc_info=True)
                    await asyncio.sleep(1.0)
                continue

            # LIVE Mode: connect using official SDK create_order_feed()
            try:
                sdk = getattr(self.engine.broker, "sdk", None)
                if not sdk:
                    raise RuntimeError("NeoAPI SDK not initialized on broker")

                # BUG 5: Assign client callbacks BEFORE calling subscribe_to_orderfeed / create_order_feed
                def _on_ws_msg(msg):
                    self._handle_raw_message(msg)

                def _on_ws_err(err):
                    logger.error(f"WEBSOCKET_ERROR {err}")

                def _on_ws_close(msg=None):
                    logger.warning(f"WEBSOCKET_CLOSED {msg}")

                def _on_ws_open(msg=None):
                    logger.info(f"WEBSOCKET_OPENED {msg}")

                if hasattr(sdk, "on_message"):
                    sdk.on_message = _on_ws_msg
                if hasattr(sdk, "on_error"):
                    sdk.on_error = _on_ws_err
                if hasattr(sdk, "on_close"):
                    sdk.on_close = _on_ws_close
                if hasattr(sdk, "on_open"):
                    sdk.on_open = _on_ws_open

                # Ensure broker session is fully authenticated before subscribing
                if hasattr(self.engine.broker, "is_logged_in") and not self.engine.broker.is_logged_in:
                    logger.info("Ensuring session is logged in (totp_login then totp_validate) before subscribing...")
                    self.engine.broker.login()

                # Call subscribe_to_orderfeed if present (legacy or wrapper)
                if hasattr(sdk, "subscribe_to_orderfeed"):
                    try:
                        sdk.subscribe_to_orderfeed()
                        logger.info("subscribe_to_orderfeed() called.")
                    except (NotImplementedError, Exception) as sub_err:
                        logger.debug(f"subscribe_to_orderfeed non-fatal: {sub_err}")

                if hasattr(sdk, "create_order_feed"):
                    logger.info("Connecting to Kotak Neo Order Feed WebSocket via create_order_feed()...")
                    self._sdk_feed = sdk.create_order_feed(
                        reconnect_delay=self.reconnect_delay,
                        max_reconnect_attempts=self.max_reconnect_attempts
                    )

                    def _on_feed_connect():
                        now_c_ts = time.time()
                        now_c_mono = time.monotonic()
                        self.is_connected = True
                        self.connected_at = now_c_ts
                        self.connected_at_mono = now_c_mono
                        self.disconnected_at = None
                        self.disconnected_at_mono = None
                        logger.info("WEBSOCKET_CONNECTED OrderFeedWebSocket transport connected.")

                    def _on_feed_disconnect():
                        now_d_ts = time.time()
                        now_d_mono = time.monotonic()
                        self.is_connected = False
                        self.connected_at = None
                        self.connected_at_mono = None
                        self.disconnected_at = now_d_ts
                        self.disconnected_at_mono = now_d_mono
                        logger.warning("WEBSOCKET_DISCONNECTED OrderFeedWebSocket transport disconnected.")

                    def _on_feed_raw(raw):
                        now_r_ts = time.time()
                        now_r_mono = time.monotonic()
                        self.last_frame_ts = now_r_ts
                        self.last_frame_mono = now_r_mono
                        self.last_msg_ts = now_r_ts
                        self.last_msg_mono = now_r_mono

                    if hasattr(self._sdk_feed, "on_connect"):
                        self._sdk_feed.on_connect = _on_feed_connect
                    if hasattr(self._sdk_feed, "on_disconnect"):
                        self._sdk_feed.on_disconnect = _on_feed_disconnect
                    if hasattr(self._sdk_feed, "on_raw"):
                        self._sdk_feed.on_raw = _on_feed_raw

                    if hasattr(self._sdk_feed, "connect"):
                        await self._sdk_feed.connect()

                now_ts = time.time()
                now_mono = time.monotonic()
                self.is_connected = True
                self.connected_at = now_ts
                self.connected_at_mono = now_mono
                self.disconnected_at = None
                self.disconnected_at_mono = None
                self._reconnect_count = 0
                logger.info("WEBSOCKET_CONNECTED Connected to Kotak Neo Order Feed WebSocket.")

                # If we reconnected after a drop, run REST reconciliation
                if self.reconcile_callback:
                    logger.info("RECONCILIATION_STARTED Running reconciliation post-connect...")
                    try:
                        self.reconcile_callback()
                        logger.info("RECONCILIATION_COMPLETED Post-connect reconciliation complete.")
                    except Exception as err:
                        logger.error(f"ERROR in post-connect reconciliation: {err}")

                # Consume messages from SDK feed
                if self._sdk_feed:
                    async for message in self._sdk_feed:
                        if not self._running:
                            break
                        self._handle_raw_message(message)
                else:
                    # If SDK uses callback only, keep worker alive while connected
                    while self._running and self.is_connected:
                        await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                now_err_ts = time.time()
                now_err_mono = time.monotonic()
                self.is_connected = False
                self.connected_at = None
                self.connected_at_mono = None
                self.disconnected_at = now_err_ts
                self.disconnected_at_mono = now_err_mono
                logger.warning(
                    f"WEBSOCKET_DISCONNECTED Order feed disconnected: {e}. "
                    f"Attempting reconnect ({self._reconnect_count + 1}/{self.max_reconnect_attempts})..."
                )

                if not self._running:
                    break

                self._reconnect_count += 1
                if self._reconnect_count > self.max_reconnect_attempts:
                    logger.error(
                        f"ERROR WEBSOCKET_RECONNECT_EXHAUSTED Reconnect attempts exhausted ({self.max_reconnect_attempts})."
                    )
                    # Pause before resetting backoff
                    await asyncio.sleep(30.0)
                    self._reconnect_count = 0
                    continue

                # Bounded exponential backoff with jitter
                delay = min(self.reconnect_delay * (2 ** (self._reconnect_count - 1)), 30.0)
                jitter = random.uniform(0.1, 0.5)
                logger.info(f"WEBSOCKET_RECONNECTING Backing off for {delay + jitter:.2f}s...")
                await asyncio.sleep(delay + jitter)

    def _handle_raw_message(self, raw_msg: Any):
        """Processes an incoming raw WebSocket message and hands off to the engine."""
        try:
            now_m_ts = time.time()
            now_m_mono = time.monotonic()
            self.last_frame_ts = now_m_ts
            self.last_frame_mono = now_m_mono
            self.last_msg_ts = now_m_ts
            self.last_msg_mono = now_m_mono
            if not self._first_raw_msg_logged:
                self._first_raw_msg_logged = True
                logger.info(f"FIRST_RAW_ORDER_FEED_MESSAGE Received first raw order-feed message: {raw_msg}")

            event = normalize_order_event(raw_msg, source="WS")
            if not event:
                return

            self.last_event_ts = now_m_ts
            self.last_event_mono = now_m_mono
            # Hand off to the engine
            self.engine.process_normalized_order_event(event)

        except Exception as e:
            logger.error(f"ERROR processing order feed message: {e}", exc_info=True)
