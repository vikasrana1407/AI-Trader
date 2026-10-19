"""
FastAPI Route Definitions.
All scenarios are exposed as clean REST endpoints with Swagger documentation.
"""

import time
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status

from .engine import engine
from .models import SimulateEntryRequest, OrderSide, PositionStatus
from .config import settings
from .notifier import TelegramAlerts

router = APIRouter()


# ──────────────────────────────────────
# Health & Status
# ──────────────────────────────────────

@router.get(
    "/health",
    summary="Bot Health Check",
    description="Returns uptime, mode (DRY_RUN/LIVE), and active position count."
)
def health_check() -> Dict[str, Any]:
    return engine.get_health()


@router.post(
    "/telegram/test",
    summary="Send Telegram test message",
    description="Verifies TELEGRAM_ALERTS_ENABLED + bot token/chat id by sending a test ping.",
)
def telegram_test() -> Dict[str, Any]:
    ok = TelegramAlerts.send(
        "🔔 <b>Telegram test</b>\n"
        f"• Mode: {'LIVE' if not settings.DRY_RUN else 'DRY_RUN'}\n"
        "• If you see this, the alerts channel is working.",
        blocking=True,
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Telegram send failed or disabled. Check TELEGRAM_ALERTS_ENABLED, "
                "TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and container logs for "
                "'Telegram dispatch failed'."
            ),
        )
    return {"ok": True, "enabled": settings.TELEGRAM_ALERTS_ENABLED}


@router.get(
    "/status",
    summary="Detailed Bot Status",
    description="Returns health info plus configuration summary. Use from Termius: curl localhost:8000/status"
)
def bot_status() -> Dict[str, Any]:
    return {
        **engine.get_health(),
        "config": {
            "flow": "even_half_target_sl__odd_sl_100",
            "order_structure": settings.ORDER_STRUCTURE,
            "target_offset": settings.TARGET_OFFSET,
            "sl_offset": settings.SL_OFFSET,
            "sl_limit_buffer": settings.SL_LIMIT_BUFFER,
            "partial_fill_mode": settings.PARTIAL_FILL_HANDLING,
            "exchange_segment": settings.EXCHANGE_SEGMENT,
            "product_type": settings.PRODUCT_TYPE,
            "tick_size": settings.TICK_SIZE,
            "auto_entry_enabled": settings.AUTO_ENTRY_ENABLED,
            "auto_reentry_enabled": settings.AUTO_REENTRY_ENABLED,
        }
    }


# ──────────────────────────────────────
# Positions
# ──────────────────────────────────────

@router.get(
    "/positions",
    summary="All Positions",
    description="Returns all known positions (active, closed, errored)."
)
def list_positions() -> List[Dict[str, Any]]:
    return engine.get_all_positions()


@router.get(
    "/positions/active",
    summary="Active Protected Positions",
    description="Returns only positions that are in PROTECTED state (both target and SL live)."
)
def active_positions() -> List[Dict[str, Any]]:
    return engine.store.active_positions()


@router.get(
    "/positions/unprotected",
    summary="Unprotected Positions",
    description="Returns positions that lack full protection — requires manual attention."
)
def unprotected_positions() -> List[Dict[str, Any]]:
    return engine.store.unprotected_positions()


@router.get(
    "/positions/{entry_order_id}",
    summary="Get Single Position",
    description="Retrieves a specific position by entry order ID."
)
def get_position(entry_order_id: str) -> Dict[str, Any]:
    pos = engine.get_position(entry_order_id)
    if not pos:
        raise HTTPException(status_code=404, detail=f"Position '{entry_order_id}' not found.")
    return pos


# ──────────────────────────────────────
# Orders (Broker View)
# ──────────────────────────────────────

@router.get(
    "/orders",
    summary="Broker Order Book",
    description="Returns current-day order book from Kotak Neo (or mock orders in DRY_RUN)."
)
def order_book() -> List[Dict[str, Any]]:
    report = engine.broker.get_order_report()
    if report is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Broker order book API currently unavailable."
        )
    return report


@router.get(
    "/orders/{order_id}",
    summary="Order detail + history",
    description=(
        "Inspect one order via order_report + order_history: status, qty, "
        "trigger/limit price, product, and rejection reason (rejRsn)."
    ),
)
def order_detail(order_id: str) -> Dict[str, Any]:
    summary = engine.broker.summarize_order(order_id)
    history = engine.broker.get_order_history(order_id)
    details = engine.broker.get_order_details(order_id)
    if details is None and not history:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order {order_id} not found in order_report or order_history.",
        )
    return {
        "summary": summary,
        "order_report_row": details,
        "order_history": history or [],
    }


# ──────────────────────────────────────
# Reconciliation (Manual Trigger)
# ──────────────────────────────────────

@router.post(
    "/reconcile",
    summary="Trigger Reconciliation Audit",
    description="Manually triggers a 3-way reconciliation: JSON state ↔ Broker Orders ↔ Positions."
)
def trigger_reconcile() -> Dict[str, Any]:
    issues = engine.reconcile()
    return {
        "timestamp": time.time(),
        "issues_found": len(issues),
        "issues": issues
    }


# ──────────────────────────────────────
# Simulation / Testing Endpoints
# ──────────────────────────────────────

@router.post(
    "/simulate/entry",
    summary="Simulate Manual Entry Fill",
    description=(
        "Injects a fake manual limit-order execution into the bot engine. "
        "Useful for end-to-end testing the full Target/SL placement pipeline "
        "without placing real orders. Only meaningful in DRY_RUN mode."
    )
)
def simulate_entry(req: SimulateEntryRequest) -> Dict[str, Any]:
    if not settings.DRY_RUN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Simulation endpoints are only available in DRY_RUN mode."
        )

    order_id = engine.broker.simulate_entry_fill(
        symbol=req.symbol,
        side=req.side,
        qty=req.quantity,
        price=req.price
    )

    # Run through the engine immediately
    detected_ts = time.time()
    order_data = engine.broker._mock_orders.get(order_id, {})
    engine.handle_order_event(order_data, event_time=detected_ts)

    pos = engine.get_position(order_id)
    return {
        "status": "PROCESSED",
        "entry_order_id": order_id,
        "position": pos
    }


@router.post(
    "/simulate/target-hit/{entry_order_id}",
    summary="Simulate Target Execution",
    description="Marks the Target order as filled and triggers OCO cancellation of companion SL."
)
def simulate_target_hit(entry_order_id: str) -> Dict[str, Any]:
    if not settings.DRY_RUN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="DRY_RUN only.")

    pos = engine.get_position(entry_order_id)
    if not pos:
        raise HTTPException(status_code=404, detail=f"Position {entry_order_id} not found.")
    if pos.get("status") != PositionStatus.PROTECTED.value:
        raise HTTPException(status_code=400, detail=f"Position not in PROTECTED state (is {pos.get('status')}).")

    target_id = pos.get("target_order_id")
    if not target_id:
        raise HTTPException(status_code=400, detail="No target order ID on position.")

    # Mark target as filled in mock broker
    if target_id in engine.broker._mock_orders:
        engine.broker._mock_orders[target_id]["ordSt"] = "TRAD"

    engine._check_oco(target_id, "TRAD")
    return {"status": "SIMULATED", "position": engine.get_position(entry_order_id)}


@router.post(
    "/simulate/sl-hit/{entry_order_id}",
    summary="Simulate Stop-Loss Trigger",
    description="Marks the SL order as filled and triggers OCO cancellation of companion Target."
)
def simulate_sl_hit(entry_order_id: str) -> Dict[str, Any]:
    if not settings.DRY_RUN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="DRY_RUN only.")

    pos = engine.get_position(entry_order_id)
    if not pos:
        raise HTTPException(status_code=404, detail=f"Position {entry_order_id} not found.")
    if pos.get("status") != PositionStatus.PROTECTED.value:
        raise HTTPException(status_code=400, detail=f"Position not in PROTECTED state (is {pos.get('status')}).")

    sl_id = pos.get("sl_order_id")
    if not sl_id:
        raise HTTPException(status_code=400, detail="No SL order ID on position.")

    # Mark SL as filled in mock broker
    if sl_id in engine.broker._mock_orders:
        engine.broker._mock_orders[sl_id]["ordSt"] = "TRAD"

    engine._check_oco(sl_id, "TRAD")
    return {"status": "SIMULATED", "position": engine.get_position(entry_order_id)}


# ──────────────────────────────────────
# Audit Log
# ──────────────────────────────────────

@router.get(
    "/events",
    summary="Recent Audit Events",
    description="Returns the last 50 bot events from the audit log."
)
def get_events(limit: int = 50) -> List[Dict[str, Any]]:
    return engine.store.recent_events(limit=limit)


@router.post(
    "/simulate/ws-event",
    summary="Simulate WebSocket Order Event",
    description="Injects a simulated raw or normalized WebSocket event directly into the primary feed (DRY_RUN only)."
)
def simulate_ws_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not settings.DRY_RUN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="DRY_RUN only.")
    from .workers import order_feed
    order_feed.inject_event(payload)
    return {"status": "INJECTED", "payload": payload}


@router.post(
    "/admin/reset-db",
    summary="Reset Development Database",
    description="Wipes all persisted positions and events for clean development testing (DRY_RUN only)."
)
def reset_dev_db() -> Dict[str, str]:
    if not settings.DRY_RUN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="DRY_RUN only.")
    engine.store.reset_db()
    with engine._lock:
        engine._positions.clear()
        engine._fill_buffers.clear()
    return {"status": "RESET", "message": "Development database wiped successfully."}
