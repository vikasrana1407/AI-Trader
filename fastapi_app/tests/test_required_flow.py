"""
Unit and regression tests for mandatory Kotak Neo trading bot required flow (100% SL-only):

1. Detect manually placed entry order.
2. Manage only that existing position.
3. SL-Limit order placed on 100% of confirmed filled qty. No Target orders.
4. Directional SL price for LONG and SHORT.
5. Any SL placement failure → UNPROTECTED, persistent entry lock, single alert, idle state.
   NO auto-flatten. NO auto-retry.
6. Qty not a whole-lot multiple → NEEDS_MANUAL_ACTION alert.
7. SL CANCELLED by client → MANUALLY_MODIFIED terminal state (never recreated).
8. SL EXECUTED → position SL_EXECUTED + remaining_qty = 0.
9. Deduplication of alerts and flat position reconciliation.
"""

import time
import pytest
from fastapi_app.models import OrderSide, PositionStatus, OrderIntent
from fastapi_app.engine import TradingEngine, calculate_exit_plan
from fastapi_app.broker_client import BrokerClient
from fastapi_app.persistence import JSONStore
from fastapi_app.config import settings
from fastapi_app.order_feed import (
    PLACE_ORDER_PRODUCTS,
    UNSUPPORTED_PLACE_PRODUCTS,
    normalize_product_type,
    resolve_protective_product,
)


@pytest.fixture
def clean_engine(tmp_path):
    db_file = str(tmp_path / "test_flow.db")
    eng = TradingEngine(persistence_file=db_file)
    broker = BrokerClient(dry_run=True)
    broker.store = eng.store
    eng.broker = broker
    return eng, broker, eng.store


@pytest.fixture
def dry_run_engine(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DRY_RUN", True)
    monkeypatch.setattr(settings, "AUTO_ENTRY_ENABLED", False)
    monkeypatch.setattr(settings, "AUTO_REENTRY_ENABLED", False)
    db_file = str(tmp_path / "replay.db")
    eng = TradingEngine(persistence_file=db_file)
    broker = BrokerClient(dry_run=True)
    broker.store = eng.store
    eng.broker = broker
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)
    return eng, broker, eng.store


def _order(nOrdNo, sym, status, fldQty=0, qty=130, side="B", avg="100.00", tag=""):
    return {
        "nOrdNo": nOrdNo,
        "trdSym": sym,
        "trnsTp": side,
        "prcTp": "L",
        "qty": qty,
        "fldQty": fldQty,
        "avgPrc": avg,
        "ordSt": status,
        "tag": tag,
        "prod": "MIS",
    }


def _entry_event(order_id, sym, side, qty, price, product="MIS"):
    return {
        "type": "order",
        "data": {
            "nOrdNo": order_id,
            "sym": sym,
            "trnsTp": side,
            "prcTp": "L",
            "qty": qty,
            "fldQty": qty,
            "avgPrc": str(price),
            "ordSt": "complete",
            "prod": product,
        }
    }


def test_long_position_100_percent_sl_only(clean_engine):
    """
    LONG: BUY 130 @ Rs.100 (lot 65, 2 lots).
    Single SL for 130 SELL trigger=98 limit=90.
    No Target order placed.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    eng.handle_order_event(_entry_event("ENTRY_LONG_001", "NIFTY26O1322500CE", "B", 130, 100.0))
    pos = eng.get_position("ENTRY_LONG_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    assert pos["total_qty"] == 130
    assert pos["remaining_qty"] == 130
    assert pos["order_structure"] == "SL_ONLY"

    # No Target order placed
    assert pos["half_a_target_order_id"] in (None, "SKIPPED_SL_ONLY", "SKIPPED_QTY_0")
    assert pos["target_qty"] == 0

    # One SL for full 130 qty
    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]
    assert sl_id and not str(sl_id).startswith("SKIPPED_")
    sl_order = broker._mock_orders[sl_id]
    assert sl_order["trnsTp"] == "S"
    assert sl_order["qty"] == 130
    assert float(sl_order["trgPrc"]) == 98.0   # entry - SL_OFFSET (2.0)
    assert float(sl_order["prc"]) == 90.0       # entry - SL_OFFSET - SL_LIMIT_BUFFER (10.0)

    # No Target orders in mock order book
    target_orders = [o for o in broker._mock_orders.values() if o.get("ordTyp") == "L"]
    assert len(target_orders) == 0


def test_short_position_100_percent_sl_only(clean_engine):
    """
    SHORT: SELL 130 @ Rs.100 (lot 65, 2 lots).
    Single SL for 130 BUY trigger=102 limit=110.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500PE", 65)

    eng.handle_order_event(_entry_event("ENTRY_SHORT_001", "NIFTY26O1322500PE", "S", 130, 100.0))
    pos = eng.get_position("ENTRY_SHORT_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    assert pos["order_structure"] == "SL_ONLY"
    assert pos["target_qty"] == 0

    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]
    assert sl_id and not str(sl_id).startswith("SKIPPED_")
    sl_order = broker._mock_orders[sl_id]
    assert sl_order["trnsTp"] == "B"
    assert sl_order["qty"] == 130
    assert float(sl_order["trgPrc"]) == 102.0
    assert float(sl_order["prc"]) == 110.0


def test_single_lot_places_100_percent_sl_only(clean_engine):
    """
    Single lot (65 qty): same as even lots - SL on 100% qty, no target.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    eng.handle_order_event(_entry_event("ENTRY_SINGLE_LOT_001", "NIFTY26O1322500CE", "B", 65, 50.0))
    pos = eng.get_position("ENTRY_SINGLE_LOT_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    assert pos["order_structure"] == "SL_ONLY"
    assert pos["half_a_target_order_id"] in (None, "SKIPPED_SL_ONLY", "SKIPPED_QTY_0")

    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]
    assert sl_id and not str(sl_id).startswith("SKIPPED_")
    sl_order = broker._mock_orders[sl_id]
    assert sl_order["trnsTp"] == "S"
    assert sl_order["qty"] == 65
    assert float(sl_order["trgPrc"]) == 48.0   # 50 - 2
    assert float(sl_order["prc"]) == 40.0       # 50 - 2 - 10

    # No limit orders in mock book (no target)
    assert len([o for o in broker._mock_orders.values() if o.get("ordTyp") == "L"]) == 0


def test_manual_sell_exit_does_not_place_buy_sl(clean_engine):
    """
    After a LONG is protected, a manual SELL reduce/close must NOT be treated as a
    new short entry (no BUY SL). Bot must not buy/sell on the exit leg.
    """
    eng, broker, store = clean_engine
    sym = "NIFTY26O1923200CE"
    broker.set_mock_lot_size(sym, 65)

    # Open long 130
    eng.handle_order_event(_entry_event("ENTRY_LONG_EXIT_001", sym, "B", 130, 60.0))
    pos = eng.get_position("ENTRY_LONG_EXIT_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    long_sl = pos["half_b_sl_order_id"] or pos["sl_order_id"]
    assert long_sl
    orders_after_entry = len(broker._mock_orders)

    # Broker still long 65 after partial sell (exit)
    broker.set_mock_positions([{
        "trdSym": sym, "flBuyQty": "130", "flSellQty": "65", "prod": "MIS",
    }])

    # Manual SELL 65 to reduce — must be ignored as entry
    eng.handle_order_event(_entry_event("MANUAL_EXIT_SELL_001", sym, "S", 65, 62.0))
    false_short = eng.get_position("MANUAL_EXIT_SELL_001")
    assert false_short is None or false_short.get("status") in (
        PositionStatus.CLOSED_EXTERNAL.value,
        None,
    )
    # No new BUY SL for the exit order
    buy_sl_for_exit = [
        o for o in broker._mock_orders.values()
        if o.get("trnsTp") == "B" and "MANUAL_EXIT_SELL_001" in str(o.get("tag", ""))
    ]
    assert len(buy_sl_for_exit) == 0
    # Should not have grown order book with a new protective BUY
    new_buys = [
        oid for oid, o in broker._mock_orders.items()
        if o.get("trnsTp") == "B" and oid != long_sl
    ]
    assert len(new_buys) == 0, f"Unexpected BUY orders after manual exit: {new_buys}"
    assert len(broker._mock_orders) <= orders_after_entry + 1  # cancel may keep record


def test_order_failure_persists_unprotected_and_locks_entry(clean_engine, monkeypatch):
    """
    If SL fails:
    - Persist UNPROTECTED status.
    - Set persistent entry lock.
    - Send Telegram alerts.
    - Do NOT square off / flatten.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    alerts_sent = []
    monkeypatch.setattr(
        "fastapi_app.notifier.TelegramAlerts.protection_failed_idle_alert",
        lambda **kwargs: alerts_sent.append(kwargs)
    )

    def rejecting_place_order(**kwargs):
        if kwargs.get("ord_type") == "SL":
            raise RuntimeError("RMS: Margin Exceeds on SL placement")
        return broker.simulate_entry_fill(
            symbol=kwargs["symbol"], side=kwargs["side"],
            qty=kwargs["qty"], price=kwargs["price"]
        )

    broker.place_order = rejecting_place_order

    eng.handle_order_event(_entry_event("ENTRY_FAIL_001", "NIFTY26O1322500CE", "B", 130, 50.0))
    pos = eng.get_position("ENTRY_FAIL_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.UNPROTECTED.value
    assert pos["protection_state"] == "UNPROTECTED"
    assert store.is_entry_locked()[0] is True
    assert len(alerts_sent) >= 1

    flatten_orders = [o for o in broker._mock_orders.values() if "FLAT" in str(o.get("tag", "")) or "EMG" in str(o.get("tag", ""))]
    assert len(flatten_orders) == 0


def test_sl_cancelled_by_client_marks_manually_modified(clean_engine):
    """
    When bot SL is CANCELLED on broker:
    - Position marked MANUALLY_MODIFIED (terminal state).
    - Bot does NOT recreate SL.
    - Duplicate events are idempotent.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    eng.handle_order_event(_entry_event("ENTRY_MANUAL_001", "NIFTY26O1322500CE", "B", 130, 100.0))
    pos = eng.get_position("ENTRY_MANUAL_001")
    assert pos["status"] == PositionStatus.PROTECTED.value

    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]
    assert sl_id

    # Simulate SL cancellation event (client cancelled the SL in Kotak Neo)
    cancel_event = {
        "type": "order",
        "data": {
            "nOrdNo": sl_id,
            "sym": "NIFTY26O1322500CE",
            "ordSt": "CANCELLED",
            "trnsTp": "S",
            "qty": 130,
            "fldQty": 0,
            "avgPrc": "0.00",
        }
    }
    eng.handle_order_event(cancel_event)

    updated_pos = eng.get_position("ENTRY_MANUAL_001")
    assert updated_pos["status"] == PositionStatus.MANUALLY_MODIFIED.value
    assert updated_pos["protection_state"] == "MANUALLY_MODIFIED"

    # Duplicate cancel event must be idempotent (stays MANUALLY_MODIFIED)
    eng.handle_order_event(cancel_event)
    recheck_pos = eng.get_position("ENTRY_MANUAL_001")
    assert recheck_pos["status"] == PositionStatus.MANUALLY_MODIFIED.value


def test_sl_executed_marks_position_closed(clean_engine):
    """
    When SL order fills:
    - Position status = SL_EXECUTED.
    - remaining_qty = 0.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    eng.handle_order_event(_entry_event("ENTRY_SL_HIT_001", "NIFTY26O1322500CE", "B", 130, 100.0))
    pos = eng.get_position("ENTRY_SL_HIT_001")
    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]

    sl_fill_event = {
        "type": "order",
        "data": {
            "nOrdNo": sl_id,
            "sym": "NIFTY26O1322500CE",
            "trnsTp": "S",
            "prcTp": "SL",
            "qty": 130,
            "fldQty": 130,
            "avgPrc": "97.50",
            "ordSt": "complete",
        }
    }
    eng.handle_order_event(sl_fill_event)

    updated_pos = eng.get_position("ENTRY_SL_HIT_001")
    assert updated_pos["status"] == PositionStatus.SL_EXECUTED.value
    assert updated_pos["remaining_qty"] == 0


def test_sl_rejected_marks_unprotected_and_locks_entry(clean_engine):
    """
    When bot SL is REJECTED by broker (broker/margin rejection):
    - Position marked UNPROTECTED (not MANUALLY_MODIFIED).
    - Entry lock is set.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    eng.handle_order_event(_entry_event("ENTRY_SL_REJ_001", "NIFTY26O1322500CE", "B", 130, 100.0))
    pos = eng.get_position("ENTRY_SL_REJ_001")
    sl_id = pos["half_b_sl_order_id"] or pos["sl_order_id"]

    rej_event = {
        "type": "order",
        "data": {
            "nOrdNo": sl_id,
            "sym": "NIFTY26O1322500CE",
            "ordSt": "REJECTED",
            "rejRsn": "RMS: Margin Exceeds",
            "trnsTp": "S",
            "qty": 130,
            "fldQty": 0,
            "avgPrc": "0.00",
        }
    }
    eng.handle_order_event(rej_event)

    updated_pos = eng.get_position("ENTRY_SL_REJ_001")
    assert updated_pos["status"] == PositionStatus.UNPROTECTED.value
    assert store.is_entry_locked()[0] is True


def test_broker_confirmed_flat_reconciliation_cleans_orphans(clean_engine):
    """
    When broker reports netQty == 0:
    - Reconcile local state to CLOSED_EXTERNAL with remaining_qty = 0.
    - Clean up leftover active bot SL orders.
    - Do NOT reopen the position.
    """
    eng, broker, store = clean_engine
    sym = "NIFTY26O1322500PE"

    store.upsert_position({
        "entry_order_id": "261013000451685",
        "symbol": sym,
        "side": "BUY",
        "total_qty": 130,
        "remaining_qty": 130,
        "status": "NEEDS_MANUAL_ACTION",
        "sl_order_id": "ORPHAN_SL_01",
        "target_order_id": None,
    })
    eng._positions["261013000451685"] = store.get_position("261013000451685")

    broker.set_mock_positions([{
        "trdSym": sym,
        "netQty": 0.0,
        "flBuyQty": "520",
        "flSellQty": "520"
    }])
    broker.set_mock_orders([
        {"nOrdNo": "ORPHAN_SL_01", "trdSym": sym, "ordSt": "OPEN", "tag": "BOT_SL_001"},
    ])

    issues = eng.reconcile()
    assert len(issues) > 0

    assert broker._mock_orders["ORPHAN_SL_01"]["ordSt"].upper() == "CANCELLED"

    pos = eng.get_position("261013000451685")
    assert pos["remaining_qty"] == 0
    assert pos["status"] == PositionStatus.CLOSED_EXTERNAL.value
    assert len(store.active_positions()) == 0


def test_already_closed_position_with_broker_flat_not_flagged(clean_engine):
    """
    Requirement: When a position is already closed (remaining_qty=0) and broker netQty is 0,
    reconciliation must NOT flag it for manual action or generate false mismatch issues.
    """
    eng, broker, store = clean_engine
    sym = "NIFTY26O1322500PE"
    store.upsert_position({
        "entry_order_id": "CLOSED_POS_001",
        "symbol": sym,
        "side": "BUY",
        "total_qty": 130,
        "remaining_qty": 0,
        "status": PositionStatus.CLOSED_EXTERNAL.value,
        "sl_order_id": None,
        "target_order_id": None
    })
    eng._positions["CLOSED_POS_001"] = store.get_position("CLOSED_POS_001")
    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    broker.set_mock_orders([])

    issues = eng.reconcile()
    assert len(issues) == 0
    pos = eng.get_position("CLOSED_POS_001")
    assert pos["status"] == PositionStatus.CLOSED_EXTERNAL.value
    assert pos["remaining_qty"] == 0


def test_stale_protected_zero_qty_closes_on_broker_flat(clean_engine):
    """
    Bug: watchdog/zero-qty left status=PROTECTED with remaining_qty=0 while broker flat.
    Recon must normalize to CLOSED_EXTERNAL and stop reporting phantom SL mismatches.
    """
    eng, broker, store = clean_engine
    sym = "NIFTY26O1923250CE"
    store.upsert_position({
        "entry_order_id": "261019000212157",
        "symbol": sym,
        "side": "BUY",
        "total_qty": 130,
        "remaining_qty": 0,
        "status": PositionStatus.PROTECTED.value,
        "sl_order_id": "DEAD_SL_01",
        "target_order_id": "SKIPPED_SL_ONLY",
    })
    eng._positions["261019000212157"] = store.get_position("261019000212157")
    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    broker.set_mock_orders([])

    assert len(store.active_positions()) == 0  # qty=0 must not count as active
    issues = eng.reconcile()
    # No SL-missing / unverified issues for a flat book
    assert not any("DEAD_SL" in i or "NEEDS_MANUAL" in i for i in issues)
    pos = eng.get_position("261019000212157")
    assert pos["status"] == PositionStatus.CLOSED_EXTERNAL.value
    assert pos["remaining_qty"] == 0
    assert len(store.active_positions()) == 0


def test_stale_open_qty_closes_when_symbol_absent_from_broker_book(clean_engine):
    """Missing symbol after successful positions fetch = flat (broker dropped the row)."""
    eng, broker, store = clean_engine
    sym = "NIFTY26O1923100CE"
    store.upsert_position({
        "entry_order_id": "261019000198233",
        "symbol": sym,
        "side": "BUY",
        "total_qty": 130,
        "remaining_qty": 130,
        "status": PositionStatus.UNPROTECTED.value,
        "sl_order_id": None,
    })
    eng._positions["261019000198233"] = store.get_position("261019000198233")
    store.set_entry_lock(True, "prior unprotected")
    broker.set_mock_positions([])  # empty book
    broker.set_mock_orders([])

    issues = eng.reconcile()
    pos = eng.get_position("261019000198233")
    assert pos["status"] == PositionStatus.CLOSED_EXTERNAL.value
    assert pos["remaining_qty"] == 0
    assert len(store.active_positions()) == 0
    assert store.is_entry_locked()[0] is False
    assert not any("NEEDS_MANUAL" in i for i in issues)


def test_no_target_orders_ever_placed(clean_engine):
    """
    Regardless of lot count, no Target (Limit) orders are placed.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    # 2-lot (even) — previously would split into target+SL; now SL-only
    eng.handle_order_event(_entry_event("ENTRY_EVEN_001", "NIFTY26O1322500CE", "B", 130, 100.0))
    # 1-lot (odd)
    eng.handle_order_event(_entry_event("ENTRY_ODD_001", "NIFTY26O1322500CE", "B", 65, 50.0))

    for oid in ("ENTRY_EVEN_001", "ENTRY_ODD_001"):
        pos = eng.get_position(oid)
        assert pos["order_structure"] == "SL_ONLY", f"{oid}: expected SL_ONLY, got {pos['order_structure']}"
        assert pos["target_qty"] == 0, f"{oid}: target_qty should be 0"

    limit_orders = [o for o in broker._mock_orders.values() if o.get("ordTyp") == "L"]
    assert len(limit_orders) == 0, f"No Limit (Target) orders should exist; found {len(limit_orders)}"


def test_duplicate_entry_events_place_sl_only_once(clean_engine):
    """
    Duplicate WS/REST events for the same fill must not place multiple SL orders.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    event = _entry_event("DEDUP_ENTRY_001", "NIFTY26O1322500CE", "B", 130, 100.0)
    eng.handle_order_event(event)
    order_count_after_first = len(broker._mock_orders)

    eng.handle_order_event(event)
    eng.handle_order_event(event)

    assert len(broker._mock_orders) == order_count_after_first, \
        "Duplicate events must not create additional orders"

    # Exactly one SL exists
    sl_orders = [o for o in broker._mock_orders.values() if o.get("ordTyp") == "SL"]
    assert len(sl_orders) == 1


def test_twenty_filled_places_exactly_one_sl_for_twenty(clean_engine):
    """
    Requirement test: Entry quantity 20 filled -> exactly one SL for 20.
    """
    eng, broker, store = clean_engine
    sym = "NIFTY_OPT_20"
    broker.set_mock_lot_size(sym, 20)

    event = _entry_event("ENTRY_20_001", sym, "B", 20, 150.0)
    eng.handle_order_event(event)

    pos = eng.get_position("ENTRY_20_001")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    assert pos["total_qty"] == 20
    assert pos["sl_qty"] == 20
    assert pos["remaining_qty"] == 20

    sl_id = pos["sl_order_id"]
    assert sl_id is not None
    sl_order = broker._mock_orders[sl_id]
    assert sl_order["qty"] == 20
    assert sl_order["ordTyp"] == "SL"
    assert sl_order["trnsTp"] == "S"

    # No target orders placed
    target_orders = [o for o in broker._mock_orders.values() if o.get("ordTyp") == "L"]
    assert len(target_orders) == 0


def test_new_entry_with_new_order_id_is_eligible_after_manual_change(clean_engine):
    """
    When entry 1 is manually cancelled / modified and excluded,
    a genuinely new entry 2 with a new broker order ID is eligible for normal one-time processing.
    """
    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    # Entry 1: protected then cancelled by client
    eng.handle_order_event(_entry_event("ENTRY_FIRST_001", "NIFTY26O1322500CE", "B", 65, 100.0))
    pos1 = eng.get_position("ENTRY_FIRST_001")
    sl1_id = pos1["sl_order_id"]

    eng.handle_order_event({
        "type": "order",
        "data": {
            "nOrdNo": sl1_id,
            "sym": "NIFTY26O1322500CE",
            "ordSt": "CANCELLED",
            "trnsTp": "S",
            "qty": 65,
            "fldQty": 0,
            "avgPrc": "0.00",
        }
    })
    pos1_updated = eng.get_position("ENTRY_FIRST_001")
    assert pos1_updated["status"] == PositionStatus.MANUALLY_MODIFIED.value

    # Re-sending event for ENTRY_FIRST_001 must NEVER touch it or place SL
    eng.handle_order_event(_entry_event("ENTRY_FIRST_001", "NIFTY26O1322500CE", "B", 65, 100.0))
    pos1_again = eng.get_position("ENTRY_FIRST_001")
    assert pos1_again["status"] == PositionStatus.MANUALLY_MODIFIED.value

    # Entry 2: genuinely new order ID -> gets processed and protected
    eng.handle_order_event(_entry_event("ENTRY_SECOND_002", "NIFTY26O1322500CE", "B", 65, 105.0))
    pos2 = eng.get_position("ENTRY_SECOND_002")
    assert pos2 is not None
    assert pos2["status"] == PositionStatus.PROTECTED.value
    assert pos2["sl_qty"] == 65
    assert pos2["sl_order_id"] is not None
    assert pos2["sl_order_id"] != sl1_id


def test_unsupported_product_skips_protection_without_silent_fallback(clean_engine, monkeypatch):
    """
    When entry order product is BO (Bracket Order) and BO_PROTECT_AS override is empty,
    bot must NOT silently map to MIS. It must skip placement safely and alert.
    """
    from fastapi_app.config import settings
    monkeypatch.setattr(settings, "BO_PROTECT_AS_PRODUCT", "")

    eng, broker, store = clean_engine
    broker.set_mock_lot_size("NIFTY26O1322500CE", 65)

    alerts = []
    monkeypatch.setattr(
        "fastapi_app.notifier.TelegramAlerts.needs_manual_action",
        lambda **kwargs: alerts.append(kwargs)
    )

    # Send BO entry fill
    bo_event = _entry_event("ENTRY_BO_001", "NIFTY26O1322500CE", "B", 65, 100.0, product="BO")
    eng.handle_order_event(bo_event)

    # Position should not have placed protective MIS SL
    assert len(broker._mock_orders) == 0
    # Telegram alert sent
    assert len(alerts) >= 1


# ─────────────────────────────────────────────────────────────
# Product-Type Normalization Tests
# ─────────────────────────────────────────────────────────────

def test_exact_codes_pass_through():
    for code in PLACE_ORDER_PRODUCTS:
        assert normalize_product_type(code) == code


def test_aliases_map():
    assert normalize_product_type("Intraday") == "MIS"
    assert normalize_product_type("Normal") == "NRML"
    assert normalize_product_type("M") == "MIS"
    assert normalize_product_type("I") == "NRML"


def test_empty_uses_fallback():
    assert normalize_product_type(None, fallback="MIS") == "MIS"
    assert normalize_product_type("", fallback="NRML") == "NRML"
    assert normalize_product_type("  ", fallback="CNC") == "CNC"


def test_bo_and_co_never_fallback_to_mis():
    assert normalize_product_type("BO", fallback="MIS") is None
    assert normalize_product_type("CO", fallback="MIS") is None
    assert normalize_product_type("bo", fallback="NRML") is None
    assert "BO" in UNSUPPORTED_PLACE_PRODUCTS
    assert "CO" in UNSUPPORTED_PLACE_PRODUCTS


def test_unknown_nonempty_never_silent_fallback():
    assert normalize_product_type("XYZ", fallback="MIS") is None
    assert normalize_product_type("BRACKET", fallback="MIS") is None


def test_resolve_protective_product_bo_uses_override(monkeypatch):
    from fastapi_app.config import settings
    monkeypatch.setattr(settings, "BO_PROTECT_AS_PRODUCT", "MIS")
    prod, err = resolve_protective_product("BO", fallback="NRML")
    assert prod == "MIS"
    assert err is None


def test_resolve_protective_product_blocks_bo_when_override_off(monkeypatch):
    from fastapi_app.config import settings
    monkeypatch.setattr(settings, "BO_PROTECT_AS_PRODUCT", "")
    prod, err = resolve_protective_product("BO", fallback="MIS")
    assert prod is None
    assert err is not None
    assert "BO" in err


def test_resolve_protective_product_ok_for_mis():
    prod, err = resolve_protective_product("MIS", fallback="NRML")
    assert prod == "MIS"
    assert err is None


def test_resolve_empty_uses_fallback():
    prod, err = resolve_protective_product(None, fallback="NRML")
    assert prod == "NRML"
    assert err is None


# ─────────────────────────────────────────────────────────────
# Historical Replay & Startup Reconciliation Tests
# ─────────────────────────────────────────────────────────────

def test_startup_historical_complete_rejected_cancelled_no_new_exits(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"

    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    broker.set_mock_orders([
        _order("HIST_COMPLETE_01", sym, "complete", fldQty=130, qty=130),
        _order("HIST_REJECT_01", sym, "rejected", fldQty=0, qty=130),
        _order("HIST_CANCEL_01", sym, "cancelled", fldQty=0, qty=130),
    ])

    before = len(broker._mock_orders)
    eng.startup_reconciliation()
    after = len(broker._mock_orders)

    assert after == before
    assert store.get_processed_qty("HIST_COMPLETE_01") >= 130
    assert store.get_order_fill_record("HIST_REJECT_01") is not None
    assert store.get_order_fill_record("HIST_CANCEL_01") is not None
    assert store.get_position("HIST_COMPLETE_01") is None


def test_startup_then_rest_replay_does_not_place_protection(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"

    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    hist = _order("HIST_COMPLETE_02", sym, "complete", fldQty=130, qty=130)
    broker.set_mock_orders([hist])

    eng.startup_reconciliation()
    place_calls = []
    original = eng._place_protective_orders

    def spy(*args, **kwargs):
        place_calls.append((args, kwargs))
        return original(*args, **kwargs)

    eng._place_protective_orders = spy

    eng.handle_order_event({"type": "order", "data": hist}, source="REST")
    eng.handle_order_event({"type": "order", "data": hist}, source="WS")

    assert place_calls == []
    assert store.get_position("HIST_COMPLETE_02") is None


def test_restart_replay_uses_persisted_fill_state(dry_run_engine, tmp_path, monkeypatch):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"
    db_file = store.db_path

    broker.set_mock_lot_size(sym, 65)
    entry = _order("LIVE_ENTRY_01", sym, "complete", fldQty=130, qty=130)
    eng.handle_order_event({"type": "order", "data": entry}, source="WS")
    pos = eng.get_position("LIVE_ENTRY_01")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    sl = pos["sl_order_id"]
    assert sl and not str(sl).startswith("SKIPPED_")
    assert pos["target_order_id"] == "SKIPPED_SL_ONLY"
    assert pos["half_a_sl_order_id"] in (None, "SKIPPED_QTY_0", "SKIPPED_SL_ONLY")

    monkeypatch.setattr(settings, "DRY_RUN", True)
    eng2 = TradingEngine(persistence_file=db_file)
    broker2 = BrokerClient(dry_run=True)
    broker2.store = eng2.store
    eng2.broker = broker2
    broker2.set_mock_lot_size(sym, 65)
    broker2.set_mock_positions([{"trdSym": sym, "netQty": 130.0}])
    broker2.set_mock_orders([
        entry,
        {
            "nOrdNo": sl, "trdSym": sym, "ordSt": "TRIGGER_PENDING", "tag": f"BOT_SL_{sl[-6:]}",
            "trnsTp": "S", "qty": 130, "fldQty": 0, "avgPrc": "90.00",
        },
    ])

    orders_before = set(broker2._mock_orders.keys())
    eng2._load_persisted_state()
    eng2.startup_reconciliation()
    orders_after = set(broker2._mock_orders.keys())

    assert orders_after == orders_before
    assert eng2.store.get_processed_qty("LIVE_ENTRY_01") >= 130

    place_calls = []
    eng2._place_protective_orders = lambda *a, **k: place_calls.append(1)
    eng2.handle_order_event({"type": "order", "data": entry}, source="REST")
    eng2.handle_order_event({"type": "order", "data": entry}, source="WS")
    assert place_calls == []


def test_duplicate_ws_and_rest_events_suppressed(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"
    broker.set_mock_lot_size(sym, 65)

    entry = _order("DUP_ENTRY_01", sym, "complete", fldQty=130, qty=130)
    eng.handle_order_event({"type": "order", "data": entry}, source="WS")
    pos1 = eng.get_position("DUP_ENTRY_01")
    assert pos1["status"] == PositionStatus.PROTECTED.value
    exit_count_1 = len(broker._mock_orders)

    eng.handle_order_event({"type": "order", "data": entry}, source="REST")
    eng.handle_order_event({"type": "order", "data": entry}, source="WS")
    assert len(broker._mock_orders) == exit_count_1
    assert store.get_processed_qty("DUP_ENTRY_01") == 130


def test_broker_flat_marks_closed_external_and_ignores_history(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"

    store.upsert_position({
        "entry_order_id": "FLAT_POS_01",
        "symbol": sym,
        "side": "BUY",
        "total_qty": 130,
        "remaining_qty": 130,
        "entry_price": 100.0,
        "status": PositionStatus.PROTECTED.value,
        "sl_order_id": "OLD_SL",
        "target_order_id": "OLD_TGT",
    })
    eng._positions["FLAT_POS_01"] = store.get_position("FLAT_POS_01")

    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    broker.set_mock_orders([
        _order("FLAT_POS_01", sym, "complete", fldQty=130, qty=130),
        _order("OLD_SL", sym, "OPEN", fldQty=0, qty=65, side="S", tag="BOT_SL_001"),
        _order("OLD_TGT", sym, "OPEN", fldQty=0, qty=65, side="S", tag="BOT_TARGET_001"),
    ])

    eng.startup_reconciliation()
    pos = eng.get_position("FLAT_POS_01")
    assert pos["status"] == PositionStatus.CLOSED_EXTERNAL.value
    assert pos["remaining_qty"] == 0

    place_calls = []
    eng._place_protective_orders = lambda *a, **k: place_calls.append(1)
    eng.handle_order_event(
        {"type": "order", "data": _order("FLAT_POS_01", sym, "complete", fldQty=130)},
        source="REST",
    )
    assert place_calls == []


def test_open_unprotected_fill_gets_first_time_protection_on_startup(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"
    broker.set_mock_lot_size(sym, 65)

    broker.set_mock_positions([{"trdSym": sym, "netQty": 130.0}])
    entry = _order("OPEN_PROT_01", sym, "complete", fldQty=130, qty=130)
    broker.set_mock_orders([entry])

    eng.startup_reconciliation()
    pos = eng.get_position("OPEN_PROT_01")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value
    assert pos.get("half_b_sl_order_id") or pos.get("sl_order_id")


def test_poisoned_startup_seed_reopened_when_broker_still_open(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"
    broker.set_mock_lot_size(sym, 65)

    store.mark_broker_order_processed(
        order_id="POISON_01",
        filled_qty=130,
        cumulative_value=13000.0,
        status="COMPLETE",
        source="STARTUP",
    )
    store.mark_execution_seen("POISON_01_130_100.0", {"startup_history": True})

    broker.set_mock_positions([{"trdSym": sym, "netQty": 130.0}])
    entry = _order("POISON_01", sym, "complete", fldQty=130, qty=130, avg="100.00")
    broker.set_mock_orders([entry])

    eng.handle_order_event({"type": "order", "data": entry}, source="REST")
    pos = eng.get_position("POISON_01")
    assert pos is not None
    assert pos["status"] == PositionStatus.PROTECTED.value


def test_flat_historical_complete_still_not_protected(dry_run_engine):
    eng, broker, store = dry_run_engine
    sym = "NIFTY26O1322500CE"
    broker.set_mock_positions([{"trdSym": sym, "netQty": 0.0}])
    broker.set_mock_orders([
        _order("FLAT_HIST_01", sym, "complete", fldQty=130, qty=130),
    ])
    eng.startup_reconciliation()
    assert eng.get_position("FLAT_HIST_01") is None
    assert store.get_processed_qty("FLAT_HIST_01") >= 130


def test_auto_entry_flags_remain_disabled():
    assert settings.AUTO_ENTRY_ENABLED is False
    assert settings.AUTO_REENTRY_ENABLED is False

