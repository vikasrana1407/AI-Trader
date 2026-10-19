"""
Pydantic Schemas and Enums for the FastAPI Bot.
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderIntent(str, Enum):
    ENTRY = "ENTRY"
    PROTECTIVE_SL = "PROTECTIVE_SL"
    PROTECTIVE_EXIT = "PROTECTIVE_SL"
    TARGET_EXIT = "TARGET_EXIT"
    MANUAL_ENTRY_DETECTED = "MANUAL_ENTRY_DETECTED"
    MANUAL_RECOVERY = "MANUAL_RECOVERY"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"


class CancelOutcome(str, Enum):
    CONFIRMED_CANCELLED = "CONFIRMED_CANCELLED"
    ALREADY_FILLED = "ALREADY_FILLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class PositionStatus(str, Enum):
    ENTRY_DETECTED = "ENTRY_DETECTED"
    PLACEMENT_PENDING = "PLACEMENT_PENDING"
    PLACEMENT_UNKNOWN = "PLACEMENT_UNKNOWN"
    PROTECTED = "PROTECTED"
    UNPROTECTED = "UNPROTECTED"
    TARGET_EXECUTED = "TARGET_EXECUTED"
    SL_EXECUTED = "SL_EXECUTED"
    PARTIAL = "PARTIAL"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    ERROR = "ERROR"
    CLOSED = "CLOSED"
    CLOSED_EXTERNAL = "CLOSED_EXTERNAL"
    FLAT_UNATTRIBUTED = "FLAT_UNATTRIBUTED"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"
    NEEDS_MANUAL_ACTION = "NEEDS_MANUAL_ACTION"
    # Terminal: client manually modified/cancelled the bot SL or exited the position.
    # The entry_order_id is permanently excluded from further automated management.
    MANUALLY_MODIFIED = "MANUALLY_MODIFIED"
    CANCELLED = "CANCELLED"
    EXITED = "EXITED"


class OrderStructure(str, Enum):
    TARGET_PLUS_FULL_SL = "TARGET_PLUS_FULL_SL"
    SL_ONLY = "SL_ONLY"


class PositionModel(BaseModel):
    entry_order_id: str
    symbol: str
    side: OrderSide
    total_qty: int
    entry_price: float
    order_structure: str = "TARGET_PLUS_FULL_SL"
    target_qty: int = 0
    target_price: float = 0.0
    target_order_id: Optional[str] = None
    sl_qty: int = 0
    sl_trigger_price: float = 0.0
    sl_limit_price: float = 0.0
    sl_order_id: Optional[str] = None
    half_a_target_order_id: Optional[str] = None
    half_a_sl_order_id: Optional[str] = None
    half_b_sl_order_id: Optional[str] = None
    half_a_qty: int = 0
    half_b_qty: int = 0
    status: PositionStatus = PositionStatus.ENTRY_DETECTED
    latency_ms: float = 0.0
    created_at: float = Field(default_factory=lambda: 0.0)
    error_message: Optional[str] = None


class SimulateEntryRequest(BaseModel):
    symbol: str = "NIFTY24OCT25000CE"
    side: OrderSide = OrderSide.BUY
    quantity: int = 100
    price: float = 200.0


class SimulateExitRequest(BaseModel):
    order_id: str
    status: str = "TRAD"  # "TRAD" or "COMPLETE"


class HealthResponse(BaseModel):
    status: str = "healthy"
    mode: str
    uptime_seconds: float
    active_positions: int
    total_trades_managed: int
