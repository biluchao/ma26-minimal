"""core/interfaces.py — Public type definitions shared across modules"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


@dataclass
class Bar:
    bar_index: int
    timestamp: int
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0


@dataclass
class SignalRequest:
    signal_id: str = ""
    action: str = ""        # OPEN_LONG / OPEN_BEAR / CLOSE_ALL / ADD_LONG / ADD_SHORT
    direction: str = ""     # BULL / BEAR / FLAT
    source: str = ""
    qty: float = 0.0
    price: float = 0.0
    order_type: str = "MARKET"
    strength: float = 0.0
    trace_id: str = ""


@dataclass
class FillResult:
    order_id: str = ""
    fill_id: str = ""
    qty: float = 0.0
    price: float = 0.0
    commission: float = 0.0
    timestamp: int = 0
    trace_id: str = ""


@dataclass
class PositionSnapshot:
    symbol: str = ""
    side: str = "FLAT"
    qty: float = 0.0
    avg_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    unrealized_pnl: float = 0.0
    last_update_bar: int = 0


@dataclass
class EquitySnapshot:
    cash: float = 0.0
    position_value: float = 0.0
    unrealized_pnl: float = 0.0
    total_equity: float = 0.0
    daily_pnl: float = 0.0
    total_pnl: float = 0.0
    peak_equity: float = 0.0
    drawdown: float = 0.0
    bar_index: int = 0


@dataclass
class RiskState:
    level: str = "NORMAL"       # NORMAL / CAUTION / WARNING / CRITICAL
    drawdown: float = 0.0
    daily_loss_pct: float = 0.0
    total_exposure: float = 0.0
    cooling: bool = False
    cooling_reason: str = ""
    peak_equity: float = 0.0
    current_equity: float = 0.0


@dataclass
class HealthStatus:
    module: str = ""
    status: str = "UNKNOWN"
    details: dict = field(default_factory=dict)
    timestamp: int = 0


@dataclass
class AlertMessage:
    level: str = "INFO"
    title: str = ""
    message: str = ""
    context: dict = field(default_factory=dict)
    timestamp: int = 0


class AlertLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    URGENT = "URGENT"

