"""m07_execution/manager.py — Backtest order execution simulator"""

import uuid
from dataclasses import dataclass, field
from typing import Optional

from core import (SystemModule, Result, Error, ErrorSeverity, EventType,
                  SignalRequest, FillResult)


@dataclass
class Order:
    order_id: str
    signal_id: str
    side: str          # BUY / SELL
    qty: float
    price: float
    order_type: str
    status: str = "OPEN"   # OPEN / FILLED / CANCELLED / REJECTED
    filled_qty: float = 0.0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    bar_index: int = 0


class ExecutionManager(SystemModule):
    """
    Backtest execution simulator.
    - rounds qty to step_size
    - applies slippage + commission
    - emits ORDER_FILLED / ORDER_REJECTED
    """

    MODULE_NAME = "M07_ExecutionManager"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = ["M05_PositionBook", "M06_EquityManager", "M04_RiskGuard"]

    def __init__(self):
        super().__init__()
        self._cfg: dict = {}
        self._orders: dict = {}
        self._trades: list = []
        self._commission_bps = 10

    def configure(self, config) -> Result[None, Error]:
        super().configure(config)
        s = config.get_section('strategy')
        self._cfg = {
            'step_size': s.get('step_size', 0.001),
            'min_qty': s.get('min_qty', 0.001),
            'min_notional': s.get('min_notional', 10.0),
            'qty_decimals': s.get('qty_decimals', 3),
        }
        self._commission_bps = config.get('simulator.commission_bps', 10)
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def execute(self, signal: SignalRequest, mark_price: float, bar_index: int,
                ctx=None) -> Result[Optional[FillResult], Error]:
        """Execute a signal. Returns Ok(FillResult) or Ok(None) if rejected."""
        qty = signal.qty
        if qty <= 0:
            return Result.err(Error("ORDER_ZERO_QTY", "qty must be > 0", ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        # round to step
        step = self._cfg['step_size']
        qty = (qty // step) * step
        if qty <= 0:
            return Result.err(Error("ORDER_BELOW_STEP", f"qty {qty} below step {step}",
                                   ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        notional = qty * mark_price
        if notional < self._cfg['min_notional']:
            return Result.err(Error("ORDER_BELOW_NOTIONAL", f"notional {notional} < {self._cfg['min_notional']}",
                                   ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        # slippage (backtest: 1 tick toward adverse)
        slip = mark_price * 0.0001
        if signal.action.startswith("OPEN") or signal.action.startswith("ADD"):
            fill_price = mark_price + slip if "LONG" in signal.action or signal.direction == "BULL" else mark_price - slip
            side = "BUY"
        else:
            fill_price = mark_price - slip if "LONG" in signal.action else mark_price + slip
            side = "SELL"

        commission = notional * (self._commission_bps / 10000.0)

        fill = FillResult(
            order_id=str(uuid.uuid4())[:12],
            fill_id=str(uuid.uuid4())[:12],
            qty=qty, price=round(fill_price, 2), commission=round(commission, 8),
            timestamp=bar_index, trace_id=ctx.trace_id if ctx else "",
        )

        # record order
        order = Order(order_id=fill.order_id, signal_id=signal.signal_id, side=side,
                     qty=qty, price=fill.price, order_type=signal.order_type,
                     status="FILLED", filled_qty=qty, avg_fill_price=fill.price,
                     commission=commission, bar_index=bar_index)
        self._orders[fill.order_id] = order

        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.ORDER_FILLED,
                                         bar_index=bar_index, order_id=fill.order_id,
                                         side=side, qty=qty, price=fill.price,
                                         commission=commission)
        return Result.ok(fill)

    def get_orders(self) -> list:
        return list(self._orders.values())

    def get_trades(self) -> list:
        return list(self._trades)

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({'order_count': len(self._orders), 'trade_count': len(self._trades)})
        return base

