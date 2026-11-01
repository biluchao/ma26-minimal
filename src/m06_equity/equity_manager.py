"""m06_equity/manager.py — Cash + equity tracking with precise accumulator"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from core import SystemModule, Result, Error, ErrorSeverity, EventType


@dataclass
class EquitySnapshot:
    cash: float
    position_value: float
    unrealized_pnl: float
    total_equity: float
    daily_pnl: float
    total_pnl: float
    peak_equity: float
    drawdown: float
    bar_index: int = 0


class EquityManager(SystemModule):
    """
    Tracks cash, equity, drawdown, daily PnL.
    Uses Decimal internally for cash arithmetic, exposes float for display.
    """

    MODULE_NAME = "M06_EquityManager"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = ["M05_PositionBook"]

    def __init__(self):
        super().__init__()
        self._cash_d = Decimal("0")
        self._peak = 0.0
        self._start = 0.0
        self._day_start = 0.0
        self._day_label = ""
        self._last_snap: Optional[EquitySnapshot] = None

    def configure(self, config) -> Result[None, Error]:
        super().configure(config)
        self._start = config.get('equity.initial_cash', 10000.0)
        self._cash_d = Decimal(str(self._start))
        self._peak = self._start
        self._day_start = self._start
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def apply_fill(self, qty: float, price: float, side: str, commission: float,
                   bar_index: int = 0, ctx=None) -> Result[float, Error]:
        """Apply a fill to cash. Returns Ok(cash_after)."""
        notional = Decimal(str(round(qty * price, 8)))
        comm = Decimal(str(round(commission, 8)))
        if side in ("BUY", "OPEN_LONG", "ADD_LONG"):
            self._cash_d = self._cash_d - notional - comm
        else:
            self._cash_d = self._cash_d + notional - comm

        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.EQUITY_REVALUED,
                                         bar_index=bar_index, cash=float(self._cash_d),
                                         notional=float(notional))
        return Result.ok(float(self._cash_d))

    def revalue(self, mark_price: float, position_qty: float, position_avg: float,
                position_side: str, bar_index: int = 0, ctx=None) -> Result[EquitySnapshot, Error]:
        """Recompute equity at mark price."""
        upnl = 0.0
        pos_val = 0.0
        if position_qty > 0:
            if position_side == "LONG":
                upnl = (mark_price - position_avg) * position_qty
            else:
                upnl = (position_avg - mark_price) * position_qty
            pos_val = position_qty * mark_price

        equity = float(self._cash_d) + upnl
        self._peak = max(self._peak, equity)
        dd = (self._peak - equity) / self._peak if self._peak > 0 else 0.0
        daily = equity - self._day_start

        snap = EquitySnapshot(
            cash=float(self._cash_d), position_value=pos_val, unrealized_pnl=upnl,
            total_equity=equity, daily_pnl=daily, total_pnl=equity - self._start,
            peak_equity=self._peak, drawdown=dd, bar_index=bar_index,
        )
        self._last_snap = snap
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.EQUITY_REVALUED,
                                         bar_index=bar_index, equity=equity,
                                         upnl=upnl, drawdown=dd)
        return Result.ok(snap)

    def reset_day(self, day_label: str):
        self._day_label = day_label
        self._day_start = float(self._cash_d) + (self._last_snap.unrealized_pnl if self._last_snap else 0)

    def get_snapshot(self) -> Optional[EquitySnapshot]:
        return self._last_snap

    def cash(self) -> float:
        return float(self._cash_d)

    def peak(self) -> float:
        return self._peak

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({
            'cash': float(self._cash_d), 'peak': self._peak,
            'start': self._start, 'day_start': self._day_start,
        })
        if self._last_snap:
            base['equity'] = self._last_snap.total_equity
            base['drawdown'] = self._last_snap.drawdown
        return base

    def state_import(self, state: dict) -> Result[None, Error]:
        self._cash_d = Decimal(str(state.get('cash', self._start)))
        self._peak = float(state.get('peak', self._start))
        self._start = float(state.get('start', self._start))
        self._day_start = float(state.get('day_start', self._start))
        return Result.ok(None)

