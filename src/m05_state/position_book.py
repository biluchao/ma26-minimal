"""m05_state/position_book.py — Position book with avg-price tracking"""

from dataclasses import dataclass, field
from typing import Optional

from core import SystemModule, Result, Error, ErrorSeverity


@dataclass
class Position:
    symbol: str = ""
    side: str = "FLAT"          # LONG / SHORT / FLAT
    qty: float = 0.0
    avg_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    opened_bar: int = 0
    unrealized_pnl: float = 0.0

    def notional(self, price: float) -> float:
        return self.qty * price

    def is_open(self) -> bool:
        return self.side != "FLAT" and self.qty > 0


class PositionBook(SystemModule):
    """Tracks current position. Single owner of position state."""

    MODULE_NAME = "M05_PositionBook"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = []

    def __init__(self):
        super().__init__()
        self._pos = Position()
        self._peak_qty = 0.0

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def open_long(self, qty: float, price: float, stop: float, tp: float,
                  bar_index: int) -> Result[Position, Error]:
        """Open or add to a long. Weighted avg price."""
        if qty <= 0:
            return Result.err(Error("INVALID_QTY", f"qty={qty}", ErrorSeverity.RECOVERABLE, self.MODULE_NAME))
        if self._pos.side == "LONG":
            new_qty = self._pos.qty + qty
            self._pos.avg_price = (self._pos.qty * self._pos.avg_price + qty * price) / new_qty
            self._pos.qty = new_qty
        else:
            self._pos = Position(symbol="BTCUSDT", side="LONG", qty=qty, avg_price=price,
                                stop_loss=stop, take_profit=tp, opened_bar=bar_index)
        self._peak_qty = max(self._peak_qty, self._pos.qty)
        return Result.ok(self._pos)

    def open_short(self, qty, price, stop, tp, bar_index):
        if qty <= 0:
            return Result.err(Error("INVALID_QTY", f"qty={qty}", ErrorSeverity.RECOVERABLE, self.MODULE_NAME))
        if self._pos.side == "SHORT":
            new_qty = self._pos.qty + qty
            self._pos.avg_price = (self._pos.qty * self._pos.avg_price + qty * price) / new_qty
            self._pos.qty = new_qty
        else:
            self._pos = Position(symbol="BTCUSDT", side="SHORT", qty=qty, avg_price=price,
                                stop_loss=stop, take_profit=tp, opened_bar=bar_index)
        return Result.ok(self._pos)

    def reduce(self, qty: float, price: float) -> Result[tuple, Error]:
        """Reduce position. Returns Ok((realized_pnl, remaining_qty))."""
        if qty <= 0 or self._pos.qty <= 0:
            return Result.err(Error("INVALID_REDUCE", f"qty={qty} pos={self._pos.qty}",
                                   ErrorSeverity.RECOVERABLE, self.MODULE_NAME))
        close_qty = min(qty, self._pos.qty)
        if self._pos.side == "LONG":
            rpnl = (price - self._pos.avg_price) * close_qty
        else:
            rpnl = (self._pos.avg_price - price) * close_qty
        self._pos.qty -= close_qty
        if self._pos.qty <= 1e-12:
            self._pos = Position()
        return Result.ok((rpnl, self._pos.qty))

    def close(self) -> Result[tuple, Error]:
        """Close entire position. Returns Ok((realized_pnl,))."""
        if self._pos.qty <= 0:
            return Result.ok((0.0,))
        # caller must supply mark price via reduce; for bookkeeping we zero out
        rpnl = self._pos.unrealized_pnl
        self._pos = Position()
        return Result.ok((rpnl,))

    def revalue(self, mark_price: float) -> float:
        """Update unrealized PnL at current mark price."""
        if self._pos.qty <= 0:
            self._pos.unrealized_pnl = 0.0
            return 0.0
        if self._pos.side == "LONG":
            self._pos.unrealized_pnl = (mark_price - self._pos.avg_price) * self._pos.qty
        else:
            self._pos.unrealized_pnl = (self._pos.avg_price - mark_price) * self._pos.qty
        return self._pos.unrealized_pnl

    def get(self) -> Position:
        return self._pos

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({
            'side': self._pos.side, 'qty': self._pos.qty,
            'avg_price': self._pos.avg_price, 'stop_loss': self._pos.stop_loss,
            'take_profit': self._pos.take_profit, 'unrealized_pnl': self._pos.unrealized_pnl,
        })
        return base

    def state_import(self, state: dict) -> Result[None, Error]:
        self._pos = Position(
            side=state.get('side', 'FLAT'),
            qty=float(state.get('qty', 0.0)),
            avg_price=float(state.get('avg_price', 0.0)),
            stop_loss=float(state.get('stop_loss', 0.0)),
            take_profit=float(state.get('take_profit', 0.0)),
            unrealized_pnl=float(state.get('unrealized_pnl', 0.0)),
        )
        return Result.ok(None)

