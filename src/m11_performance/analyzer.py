"""m11_performance/analyzer.py — Equity curve stats: returns, Sharpe, drawdown, win rate"""

import math
from collections import deque
from dataclasses import dataclass, field

from core import SystemModule, Result, Error, EventType


@dataclass
class Trade:
    entry_bar: int
    exit_bar: int
    side: str
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    pnl_pct: float
    bars_held: int


@dataclass
class PerfReport:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    profit_factor: float = 0.0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    sharpe: float = 0.0
    sortino: float = 0.0
    equity_final: float = 0.0
    equity_peak: float = 0.0
    num_bars: int = 0


class PerformanceAnalyzer(SystemModule):
    """Tracks trades and computes performance statistics."""

    MODULE_NAME = "M11_PerformanceAnalyzer"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = []

    def __init__(self, max_equity_points: int = 100000):
        super().__init__()
        self._equity: deque = deque(maxlen=max_equity_points)
        self._returns: deque = deque(maxlen=max_equity_points)
        self._trades: list = []
        self._open_trade: dict = None
        self._prev_equity = 0.0

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def on_equity(self, equity: float, bar_index: int, ctx=None):
        """Record equity point. Called once per bar after revalue."""
        if self._prev_equity > 0:
            r = (equity - self._prev_equity) / self._prev_equity
            self._returns.append(r)
        self._prev_equity = equity
        self._equity.append((bar_index, equity))

    def on_trade_open(self, side: str, price: float, qty: float, bar_index: int, ctx=None):
        self._open_trade = {'side': side, 'entry_price': price, 'qty': qty, 'entry_bar': bar_index}

    def on_trade_close(self, price: float, bar_index: int, ctx=None) -> Result[None, Error]:
        if not self._open_trade:
            return Result.ok(None)
        t = self._open_trade
        if t['side'] == "LONG":
            pnl = (price - t['entry_price']) * t['qty']
        else:
            pnl = (t['entry_price'] - price) * t['qty']
        pnl_pct = pnl / (t['entry_price'] * t['qty']) if t['entry_price'] > 0 else 0
        trade = Trade(entry_bar=t['entry_bar'], exit_bar=bar_index, side=t['side'],
                      entry_price=t['entry_price'], exit_price=price, qty=t['qty'],
                      pnl=pnl, pnl_pct=pnl_pct, bars_held=bar_index - t['entry_bar'])
        self._trades.append(trade)
        self._open_trade = None
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.TRADE_CLOSED,
                                         bar_index=bar_index, side=t['side'],
                                         pnl=pnl, pnl_pct=pnl_pct)
        return Result.ok(None)

    def report(self, equity_final: float, equity_peak: float) -> PerfReport:
        r = PerfReport(num_bars=len(self._equity), equity_final=equity_final,
                      equity_peak=equity_peak)
        if not self._trades:
            return r

        r.total_trades = len(self._trades)
        for t in self._trades:
            if t.pnl > 0:
                r.wins += 1
                r.gross_profit += t.pnl
            else:
                r.losses += 1
                r.gross_loss += abs(t.pnl)
        r.win_rate = r.wins / r.total_trades
        r.avg_win = r.gross_profit / r.wins if r.wins else 0
        r.avg_loss = r.gross_loss / r.losses if r.losses else 0
        r.profit_factor = r.gross_profit / r.gross_loss if r.gross_loss > 0 else float('inf')
        r.total_pnl = sum(t.pnl for t in self._trades)
        r.max_drawdown = self._calc_max_drawdown()
        r.sharpe = self._calc_sharpe()
        r.sortino = self._calc_sortino()
        return r

    def _calc_max_drawdown(self) -> float:
        if not self._equity:
            return 0.0
        peak = self._equity[0][1]
        mdd = 0.0
        for _, e in self._equity:
            peak = max(peak, e)
            dd = (peak - e) / peak if peak > 0 else 0
            mdd = max(mdd, dd)
        return mdd

    def _calc_sharpe(self) -> float:
        if len(self._returns) < 2:
            return 0.0
        mu = sum(self._returns) / len(self._returns)
        var = sum((r - mu) ** 2 for r in self._returns) / (len(self._returns) - 1)
        sd = math.sqrt(var)
        if sd == 0:
            return 0.0
        return (mu / sd) * math.sqrt(252 * 24)  # annualized (1h bars)

    def _calc_sortino(self) -> float:
        if len(self._returns) < 2:
            return 0.0
        downs = [r for r in self._returns if r < 0]
        if not downs:
            return 0.0
        dd = math.sqrt(sum(d * d for d in downs) / len(downs))
        if dd == 0:
            return 0.0
        mu = sum(self._returns) / len(self._returns)
        return (mu / dd) * math.sqrt(252 * 24)

    def get_trades(self) -> list:
        return list(self._trades)

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({'trade_count': len(self._trades), 'equity_points': len(self._equity)})
        return base

