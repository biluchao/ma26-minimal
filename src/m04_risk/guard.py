"""m04_risk/guard.py — Risk guard with cooling + daily loss circuit"""

import time
from dataclasses import dataclass, field
from typing import Optional

from core import (SystemModule, Result, Error, ErrorSeverity, EventType,
                  SignalRequest, RiskState as CoreRiskState)


class RiskGuard(SystemModule):
    """
    Pre-trade + post-bar risk checks.
    Triggers cooling on daily loss / drawdown / open-order limit breach.
    """

    MODULE_NAME = "M04_RiskGuard"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = ["M05_PositionBook", "M06_EquityManager"]

    def __init__(self):
        super().__init__()
        self._cfg: dict = {}
        self._state: CoreRiskState = CoreRiskState()
        self._cooling = False
        self._cooling_reason = ""
        self._cooling_until_bar = 0
        self._day_start_equity = 0.0
        self._day_label = ""
        self._open_orders = 0

    def configure(self, config) -> Result[None, Error]:
        super().configure(config)
        r = config.get_section('risk')
        self._cfg = {
            'max_position_pct': r.get('max_position_pct', 0.95),
            'max_single_loss_pct': r.get('max_single_loss_pct', 0.02),
            'max_daily_loss_pct': r.get('max_daily_loss_pct', 0.05),
            'max_drawdown_pct': r.get('max_drawdown_pct', 0.15),
            'max_open_orders': r.get('max_open_orders', 5),
            'max_slippage_bps': r.get('max_slippage_bps', 50),
        }
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def reset_day(self, equity: float, day_label: str):
        """Call once per day (or on date change) to reset daily loss baseline."""
        self._day_start_equity = equity
        self._day_label = day_label

    def pre_trade(self, signal: SignalRequest, ctx=None) -> Result[bool, Error]:
        """Pre-trade gate. Returns Ok(True) if allowed."""
        if self._cooling:
            return Result.err(Error("RISK_COOLING", f"cooling: {self._cooling_reason}",
                                   ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        if self._open_orders >= self._cfg['max_open_orders']:
            return Result.err(Error("RISK_TOO_MANY_ORDERS",
                                   f"{self._open_orders} >= {self._cfg['max_open_orders']}",
                                   ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.RISK_CHECK_PASSED,
                                         bar_index=ctx.bar_index if ctx else 0,
                                         signal=signal.action)
        return Result.ok(True)

    def on_fill(self, qty: float, price: float, side: str):
        self._open_orders = max(0, self._open_orders - 1)

    def on_order_opened(self):
        self._open_orders += 1

    def post_bar(self, equity: float, peak_equity: float, bar_index: int,
                 ctx=None) -> Result[CoreRiskState, Error]:
        """Post-bar check: drawdown + daily loss."""
        # drawdown
        dd = 0.0
        if peak_equity > 0:
            dd = (peak_equity - equity) / peak_equity

        self._state.drawdown = dd
        self._state.peak_equity = peak_equity
        self._state.current_equity = equity
        self._state.cooling = self._cooling
        self._state.cooling_reason = self._cooling_reason

        # daily loss
        if self._day_start_equity > 0:
            dl = (self._day_start_equity - equity) / self._day_start_equity
            self._state.daily_loss_pct = dl
            if dl >= self._cfg['max_daily_loss_pct'] and not self._cooling:
                self._enter_cooling(f"daily loss {dl:.2%} >= {self._cfg['max_daily_loss_pct']:.2%}",
                                   bar_index)

        # drawdown
        if dd >= self._cfg['max_drawdown_pct'] and not self._cooling:
            self._enter_cooling(f"drawdown {dd:.2%} >= {self._cfg['max_drawdown_pct']:.2%}",
                               bar_index)

        # cooling expiry
        if self._cooling and bar_index >= self._cooling_until_bar:
            self._exit_cooling()

        self._update_level()
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.RISK_LEVEL_CHANGED,
                                         bar_index=bar_index, level=self._state.level,
                                         drawdown=dd)
        return Result.ok(self._state)

    def _enter_cooling(self, reason: str, bar_index: int):
        self._cooling = True
        self._cooling_reason = reason
        self._cooling_until_bar = bar_index + 100  # cool for ~100 bars
        if self._alert_manager:
            self._alert_manager.send("URGENT", "Risk cooling triggered", reason)
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.RISK_COOLING_TRIGGERED,
                                         bar_index=bar_index, reason=reason)

    def _exit_cooling(self):
        self._cooling = False
        reason = self._cooling_reason
        self._cooling_reason = ""
        if self._alert_manager:
            self._alert_manager.send("INFO", "Risk cooling ended", reason)
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.RISK_COOLING_ENDED,
                                         reason=reason)

    def _update_level(self):
        dd = self._state.drawdown
        dl = self._state.daily_loss_pct
        max_dd = self._cfg['max_drawdown_pct']
        max_dl = self._cfg['max_daily_loss_pct']
        if dd >= max_dd * 0.8 or dl >= max_dl * 0.8:
            self._state.level = "CRITICAL"
        elif dd >= max_dd * 0.5 or dl >= max_dl * 0.5:
            self._state.level = "WARNING"
        elif dd > 0 or dl > 0:
            self._state.level = "CAUTION"
        else:
            self._state.level = "NORMAL"

    def is_cooling(self) -> bool:
        return self._cooling

    def get_state(self) -> CoreRiskState:
        return self._state

    def force_cool(self, reason: str, bar_index: int):
        self._enter_cooling(reason, bar_index)

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({
            'cooling': self._cooling,
            'cooling_reason': self._cooling_reason,
            'cooling_until_bar': self._cooling_until_bar,
            'drawdown': self._state.drawdown,
            'daily_loss_pct': self._state.daily_loss_pct,
            'level': self._state.level,
            'open_orders': self._open_orders,
        })
        return base

    def state_import(self, state: dict) -> Result[None, Error]:
        self._cooling = state.get('cooling', False)
        self._cooling_reason = state.get('cooling_reason', '')
        self._cooling_until_bar = state.get('cooling_until_bar', 0)
        self._state.drawdown = state.get('drawdown', 0.0)
        self._state.daily_loss_pct = state.get('daily_loss_pct', 0.0)
        self._state.level = state.get('level', 'NORMAL')
        self._open_orders = state.get('open_orders', 0)
        return Result.ok(None)

