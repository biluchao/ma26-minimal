"""m01_strategy/engine.py — MA26-ATR strategy engine"""

import time
from dataclasses import dataclass, field
from typing import Optional

from core import (SystemModule, Result, Error, ErrorSeverity, EventType,
                  TraceContext, SignalRequest as CoreSignalRequest)


@dataclass
class IndicatorState:
    """Rolling indicator state using Welford-style updates (no full history kept)."""
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0          # for variance (ATR / std)
    prev_close: float = 0.0
    prev_tr: float = 0.0
    value: float = 0.0
    atr: float = 0.0
    ready: bool = False


class MA26:
    """Simple moving average over `period` bars. O(1) update, no history retained."""

    def __init__(self, period: int = 26):
        self.period = period
        self.values: list = []
        self.sum = 0.0

    def update(self, close: float) -> float:
        self.values.append(close)
        self.sum += close
        if len(self.values) > self.period:
            self.sum -= self.values.pop(0)
        return self.value()

    def value(self) -> float:
        n = len(self.values)
        return self.sum / n if n > 0 else 0.0

    def is_ready(self) -> bool:
        return len(self.values) >= self.period


class ATR14:
    """Average True Range. Rolling update, keeps only last ATR value."""

    def __init__(self, period: int = 14):
        self.period = period
        self.trs: list = []
        self.atr = 0.0

    def update(self, high: float, low: float, close: float) -> float:
        prev_close = self._prev_close
        self._prev_close = close
        if prev_close == 0.0:
            return self.atr
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        self.trs.append(tr)
        if len(self.trs) > self.period:
            self.trs.pop(0)
        n = len(self.trs)
        self.atr = sum(self.trs) / n if n > 0 else 0.0
        return self.atr

    def value(self) -> float:
        return self.atr

    def is_ready(self) -> bool:
        return len(self.trs) >= self.period

    @property
    def _prev_close(self):
        return getattr(self, '_pc', 0.0)

    @_prev_close.setter
    def _prev_close(self, v):
        setattr(self, '_pc', v)


class DirectionMachine:
    """Trend direction state machine: FLAT → BULL / BEAR → FLAT."""

    def __init__(self, atr_mult: float = 0.5):
        self.atr_mult = atr_mult
        self.direction = "FLAT"
        self.entry_price = 0.0
        self.entry_bar = 0

    def evaluate(self, close: float, ma26: float, atr: float) -> Optional[dict]:
        """Returns a signal dict if direction flips, else None."""
        if atr <= 0:
            return None
        upper = ma26 + self.atr_mult * atr
        lower = ma26 - self.atr_mult * atr

        new_dir = self.direction
        if self.direction == "FLAT":
            if close > upper:
                new_dir = "BULL"
            elif close < lower:
                new_dir = "BEAR"
        else:
            ref = upper if self.direction == "BULL" else lower
            if self.direction == "BULL" and close < ref:
                new_dir = "FLAT"
            elif self.direction == "BEAR" and close > ref:
                new_dir = "FLAT"

        if new_dir != self.direction:
            signal = {'from': self.direction, 'to': new_dir,
                      'price': close, 'ma26': ma26, 'atr': atr}
            self.direction = new_dir
            if new_dir in ("BULL", "BEAR"):
                self.entry_price = close
            return signal
        return None


class StrategyEngine(SystemModule):
    """
    MA26-ATR strategy engine.

    Produces SignalRequest objects consumed by RiskGuard + ExecutionManager.
    """

    MODULE_NAME = "M01_StrategyEngine"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = ["M03_BarCache", "M04_RiskGuard"]

    def __init__(self):
        super().__init__()
        self._ma26: Optional[MA26] = None
        self._atr: Optional[ATR14] = None
        self._dir_machine: Optional[DirectionMachine] = None
        self._cfg: dict = {}
        self._last_bar_index = -1

    def configure(self, config: ConfigSnapshot) -> Result[None, Error]:
        super().configure(config)
        s = config.get_section('strategy')
        self._cfg = {
            'ma_period': s.get('ma_period', 26),
            'atr_period': s.get('atr_period', 14),
            'trend_atr_mult': s.get('trend_atr_mult', 0.5),
            'tp_atr_mult': s.get('tp_atr_mult', 3.0),
            'stop_atr_mult': s.get('stop_atr_mult', 2.0),
            'max_size_pct': s.get('max_size_pct', 0.25),
        }
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        self._ma26 = MA26(self._cfg['ma_period'])
        self._atr = ATR14(self._cfg['atr_period'])
        self._dir_machine = DirectionMachine(self._cfg['trend_atr_mult'])
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        self._ma26 = None
        self._atr = None
        self._dir_machine = None
        return Result.ok(None)

    def on_bar(self, bar: dict, ctx: Optional[TraceContext] = None) -> Result[list, Error]:
        """
        Process one bar, emit signals.
        Returns Result[List[SignalRequest], Error].
        """
        if self._ma26 is None:
            return Result.ok([])

        close = bar.get('close', 0.0)
        high = bar.get('high', close)
        low = bar.get('low', close)
        bi = bar.get('bar_index', 0)

        # NaN guard
        if close != close or high != high or low != low:  # NaN check
            if self._alert_manager:
                self._alert_manager.send("WARN", "NaN in bar data", f"bar_index={bi}")
            return Result.ok([])

        self._ma26.update(close)
        self._atr.update(high, low, close)

        signals = []
        if self._ma26.is_ready() and self._atr.is_ready():
            sig = self._dir_machine.evaluate(close, self._ma26.value(), self._atr.value())
            if sig:
                signals.append(self._build_signal(sig, bar))

        self._last_bar_index = bi
        if signals and self._event_bus:
            for s in signals:
                self._event_bus.publish_from(self.MODULE_NAME, EventType.SIGNAL_GENERATED,
                                             trace_id=ctx.trace_id if ctx else "",
                                             bar_index=bi, signal=vars(s))
        return Result.ok(signals)

    def _build_signal(self, sig: dict, bar: dict) -> CoreSignalRequest:
        direction = "BULL" if sig['to'] == "BULL" else ("BEAR" if sig['to'] == "BEAR" else "FLAT")
        action = f"OPEN_{direction}" if sig['to'] in ("BULL", "BEAR") else "CLOSE_ALL"
        return CoreSignalRequest(
            signal_id=f"sig_{bar.get('bar_index')}_{sig['to']}",
            action=action,
            direction=direction,
            source="ma26_atr",
            qty=0.0,          # filled by risk/position sizing
            price=bar.get('close', 0.0),
            order_type="MARKET",
            strength=abs(bar.get('close', 0) - sig['ma26']) / max(sig['atr'], 1e-12),
            trace_id="",
        )

    def get_state(self) -> dict:
        return {
            'direction': self._dir_machine.direction if self._dir_machine else "FLAT",
            'ma26': self._ma26.value() if self._ma26 else 0.0,
            'atr': self._atr.value() if self._atr else 0.0,
            'ma_ready': self._ma26.is_ready() if self._ma26 else False,
            'atr_ready': self._atr.is_ready() if self._atr else False,
        }

    def state_export(self) -> dict:
        base = super().state_export()
        base.update(self.get_state())
        return base

