"""
src/m01_strategy/ma26_strategy.py
M01 — MA26-ATR 趋势策略引擎

策略核心逻辑（v4.0.0 规格书）:
─────────────────────────────
1. MA26 趋势判定
   - MA26 = 最近26根收盘价的简单移动平均
   - direction = BULL 当 close > MA26
   - direction = BEAR 当 close < MA26

2. ATR14 检测带
   - ATR14 = 最近14根的真实波幅平均
   - 上轨 = MA26 + 0.5 * ATR14
   - 下轨 = MA26 - 0.5 * ATR14
   - 贴线 = close 落在 [下轨, 上轨] 区间内

3. 贴线脱离判定
   - 脱离 = close 从贴线状态突破上轨（BULL）或跌破下轨（BEAR）
   - 脱离幅度 >= 0.3 * ATR14 视为有效脱离

4. 信号生成
   - BULL 有效脱离 → OPEN_LONG
   - BEAR 有效脱离 → OPEN_SHORT（如允许做空）
   - 贴线内不交易

状态机:
  FLAT → (贴线脱离) → POSITION
  POSITION → (止盈/止损/反向信号) → FLAT
"""
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

from core.result import Result, Error, ErrorSeverity
from core.event_bus import EventBus, EventType
from core.trace_context import TraceContext, TraceManager
from core.state_registry import StateRegistry
from core.lifecycle import SystemModule


# ═══════════════════════════════════════════════════════════════
# 数据结构
# ═══════════════════════════════════════════════════════════════

@dataclass
class Bar:
    """单根K线"""
    bar_index: int = 0
    timestamp: int = 0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time() * 1000)


@dataclass
class SignalRequest:
    """交易信号"""
    signal_id: str = ""
    action: str = ""           # OPEN_LONG / CLOSE_LONG / OPEN_SHORT / CLOSE_SHORT
    direction: str = ""        # BULL / BEAR
    source: str = "ma26_strategy"
    qty: float = 0.0
    price: float = 0.0
    order_type: str = "LIMIT"
    strength: float = 0.0
    trace_id: str = ""
    bar_index: int = 0
    extra: dict = field(default_factory=dict)


@dataclass
class IndicatorState:
    """指标状态快照"""
    ma26: float = 0.0
    atr14: float = 0.0
    upper_band: float = 0.0    # MA26 + 0.5 * ATR14
    lower_band: float = 0.0    # MA26 - 0.5 * ATR14
    is_tape: bool = False       # 是否贴线
    direction: str = "FLAT"     # BULL / BEAR / FLAT


# ═══════════════════════════════════════════════════════════════
# 指标计算器
# ═══════════════════════════════════════════════════════════════

class MA26Calculator:
    """MA26 简单移动平均计算器"""

    def __init__(self, period: int = 26):
        self.period = period
        self._closes = deque(maxlen=period)
        self._sum = 0.0

    def update(self, bar: Bar) -> float:
        if len(self._closes) == self.period:
            self._sum -= self._closes[0]
        self._closes.append(bar.close)
        self._sum += bar.close
        return self.get_value()

    def get_value(self) -> float:
        if len(self._closes) < self.period:
            return 0.0
        return self._sum / len(self._closes)

    def is_ready(self) -> bool:
        return len(self._closes) >= self.period

    def reset(self):
        self._closes.clear()
        self._sum = 0.0


class ATR14Calculator:
    """ATR14 平均真实波幅计算器"""

    def __init__(self, period: int = 14):
        self.period = period
        self._values = deque(maxlen=period)
        self._prev_close: Optional[float] = None

    def update(self, bar: Bar) -> float:
        if self._prev_close is not None:
            tr = max(
                bar.high - bar.low,
                abs(bar.high - self._prev_close),
                abs(bar.low - self._prev_close),
            )
            self._values.append(tr)
        self._prev_close = bar.close
        return self.get_value()

    def get_value(self) -> float:
        if not self._values:
            return 0.0
        return sum(self._values) / len(self._values)

    def is_ready(self) -> bool:
        return len(self._values) >= self.period

    def reset(self):
        self._values.clear()
        self._prev_close = None


# ═══════════════════════════════════════════════════════════════
# 策略引擎
# ═══════════════════════════════════════════════════════════════

class MA26StrategyEngine(SystemModule):
    """
    MA26-ATR 趋势策略引擎。

    继承 SystemModule，实现统一生命周期契约。
    通过 EventBus 发布信号事件，不直接调用其他模块。
    """

    MODULE_NAME = "M01_StrategyEngine"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = ["M04_RiskGuard", "M07_ExecutionManager"]

    def __init__(self):
        super().__init__()
        self._ma26 = MA26Calculator()
        self._atr14 = ATR14Calculator()
        self._state = IndicatorState()
        self._position_side: str = "FLAT"   # FLAT / LONG / SHORT
        self._warmup_bars: int = 35
        self._bar_count: int = 0
        self._tape_band_mult: float = 0.5    # 检测带倍数
        self._breakout_mult: float = 0.3     # 脱离幅度倍数
        self._allow_short: bool = False
        self._last_signal_bar: int = -999
        self._signal_dedup_bars: int = 3
        self._signals_generated: int = 0
        self._bars_processed: int = 0

    # ─── 生命周期 ───

    def configure(self, config) -> Result[None, Error]:
        cs = config.get('strategy', {})
        self._ma26.period = cs.get('ma_period', 26)
        self._atr14.period = cs.get('atr_period', 14)
        self._warmup_bars = cs.get('warmup_bars', self._ma26.period + 10)
        self._tape_band_mult = cs.get('tape_band_multiplier', 0.5)
        self._breakout_mult = cs.get('breakout_multiplier', 0.3)
        self._allow_short = cs.get('allow_short', False)
        self._signal_dedup_bars = cs.get('signal_dedup_bars', 3)

        # 重建计算器（周期可能变了）
        self._ma26 = MA26Calculator(self._ma26.period)
        self._atr14 = ATR14Calculator(self._atr14.period)
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        if self._event_bus:
            self._event_bus.subscribe(EventType.BAR_PROCESSED, self._on_bar_event)
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        self._ma26.reset()
        self._atr14.reset()
        return Result.ok(None)

    # ─── 指标计算 ───

    def update_indicators(self, bar: Bar) -> Result[IndicatorState, Error]:
        """更新 MA26 / ATR14 / 检测带状态"""
        if bar.close <= 0:
            return Result.err(Error(
                "INVALID_BAR", f"Bar close must be positive, got {bar.close}",
                ErrorSeverity.RECOVERABLE, self.MODULE_NAME,
                context={'bar_index': bar.bar_index},
            ))

        ma = self._ma26.update(bar)
        atr = self._atr14.update(bar)

        self._state.ma26 = ma
        self._state.atr14 = atr
        self._state.upper_band = ma + self._tape_band_mult * atr
        self._state.lower_band = ma - self._tape_band_mult * atr
        self._state.is_tape = self._state.lower_band <= bar.close <= self._state.upper_band

        if bar.close > ma:
            self._state.direction = "BULL"
        elif bar.close < ma:
            self._state.direction = "BEAR"
        else:
            self._state.direction = "FLAT"

        return Result.ok(self._state)

    def get_state(self) -> IndicatorState:
        return self._state

    def is_ready(self) -> bool:
        """指标是否完成预热"""
        return (self._ma26.is_ready() and self._atr14.is_ready() and
                self._bar_count >= self._warmup_bars)

    # ─── 信号评估 ───

    def evaluate(self, bar: Bar, ctx: Optional[TraceContext] = None) -> Result[List[SignalRequest], Error]:
        """
        评估当前K线，生成交易信号。

        返回 Result[List[SignalRequest], Error]。
        空列表表示无信号（贴线内或预热中）。
        """
        self._bar_count += 1
        self._bars_processed += 1

        # 更新指标
        state_result = self.update_indicators(bar)
        if state_result.is_err():
            return Result.err(state_result.error)

        state = state_result.unwrap()

        # 预热期不交易
        if not self.is_ready():
            return Result.ok([])

        signals: List[SignalRequest] = []
        trace_id = ctx.trace_id if ctx else ""

        # 去重：同一方向近期已发过信号
        if bar.bar_index - self._last_signal_bar <= self._signal_dedup_bars:
            return Result.ok([])

        # ── 判定逻辑 ──
        close = bar.close

        if state.direction == "BULL":
            # 贴线脱离（向上突破上轨）
            breakout = close - state.upper_band
            if state.is_tape and breakout >= self._breakout_mult * state.atr14:
                if self._position_side != "LONG":
                    sig = SignalRequest(
                        signal_id=f"sig_{bar.bar_index}_OPEN_LONG",
                        action="OPEN_LONG",
                        direction="BULL",
                        qty=0.0,
                        price=close,
                        order_type="LIMIT",
                        strength=min(breakout / (state.atr14 + 1e-12), 5.0),
                        trace_id=trace_id,
                        bar_index=bar.bar_index,
                        extra={'ma26': state.ma26, 'atr14': state.atr14,
                               'breakout': breakout},
                    )
                    signals.append(sig)
                    self._last_signal_bar = bar.bar_index
                    self._position_side = "LONG"

            # 多头持仓中，跌破下轨 → 平仓
            elif self._position_side == "LONG" and close < state.lower_band:
                sig = SignalRequest(
                    signal_id=f"sig_{bar.bar_index}_CLOSE_LONG",
                    action="CLOSE_LONG",
                    direction="BULL",
                    qty=0.0,
                    price=close,
                    order_type="LIMIT",
                    trace_id=trace_id,
                    bar_index=bar.bar_index,
                )
                signals.append(sig)
                self._last_signal_bar = bar.bar_index
                self._position_side = "FLAT"

        elif state.direction == "BEAR":
            breakout = state.lower_band - close
            if state.is_tape and breakout >= self._breakout_mult * state.atr14:
                if self._allow_short and self._position_side != "SHORT":
                    sig = SignalRequest(
                        signal_id=f"sig_{bar.bar_index}_OPEN_SHORT",
                        action="OPEN_SHORT",
                        direction="BEAR",
                        qty=0.0,
                        price=close,
                        order_type="LIMIT",
                        strength=min(breakout / (state.atr14 + 1e-12), 5.0),
                        trace_id=trace_id,
                        bar_index=bar.bar_index,
                        extra={'ma26': state.ma26, 'atr14': state.atr14,
                               'breakout': breakout},
                    )
                    signals.append(sig)
                    self._last_signal_bar = bar.bar_index
                    self._position_side = "SHORT"

            elif self._position_side == "SHORT" and close > state.upper_band:
                sig = SignalRequest(
                    signal_id=f"sig_{bar.bar_index}_CLOSE_SHORT",
                    action="CLOSE_SHORT",
                    direction="BEAR",
                    qty=0.0,
                    price=close,
                    order_type="LIMIT",
                    trace_id=trace_id,
                    bar_index=bar.bar_index,
                )
                signals.append(sig)
                self._last_signal_bar = bar.bar_index
                self._position_side = "FLAT"

        # 发布信号事件
        if signals and self._event_bus:
            for sig in signals:
                self._event_bus.publish_from(
                    self.MODULE_NAME, EventType.SIGNAL_GENERATED,
                    trace_id=sig.trace_id,
                    bar_index=bar.bar_index,
                    signal=sig.__dict__,
                )
                self._signals_generated += 1

        return Result.ok(signals)

    def _on_bar_event(self, event):
        """事件总线回调：BAR_PROCESSED"""
        bar_data = event.data.get('bar')
        if not bar_data:
            return
        bar = Bar(**bar_data)
        ctx = TraceContext(trace_id=event.trace_id, bar_index=event.bar_index)
        self.evaluate(bar, ctx)

    # ─── 仓位反馈 ───

    def notify_position_closed(self):
        """外部通知仓位已平仓，重置方向状态"""
        self._position_side = "FLAT"

    def set_position_side(self, side: str):
        """由M05持仓同步（恢复快照时调用）"""
        if side in ("FLAT", "LONG", "SHORT"):
            self._position_side = side

    # ─── 状态导出/导入 ───

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({
            'position_side': self._position_side,
            'bar_count': self._bar_count,
            'last_signal_bar': self._last_signal_bar,
            'signals_generated': self._signals_generated,
            'bars_processed': self._bars_processed,
            'indicators': {
                'ma26': self._state.ma26,
                'atr14': self._state.atr14,
                'direction': self._state.direction,
                'is_tape': self._state.is_tape,
            },
        })
        return base

    def state_import(self, state: dict) -> Result[None, Error]:
        self._position_side = state.get('position_side', 'FLAT')
        self._bar_count = state.get('bar_count', 0)
        self._last_signal_bar = state.get('last_signal_bar', -999)
        self._signals_generated = state.get('signals_generated', 0)
        self._bars_processed = state.get('bars_processed', 0)
        return Result.ok(None)

    # ─── 健康检查 ───

    def health_check(self) -> dict:
        base = super().health_check()
        base['details'].update({
            'ma26_ready': self._ma26.is_ready(),
            'atr14_ready': self._atr14.is_ready(),
            'is_ready': self.is_ready(),
            'position_side': self._position_side,
            'bars_processed': self._bars_processed,
            'signals_generated': self._signals_generated,
            'direction': self._state.direction,
            'is_tape': self._state.is_tape,
            'ma26': round(self._state.ma26, 4),
            'atr14': round(self._state.atr14, 4),
        })
        return base

