"""
src/modules/strategy/ma26_strategy.py
MA26-ATR 量化交易系统 — 策略模块：MA26-ATR 趋势策略

策略逻辑:
  1. 趋势判断：价格与 MA26 的关系 + 趋势强度过滤
  2. 入场信号：价格偏离 MA 超过 band_multiplier × ATR（突破通道）
  3. 出场信号：价格回归 MA 或 detach 条件触发
  4. 仓位管理：基于 ATR 动态计算仓位大小
  5. 与风控模块联动（下单前必须通过 m04_risk 校验）

  信号生成流程:
    on_kline_update()
      → 更新指标（MA26, ATR）
      → 计算通道上下轨
      → 检测突破/回归
      → 生成 Signal 事件 → EventBus
      → 风控模块消费 → 执行模块消费
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import math

from src.core.result import Result, Error, ErrorSeverity, ErrorCategory
from src.core.lifecycle import LifecycleAware
from src.core.event_bus import EventBus, Event, EventType, get_global_event_bus
from src.core.state_registry import get_global_state_registry
from src.core.config import get_global_config_manager, get_config


# ═══════════════════════════════════════════════════════════════
# 信号类型
# ═══════════════════════════════════════════════════════════════

class SignalType(Enum):
    """交易信号类型"""
    BUY = "buy"                    # 做多入场
    SELL = "sell"                  # 做空入场
    CLOSE_LONG = "close_long"      # 平多
    CLOSE_SHORT = "close_short"    # 平空
    STOP_LOSS = "stop_loss"        # 止损
    TAKE_PROFIT = "take_profit"    # 止盈


class SignalStrength(Enum):
    """信号强度"""
    WEAK = 1        # 弱信号（观望或小仓位）
    MODERATE = 2    # 中等信号
    STRONG = 3      # 强信号（满仓或加仓）


# ═══════════════════════════════════════════════════════════════
# 信号数据类
# ═══════════════════════════════════════════════════════════════

@dataclass
class TradeSignal:
    """
    交易信号 — 策略生成的交易指令。
    """
    signal_type: SignalType
    symbol: str
    price: float
    timestamp: int                    # 毫秒时间戳
    strength: SignalStrength = SignalStrength.MODERATE

    # 指标快照
    ma26: float = 0.0
    atr: float = 0.0
    upper_band: float = 0.0
    lower_band: float = 0.0

    # 仓位建议
    suggested_size: float = 0.0       # 建议仓位大小（基础货币）
    suggested_size_quote: float = 0.0 # 建议仓位大小（计价货币）

    # 风控参数
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0

    # 元数据
    reason: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'signal_type': self.signal_type.value,
            'symbol': self.symbol,
            'price': self.price,
            'timestamp': self.timestamp,
            'strength': self.strength.value,
            'ma26': self.ma26,
            'atr': self.atr,
            'upper_band': self.upper_band,
            'lower_band': self.lower_band,
            'suggested_size': self.suggested_size,
            'stop_loss_price': self.stop_loss_price,
            'take_profit_price': self.take_profit_price,
            'reason': self.reason,
        }


# ═══════════════════════════════════════════════════════════════
# K线数据
# ═══════════════════════════════════════════════════════════════

@dataclass
class Kline:
    """
    K线数据。
    """
    symbol: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def typical_price(self) -> float:
        """典型价格 (H+L+C)/3"""
        return (self.high + self.low + self.close) / 3

    def hl2(self) -> float:
        """(H+L)/2"""
        return (self.high + self.low) / 2

    def hlc3(self) -> float:
        """(H+L+C)/3"""
        return self.typical_price()

    def ohlc4(self) -> float:
        """(O+H+L+C)/4"""
        return (self.open + self.high + self.low + self.close) / 4


# ═══════════════════════════════════════════════════════════════
# 指标计算
# ═══════════════════════════════════════════════════════════════

class IndicatorEngine:
    """
    指标计算引擎 — 纯函数式指标计算。
    不依赖任何外部状态，便于测试和回测。
    """

    @staticmethod
    def sma(values: List[float], period: int) -> Optional[float]:
        """简单移动平均"""
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    @staticmethod
    def ema(values: List[float], period: int) -> Optional[float]:
        """指数移动平均"""
        if len(values) < period:
            return None
        multiplier = 2.0 / (period + 1)
        ema_val = sum(values[:period]) / period
        for price in values[period:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    @staticmethod
    def atr(klines: List[Kline], period: int) -> Optional[float]:
        """
        平均真实波幅 (ATR)。
        使用 Wilder 平滑法。
        """
        if len(klines) < period:
            return None

        tr_values = []
        for i in range(1, len(klines)):
            high = klines[i].high
            low = klines[i].low
            prev_close = klines[i-1].close
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
            tr_values.append(tr)

        if len(tr_values) < period:
            return None

        # Wilder 平滑
        atr_val = sum(tr_values[:period]) / period
        for tr in tr_values[period:]:
            atr_val = (atr_val * (period - 1) + tr) / period

        return atr_val

    @staticmethod
    def true_range(kline: Kline, prev_close: Optional[float] = None) -> float:
        """真实波幅 (单根)"""
        if prev_close is None:
            return kline.high - kline.low
        return max(
            kline.high - kline.low,
            abs(kline.high - prev_close),
            abs(kline.low - prev_close),
        )

    @staticmethod
    def rsi(closes: List[float], period: int = 14) -> Optional[float]:
        """相对强弱指数 (RSI)"""
        if len(closes) < period + 1:
            return None

        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        if len(gains) < period:
            return None

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1 + rs))

    @staticmethod
    def trend_strength(klines: List[Kline], ma_period: int = 26) -> float:
        """
        计算趋势强度 [0, 1]。
        基于价格与 MA 的距离 + 连续同向K线数量。
        """
        if len(klines) < ma_period:
            return 0.0

        closes = [k.close for k in klines]
        ma = IndicatorEngine.sma(closes, ma_period)
        if ma is None or ma == 0:
            return 0.0

        current_price = closes[-1]
        distance = abs(current_price - ma) / ma

        # 连续同向计数
        consecutive = 0
        direction = 1 if current_price > ma else -1
        for i in range(len(closes) - 1, 0, -1):
            if (closes[i] > ma and direction > 0) or (closes[i] < ma and direction < 0):
                consecutive += 1
            else:
                break

        # 综合评分
        distance_score = min(distance / 0.05, 1.0)   # 5% 偏离 = 满分
        consecutive_score = min(consecutive / 10, 1.0) # 10根连续 = 满分

        return (distance_score * 0.6 + consecutive_score * 0.4)


# ═══════════════════════════════════════════════════════════════
# MA26-ATR 策略模块
# ═══════════════════════════════════════════════════════════════

class MA26Strategy(LifecycleAware):
    """
    MA26-ATR 趋势策略模块。

    策略规则:
      做多条件:
        1. 价格 > MA26（趋势向上）
        2. 趋势强度 > trend_strength 阈值
        3. 价格从下方突破 lower_band（均值回归）或 upper_band（动量突破）

      做空条件:
        1. 价格 < MA26（趋势向下）
        2. 趋势强度 > trend_strength 阈值
        3. 价格从上方突破 upper_band

      平仓条件:
        1. 价格回归 MA26 附近（detach 以内）
        2. 或止损触发
        3. 或趋势反转

    仓位计算:
      size = (equity × risk_per_trade) / ATR
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        ma_period: int = 26,
        atr_period: int = 14,
        band_multiplier: float = 0.5,
        detach: float = 0.3,
        trend_strength: float = 0.6,
        risk_per_trade: float = 0.02,
        max_position_size: float = 10000.0,
        kline_max_bars: int = 500,
    ):
        self._symbol = symbol
        self._ma_period = ma_period
        self._atr_period = atr_period
        self._band_multiplier = band_multiplier
        self._detach = detach
        self._trend_strength = trend_strength
        self._risk_per_trade = risk_per_trade
        self._max_position_size = max_position_size
        self._kline_max_bars = kline_max_bars

        # 运行时状态
        self._klines: List[Kline] = []
        self._running = False
        self._lock = threading.RLock()
        self._position = 0          # 0=空仓, >0=多仓, <0=空仓
        self._entry_price = 0.0
        self._entry_time = 0
        self._last_signal: Optional[TradeSignal] = None
        self._signal_count = 0
        self._last_kline_time = 0

        # 外部依赖
        self._event_bus: Optional[EventBus] = None
        self._config_mgr = None

    # ─── LifecycleAware 接口 ─────────────────────────────────

    @property
    def module_name(self) -> str:
        return "m01_strategy"

    @property
    def dependencies(self) -> List[str]:
        return ["core_config", "core_event_bus", "core_state"]

    @property
    def init_timeout_ms(self) -> int:
        return 15000

    @property
    def start_timeout_ms(self) -> int:
        return 10000

    @property
    def drain_timeout_ms(self) -> int:
        return 5000

    def on_init(self) -> Result[None]:
        """初始化策略"""
        # 获取配置
        try:
            self._config_mgr = get_global_config_manager()
        except Exception:
            pass

        if self._config_mgr:
            self._ma_period = self._config_mgr.get_int("strategy.ma_period", self._ma_period)
            self._atr_period = self._config_mgr.get_int("strategy.atr_period", self._atr_period)
            self._band_multiplier = self._config_mgr.get_float("strategy.band_multiplier", self._band_multiplier)
            self._detach = self._config_mgr.get_float("strategy.detach", self._detach)
            self._trend_strength = self._config_mgr.get_float("strategy.trend_strength", self._trend_strength)
            self._risk_per_trade = self._config_mgr.get_float("risk.max_position_risk", self._risk_per_trade)
            self._max_position_size = self._config_mgr.get_float("execution.max_position_size", self._max_position_size)
            self._symbol = self._config_mgr.get_str("strategy.symbol", self._symbol)
            self._kline_max_bars = self._config_mgr.get_int("kline.max_bars", self._kline_max_bars)

        # 获取事件总线
        try:
            self._event_bus = get_global_event_bus()
        except Exception:
            pass

        return Result.ok(None)

    def on_start(self) -> Result[None]:
        """启动策略"""
        self._running = True

        # 订阅 K线更新事件
        if self._event_bus:
            self._event_bus.subscribe(
                EventType.KLINE_UPDATE,
                self._on_kline_event,
            )
            self._event_bus.subscribe(
                EventType.TRADE_UPDATE,
                self._on_trade_event,
            )

        return Result.ok(None)

    def on_drain(self) -> Result[None]:
        """优雅停机：停止处理新信号"""
        self._running = False
        return Result.ok(None)

    def on_stop(self) -> Result[None]:
        """停止策略"""
        self._running = False

        # 取消订阅
        if self._event_bus:
            try:
                self._event_bus.unsubscribe(
                    EventType.KLINE_UPDATE,
                    self._on_kline_event,
                )
            except Exception:
                pass

        # 清空数据
        with self._lock:
            self._klines.clear()

        return Result.ok(None)

    def on_health_check(self) -> Result[dict]:
        return Result.ok({
            "running": self._running,
            "kline_count": len(self._klines),
            "position": self._position,
            "entry_price": self._entry_price,
            "signal_count": self._signal_count,
            "last_kline_time": self._last_kline_time,
        })

    # ─── 事件处理 ─────────────────────────────────────────────

    def _on_kline_event(self, event: Event):
        """处理 K线更新事件"""
        if not self._running:
            return

        payload = event.payload
        try:
            kline = Kline(
                symbol=payload.get('symbol', self._symbol),
                open_time=payload.get('open_time', 0),
                close_time=payload.get('close_time', 0),
                open=float(payload.get('open', 0)),
                high=float(payload.get('high', 0)),
                low=float(payload.get('low', 0)),
                close=float(payload.get('close', 0)),
                volume=float(payload.get('volume', 0)),
            )
            self._process_kline(kline)
        except Exception as e:
            pass  # 静默处理，避免影响事件总线

    def _on_trade_event(self, event: Event):
        """处理成交更新事件（同步持仓状态）"""
        payload = event.payload
        side = payload.get('side', '')
        size = float(payload.get('size', 0))
        price = float(payload.get('price', 0))

        with self._lock:
            if side == 'buy':
                self._position += size
                if self._position > 0 and self._entry_price == 0:
                    self._entry_price = price
                    self._entry_time = int(time.time() * 1000)
            elif side == 'sell':
                self._position -= size
                if self._position <= 0:
                    self._entry_price = 0.0
                    self._entry_time = 0

    # ─── 核心策略逻辑 ─────────────────────────────────────────

    def _process_kline(self, kline: Kline):
        """处理新 K线"""
        with self._lock:
            self._klines.append(kline)
            # 限制最大数量
            if len(self._klines) > self._kline_max_bars:
                self._klines = self._klines[-self._kline_max_bars:]
            self._last_kline_time = kline.close_time

        # 需要足够数据
        if len(self._klines) < max(self._ma_period, self._atr_period) + 5:
            return

        # 计算指标
        closes = [k.close for k in self._klines]
        ma26 = IndicatorEngine.sma(closes, self._ma_period)
        atr = IndicatorEngine.atr(self._klines, self._atr_period)
        trend_str = IndicatorEngine.trend_strength(self._klines, self._ma_period)

        if ma26 is None or atr is None:
            return

        current_price = kline.close

        # 计算通道
        upper_band = ma26 + self._band_multiplier * atr
        lower_band = ma26 - self._band_multiplier * atr

        # 生成信号
        signal = self._evaluate_signal(
            current_price=current_price,
            ma26=ma26,
            atr=atr,
            upper_band=upper_band,
            lower_band=lower_band,
            trend_strength=trend_str,
            kline=kline,
        )

        if signal:
            self._emit_signal(signal)

    def _evaluate_signal(
        self,
        current_price: float,
        ma26: float,
        atr: float,
        upper_band: float,
        lower_band: float,
        trend_strength: float,
        kline: Kline,
    ) -> Optional[TradeSignal]:
        """
        评估是否产生交易信号。
        """
        timestamp = int(time.time() * 1000)

        # 已有持仓时的管理
        if self._position > 0:
            # 多仓管理
            # 1. 止损检查
            stop_loss = self._entry_price - 2 * atr
            if current_price <= stop_loss:
                return TradeSignal(
                    signal_type=SignalType.STOP_LOSS,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    stop_loss_price=stop_loss,
                    reason=f"Stop loss triggered: {current_price} <= {stop_loss:.2f}",
                )

            # 2. 止盈检查
            take_profit = self._entry_price + 3 * atr
            if current_price >= take_profit:
                return TradeSignal(
                    signal_type=SignalType.TAKE_PROFIT,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    take_profit_price=take_profit,
                    reason=f"Take profit triggered: {current_price} >= {take_profit:.2f}",
                )

            # 3. 回归平仓
            distance_from_ma = abs(current_price - ma26) / ma26
            if distance_from_ma < self._detach * 0.5:
                return TradeSignal(
                    signal_type=SignalType.CLOSE_LONG,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    reason=f"Price returned to MA (detach={distance_from_ma:.4f})",
                )

            return None  # 持仓中，无新信号

        elif self._position < 0:
            # 空仓管理
            stop_loss = self._entry_price + 2 * atr
            if current_price >= stop_loss:
                return TradeSignal(
                    signal_type=SignalType.STOP_LOSS,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    stop_loss_price=stop_loss,
                    reason=f"Stop loss triggered: {current_price} >= {stop_loss:.2f}",
                )

            take_profit = self._entry_price - 3 * atr
            if current_price <= take_profit:
                return TradeSignal(
                    signal_type=SignalType.TAKE_PROFIT,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    take_profit_price=take_profit,
                    reason=f"Take profit triggered: {current_price} <= {take_profit:.2f}",
                )

            distance_from_ma = abs(current_price - ma26) / ma26
            if distance_from_ma < self._detach * 0.5:
                return TradeSignal(
                    signal_type=SignalType.CLOSE_SHORT,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    reason=f"Price returned to MA (detach={distance_from_ma:.4f})",
                )

            return None

        else:
            # 空仓 → 寻找入场机会
            # 趋势强度过滤
            if trend_strength < self._trend_strength:
                return None

            # 做多条件：价格 > MA26 且突破上轨
            if current_price > ma26 and current_price >= upper_band:
                strength = SignalStrength.STRONG if trend_strength > 0.8 else SignalStrength.MODERATE
                suggested_size = self._calculate_position_size(current_price, atr)

                return TradeSignal(
                    signal_type=SignalType.BUY,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    strength=strength,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    suggested_size=suggested_size,
                    stop_loss_price=current_price - 2 * atr,
                    take_profit_price=current_price + 3 * atr,
                    reason=f"Bullish breakout: price={current_price:.2f} > upper_band={upper_band:.2f}, trend={trend_strength:.2f}",
                )

            # 做空条件：价格 < MA26 且突破下轨
            if current_price < ma26 and current_price <= lower_band:
                strength = SignalStrength.STRONG if trend_strength > 0.8 else SignalStrength.MODERATE
                suggested_size = self._calculate_position_size(current_price, atr)

                return TradeSignal(
                    signal_type=SignalType.SELL,
                    symbol=self._symbol,
                    price=current_price,
                    timestamp=timestamp,
                    strength=strength,
                    ma26=ma26,
                    atr=atr,
                    upper_band=upper_band,
                    lower_band=lower_band,
                    suggested_size=suggested_size,
                    stop_loss_price=current_price + 2 * atr,
                    take_profit_price=current_price - 3 * atr,
                    reason=f"Bearish breakout: price={current_price:.2f} < lower_band={lower_band:.2f}, trend={trend_strength:.2f}",
                )

        return None

    def _calculate_position_size(self, price: float, atr: float) -> float:
        """
        基于 ATR 的仓位计算。
        size = (equity × risk_per_trade) / ATR_dollar
        """
        equity = 100000.0  # 默认权益，实际应从组合模块获取
        if self._config_mgr:
            equity = self._config_mgr.get_float("initial_equity", equity)

        if atr <= 0 or price <= 0:
            return 0.0

        # ATR 美元价值
        atr_dollar = atr

        # 风险金额
        risk_amount = equity * self._risk_per_trade

        # 基础仓位（按 ATR 风险计算）
        size = risk_amount / atr_dollar

        # 限制最大仓位
        max_size = self._max_position_size / price
        size = min(size, max_size)

        # 最小交易单位
        min_size = 0.001
        if size < min_size:
            return 0.0

        return round(size, 3)

    def _emit_signal(self, signal: TradeSignal):
        """发布信号事件"""
        self._last_signal = signal
        self._signal_count += 1

        if self._event_bus:
            self._event_bus.publish(Event(
                event_type=EventType.SIGNAL_GENERATED,
                source="m01_strategy",
                payload=signal.to_dict(),
            ))

    # ─── 公共接口（供测试和外部调用） ──────────────────────────

    def feed_kline(self, kline: Kline) -> Optional[TradeSignal]:
        """
        手动喂入 K线（用于回测和测试）。
        返回生成的信号（如果有）。
        """
        self._process_kline(kline)
        return self._last_signal if self._last_signal and self._last_signal.timestamp >= kline.close_time else None

    def get_current_indicators(self) -> dict:
        """获取当前指标值"""
        if len(self._klines) < max(self._ma_period, self._atr_period):
            return {}

        closes = [k.close for k in self._klines]
        ma26 = IndicatorEngine.sma(closes, self._ma_period)
        atr = IndicatorEngine.atr(self._klines, self._atr_period)
        trend_str = IndicatorEngine.trend_strength(self._klines, self._ma_period)

        if ma26 is None or atr is None:
            return {}

        return {
            'ma26': round(ma26, 4),
            'atr': round(atr, 4),
            'upper_band': round(ma26 + self._band_multiplier * atr, 4),
            'lower_band': round(ma26 - self._band_multiplier * atr, 4),
            'trend_strength': round(trend_str, 4),
            'current_price': self._klines[-1].close if self._klines else 0,
            'position': self._position,
        }

    def reset(self):
        """重置策略状态"""
        with self._lock:
            self._klines.clear()
            self._position = 0
            self._entry_price = 0.0
            self._entry_time = 0
            self._last_signal = None
            self._signal_count = 0
