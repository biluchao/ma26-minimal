"""
src/m03_kline/kline_cache.py
M03 K线缓存与指标预计算模块

职责:
- 接收tick/原始K线，聚合到目标周期（默认3分钟）
- 维护滑动窗口（最近N根K线，支持MA26+ATR14计算）
- 缓存MA26/ATR14/检测带计算结果，避免重复计算
- 提供线程安全的K线读取接口
- 对接事件总线：发布K线更新/关闭事件

硬约束:
- H-K-01: K线聚合时间戳必须对齐到interval边界（毫秒级）
- H-K-02: 滑动窗口满时自动淘汰最旧K线，不可无限增长
- H-K-03: 指标缓存必须在K线关闭后标记为dirty，下次读取时重算
- H-K-04: warmup()仅允许在模块STOPPED状态调用
- H-K-05: 单symbol最大K线数不超过window_size+1（历史+当前）
"""

import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List

from core import (
    SystemModule, Result, Error, ErrorSeverity,
    ConfigSnapshot, EventType, TraceContext,
)


@dataclass
class Candle:
    """单根K线数据"""
    timestamp: int      # 开盘时间戳（毫秒，对齐到interval边界）
    open: float
    high: float
    low: float
    close: float
    volume: float
    closed: bool = False

    def update(self, price: float, volume_delta: float,
               high: float = None, low: float = None):
        """用tick数据更新当前K线"""
        self.close = price
        self.high = max(self.high, high if high is not None else price)
        self.low = min(self.low, low if low is not None else price)
        self.volume += volume_delta

    def typical_price(self) -> float:
        """典型价格 = (H+L+C)/3"""
        return (self.high + self.low + self.close) / 3.0

    def true_range(self, prev_close: float = None) -> float:
        """True Range计算"""
        if prev_close is None:
            return self.high - self.low
        return max(
            self.high - self.low,
            abs(self.high - prev_close),
            abs(self.low - prev_close),
        )

    def to_dict(self) -> dict:
        """序列化为dict"""
        return {
            't': self.timestamp,
            'o': self.open,
            'h': self.high,
            'l': self.low,
            'c': self.close,
            'v': self.volume,
            'closed': self.closed,
        }


class KlineCache(SystemModule):
    """
    K线缓存模块。

    启动序列:
    1. configure() — 加载配置
    2. start() → _do_start() — 初始化内部状态
    3. warmup() — 从历史数据预热（可选）
    4. on_tick() — 持续接收tick聚合

    停止序列:
    1. stop() → _do_stop() — 清空所有缓存
    """

    MODULE_NAME = "M03_KlineCache"
    MODULE_VERSION = "1.0.0"
    DEPENDENCIES = []

    def __init__(self):
        super().__init__()
        # symbol → 历史K线队列（maxlen=window_size）
        self._history: dict[str, deque] = {}
        # symbol → 当前未关闭的K线
        self._current: dict[str, Candle] = {}
        # 配置
        self._window_size: int = 200
        self._interval_ms: int = 3 * 60 * 1000  # 默认3分钟
        self._max_symbols: int = 50
        # 指标缓存
        self._ma26: dict[str, float] = {}
        self._atr14: dict[str, float] = {}
        self._band_upper: dict[str, float] = {}
        self._band_lower: dict[str, float] = {}
        self._dirty: dict[str, bool] = {}
        # 统计
        self._tick_count: int = 0
        self._candle_close_count: int = 0
        # 线程锁
        self._lock = threading.RLock()

    # ─── 生命周期 ───────────────────────────────────────────────

    def configure(self, config: ConfigSnapshot) -> Result[None, Error]:
        """加载配置"""
        self._window_size = config.get('kline.window_size', 200)
        interval_min = config.get('kline.interval_minutes', 3)
        self._interval_ms = interval_min * 60 * 1000
        self._max_symbols = config.get('kline.max_symbols', 50)

        if self._window_size < 26:
            return Result.err(Error(
                "INVALID_WINDOW_SIZE",
                f"window_size must be >= 26 for MA26, got {self._window_size}",
                ErrorSeverity.FATAL,
                self.MODULE_NAME,
            ))

        if self._interval_ms <= 0:
            return Result.err(Error(
                "INVALID_INTERVAL",
                f"interval_ms must be positive, got {self._interval_ms}",
                ErrorSeverity.FATAL,
                self.MODULE_NAME,
            ))

        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        """启动初始化"""
        self._tick_count = 0
        self._candle_close_count = 0
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        """停止清理"""
        with self._lock:
            self._history.clear()
            self._current.clear()
            self._ma26.clear()
            self._atr14.clear()
            self._band_upper.clear()
            self._band_lower.clear()
            self._dirty.clear()
            self._tick_count = 0
            self._candle_close_count = 0
        return Result.ok(None)

    # ─── 核心接口 ───────────────────────────────────────────────

    def on_tick(self, symbol: str, price: float, volume_delta: float,
                timestamp_ms: int = None,
                ctx: TraceContext = None) -> Result[Candle, Error]:
        """
        接收tick数据，聚合到当前K线。

        Args:
            symbol: 交易对
            price: 成交价
            volume_delta: 本次tick成交量
            timestamp_ms: 时间戳（毫秒，默认当前时间）
            ctx: 链路追踪

        Returns:
            Result[Candle, Error] — 当前K线引用
        """
        if price <= 0:
            return Result.err(Error(
                "INVALID_PRICE", f"price must be positive, got {price}",
                ErrorSeverity.RECOVERABLE, self.MODULE_NAME,
            ))

        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)

        with self._lock:
            # 符号数量限制
            if symbol not in self._history:
                if len(self._history) >= self._max_symbols:
                    return Result.err(Error(
                        "MAX_SYMBOLS_EXCEEDED",
                        f"Max symbols {self._max_symbols} reached",
                        ErrorSeverity.RECOVERABLE,
                        self.MODULE_NAME,
                    ))
                self._history[symbol] = deque(maxlen=self._window_size)
                self._dirty[symbol] = True

            # 计算当前K线起始时间（对齐到interval边界）
            bucket_start = (timestamp_ms // self._interval_ms) * self._interval_ms

            current = self._current.get(symbol)

            # 需要新K线
            if current is None or current.timestamp != bucket_start:
                # 关闭旧K线
                if current is not None:
                    current.closed = True
                    self._history[symbol].append(current)
                    self._candle_close_count += 1
                    self._dirty[symbol] = True

                    # 发布K线关闭事件
                    if self._event_bus:
                        self._event_bus.publish_from(
                            self.MODULE_NAME, EventType.KLINE_CLOSED,
                            symbol=symbol,
                            candle=current.to_dict(),
                            trace_id=ctx.trace_id if ctx else "",
                        )

                # 创建新K线
                self._current[symbol] = Candle(
                    timestamp=bucket_start,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=volume_delta,
                    closed=False,
                )
            else:
                # 更新当前K线
                current.update(price, volume_delta)

            self._tick_count += 1

            # 发布tick事件（轻量，不阻塞）
            if self._event_bus and self._tick_count % 100 == 0:
                self._event_bus.publish_from(
                    self.MODULE_NAME, EventType.TICK_PROCESSED,
                    symbol=symbol,
                    price=price,
                    volume_delta=volume_delta,
                    trace_id=ctx.trace_id if ctx else "",
                )

            return Result.ok(self._current[symbol])

    def warmup(self, symbol: str,
               candles: List[Candle]) -> Result[None, Error]:
        """
        从历史K线预热缓存。

        硬约束 H-K-04: 仅允许在STOPPED状态调用。
        """
        if self.state != "STOPPED":
            return Result.err(Error(
                "INVALID_STATE",
                f"warmup() requires STOPPED state, current: {self.state}",
                ErrorSeverity.RECOVERABLE,
                self.MODULE_NAME,
            ))

        if not candles:
            return Result.ok(None)

        with self._lock:
            if symbol in self._history and len(self._history[symbol]) > 0:
                return Result.err(Error(
                    "ALREADY_WARMED",
                    f"Symbol {symbol} already has {len(self._history[symbol])} candles",
                    ErrorSeverity.RECOVERABLE,
                    self.MODULE_NAME,
                ))

            self._history[symbol] = deque(
                candles[-self._window_size:],
                maxlen=self._window_size,
            )
            self._dirty[symbol] = True

            # 预计算指标
            self._compute_ma26_internal(symbol)
            self._compute_atr14_internal(symbol)

            if self._event_bus:
                self._event_bus.publish_from(
                    self.MODULE_NAME, EventType.ALERT_SENT,
                    level="INFO",
                    message=f"Warmed up {symbol}: {len(candles)} candles → {len(self._history[symbol])} cached",
                )

            return Result.ok(None)

    # ─── 读取接口 ───────────────────────────────────────────────

    def get_current(self, symbol: str) -> Optional[Candle]:
        """获取当前未关闭的K线（只读引用）"""
        with self._lock:
            candle = self._current.get(symbol)
            return candle

    def get_history(self, symbol: str, count: int = None) -> List[Candle]:
        """
        获取历史K线列表（不含当前未关闭的）。

        Args:
            symbol: 交易对
            count: 返回数量，None返回全部
        """
        with self._lock:
            hist = list(self._history.get(symbol, []))
            if count is not None:
                hist = hist[-count:]
            return hist

    def get_all_candles(self, symbol: str, count: int = None) -> List[Candle]:
        """获取所有K线（历史+当前未关闭）"""
        with self._lock:
            candles = list(self._history.get(symbol, []))
            current = self._current.get(symbol)
            if current:
                candles.append(current)
            if count is not None:
                candles = candles[-count:]
            return candles

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """获取最新价格"""
        with self._lock:
            current = self._current.get(symbol)
            if current:
                return current.close
            hist = self._history.get(symbol, [])
            if hist:
                return hist[-1].close
            return None

    def get_candle_count(self, symbol: str) -> int:
        """获取指定symbol的K线总数"""
        with self._lock:
            return len(self._history.get(symbol, [])) + (
                1 if symbol in self._current else 0
            )

    # ─── 指标计算 ───────────────────────────────────────────────

    def compute_ma26(self, symbol: str) -> Result[float, Error]:
        """
        计算MA26（26周期简单移动平均）。
        使用缓存，dirty时才重算。
        """
        with self._lock:
            if not self._dirty.get(symbol, True):
                if symbol in self._ma26:
                    return Result.ok(self._ma26[symbol])

            return self._compute_ma26_internal(symbol)

    def _compute_ma26_internal(self, symbol: str) -> Result[float, Error]:
        """内部MA26计算"""
        candles = self.get_all_candles(symbol, 26)
        if len(candles) < 26:
            return Result.err(Error(
                "INSUFFICIENT_DATA",
                f"MA26 needs 26 candles, have {len(candles)}",
                ErrorSeverity.RECOVERABLE,
                self.MODULE_NAME,
            ))

        closes = [c.close for c in candles[-26:]]
        ma26 = sum(closes) / 26.0
        self._ma26[symbol] = ma26
        return Result.ok(ma26)

    def compute_atr14(self, symbol: str) -> Result[float, Error]:
        """
        计算ATR14（14周期平均真实波幅）。
        使用缓存，dirty时才重算。
        """
        with self._lock:
            if not self._dirty.get(symbol, True):
                if symbol in self._atr14:
                    return Result.ok(self._atr14[symbol])

            return self._compute_atr14_internal(symbol)

    def _compute_atr14_internal(self, symbol: str) -> Result[float, Error]:
        """内部ATR14计算"""
        candles = self.get_all_candles(symbol, 15)  # 14个TR需要15根K线
        if len(candles) < 15:
            return Result.err(Error(
                "INSUFFICIENT_DATA",
                f"ATR14 needs 15 candles, have {len(candles)}",
                ErrorSeverity.RECOVERABLE,
                self.MODULE_NAME,
            ))

        tr_values = []
        for i in range(1, len(candles)):
            tr = candles[i].true_range(candles[i-1].close)
            tr_values.append(tr)

        if len(tr_values) >= 14:
            atr14 = sum(tr_values[-14:]) / 14.0
        else:
            atr14 = sum(tr_values) / len(tr_values)

        self._atr14[symbol] = atr14
        return Result.ok(atr14)

    def compute_detection_bands(self, symbol: str,
                                multiplier: float = 0.5
                                ) -> Result[tuple, Error]:
        """
        计算检测带: MA26 ± multiplier × ATR14

        Returns:
            Result[(upper_band, lower_band), Error]
        """
        ma_result = self.compute_ma26(symbol)
        if ma_result.is_err():
            return Result.err(ma_result.error)

        atr_result = self.compute_atr14(symbol)
        if atr_result.is_err():
            return Result.err(atr_result.error)

        ma26 = ma_result.unwrap()
        atr14 = atr_result.unwrap()

        upper = ma26 + multiplier * atr14
        lower = ma26 - multiplier * atr14

        self._band_upper[symbol] = upper
        self._band_lower[symbol] = lower

        return Result.ok((upper, lower))

    def is_price_in_band(self, symbol: str, price: float,
                         multiplier: float = 0.5) -> Result[bool, Error]:
        """判断价格是否在检测带内"""
        bands_result = self.compute_detection_bands(symbol, multiplier)
        if bands_result.is_err():
            return Result.err(bands_result.error)

        upper, lower = bands_result.unwrap()
        return Result.ok(lower <= price <= upper)

    def get_indicator_snapshot(self, symbol: str) -> dict:
        """获取指标快照（供策略模块快速读取）"""
        with self._lock:
            return {
                'symbol': symbol,
                'ma26': self._ma26.get(symbol, 0.0),
                'atr14': self._atr14.get(symbol, 0.0),
                'band_upper': self._band_upper.get(symbol, 0.0),
                'band_lower': self._band_lower.get(symbol, 0.0),
                'candle_count': self.get_candle_count(symbol),
                'dirty': self._dirty.get(symbol, False),
            }

    # ─── 管理接口 ───────────────────────────────────────────────

    def clear_symbol(self, symbol: str) -> Result[None, Error]:
        """清除指定symbol的所有数据"""
        with self._lock:
            self._history.pop(symbol, None)
            self._current.pop(symbol, None)
            self._ma26.pop(symbol, None)
            self._atr14.pop(symbol, None)
            self._band_upper.pop(symbol, None)
            self._band_lower.pop(symbol, None)
            self._dirty.pop(symbol, None)
            return Result.ok(None)

    def mark_clean(self, symbol: str):
        """标记指标缓存为干净（外部调用）"""
        with self._lock:
            self._dirty[symbol] = False

    def get_stats(self) -> dict:
        """模块运行统计"""
        with self._lock:
            return {
                'module': self.MODULE_NAME,
                'state': self.state,
                'total_symbols': len(self._history),
                'total_history_candles': sum(
                    len(h) for h in self._history.values()
                ),
                'current_open_candles': len(self._current),
                'tick_count': self._tick_count,
                'candle_close_count': self._candle_close_count,
                'indicator_cache': {
                    'ma26': len(self._ma26),
                    'atr14': len(self._atr14),
                    'bands': len(self._band_upper),
                },
                'dirty_symbols': sum(
                    1 for d in self._dirty.values() if d
                ),
            }
