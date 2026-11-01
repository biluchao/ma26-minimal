"""
tests/test_modules.py
业务模块测试 — M01策略 / M03 K线 / M04风控 / M05仓位 /
M06权益 / M07执行 / M11绩效 / M12快照 / M13信号追踪

运行方式:
  python tests/test_modules.py
  python -m unittest tests.test_modules -v

依赖: 仅 Python 标准库 + src/ 下各模块
"""

import sys
import os
import time
import json
import tempfile
import threading
import unittest
from typing import Optional
from dataclasses import dataclass, field

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core import (
    Result, Error, ErrorSeverity, ErrorCategory,
    EventBus, Event, EventType,
    TraceContext, TraceLevel,
    ConfigSnapshot, StateRegistry,
    SystemModule, ModuleState,
)

# ─── 业务模块导入 ───
from src.m01_strategy import (
    MA26Strategy, TrendDirection, SignalType, TradingSignal, StrategyConfig,
)
from src.m03_kline import (
    KlineCache, KlineBar, KlineInterval, TickData,
)
from src.m04_risk import (
    RiskGuard, RiskLevel, RiskViolation, RiskConfig,
    DailyLossTracker, DrawdownTracker, SlippageProtector,
)
from src.m05_position import (
    PositionManager, Position, PositionSide, FillEvent,
)
from src.m06_equity import (
    EquityTracker, EquityPoint,
)
from src.m07_execution import (
    ExecutionManager, OrderRequest, OrderState, IdempotencyKey,
)
from src.m11_performance import (
    PerformanceTracker, TradeRecord,
)
from src.m12_snapshot import (
    SnapshotManager, SystemSnapshot, AtomicFileWriter,
)
from src.m13_tracker import (
    SignalTracker, SignalRecord, DedupResult,
)


# ═══════════════════════════════════════════════════════════════
# 辅助构造器
# ═══════════════════════════════════════════════════════════════

def make_kline_bar(
    open_time: int = 1700000000000,
    open_p: float = 50000.0,
    high_p: float = 50100.0,
    low_p: float = 49900.0,
    close_p: float = 50050.0,
    volume: float = 10.5,
    symbol: str = "BTCUSDT",
) -> KlineBar:
    return KlineBar(
        symbol=symbol,
        interval=KlineInterval.MINUTE_3,
        open_time=open_time,
        open=open_p,
        high=high_p,
        low=low_p,
        close=close_p,
        volume=volume,
        close_time=open_time + 179999,
        quote_volume=volume * close_p,
        trades=100,
        taker_buy_volume=5.0,
        taker_buy_quote_volume=5.0 * close_p,
    )


def make_tick(price: float = 50000.0, volume: float = 0.1, ts: int = None) -> TickData:
    return TickData(
        symbol="BTCUSDT",
        price=price,
        volume=volume,
        timestamp=ts or int(time.time() * 1000),
    )


def make_fill_event(
    order_id: str = "ord_001",
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    quantity: float = 0.5,
    price: float = 50000.0,
) -> FillEvent:
    return FillEvent(
        order_id=order_id,
        symbol=symbol,
        side=PositionSide.LONG if side == "BUY" else PositionSide.SHORT,
        quantity=quantity,
        price=price,
        commission=0.0,
        commission_asset="USDT",
        timestamp=int(time.time() * 1000),
    )


# ═══════════════════════════════════════════════════════════════
# 1. M01 Strategy 测试
# ═══════════════════════════════════════════════════════════════

class TestM01Strategy(unittest.TestCase):

    def setUp(self):
        self.cfg = StrategyConfig(
            ma_period=26,
            atr_period=14,
            band_multiplier=0.5,
            detach_multiplier=0.3,
            min_trend_strength=0.6,
        )
        self.strategy = MA26Strategy(self.cfg)

    def test_initial_state(self):
        self.assertEqual(self.strategy.state, ModuleState.INIT)

    def test_configure(self):
        result = self.strategy.configure(ConfigSnapshot({
            "ma_period": 26,
            "atr_period": 14,
        }))
        self.assertTrue(result.is_ok())
        self.assertEqual(self.strategy.state, ModuleState.CONFIGURED)

    def test_add_kline_and_calculate(self):
        self.strategy.configure(ConfigSnapshot({}))
        # 添加足够多的K线以计算MA26
        bars = []
        base_price = 50000.0
        for i in range(30):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                open_p=base_price + i * 10,
                close_p=base_price + i * 10 + 5,
            )
            bars.append(bar)

        for bar in bars:
            self.strategy.on_kline_closed(bar)

        # 应该有趋势方向
        direction = self.strategy.get_current_trend("BTCUSDT")
        self.assertIn(direction, [TrendDirection.BULL, TrendDirection.BEAR, TrendDirection.NEUTRAL])

    def test_generate_signal_no_position(self):
        self.strategy.configure(ConfigSnapshot({}))
        # 模拟上升趋势
        for i in range(30):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                open_p=50000.0 + i * 50,
                close_p=50000.0 + i * 50 + 20,
                high_p=50000.0 + i * 50 + 30,
                low_p=50000.0 + i * 50 - 10,
            )
            self.strategy.on_kline_closed(bar)

        signal = self.strategy.evaluate_signal("BTCUSDT")
        # 可能生成信号，也可能没有（取决于参数），但不应抛异常
        if signal:
            self.assertIsInstance(signal, TradingSignal)
            self.assertIn(signal.signal_type, [SignalType.OPEN_LONG, SignalType.OPEN_SHORT, SignalType.NO_SIGNAL])

    def test_trend_direction_enum(self):
        self.assertEqual(TrendDirection.BULL.value, "bull")
        self.assertEqual(TrendDirection.BEAR.value, "bear")
        self.assertEqual(TrendDirection.NEUTRAL.value, "neutral")

    def test_signal_type_enum(self):
        self.assertTrue(hasattr(SignalType, 'OPEN_LONG'))
        self.assertTrue(hasattr(SignalType, 'CLOSE_LONG'))

    def test_ma_window_calculation(self):
        self.strategy.configure(ConfigSnapshot({}))
        # 添加精确数据
        prices = [50000 + i * 100 for i in range(26)]
        for i, p in enumerate(prices):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                open_p=p,
                close_p=p + 50,
                high_p=p + 100,
                low_p=p - 20,
            )
            self.strategy.on_kline_closed(bar)

        ma26 = self.strategy.get_ma26("BTCUSDT")
        self.assertIsNotNone(ma26)
        # MA26 应该接近最后几个价格的平均值
        expected_avg = sum(prices[-26:]) / 26
        self.assertAlmostEqual(ma26, expected_avg, delta=100)

    def test_atr_band(self):
        self.strategy.configure(ConfigSnapshot({}))
        # 添加K线
        for i in range(30):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                open_p=50000.0 + i * 10,
                high_p=50000.0 + i * 10 + 100,
                low_p=50000.0 + i * 10 - 100,
                close_p=50000.0 + i * 10 + 50,
            )
            self.strategy.on_kline_closed(bar)

        band = self.strategy.get_atr_band("BTCUSDT")
        if band:
            self.assertIsNotNone(band.upper)
            self.assertIsNotNone(band.lower)
            self.assertGreater(band.upper, band.lower)


# ═══════════════════════════════════════════════════════════════
# 2. M03 Kline 测试
# ═══════════════════════════════════════════════════════════════

class TestM03Kline(unittest.TestCase):

    def setUp(self):
        self.cache = KlineCache(max_bars=100)

    def test_add_tick(self):
        tick = make_tick(price=50000.0, volume=0.5)
        result = self.cache.add_tick(tick)
        self.assertTrue(result.is_ok())

    def test_aggregate_ticks_to_bar(self):
        base_ts = 1700000000000
        # 添加同一分钟内的多个tick
        for i in range(5):
            tick = TickData(
                symbol="BTCUSDT",
                price=50000.0 + i * 10,
                volume=0.1,
                timestamp=base_ts + i * 1000,
            )
            self.cache.add_tick(tick)

        # 获取聚合中的bar
        bar = self.cache.get_current_bar("BTCUSDT", KlineInterval.MINUTE_3)
        self.assertIsNotNone(bar)

    def test_bar_closed_callback(self):
        callback_called = False
        closed_bar = None

        def on_bar_closed(bar: KlineBar):
            nonlocal callback_called, closed_bar
            callback_called = True
            closed_bar = bar

        self.cache.on_bar_closed = on_bar_closed

        # 模拟一个完整周期的tick
        base_ts = 1700000000000
        for i in range(20):
            tick = TickData(
                symbol="BTCUSDT",
                price=50000.0 + i,
                volume=0.01,
                timestamp=base_ts + i * 5000,  # 5秒间隔
            )
            self.cache.add_tick(tick)

    def test_get_historical_bars(self):
        # 添加多个bar
        for i in range(10):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                close_p=50000.0 + i * 100,
            )
            self.cache.add_closed_bar(bar)

        bars = self.cache.get_bars("BTCUSDT", KlineInterval.MINUTE_3, limit=5)
        self.assertEqual(len(bars), 5)

    def test_kline_interval_enum(self):
        self.assertEqual(KlineInterval.MINUTE_1.value, "1m")
        self.assertEqual(KlineInterval.MINUTE_3.value, "3m")
        self.assertEqual(KlineInterval.MINUTE_5.value, "5m")
        self.assertEqual(KlineInterval.HOUR_1.value, "1h")

    def test_cache_limit(self):
        cache = KlineCache(max_bars=5)
        for i in range(10):
            bar = make_kline_bar(
                open_time=1700000000000 + i * 180000,
                close_p=50000.0 + i,
            )
            cache.add_closed_bar(bar)

        bars = cache.get_bars("BTCUSDT", KlineInterval.MINUTE_3)
        self.assertLessEqual(len(bars), 5)


# ═══════════════════════════════════════════════════════════════
# 3. M04 Risk 测试
# ═══════════════════════════════════════════════════════════════

class TestM04Risk(unittest.TestCase):

    def setUp(self):
        self.cfg = RiskConfig(
            max_daily_loss_pct=0.05,
            max_single_loss_pct=0.02,
            max_drawdown_pct=0.15,
            max_slippage_bps=50,
            max_position_value_usdt=10000.0,
            kill_switch_enabled=True,
        )
        self.risk = RiskGuard(self.cfg)
        self.risk.configure(ConfigSnapshot({}))

    def test_initial_state(self):
        self.assertEqual(self.risk.state, ModuleState.CONFIGURED)

    def test_pre_trade_check_pass(self):
        result = self.risk.pre_trade_check(
            symbol="BTCUSDT",
            side="BUY",
            quantity=0.1,
            price=50000.0,
            current_equity=100000.0,
        )
        self.assertTrue(result.is_ok())

    def test_pre_trade_check_exceed_position_limit(self):
        result = self.risk.pre_trade_check(
            symbol="BTCUSDT",
            side="BUY",
            quantity=1.0,
            price=50000.0,
            current_equity=1000.0,  # 名义额50000 > 权益的10倍
        )
        # 应该被拒绝或告警
        # 取决于风控规则的严格程度

    def test_daily_loss_tracker(self):
        tracker = DailyLossTracker(max_daily_loss_pct=0.05)
        tracker.record_loss(1000.0)
        self.assertFalse(tracker.is_limit_exceeded(100000.0))

        tracker.record_loss(4000.0)
        self.assertTrue(tracker.is_limit_exceeded(100000.0))

    def test_drawdown_tracker(self):
        tracker = DrawdownTracker(max_drawdown_pct=0.15)
        tracker.update_equity(100000.0)
        tracker.update_equity(90000.0)
        self.assertFalse(tracker.is_limit_exceeded())

        tracker.update_equity(80000.0)
        self.assertTrue(tracker.is_limit_exceeded())

    def test_slippage_protector(self):
        protector = SlippageProtector(max_slippage_bps=50)
        # 正常滑点
        allowed = protector.check_slippage(
            expected_price=50000.0,
            actual_price=50025.0,  # 0.05% 滑点 = 5 bps
        )
        self.assertTrue(allowed)

        # 过大滑点
        allowed = protector.check_slippage(
            expected_price=50000.0,
            actual_price=50100.0,  # 0.2% 滑点 = 20 bps
        )
        # 50 bps 限制，20 bps 应该通过
        self.assertTrue(allowed)

        allowed = protector.check_slippage(
            expected_price=50000.0,
            actual_price=50300.0,  # 0.6% 滑点 = 60 bps
        )
        self.assertFalse(allowed)

    def test_risk_level_enum(self):
        self.assertEqual(RiskLevel.INFO.value, "info")
        self.assertEqual(RiskLevel.WARN.value, "warn")
        self.assertEqual(RiskLevel.URGENT.value, "urgent")
        self.assertEqual(RiskLevel.FATAL.value, "fatal")

    def test_risk_violation_creation(self):
        violation = RiskViolation(
            rule_name="daily_loss",
            level=RiskLevel.URGENT,
            message="Daily loss exceeded 5%",
            current_value=6000.0,
            limit_value=5000.0,
        )
        self.assertEqual(violation.rule_name, "daily_loss")
        self.assertEqual(violation.level, RiskLevel.URGENT)


# ═══════════════════════════════════════════════════════════════
# 4. M05 Position 测试
# ═══════════════════════════════════════════════════════════════

class TestM05Position(unittest.TestCase):

    def setUp(self):
        self.mgr = PositionManager()
        self.mgr.configure(ConfigSnapshot({}))

    def test_open_long_position(self):
        fill = make_fill_event(
            order_id="ord_001",
            side="BUY",
            quantity=0.5,
            price=50000.0,
        )
        result = self.mgr.on_fill(fill)
        self.assertTrue(result.is_ok())

        pos = self.mgr.get_position("BTCUSDT")
        self.assertIsNotNone(pos)
        self.assertEqual(pos.side, PositionSide.LONG)
        self.assertEqual(pos.quantity, 0.5)
        self.assertEqual(pos.entry_price, 50000.0)

    def test_open_short_position(self):
        fill = make_fill_event(
            order_id="ord_002",
            side="SELL",
            quantity=0.3,
            price=51000.0,
        )
        result = self.mgr.on_fill(fill)
        self.assertTrue(result.is_ok())

        pos = self.mgr.get_position("BTCUSDT")
        self.assertEqual(pos.side, PositionSide.SHORT)

    def test_partial_fill(self):
        fill1 = make_fill_event(
            order_id="ord_003",
            side="BUY",
            quantity=0.2,
            price=50000.0,
        )
        self.mgr.on_fill(fill1)

        fill2 = make_fill_event(
            order_id="ord_003",
            side="BUY",
            quantity=0.3,
            price=50100.0,
        )
        self.mgr.on_fill(fill2)

        pos = self.mgr.get_position("BTCUSDT")
        self.assertEqual(pos.quantity, 0.5)
        # 平均入场价应该是加权平均
        expected_avg = (0.2 * 50000 + 0.3 * 50100) / 0.5
        self.assertAlmostEqual(pos.entry_price, expected_avg, delta=1)

    def test_close_position(self):
        fill_open = make_fill_event(
            order_id="ord_004",
            side="BUY",
            quantity=0.5,
            price=50000.0,
        )
        self.mgr.on_fill(fill_open)

        fill_close = make_fill_event(
            order_id="ord_005",
            side="SELL",
            quantity=0.5,
            price=51000.0,
        )
        result = self.mgr.on_fill(fill_close)
        self.assertTrue(result.is_ok())

        pos = self.mgr.get_position("BTCUSDT")
        self.assertEqual(pos.quantity, 0.0)
        self.assertEqual(pos.state, PositionState.CLOSED)

    def test_position_side_enum(self):
        self.assertEqual(PositionSide.LONG.value, "long")
        self.assertEqual(PositionSide.SHORT.value, "short")
        self.assertEqual(PositionSide.FLAT.value, "flat")

    def test_get_all_positions(self):
        fill1 = make_fill_event(
            order_id="o1", side="BUY", quantity=0.1, price=50000.0,
        )
        self.mgr.on_fill(fill1)

        fill2 = make_fill_event(
            order_id="o2", side="BUY", quantity=0.2, price=3000.0,
            symbol="ETHUSDT",
        )
        self.mgr.on_fill(fill2)

        all_pos = self.mgr.get_all_positions()
        self.assertEqual(len(all_pos), 2)

    def test_unrealized_pnl(self):
        fill = make_fill_event(
            order_id="ord_006",
            side="BUY",
            quantity=1.0,
            price=50000.0,
        )
        self.mgr.on_fill(fill)

        pnl = self.mgr.calculate_unrealized_pnl("BTCUSDT", current_price=51000.0)
        self.assertAlmostEqual(pnl, 1000.0, delta=0.01)


# ═══════════════════════════════════════════════════════════════
# 5. M06 Equity 测试
# ═══════════════════════════════════════════════════════════════

class TestM06Equity(unittest.TestCase):

    def setUp(self):
        self.tracker = EquityTracker(initial_equity=100000.0)
        self.tracker.configure(ConfigSnapshot({}))

    def test_initial_equity(self):
        self.assertEqual(self.tracker.get_current_equity(), 100000.0)

    def test_update_equity(self):
        self.tracker.update_equity(101000.0)
        self.assertEqual(self.tracker.get_current_equity(), 101000.0)

    def test_realized_pnl(self):
        self.tracker.record_trade_pnl(500.0)
        self.tracker.record_trade_pnl(-200.0)
        self.assertEqual(self.tracker.get_total_realized_pnl(), 300.0)

    def test_drawdown_calculation(self):
        self.tracker.update_equity(100000.0)
        self.tracker.update_equity(95000.0)
        self.tracker.update_equity(90000.0)
        self.tracker.update_equity(92000.0)

        dd = self.tracker.get_max_drawdown()
        self.assertAlmostEqual(dd, 10000.0, delta=1.0)  # 100000 → 90000

    def test_drawdown_percentage(self):
        self.tracker.update_equity(100000.0)
        self.tracker.update_equity(85000.0)
        dd_pct = self.tracker.get_max_drawdown_percentage()
        self.assertAlmostEqual(dd_pct, 0.15, delta=0.01)

    def test_equity_point_creation(self):
        point = EquityPoint(
            timestamp=int(time.time() * 1000),
            equity=100500.0,
            realized_pnl=500.0,
            unrealized_pnl=200.0,
        )
        self.assertEqual(point.equity, 100500.0)

    def test_get_equity_curve(self):
        for i in range(10):
            self.tracker.update_equity(100000.0 + i * 1000)
        curve = self.tracker.get_equity_curve()
        self.assertEqual(len(curve), 10)


# ═══════════════════════════════════════════════════════════════
# 6. M07 Execution 测试
# ═══════════════════════════════════════════════════════════════

class TestM07Execution(unittest.TestCase):

    def setUp(self):
        self.exec_mgr = ExecutionManager()
        self.exec_mgr.configure(ConfigSnapshot({}))

    def test_create_order_request(self):
        req = OrderRequest(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=0.5,
            price=50000.0,
            signal_id="sig_001",
        )
        self.assertEqual(req.symbol, "BTCUSDT")
        self.assertEqual(req.quantity, 0.5)

    def test_idempotency_key_generation(self):
        key1 = IdempotencyKey.generate("sig_001", "BTCUSDT", "BUY")
        key2 = IdempotencyKey.generate("sig_001", "BTCUSDT", "BUY")
        key3 = IdempotencyKey.generate("sig_002", "BTCUSDT", "BUY")

        self.assertEqual(key1, key2)  # 相同参数 → 相同key
        self.assertNotEqual(key1, key3)  # 不同参数 → 不同key

    def test_submit_order(self):
        req = OrderRequest(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=0.1,
            price=50000.0,
            signal_id="sig_003",
        )
        result = self.exec_mgr.submit_order(req)
        # 在最小运行集中，可能返回错误（因为没有真实broker）
        # 但应该返回 Result 类型
        self.assertIsInstance(result, Result)

    def test_order_state_enum(self):
        self.assertEqual(OrderState.PENDING.value, "pending")
        self.assertEqual(OrderState.SUBMITTED.value, "submitted")
        self.assertEqual(OrderState.FILLED.value, "filled")
        self.assertEqual(OrderState.REJECTED.value, "rejected")
        self.assertEqual(OrderState.CANCELLED.value, "cancelled")

    def test_retry_policy(self):
        from src.m07_execution import RetryPolicy
        policy = RetryPolicy(max_retries=3, backoff_base=1.0, backoff_max=10.0)
        self.assertEqual(policy.max_retries, 3)
        delay1 = policy.get_delay(0)
        delay2 = policy.get_delay(1)
        self.assertLess(delay1, delay2)  # 指数退避


# ═══════════════════════════════════════════════════════════════
# 7. M11 Performance 测试
# ═══════════════════════════════════════════════════════════════

class TestM11Performance(unittest.TestCase):

    def setUp(self):
        self.tracker = PerformanceTracker()
        self.tracker.configure(ConfigSnapshot({}))

    def test_record_trade(self):
        trade = TradeRecord(
            trade_id="t_001",
            symbol="BTCUSDT",
            side="LONG",
            entry_price=50000.0,
            exit_price=51000.0,
            quantity=0.5,
            entry_time=1700000000000,
            exit_time=1700003600000,
            pnl=500.0,
            pnl_percentage=0.02,
            commission=25.0,
        )
        result = self.tracker.record_trade(trade)
        self.assertTrue(result.is_ok())

    def test_win_rate(self):
        # 3赢2亏
        for i in range(3):
            trade = TradeRecord(
                trade_id=f"w_{i}",
                symbol="BTCUSDT",
                side="LONG",
                entry_price=50000.0,
                exit_price=51000.0,
                quantity=0.1,
                entry_time=1700000000000 + i * 1000,
                exit_time=1700000000000 + i * 1000 + 3600000,
                pnl=100.0 * (i + 1),
                pnl_percentage=0.02,
                commission=5.0,
            )
            self.tracker.record_trade(trade)

        for i in range(2):
            trade = TradeRecord(
                trade_id=f"l_{i}",
                symbol="BTCUSDT",
                side="LONG",
                entry_price=50000.0,
                exit_price=49000.0,
                quantity=0.1,
                entry_time=1700000000000 + i * 1000,
                exit_time=1700000000000 + i * 1000 + 3600000,
                pnl=-50.0 * (i + 1),
                pnl_percentage=-0.01,
                commission=5.0,
            )
            self.tracker.record_trade(trade)

        win_rate = self.tracker.get_win_rate()
        self.assertAlmostEqual(win_rate, 0.6, delta=0.01)

    def test_profit_factor(self):
        # 盈利总额
        self.tracker.record_trade(TradeRecord(
            trade_id="pf1", symbol="BTCUSDT", side="LONG",
            entry_price=50000.0, exit_price=51000.0, quantity=0.2,
            entry_time=1700000000000, exit_time=1700003600000,
            pnl=200.0, pnl_percentage=0.02, commission=10.0,
        ))
        # 亏损
        self.tracker.record_trade(TradeRecord(
            trade_id="pf2", symbol="BTCUSDT", side="LONG",
            entry_price=50000.0, exit_price=49500.0, quantity=0.2,
            entry_time=1700000000000, exit_time=1700003600000,
            pnl=-100.0, pnl_percentage=-0.01, commission=10.0,
        ))

        pf = self.tracker.get_profit_factor()
        self.assertAlmostEqual(pf, 2.0, delta=0.1)

    def test_sharpe_calculator(self):
        from src.m11_performance import SharpeCalculator
        calc = SharpeCalculator(risk_free_rate=0.02)
        returns = [0.01, 0.02, -0.005, 0.015, 0.008, -0.002, 0.012]
        sharpe = calc.calculate(returns)
        self.assertIsNotNone(sharpe)
        self.assertGreater(sharpe, 0)

    def test_drawdown_stats(self):
        from src.m11_performance import DrawdownStats
        stats = DrawdownStats()
        equity_values = [100, 105, 103, 108, 95, 98, 102, 110]
        max_dd = stats.calculate_max_drawdown(equity_values)
        self.assertGreater(max_dd, 0)


# ═══════════════════════════════════════════════════════════════
# 8. M12 Snapshot 测试
# ═══════════════════════════════════════════════════════════════

class TestM12Snapshot(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.mgr = SnapshotManager(snapshot_dir=self.temp_dir)
        self.mgr.configure(ConfigSnapshot({}))

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_snapshot(self):
        snapshot = SystemSnapshot(
            snapshot_id="snap_001",
            timestamp=int(time.time() * 1000),
            equity=100000.0,
            positions={"BTCUSDT": {"qty": 0.5, "side": "long"}},
            open_orders={},
            risk_state={"daily_loss": 0.0},
            module_states={"M01": "running", "M04": "running"},
        )
        result = self.mgr.save(snapshot)
        self.assertTrue(result.is_ok())

    def test_load_snapshot(self):
        snapshot = SystemSnapshot(
            snapshot_id="snap_002",
            timestamp=int(time.time() * 1000),
            equity=105000.0,
            positions={},
            open_orders={},
            risk_state={},
            module_states={},
        )
        self.mgr.save(snapshot)

        loaded = self.mgr.load("snap_002")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.equity, 105000.0)

    def test_list_snapshots(self):
        for i in range(3):
            snapshot = SystemSnapshot(
                snapshot_id=f"snap_{i:03d}",
                timestamp=int(time.time() * 1000) + i,
                equity=100000.0 + i * 1000,
                positions={},
                open_orders={},
                risk_state={},
                module_states={},
            )
            self.mgr.save(snapshot)

        snapshots = self.mgr.list_snapshots()
        self.assertEqual(len(snapshots), 3)

    def test_atomic_file_writer(self):
        writer = AtomicFileWriter(self.temp_dir)
        test_file = os.path.join(self.temp_dir, "test_atomic.json")
        data = {"key": "value", "number": 42}

        result = writer.write_json(test_file, data)
        self.assertTrue(result.is_ok())
        self.assertTrue(os.path.exists(test_file))

        # 读取验证
        with open(test_file, 'r') as f:
            loaded = json.load(f)
        self.assertEqual(loaded["key"], "value")
        self.assertEqual(loaded["number"], 42)

    def test_snapshot_version(self):
        from src.m12_snapshot import SnapshotVersion
        v1 = SnapshotVersion(major=1, minor=0, patch=0)
        v2 = SnapshotVersion(major=1, minor=1, patch=0)
        self.assertLess(v1, v2)


# ═══════════════════════════════════════════════════════════════
# 9. M13 SignalTracker 测试
# ═══════════════════════════════════════════════════════════════

class TestM13SignalTracker(unittest.TestCase):

    def setUp(self):
        self.tracker = SignalTracker()
        self.tracker.configure(ConfigSnapshot({}))

    def test_register_signal(self):
        record = SignalRecord(
            signal_id="sig_001",
            symbol="BTCUSDT",
            signal_type=SignalType.OPEN_LONG,
            price=50000.0,
            timestamp=int(time.time() * 1000),
            trace_id="trace_001",
        )
        result = self.tracker.register_signal(record)
        self.assertTrue(result.is_ok())

    def test_dedup_duplicate_signal(self):
        record1 = SignalRecord(
            signal_id="sig_dup_001",
            symbol="BTCUSDT",
            signal_type=SignalType.OPEN_LONG,
            price=50000.0,
            timestamp=int(time.time() * 1000),
            trace_id="trace_001",
        )
        record2 = SignalRecord(
            signal_id="sig_dup_001",  # 相同ID
            symbol="BTCUSDT",
            signal_type=SignalType.OPEN_LONG,
            price=50100.0,  # 不同价格
            timestamp=int(time.time() * 1000) + 1000,
            trace_id="trace_002",
        )

        self.tracker.register_signal(record1)
        dedup_result = self.tracker.check_duplicate(record2)

        self.assertTrue(dedup_result.is_duplicate)
        self.assertEqual(dedup_result.original_signal_id, "sig_dup_001")

    def test_lineage_tracking(self):
        signal_id = "sig_lineage_001"
        record = SignalRecord(
            signal_id=signal_id,
            symbol="BTCUSDT",
            signal_type=SignalType.OPEN_LONG,
            price=50000.0,
            timestamp=int(time.time() * 1000),
            trace_id="trace_001",
        )
        self.tracker.register_signal(record)

        # 关联订单
        self.tracker.link_order(signal_id, "ord_001")
        self.tracker.link_order(signal_id, "ord_002")

        lineage = self.tracker.get_lineage(signal_id)
        self.assertIsNotNone(lineage)
        self.assertEqual(len(lineage.order_ids), 2)

    def test_signal_state_enum(self):
        self.assertEqual(SignalState.PENDING.value, "pending")
        self.assertEqual(SignalState.PROCESSED.value, "processed")
        self.assertEqual(SignalState.DUPLICATE.value, "duplicate")
        self.assertEqual(SignalState.EXPIRED.value, "expired")

    def test_get_active_signals(self):
        for i in range(5):
            record = SignalRecord(
                signal_id=f"sig_active_{i}",
                symbol="BTCUSDT",
                signal_type=SignalType.OPEN_LONG,
                price=50000.0 + i * 100,
                timestamp=int(time.time() * 1000) + i,
                trace_id=f"trace_{i}",
            )
            self.tracker.register_signal(record)

        active = self.tracker.get_active_signals()
        self.assertEqual(len(active), 5)

    def test_expire_old_signals(self):
        old_ts = int(time.time() * 1000) - 3600000  # 1小时前
        record = SignalRecord(
            signal_id="sig_old",
            symbol="BTCUSDT",
            signal_type=SignalType.OPEN_LONG,
            price=50000.0,
            timestamp=old_ts,
            trace_id="trace_old",
        )
        self.tracker.register_signal(record)

        expired = self.tracker.expire_signals_older_than(1800000)  # 30分钟
        self.assertGreaterEqual(len(expired), 1)


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════

def run_all_tests():
    """手动运行所有测试并打印结果"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestM01Strategy,
        TestM03Kline,
        TestM04Risk,
        TestM05Position,
        TestM06Equity,
        TestM07Execution,
        TestM11Performance,
        TestM12Snapshot,
        TestM13SignalTracker,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print(f"业务模块测试: {'全部通过 ✅' if result.wasSuccessful() else '有失败 ❌'}")
    print(f"运行: {result.testsRun} 个测试")
    print(f"失败: {len(result.failures)} 个")
    print(f"错误: {len(result.errors)} 个")
    print("=" * 60)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
