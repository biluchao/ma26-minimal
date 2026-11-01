"""
src/modules/risk/risk_guard.py
MA26-ATR 量化交易系统 — 风控模块：Risk Guard

功能:
  1. 消费策略生成的 SIGNANAL_GENERATED 事件
  2. 多层风控校验（日亏/单笔/回撤/仓位/熔断）
  3. 通过校验 → 发布 ORDER_REQUEST → 执行模块
  4. 拒绝 → 发布 RISK_REJECTED 事件 + 记录原因
  5. 实时追踪权益曲线、日盈亏、最大回撤
  6. 与 CircuitBreaker 联动（系统级熔断）
  7. 实现 LifecycleAware 接口

风控层级:
  L1: 信号基础校验（价格/数量/格式）
  L2: 仓位限制（最大仓位/方向冲突）
  L3: 单笔风险限制（max_risk_per_trade）
  L4: 日亏损限制（daily_loss_limit → 触发熔断）
  L5: 最大回撤限制（max_drawdown → 停止交易）
  L6: 系统熔断检查（CircuitBreaker）
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum, auto

from src.core.result import Result, Error, ErrorSeverity, ErrorCategory
from src.core.lifecycle import LifecycleAware
from src.core.event_bus import EventBus, Event, EventType, get_global_event_bus
from src.core.state_registry import get_global_state_registry, StateScope
from src.core.config import get_global_config_manager
from src.core.circuit_breaker import (
    get_global_circuit_breaker,
    CircuitOpenError,
    CircuitState,
)


# ═══════════════════════════════════════════════════════════════
# 风控规则枚举
# ═══════════════════════════════════════════════════════════════

class RiskRejectReason(Enum):
    """风控拒绝原因"""
    NONE = "none"
    INVALID_SIGNAL = "invalid_signal"           # 信号格式错误
    POSITION_LIMIT = "position_limit"           # 仓位超限
    DIRECTION_CONFLICT = "direction_conflict"   # 方向冲突（已有多仓又开多）
    DAILY_LOSS_EXCEEDED = "daily_loss_exceeded" # 日亏损超限
    MAX_DRAWDOWN_EXCEEDED = "max_drawdown"      # 最大回撤超限
    SINGLE_TRADE_RISK = "single_trade_risk"     # 单笔风险超限
    CIRCUIT_BREAKER_OPEN = "circuit_breaker"    # 熔断开启
    MIN_SIZE_NOT_MET = "min_size_not_met"       # 最小交易单位
    MAX_SIZE_EXCEEDED = "max_size_exceeded"     # 最大交易单位
    COOLDOWN_ACTIVE = "cooldown_active"         # 冷却期
    DUPLICATE_SIGNAL = "duplicate_signal"       # 重复信号


class RiskLevel(Enum):
    """风控等级"""
    PASS = 0
    WARN = 1
    REJECT = 2
    FATAL = 3


# ═══════════════════════════════════════════════════════════════
# 风控检查结果
# ═══════════════════════════════════════════════════════════════

@dataclass
class RiskCheckResult:
    """风控检查结果"""
    level: RiskLevel
    passed: bool
    reason: RiskRejectReason
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'passed': self.passed,
            'level': self.level.name,
            'reason': self.reason.value,
            'message': self.message,
            'metadata': self.metadata,
        }


# ═══════════════════════════════════════════════════════════════
# 日盈亏追踪
# ═══════════════════════════════════════════════════════════════

class DailyPnLTracker:
    """
    日盈亏追踪器 — 按 UTC 日期追踪盈亏。
    """

    def __init__(self, initial_equity: float = 100000.0):
        self._initial_equity = initial_equity
        self._current_equity = initial_equity
        self._peak_equity = initial_equity
        self._daily_pnl: Dict[str, float] = {}       # YYYY-MM-DD → pnl
        self._current_date: str = ""
        self._lock = threading.RLock()
        self._trades_today = 0
        self._wins_today = 0
        self._losses_today = 0

    def _get_today_key(self) -> str:
        """获取今天的日期键"""
        return time.strftime("%Y-%m-%d", time.gmtime())

    def update_equity(self, new_equity: float):
        """更新当前权益"""
        with self._lock:
            today = self._get_today_key()
            if today != self._current_date:
                # 新的一天，重置日盈亏
                self._current_date = today
                self._daily_pnl[today] = 0.0
                self._trades_today = 0
                self._wins_today = 0
                self._losses_today = 0

            pnl_change = new_equity - self._current_equity
            self._daily_pnl[today] = self._daily_pnl.get(today, 0.0) + pnl_change
            self._current_equity = new_equity

            if new_equity > self._peak_equity:
                self._peak_equity = new_equity

    def record_trade(self, pnl: float):
        """记录一笔交易"""
        with self._lock:
            self._trades_today += 1
            if pnl > 0:
                self._wins_today += 1
            elif pnl < 0:
                self._losses_today += 1

    def get_daily_pnl(self, date: Optional[str] = None) -> float:
        """获取指定日期的盈亏"""
        with self._lock:
            if date is None:
                date = self._get_today_key()
            return self._daily_pnl.get(date, 0.0)

    def get_daily_pnl_percent(self) -> float:
        """获取今日盈亏百分比"""
        with self._lock:
            daily_pnl = self.get_daily_pnl()
            return daily_pnl / self._initial_equity if self._initial_equity > 0 else 0.0

    def get_max_drawdown(self) -> float:
        """获取当前最大回撤（从峰值）"""
        with self._lock:
            if self._peak_equity <= 0:
                return 0.0
            dd = (self._peak_equity - self._current_equity) / self._peak_equity
            return max(dd, 0.0)

    def get_current_equity(self) -> float:
        with self._lock:
            return self._current_equity

    def get_peak_equity(self) -> float:
        with self._lock:
            return self._peak_equity

    def get_today_stats(self) -> dict:
        with self._lock:
            return {
                'date': self._current_date,
                'daily_pnl': self._daily_pnl.get(self._current_date, 0.0),
                'daily_pnl_percent': self.get_daily_pnl_percent(),
                'trades': self._trades_today,
                'wins': self._wins_today,
                'losses': self._losses_today,
                'current_equity': self._current_equity,
                'peak_equity': self._peak_equity,
                'max_drawdown': self.get_max_drawdown(),
            }

    def reset(self):
        with self._lock:
            self._current_equity = self._initial_equity
            self._peak_equity = self._initial_equity
            self._daily_pnl.clear()
            self._current_date = ""
            self._trades_today = 0
            self._wins_today = 0
            self._losses_today = 0


# ═══════════════════════════════════════════════════════════════
# 风控模块主类
# ═══════════════════════════════════════════════════════════════

class RiskGuard(LifecycleAware):
    """
    风控守卫模块 — 系统交易安全的核心防线。

    配置参数（从 ConfigManager 读取）:
      risk.daily_loss_limit:      日亏损限制（默认 0.05 = 5%）
      risk.max_drawdown:          最大回撤限制（默认 0.20 = 20%）
      risk.max_position_size:     最大单笔仓位（默认 10000）
      risk.max_position_risk:    单笔最大风险（默认 0.02 = 2%）
      risk.min_trade_size:        最小交易数量（默认 0.001）
      risk.cooldown_seconds:     风控拒绝后冷却期（默认 60s）
      risk.enable_circuit_breaker: 是否启用熔断（默认 true）
      risk.max_concurrent_positions: 最大并发仓位（默认 1）
    """

    def __init__(self):
        # 风控参数
        self._daily_loss_limit = 0.05
        self._max_drawdown = 0.20
        self._max_position_size = 10000.0
        self._max_position_risk = 0.02
        self._min_trade_size = 0.001
        self._max_trade_size = 100000.0
        self._cooldown_seconds = 60
        self._enable_circuit_breaker = True
        self._max_concurrent_positions = 1

        # 运行时状态
        self._running = False
        self._last_reject_time = 0
        self._reject_count = 0
        self._approve_count = 0
        self._position_count = 0
        self._current_positions: Dict[str, dict] = {}  # symbol → position info

        # 依赖
        self._event_bus: Optional[EventBus] = None
        self._config_mgr = None
        self._circuit_breaker = None
        self._state_registry = None

        # 日盈亏追踪
        self._pnl_tracker = DailyPnLTracker(initial_equity=100000.0)

        # 线程安全
        self._lock = threading.RLock()

    # ─── LifecycleAware 接口 ─────────────────────────────────

    @property
    def module_name(self) -> str:
        return "m04_risk"

    @property
    def dependencies(self) -> List[str]:
        return [
            "core_config",
            "core_event_bus",
            "core_state",
            "core_circuit_breaker",
        ]

    @property
    def init_timeout_ms(self) -> int:
        return 10000

    @property
    def start_timeout_ms(self) -> int:
        return 5000

    @property
    def drain_timeout_ms(self) -> int:
        return 3000

    def on_init(self) -> Result[None]:
        """初始化风控模块"""
        # 配置
        try:
            self._config_mgr = get_global_config_manager()
        except Exception:
            pass

        if self._config_mgr:
            self._daily_loss_limit = self._config_mgr.get_float(
                "risk.daily_loss_limit", self._daily_loss_limit
            )
            self._max_drawdown = self._config_mgr.get_float(
                "risk.max_drawdown", self._max_drawdown
            )
            self._max_position_size = self._config_mgr.get_float(
                "risk.max_position_size", self._max_position_size
            )
            self._max_position_risk = self._config_mgr.get_float(
                "risk.max_position_risk", self._max_position_risk
            )
            self._min_trade_size = self._config_mgr.get_float(
                "risk.min_trade_size", self._min_trade_size
            )
            self._max_trade_size = self._config_mgr.get_float(
                "risk.max_trade_size", self._max_trade_size
            )
            self._cooldown_seconds = self._config_mgr.get_int(
                "risk.cooldown_seconds", self._cooldown_seconds
            )
            self._enable_circuit_breaker = self._config_mgr.get_bool(
                "risk.enable_circuit_breaker", self._enable_circuit_breaker
            )
            self._max_concurrent_positions = self._config_mgr.get_int(
                "risk.max_concurrent_positions", self._max_concurrent_positions
            )

            # 初始权益
            equity = self._config_mgr.get_float("initial_equity", 100000.0)
            self._pnl_tracker = DailyPnLTracker(initial_equity=equity)

        # 事件总线
        try:
            self._event_bus = get_global_event_bus()
        except Exception:
            pass

        # 熔断
        if self._enable_circuit_breaker:
            try:
                self._circuit_breaker = get_global_circuit_breaker()
            except Exception:
                pass

        # 状态注册
        try:
            self._state_registry = get_global_state_registry()
        except Exception:
            pass

        return Result.ok(None)

    def on_start(self) -> Result[None]:
        """启动风控"""
        self._running = True

        # 订阅信号事件
        if self._event_bus:
            self._event_bus.subscribe(
                EventType.SIGNAL_GENERATED,
                self._on_signal_event,
            )
            # 订阅成交事件（更新权益）
            self._event_bus.subscribe(
                EventType.TRADE_UPDATE,
                self._on_trade_event,
            )
            # 订阅权益更新
            self._event_bus.subscribe(
                EventType.EQUITY_UPDATE,
                self._on_equity_event,
            )

        return Result.ok(None)

    def on_drain(self) -> Result[None]:
        """优雅停机"""
        self._running = False
        return Result.ok(None)

    def on_stop(self) -> Result[None]:
        """停止风控"""
        self._running = False

        if self._event_bus:
            try:
                self._event_bus.unsubscribe(
                    EventType.SIGNAL_GENERATED, self._on_signal_event
                )
            except Exception:
                pass

        with self._lock:
            self._current_positions.clear()

        return Result.ok(None)

    def on_health_check(self) -> Result[dict]:
        return Result.ok({
            'running': self._running,
            'approve_count': self._approve_count,
            'reject_count': self._reject_count,
            'position_count': self._position_count,
            'daily_pnl_percent': self._pnl_tracker.get_daily_pnl_percent(),
            'max_drawdown': self._pnl_tracker.get_max_drawdown(),
            'current_equity': self._pnl_tracker.get_current_equity(),
        })

    # ─── 事件处理 ─────────────────────────────────────────────

    def _on_signal_event(self, event: Event):
        """处理策略信号事件 — 核心风控入口"""
        if not self._running:
            return

        signal = event.payload
        if not signal:
            return

        # 执行风控检查
        result = self._evaluate_risk(signal)

        if result.passed:
            self._approve_count += 1
            self._forward_order(signal, result)
        else:
            self._reject_count += 1
            self._last_reject_time = time.time()
            self._publish_rejection(signal, result)

    def _on_trade_event(self, event: Event):
        """处理成交事件 — 更新持仓和盈亏"""
        payload = event.payload
        symbol = payload.get('symbol', '')
        side = payload.get('side', '')
        size = float(payload.get('size', 0))
        price = float(payload.get('price', 0))
        pnl = float(payload.get('pnl', 0))

        with self._lock:
            if side == 'buy':
                if symbol not in self._current_positions:
                    self._current_positions[symbol] = {
                        'size': 0, 'entry_price': 0, 'entry_time': 0
                    }
                pos = self._current_positions[symbol]
                # 更新持仓
                old_size = pos['size']
                if old_size >= 0:
                    # 加多
                    new_size = old_size + size
                    pos['entry_price'] = (
                        (old_size * pos['entry_price'] + size * price) / new_size
                        if new_size > 0 else price
                    )
                    pos['size'] = new_size
                else:
                    # 平空
                    pos['size'] = old_size + size
                    if pos['size'] >= 0:
                        pos['entry_price'] = 0

                if pos['size'] > 0 and pos['entry_time'] == 0:
                    pos['entry_time'] = int(time.time() * 1000)

            elif side == 'sell':
                if symbol not in self._current_positions:
                    self._current_positions[symbol] = {
                        'size': 0, 'entry_price': 0, 'entry_time': 0
                    }
                pos = self._current_positions[symbol]
                old_size = pos['size']

                if old_size <= 0:
                    # 加空
                    new_size = old_size - size
                    pos['entry_price'] = (
                        (abs(old_size) * pos['entry_price'] + size * price) / abs(new_size)
                        if new_size < 0 else price
                    )
                    pos['size'] = new_size
                else:
                    # 平多
                    pos['size'] = old_size - size
                    if pos['size'] <= 0:
                        pos['entry_price'] = 0

                if pos['size'] < 0 and pos['entry_time'] == 0:
                    pos['entry_time'] = int(time.time() * 1000)

            # 清理零仓位
            if symbol in self._current_positions and self._current_positions[symbol]['size'] == 0:
                del self._current_positions[symbol]

            self._position_count = sum(
                1 for p in self._current_positions.values() if p['size'] != 0
            )

        # 更新盈亏
        if pnl != 0:
            self._pnl_tracker.record_trade(pnl)

    def _on_equity_event(self, event: Event):
        """处理权益更新"""
        payload = event.payload
        equity = float(payload.get('equity', 0))
        if equity > 0:
            self._pnl_tracker.update_equity(equity)

    # ─── 风控评估 ─────────────────────────────────────────────

    def _evaluate_risk(self, signal: dict) -> RiskCheckResult:
        """
        多层风控评估。
        任何一层拒绝即返回 REJECT。
        """
        # L1: 信号基础校验
        r = self._check_l1_basic(signal)
        if not r.passed:
            return r

        # L2: 仓位限制
        r = self._check_l2_position(signal)
        if not r.passed:
            return r

        # L3: 单笔风险
        r = self._check_l3_single_trade_risk(signal)
        if not r.passed:
            return r

        # L4: 日亏损限制
        r = self._check_l4_daily_loss()
        if not r.passed:
            return r

        # L5: 最大回撤
        r = self._check_l5_drawdown()
        if not r.passed:
            return r

        # L6: 熔断检查
        if self._enable_circuit_breaker and self._circuit_breaker:
            r = self._check_l6_circuit_breaker()
            if not r.passed:
                return r

        # L7: 冷却期
        r = self._check_l7_cooldown()
        if not r.passed:
            return r

        # 全部通过
        return RiskCheckResult(
            level=RiskLevel.PASS,
            passed=True,
            reason=RiskRejectReason.NONE,
            message="All risk checks passed",
        )

    def _check_l1_basic(self, signal: dict) -> RiskCheckResult:
        """L1: 基础校验"""
        # 必需字段
        required = ['signal_type', 'symbol', 'price', 'timestamp']
        for field_name in required:
            if field_name not in signal:
                return RiskCheckResult(
                    level=RiskLevel.REJECT,
                    passed=False,
                    reason=RiskRejectReason.INVALID_SIGNAL,
                    message=f"Missing required field: {field_name}",
                )

        # 价格有效性
        price = float(signal.get('price', 0))
        if price <= 0:
            return RiskCheckResult(
                level=RiskLevel.REJECT,
                passed=False,
                reason=RiskRejectReason.INVALID_SIGNAL,
                message=f"Invalid price: {price}",
            )

        # 建议仓位大小
        suggested_size = float(signal.get('suggested_size', 0))
        if suggested_size > 0:
            if suggested_size < self._min_trade_size:
                return RiskCheckResult(
                    level=RiskLevel.REJECT,
                    passed=False,
                    reason=RiskRejectReason.MIN_SIZE_NOT_MET,
                    message=f"Size {suggested_size} < min {self._min_trade_size}",
                    metadata={'suggested_size': suggested_size, 'min_size': self._min_trade_size},
                )
            if suggested_size > self._max_trade_size:
                return RiskCheckResult(
                    level=RiskLevel.REJECT,
                    passed=False,
                    reason=RiskRejectReason.MAX_SIZE_EXCEEDED,
                    message=f"Size {suggested_size} > max {self._max_trade_size}",
                    metadata={'suggested_size': suggested_size, 'max_size': self._max_trade_size},
                )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l2_position(self, signal: dict) -> RiskCheckResult:
        """L2: 仓位限制"""
        signal_type = signal.get('signal_type', '')
        symbol = signal.get('symbol', '')

        # 如果是平仓/止损/止盈信号，直接通过
        if signal_type in ('close_long', 'close_short', 'stop_loss', 'take_profit'):
            return RiskCheckResult(
                level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
            )

        # 开仓信号
        if signal_type in ('buy', 'sell'):
            with self._lock:
                current_pos = self._current_positions.get(symbol, {}).get('size', 0)

                # 方向冲突检查
                if signal_type == 'buy' and current_pos > 0:
                    return RiskCheckResult(
                        level=RiskLevel.WARN,
                        passed=True,  # 允许（可能是加仓），但降级为 WARN
                        reason=RiskRejectReason.DIRECTION_CONFLICT,
                        message=f"Already long {current_pos}, buy signal received",
                    )
                if signal_type == 'sell' and current_pos < 0:
                    return RiskCheckResult(
                        level=RiskLevel.WARN,
                        passed=True,
                        reason=RiskRejectReason.DIRECTION_CONFLICT,
                        message=f"Already short {abs(current_pos)}, sell signal received",
                    )

                # 并发仓位限制
                active_positions = sum(
                    1 for p in self._current_positions.values() if p.get('size', 0) != 0
                )
                if active_positions >= self._max_concurrent_positions and current_pos == 0:
                    return RiskCheckResult(
                        level=RiskLevel.REJECT,
                        passed=False,
                        reason=RiskRejectReason.POSITION_LIMIT,
                        message=f"Max concurrent positions ({self._max_concurrent_positions}) reached",
                        metadata={'active_positions': active_positions},
                    )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l3_single_trade_risk(self, signal: dict) -> RiskCheckResult:
        """L3: 单笔风险限制"""
        signal_type = signal.get('signal_type', '')
        if signal_type not in ('buy', 'sell'):
            return RiskCheckResult(
                level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
            )

        price = float(signal.get('price', 0))
        suggested_size = float(signal.get('suggested_size', 0))
        atr = float(signal.get('atr', 0))

        if suggested_size <= 0 or price <= 0:
            return RiskCheckResult(
                level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
            )

        # 计算单笔风险金额
        if atr > 0:
            risk_amount = suggested_size * atr * 2  # 2×ATR 止损
        else:
            risk_amount = suggested_size * price * 0.02  # 默认 2%

        equity = self._pnl_tracker.get_current_equity()
        if equity <= 0:
            return RiskCheckResult(
                level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
            )

        risk_percent = risk_amount / equity
        if risk_percent > self._max_position_risk:
            return RiskCheckResult(
                level=RiskLevel.REJECT,
                passed=False,
                reason=RiskRejectReason.SINGLE_TRADE_RISK,
                message=f"Single trade risk {risk_percent:.2%} > limit {self._max_position_risk:.2%}",
                metadata={
                    'risk_percent': risk_percent,
                    'limit': self._max_position_risk,
                    'risk_amount': risk_amount,
                    'equity': equity,
                },
            )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l4_daily_loss(self) -> RiskCheckResult:
        """L4: 日亏损限制"""
        daily_pnl_pct = self._pnl_tracker.get_daily_pnl_percent()
        if daily_pnl_pct < -self._daily_loss_limit:
            # 触发熔断
            if self._circuit_breaker:
                try:
                    self._circuit_breaker.force_open(
                        f"Daily loss exceeded: {daily_pnl_pct:.2%} < -{self._daily_loss_limit:.2%}"
                    )
                except Exception:
                    pass

            return RiskCheckResult(
                level=RiskLevel.FATAL,
                passed=False,
                reason=RiskRejectReason.DAILY_LOSS_EXCEEDED,
                message=f"Daily loss {daily_pnl_pct:.2%} exceeded limit -{self._daily_loss_limit:.2%}",
                metadata={
                    'daily_pnl_percent': daily_pnl_pct,
                    'limit': self._daily_loss_limit,
                },
            )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l5_drawdown(self) -> RiskCheckResult:
        """L5: 最大回撤限制"""
        drawdown = self._pnl_tracker.get_max_drawdown()
        if drawdown > self._max_drawdown:
            return RiskCheckResult(
                level=RiskLevel.FATAL,
                passed=False,
                reason=RiskRejectReason.MAX_DRAWDOWN_EXCEEDED,
                message=f"Max drawdown {drawdown:.2%} exceeded limit {self._max_drawdown:.2%}",
                metadata={'drawdown': drawdown, 'limit': self._max_drawdown},
            )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l6_circuit_breaker(self) -> RiskCheckResult:
        """L6: 系统熔断检查"""
        if not self._circuit_breaker:
            return RiskCheckResult(
                level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
            )

        try:
            state = self._circuit_breaker.get_state()
            if state != CircuitState.CLOSED:
                return RiskCheckResult(
                    level=RiskLevel.FATAL,
                    passed=False,
                    reason=RiskRejectReason.CIRCUIT_BREAKER_OPEN,
                    message=f"Circuit breaker is {state.value}",
                    metadata={'circuit_state': state.value},
                )
        except Exception:
            pass

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    def _check_l7_cooldown(self) -> RiskCheckResult:
        """L7: 冷却期检查"""
        if self._last_reject_time > 0:
            elapsed = time.time() - self._last_reject_time
            if elapsed < self._cooldown_seconds:
                return RiskCheckResult(
                    level=RiskLevel.REJECT,
                    passed=False,
                    reason=RiskRejectReason.COOLDOWN_ACTIVE,
                    message=f"Cooldown active: {elapsed:.0f}s < {self._cooldown_seconds}s",
                    metadata={'elapsed': elapsed, 'cooldown': self._cooldown_seconds},
                )

        return RiskCheckResult(
            level=RiskLevel.PASS, passed=True, reason=RiskRejectReason.NONE
        )

    # ─── 信号转发 ─────────────────────────────────────────────

    def _forward_order(self, signal: dict, result: RiskCheckResult):
        """通过风控，转发为下单请求"""
        if not self._event_bus:
            return

        order_payload = {
            **signal,
            'risk_checked': True,
            'risk_check_time': int(time.time() * 1000),
            'risk_metadata': result.metadata,
        }

        self._event_bus.publish(Event(
            event_type=EventType.ORDER_REQUEST,
            source="m04_risk",
            payload=order_payload,
        ))

    def _publish_rejection(self, signal: dict, result: RiskCheckResult):
        """发布风控拒绝事件"""
        if not self._event_bus:
            return

        rejection_payload = {
            'original_signal': signal,
            'reject_reason': result.reason.value,
            'message': result.message,
            'metadata': result.metadata,
            'timestamp': int(time.time() * 1000),
        }

        self._event_bus.publish(Event(
            event_type=EventType.RISK_REJECTED,
            source="m04_risk",
            payload=rejection_payload,
        ))

    # ─── 公共接口（供测试和外部调用） ──────────────────────────

    def force_close_all(self, reason: str = "Manual force close"):
        """强制平掉所有仓位"""
        if not self._event_bus:
            return

        for symbol in list(self._current_positions.keys()):
            pos = self._current_positions[symbol]
            if pos['size'] > 0:
                self._event_bus.publish(Event(
                    event_type=EventType.SIGNAL_GENERATED,
                    source="m04_risk",
                    payload={
                        'signal_type': 'close_long',
                        'symbol': symbol,
                        'price': 0,
                        'timestamp': int(time.time() * 1000),
                        'reason': f"Force close: {reason}",
                    },
                ))
            elif pos['size'] < 0:
                self._event_bus.publish(Event(
                    event_type=EventType.SIGNAL_GENERATED,
                    source="m04_risk",
                    payload={
                        'signal_type': 'close_short',
                        'symbol': symbol,
                        'price': 0,
                        'timestamp': int(time.time() * 1000),
                        'reason': f"Force close: {reason}",
                    },
                ))

    def get_risk_stats(self) -> dict:
        """获取风控统计"""
        return {
            'approve_count': self._approve_count,
            'reject_count': self._reject_count,
            'position_count': self._position_count,
            'current_positions': dict(self._current_positions),
            'daily_pnl': self._pnl_tracker.get_today_stats(),
            'daily_loss_limit': self._daily_loss_limit,
            'max_drawdown_limit': self._max_drawdown,
            'max_position_risk': self._max_position_risk,
            'last_reject_time': self._last_reject_time,
            'cooldown_active': (
                time.time() - self._last_reject_time < self._cooldown_seconds
                if self._last_reject_time > 0 else False
            ),
        }

    def reset(self):
        """重置风控状态"""
        with self._lock:
            self._last_reject_time = 0
            self._reject_count = 0
            self._approve_count = 0
            self._position_count = 0
            self._current_positions.clear()
            self._pnl_tracker.reset()
