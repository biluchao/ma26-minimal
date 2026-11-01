"""
src/core/circuit_breaker.py
MA26-ATR 量化交易系统 — 核心原语：熔断器

设计理念:
  借鉴微服务熔断器模式，防止级联故障。
  在量化交易系统中用于:
    1. 风控熔断（连续亏损触发）
    2. 网络熔断（API 连续失败）
    3. 执行熔断（订单连续被拒）

  三种状态:
    CLOSED (关闭)    → 正常通行，统计失败次数
    OPEN (打开)     → 直接拒绝，进入冷却计时
    HALF_OPEN (半开) → 放行单个探测请求，成功则恢复，失败则重新打开

  状态转换:
    CLOSED ──(失败≥阈值)──→ OPEN
    OPEN ──(冷却超时)──→ HALF_OPEN
    HALF_OPEN ──(探测成功)──→ CLOSED
    HALF_OPEN ──(探测失败)──→ OPEN
"""

import time
import threading
from enum import Enum
from typing import Callable, Optional, Any, TypeVar

from .result import Result, Error, ErrorSeverity, ErrorCategory

T = TypeVar('T')


# ═══════════════════════════════════════════════════════════════
# 熔断器状态
# ═══════════════════════════════════════════════════════════════

class BreakerState(Enum):
    """熔断器状态枚举"""
    CLOSED = "CLOSED"           # 关闭（正常通行）
    OPEN = "OPEN"               # 打开（熔断拒绝）
    HALF_OPEN = "HALF_OPEN"     # 半开（探测放行）


# ═══════════════════════════════════════════════════════════════
# 熔断器配置
# ═══════════════════════════════════════════════════════════════

@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5          # 连续失败次数阈值（触发 OPEN）
    success_threshold: int = 3          # 半开状态连续成功次数（恢复 CLOSED）
    open_duration_ms: int = 60000       # OPEN 状态持续时间（默认 60 秒）
    half_open_max_calls: int = 1        # 半开状态最大探测请求数
    expected_exceptions: tuple = ()     # 需要计数的异常类型（空=全部）
    ignored_exceptions: tuple = ()      # 不计数的异常类型

    def __post_init__(self):
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold 必须 ≥ 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold 必须 ≥ 1")
        if self.open_duration_ms < 100:
            raise ValueError("open_duration_ms 必须 ≥ 100")


# ═══════════════════════════════════════════════════════════════
# 熔断器统计
# ═══════════════════════════════════════════════════════════════

class BreakerStats:
    """熔断器运行统计（线程安全）"""

    __slots__ = (
        '_lock',
        'total_calls', 'successful_calls', 'failed_calls',
        'rejected_calls', 'state_transitions',
        'last_failure_time', 'last_success_time',
        'consecutive_failures', 'consecutive_successes',
        'open_count', 'half_open_count',
    )

    def __init__(self):
        self._lock = threading.Lock()
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.rejected_calls = 0
        self.state_transitions = 0
        self.last_failure_time: Optional[int] = None
        self.last_success_time: Optional[int] = None
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.open_count = 0               # 进入 OPEN 的次数
        self.half_open_count = 0          # 进入 HALF_OPEN 的次数

    def record_success(self):
        with self._lock:
            self.total_calls += 1
            self.successful_calls += 1
            self.consecutive_successes += 1
            self.consecutive_failures = 0
            self.last_success_time = int(time.time() * 1000)

    def record_failure(self):
        with self._lock:
            self.total_calls += 1
            self.failed_calls += 1
            self.consecutive_failures += 1
            self.consecutive_successes = 0
            self.last_failure_time = int(time.time() * 1000)

    def record_rejection(self):
        with self._lock:
            self.total_calls += 1
            self.rejected_calls += 1

    def record_state_transition(self):
        with self._lock:
            self.state_transitions += 1

    def record_open(self):
        with self._lock:
            self.open_count += 1

    def record_half_open(self):
        with self._lock:
            self.half_open_count += 1

    def reset_consecutive(self):
        with self._lock:
            self.consecutive_failures = 0
            self.consecutive_successes = 0

    def snapshot(self) -> dict:
        with self._lock:
            return {
                'total_calls': self.total_calls,
                'successful_calls': self.successful_calls,
                'failed_calls': self.failed_calls,
                'rejected_calls': self.rejected_calls,
                'state_transitions': self.state_transitions,
                'consecutive_failures': self.consecutive_failures,
                'consecutive_successes': self.consecutive_successes,
                'open_count': self.open_count,
                'half_open_count': self.half_open_count,
                'last_failure_time': self.last_failure_time,
                'last_success_time': self.last_success_time,
            }


# ═══════════════════════════════════════════════════════════════
# 熔断器主类
# ═══════════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    熔断器 — 防止级联故障的核心组件。

    用法:
      cb = CircuitBreaker("risk_breaker", config)

      # 方式1: 装饰器
      @cb.decorate
      def risky_call():
          ...

      # 方式2: 手动控制
      with cb.context():
          risky_call()

      # 方式3: 包装 Result 返回函数
      result = cb.call(lambda: do_something())
    """

    __slots__ = (
        '_name', '_config', '_state', '_stats',
        '_half_open_calls', '_lock', '_opened_at',
    )

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self._name = name
        self._config = config or CircuitBreakerConfig()
        self._state = BreakerState.CLOSED
        self._stats = BreakerStats()
        self._half_open_calls = 0        # 半开状态已放行的探测数
        self._lock = threading.RLock()
        self._opened_at: Optional[int] = None  # 进入 OPEN 的时间戳 (ms)

    # ─── 属性 ───────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def stats(self) -> BreakerStats:
        return self._stats

    @property
    def is_closed(self) -> bool:
        return self._state == BreakerState.CLOSED

    @property
    def is_open(self) -> bool:
        return self._state == BreakerState.OPEN

    @property
    def is_half_open(self) -> bool:
        return self._state == BreakerState.HALF_OPEN

    # ─── 状态转换 ───────────────────────────────────────────────

    def _transition_to(self, new_state: BreakerState):
        """状态转换（内部方法）"""
        if self._state == new_state:
            return
        old_state = self._state
        self._state = new_state
        self._stats.record_state_transition()

        if new_state == BreakerState.OPEN:
            self._opened_at = int(time.time() * 1000)
            self._stats.record_open()
        elif new_state == BreakerState.HALF_OPEN:
            self._half_open_calls = 0
            self._stats.record_half_open()
        elif new_state == BreakerState.CLOSED:
            self._stats.reset_consecutive()

    def _check_open_timeout(self) -> bool:
        """检查 OPEN 状态是否超时，超时则转入 HALF_OPEN"""
        if self._state != BreakerState.OPEN or not self._opened_at:
            return False
        now = int(time.time() * 1000)
        if now - self._opened_at >= self._config.open_duration_ms:
            self._transition_to(BreakerState.HALF_OPEN)
            return True
        return False

    # ─── 核心逻辑 ───────────────────────────────────────────────

    def can_execute(self) -> bool:
        """
        检查是否允许执行请求。
        返回 True 表示允许，False 表示被熔断拒绝。
        """
        with self._lock:
            # OPEN 状态：检查是否超时
            if self._state == BreakerState.OPEN:
                if self._check_open_timeout():
                    return True  # 转入 HALF_OPEN，允许探测
                self._stats.record_rejection()
                return False

            # HALF_OPEN 状态：限制探测请求数
            if self._state == BreakerState.HALF_OPEN:
                if self._half_open_calls < self._config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                self._stats.record_rejection()
                return False

            # CLOSED 状态：正常通行
            return True

    def record_success(self):
        """记录成功调用"""
        with self._lock:
            self._stats.record_success()

            if self._state == BreakerState.HALF_OPEN:
                if self._stats.consecutive_successes >= self._config.success_threshold:
                    self._transition_to(BreakerState.CLOSED)

    def record_failure(self, exception: Optional[Exception] = None):
        """
        记录失败调用。
        根据配置判断是否触发熔断。
        """
        with self._lock:
            # 检查是否忽略此异常
            if exception and self._config.ignored_exceptions:
                if isinstance(exception, self._config.ignored_exceptions):
                    return

            # 检查是否只计数特定异常
            if exception and self._config.expected_exceptions:
                if not isinstance(exception, self._config.expected_exceptions):
                    return

            self._stats.record_failure()

            if self._state == BreakerState.HALF_OPEN:
                # 半开状态任何失败都重新打开
                self._transition_to(BreakerState.OPEN)
                return

            if self._state == BreakerState.CLOSED:
                if self._stats.consecutive_failures >= self._config.failure_threshold:
                    self._transition_to(BreakerState.OPEN)

    # ─── 调用包装 ───────────────────────────────────────────────

    def call(self, fn: Callable[..., T], *args, **kwargs) -> Result[T]:
        """
        包装函数调用，自动处理熔断逻辑。
        返回 Result，被熔断时返回 Err。
        """
        if not self.can_execute():
            return Result.err(Error(
                code="CB_OPEN",
                message=f"熔断器 '{self._name}' 处于 OPEN 状态，请求被拒绝",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.RISK_VIOLATION,
                source=f"circuit_breaker.{self._name}",
                context={
                    'state': self._state.value,
                    'consecutive_failures': self._stats.consecutive_failures,
                    'opened_at': self._opened_at,
                },
            ))

        try:
            result = fn(*args, **kwargs)
            self.record_success()
            return Result.ok(result)
        except Exception as e:
            self.record_failure(e)
            return Result.err(Error(
                code="CB_EXEC_ERR",
                message=f"熔断器 '{self._name}' 调用异常: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
                source=f"circuit_breaker.{self._name}",
                context={'exception_type': type(e).__name__},
            ))

    def call_async(self, fn: Callable[..., T], *args, **kwargs) -> Result[T]:
        """
        异步调用包装（返回 Result，实际为同步执行但预留异步接口）
        """
        # 当前实现为同步，预留异步扩展
        return self.call(fn, *args, **kwargs)

    # ─── 上下文管理器 ───────────────────────────────────────────

    class _ContextManager:
        """熔断器上下文管理器"""

        def __init__(self, breaker: 'CircuitBreaker'):
            self._breaker = breaker
            self._allowed = False

        def __enter__(self):
            if not self._breaker.can_execute():
                raise CircuitOpenError(
                    f"熔断器 '{self._breaker._name}' 处于 OPEN 状态"
                )
            self._allowed = True
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not self._allowed:
                return False
            if exc_type is not None:
                self._breaker.record_failure(exc_val)
                return False  # 不吞掉异常
            self._breaker.record_success()
            return False

    def context(self) -> _ContextManager:
        """返回上下文管理器，用于 with 语句"""
        return self._ContextManager(self)

    # ─── 装饰器 ─────────────────────────────────────────────────

    def decorate(self, fn: Callable) -> Callable:
        """装饰器：自动应用熔断逻辑"""
        def wrapper(*args, **kwargs):
            result = self.call(fn, *args, **kwargs)
            if result.is_err():
                raise CircuitCallError(str(result.unwrap_err()))
            return result.unwrap()
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    # ─── 管理操作 ───────────────────────────────────────────────

    def force_open(self):
        """强制打开熔断器（运维操作）"""
        with self._lock:
            self._transition_to(BreakerState.OPEN)

    def force_close(self):
        """强制关闭熔断器（恢复）"""
        with self._lock:
            self._transition_to(BreakerState.CLOSED)
            self._stats.reset_consecutive()

    def reset(self):
        """完全重置熔断器"""
        with self._lock:
            self._state = BreakerState.CLOSED
            self._stats = BreakerStats()
            self._half_open_calls = 0
            self._opened_at = None

    # ─── 序列化 ─────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            'name': self._name,
            'state': self._state.value,
            'config': {
                'failure_threshold': self._config.failure_threshold,
                'success_threshold': self._config.success_threshold,
                'open_duration_ms': self._config.open_duration_ms,
                'half_open_max_calls': self._config.half_open_max_calls,
            },
            'stats': self._stats.snapshot(),
            'opened_at': self._opened_at,
        }

    def __repr__(self) -> str:
        return (f"CircuitBreaker(name={self._name!r}, "
                f"state={self._state.value}, "
                f"failures={self._stats.consecutive_failures})")


# ═══════════════════════════════════════════════════════════════
# 自定义异常
# ═══════════════════════════════════════════════════════════════

class CircuitOpenError(Exception):
    """熔断器打开异常"""
    pass


class CircuitCallError(Exception):
    """熔断器调用异常"""
    pass


# ═══════════════════════════════════════════════════════════════
# 熔断器注册表（全局管理）
# ═══════════════════════════════════════════════════════════════

class CircuitBreakerRegistry:
    """
    熔断器注册表 — 集中管理所有熔断器实例。

    用法:
      registry = CircuitBreakerRegistry()
      cb = registry.get_or_create("risk_daily_loss", config)
      cb.call(risky_function)
    """

    __slots__ = ('_breakers', '_lock')

    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """获取或创建熔断器"""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """获取熔断器"""
        return self._breakers.get(name)

    def remove(self, name: str) -> bool:
        """移除熔断器"""
        with self._lock:
            if name in self._breakers:
                del self._breakers[name]
                return True
            return False

    def list_all(self) -> list[str]:
        """列出所有熔断器名称"""
        return list(self._breakers.keys())

    def snapshot_all(self) -> dict:
        """获取所有熔断器的快照"""
        with self._lock:
            return {
                name: cb.to_dict()
                for name, cb in self._breakers.items()
            }

    def force_open_all(self):
        """强制打开所有熔断器（紧急停止）"""
        with self._lock:
            for cb in self._breakers.values():
                cb.force_open()

    def reset_all(self):
        """重置所有熔断器"""
        with self._lock:
            for cb in self._breakers.values():
                cb.reset()

    def get_open_breakers(self) -> list[CircuitBreaker]:
        """获取所有处于 OPEN 状态的熔断器"""
        with self._lock:
            return [cb for cb in self._breakers.values() if cb.is_open]


# ═══════════════════════════════════════════════════════════════
# 预配置工厂函数
# ═══════════════════════════════════════════════════════════════

def create_risk_circuit_breaker(name: str = "risk_breaker") -> CircuitBreaker:
    """
    风控专用熔断器（激进熔断，快速恢复）
    - 连续 3 次风控拒绝 → OPEN
    - 冷却 30 秒后 → HALF_OPEN
    - 1 次成功 → CLOSED
    """
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=1,
        open_duration_ms=30000,
        half_open_max_calls=1,
    )
    return CircuitBreaker(name, config)


def create_network_circuit_breaker(name: str = "network_breaker") -> CircuitBreaker:
    """
    网络请求熔断器（容忍更多失败，较长冷却）
    - 连续 5 次失败 → OPEN
    - 冷却 60 秒 → HALF_OPEN
    - 3 次成功 → CLOSED
    """
    config = CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=3,
        open_duration_ms=60000,
        half_open_max_calls=1,
    )
    return CircuitBreaker(name, config)


def create_execution_circuit_breaker(name: str = "execution_breaker") -> CircuitBreaker:
    """
    执行层熔断器（严格熔断，防止重复下单）
    - 连续 2 次执行失败 → OPEN
    - 冷却 120 秒 → HALF_OPEN
    - 2 次成功 → CLOSED
    """
    config = CircuitBreakerConfig(
        failure_threshold=2,
        success_threshold=2,
        open_duration_ms=120000,
        half_open_max_calls=1,
    )
    return CircuitBreaker(name, config)


# ═══════════════════════════════════════════════════════════════
# 全局注册表实例
# ═══════════════════════════════════════════════════════════════

_global_registry = CircuitBreakerRegistry()


def get_global_registry() -> CircuitBreakerRegistry:
    """获取全局熔断器注册表"""
    return _global_registry
