"""
src/core/state_registry.py
MA26-ATR 量化交易系统 — 核心原语：状态注册表

设计理念:
  集中管理系统的所有状态：
    1. 系统级状态机（生命周期管理）
    2. 模块级状态追踪（每个模块独立状态）
    3. 状态转换守卫（防止非法转换）
    4. 状态变更通知（与 EventBus 集成）

  状态层级:
    SystemState  → 全局系统状态
    ModuleState  → 各模块运行状态
    HealthState  → 健康度子状态

  状态转换验证:
    合法转换白名单 + 守卫条件检查
"""

import time
import threading
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from collections import defaultdict, deque

from .result import Result, Error, ErrorSeverity, ErrorCategory
from .event_bus import EventBus, Event, EventType, get_global_event_bus


# ═══════════════════════════════════════════════════════════════
# 系统状态枚举
# ═══════════════════════════════════════════════════════════════

class SystemState(Enum):
    """
    系统生命周期状态。

    状态转换图:
      IDLE ──→ INITIALIZING ──→ RUNNING ──→ STOPPING ──→ STOPPED
                     │              │            │
                     │              └──→ PAUSED ──┘
                     │                     │
                     └──→ ERROR ◄──────────┘
                            │
                            └──→ RECOVERING ──→ RUNNING
    """
    IDLE = "IDLE"                         # 空闲（未启动）
    INITIALIZING = "INITIALIZING"         # 初始化中
    RUNNING = "RUNNING"                   # 运行中
    PAUSED = "PAUSED"                     # 暂停（可恢复）
    STOPPING = "STOPPING"                 # 停止中
    STOPPED = "STOPPED"                   # 已停止
    ERROR = "ERROR"                       # 错误状态
    RECOVERING = "RECOVERING"             # 恢复中


class ModuleStatus(Enum):
    """
    模块运行状态。
    """
    UNLOADED = "UNLOADED"                 # 未加载
    LOADED = "LOADED"                     # 已加载
    INITIALIZING = "INITIALIZING"         # 初始化中
    READY = "READY"                       # 就绪（可工作）
    RUNNING = "RUNNING"                   # 运行中
    DEGRADED = "DEGRADED"                 # 降级运行（部分功能异常）
    PAUSED = "PAUSED"                     # 暂停
    STOPPING = "STOPPING"                 # 停止中
    STOPPED = "STOPPED"                   # 已停止
    ERROR = "ERROR"                       # 错误
    DISABLED = "DISABLED"                 # 已禁用


class HealthLevel(Enum):
    """
    健康度级别。
    """
    HEALTHY = "HEALTHY"                   # 健康
    DEGRADED = "DEGRADED"                 # 降级（可继续运行）
    UNHEALTHY = "UNHEALTHY"               # 不健康（需要干预）
    CRITICAL = "CRITICAL"                 # 严重（系统可能崩溃）


# ═══════════════════════════════════════════════════════════════
# 状态转换规则
# ═══════════════════════════════════════════════════════════════

# 系统状态合法转换白名单
SYSTEM_STATE_TRANSITIONS: Dict[SystemState, Set[SystemState]] = {
    SystemState.IDLE: {
        SystemState.INITIALIZING,
        SystemState.STOPPED,  # 允许从 STOPPED 重启
    },
    SystemState.INITIALIZING: {
        SystemState.RUNNING,
        SystemState.ERROR,
        SystemState.STOPPED,
    },
    SystemState.RUNNING: {
        SystemState.PAUSED,
        SystemState.STOPPING,
        SystemState.ERROR,
    },
    SystemState.PAUSED: {
        SystemState.RUNNING,
        SystemState.STOPPING,
        SystemState.ERROR,
    },
    SystemState.STOPPING: {
        SystemState.STOPPED,
        SystemState.ERROR,
    },
    SystemState.STOPPED: {
        SystemState.IDLE,
        SystemState.INITIALIZING,
    },
    SystemState.ERROR: {
        SystemState.RECOVERING,
        SystemState.STOPPING,
        SystemState.STOPPED,
    },
    SystemState.RECOVERING: {
        SystemState.RUNNING,
        SystemState.ERROR,
        SystemState.STOPPED,
    },
}

# 模块状态合法转换白名单
MODULE_STATUS_TRANSITIONS: Dict[ModuleStatus, Set[ModuleStatus]] = {
    ModuleStatus.UNLOADED: {ModuleStatus.LOADED},
    ModuleStatus.LOADED: {ModuleStatus.INITIALIZING, ModuleStatus.DISABLED},
    ModuleStatus.INITIALIZING: {
        ModuleStatus.READY, ModuleStatus.ERROR, ModuleStatus.DISABLED,
    },
    ModuleStatus.READY: {
        ModuleStatus.RUNNING, ModuleStatus.PAUSED, ModuleStatus.STOPPED,
    },
    ModuleStatus.RUNNING: {
        ModuleStatus.PAUSED, ModuleStatus.DEGRADED, ModuleStatus.STOPPING,
        ModuleStatus.ERROR,
    },
    ModuleStatus.DEGRADED: {
        ModuleStatus.RUNNING, ModuleStatus.PAUSED, ModuleStatus.STOPPING,
        ModuleStatus.ERROR,
    },
    ModuleStatus.PAUSED: {
        ModuleStatus.RUNNING, ModuleStatus.STOPPING, ModuleStatus.ERROR,
    },
    ModuleStatus.STOPPING: {
        ModuleStatus.STOPPED, ModuleStatus.ERROR,
    },
    ModuleStatus.STOPPED: {
        ModuleStatus.UNLOADED, ModuleStatus.INITIALIZING,
    },
    ModuleStatus.ERROR: {
        ModuleStatus.INITIALIZING, ModuleStatus.STOPPED, ModuleStatus.DISABLED,
    },
    ModuleStatus.DISABLED: {
        ModuleStatus.UNLOADED, ModuleStatus.LOADED,
    },
}


# ═══════════════════════════════════════════════════════════════
# 状态转换记录
# ═══════════════════════════════════════════════════════════════

@dataclass
class StateTransition:
    """
    状态转换记录 — 审计和调试用。
    """
    entity_id: str                        # 实体 ID（系统或模块名）
    entity_type: str                      # "system" 或 "module"
    from_state: str                       # 旧状态
    to_state: str                         # 新状态
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    reason: str = ""                      # 转换原因
    triggered_by: str = ""                # 触发者
    success: bool = True                  # 是否成功
    error_message: str = ""               # 失败原因
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'entity_id': self.entity_id,
            'entity_type': self.entity_type,
            'from_state': self.from_state,
            'to_state': self.to_state,
            'timestamp': self.timestamp,
            'reason': self.reason,
            'triggered_by': self.triggered_by,
            'success': self.success,
            'error_message': self.error_message,
            'metadata': self.metadata,
        }


# ═══════════════════════════════════════════════════════════════
# 模块状态信息
# ═══════════════════════════════════════════════════════════════

@dataclass
class ModuleState:
    """
    模块状态信息。
    """
    module_name: str
    status: ModuleStatus = ModuleStatus.UNLOADED
    health: HealthLevel = HealthLevel.HEALTHY
    last_updated: int = field(default_factory=lambda: int(time.time() * 1000))
    last_error: Optional[str] = None
    last_error_time: Optional[int] = None
    uptime_ms: int = 0                   # 连续运行时间
    started_at: Optional[int] = None      # 最近一次启动时间
    restart_count: int = 0               # 重启次数
    degraded_reasons: List[str] = field(default_factory=list)
    custom_state: Dict[str, Any] = field(default_factory=dict)

    def update_status(
        self,
        new_status: ModuleStatus,
        reason: str = "",
        error: Optional[Exception] = None,
    ) -> StateTransition:
        """更新状态并记录转换"""
        old_status = self.status
        self.status = new_status
        self.last_updated = int(time.time() * 1000)

        if error:
            self.last_error = str(error)
            self.last_error_time = self.last_updated

        if new_status == ModuleStatus.RUNNING and old_status != ModuleStatus.RUNNING:
            self.started_at = self.last_updated
            self.uptime_ms = 0
        elif new_status == ModuleStatus.RUNNING and self.started_at:
            self.uptime_ms = self.last_updated - self.started_at

        if new_status == ModuleStatus.ERROR:
            self.restart_count += 1

        if new_status != ModuleStatus.DEGRADED:
            self.degraded_reasons.clear()

        return StateTransition(
            entity_id=self.module_name,
            entity_type="module",
            from_state=old_status.value,
            to_state=new_status.value,
            reason=reason,
            success=True,
        )

    def set_degraded(self, reason: str):
        """设置为降级状态"""
        self.degraded_reasons.append(reason)
        self.health = HealthLevel.DEGRADED
        self.status = ModuleStatus.DEGRADED
        self.last_updated = int(time.time() * 1000)

    def set_healthy(self):
        """恢复为健康"""
        self.degraded_reasons.clear()
        self.health = HealthLevel.HEALTHY
        if self.status == ModuleStatus.DEGRADED:
            self.status = ModuleStatus.RUNNING

    def set_custom_state(self, key: str, value: Any):
        """设置自定义状态"""
        self.custom_state[key] = value
        self.last_updated = int(time.time() * 1000)

    def get_custom_state(self, key: str, default: Any = None) -> Any:
        return self.custom_state.get(key, default)

    def to_dict(self) -> dict:
        return {
            'module_name': self.module_name,
            'status': self.status.value,
            'health': self.health.value,
            'last_updated': self.last_updated,
            'last_error': self.last_error,
            'uptime_ms': self.uptime_ms,
            'restart_count': self.restart_count,
            'degraded_reasons': self.degraded_reasons,
            'custom_state': self.custom_state,
        }


# ═══════════════════════════════════════════════════════════════
# 状态守卫
# ═══════════════════════════════════════════════════════════════

class StateGuard:
    """
    状态转换守卫 — 验证状态转换的合法性。

    可注册自定义守卫条件，在状态转换前检查。
    """

    def __init__(self):
        self._system_guards: List[Callable[[SystemState, SystemState, str], Optional[str]]] = []
        self._module_guards: List[Callable[[str, ModuleStatus, ModuleStatus, str], Optional[str]]] = []

    def register_system_guard(
        self,
        guard: Callable[[SystemState, SystemState, str], Optional[str]],
    ):
        """
        注册系统状态守卫。
        guard(from_state, to_state, reason) → None 表示通过，返回字符串表示拒绝原因。
        """
        self._system_guards.append(guard)

    def register_module_guard(
        self,
        guard: Callable[[str, ModuleStatus, ModuleStatus, str], Optional[str]],
    ):
        """
        注册模块状态守卫。
        guard(module_name, from_status, to_status, reason) → None 表示通过。
        """
        self._module_guards.append(guard)

    def check_system_transition(
        self, from_state: SystemState, to_state: SystemState, reason: str = ""
    ) -> Result[None]:
        """检查系统状态转换是否合法"""
        # 白名单检查
        allowed = SYSTEM_STATE_TRANSITIONS.get(from_state, set())
        if to_state not in allowed:
            return Result.err(Error(
                code="STATE_001",
                message=f"非法系统状态转换: {from_state.value} → {to_state.value}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.VALIDATION,
                source="state_registry",
                context={'from': from_state.value, 'to': to_state.value, 'reason': reason},
            ))

        # 自定义守卫
        for guard in self._system_guards:
            try:
                result = guard(from_state, to_state, reason)
                if result is not None:
                    return Result.err(Error(
                        code="STATE_002",
                        message=f"系统状态守卫拒绝转换: {result}",
                        severity=ErrorSeverity.RECOVERABLE,
                        category=ErrorCategory.VALIDATION,
                        source="state_registry",
                    ))
            except Exception as e:
                return Result.err(Error(
                    code="STATE_003",
                    message=f"系统状态守卫异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))

        return Result.ok(None)

    def check_module_transition(
        self,
        module_name: str,
        from_status: ModuleStatus,
        to_status: ModuleStatus,
        reason: str = "",
    ) -> Result[None]:
        """检查模块状态转换是否合法"""
        allowed = MODULE_STATUS_TRANSITIONS.get(from_status, set())
        if to_status not in allowed:
            return Result.err(Error(
                code="STATE_004",
                message=f"非法模块状态转换: {module_name} {from_status.value} → {to_status.value}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.VALIDATION,
                source="state_registry",
                context={
                    'module': module_name,
                    'from': from_status.value,
                    'to': to_status.value,
                    'reason': reason,
                },
            ))

        for guard in self._module_guards:
            try:
                result = guard(module_name, from_status, to_status, reason)
                if result is not None:
                    return Result.err(Error(
                        code="STATE_005",
                        message=f"模块状态守卫拒绝转换 [{module_name}]: {result}",
                        severity=ErrorSeverity.RECOVERABLE,
                        category=ErrorCategory.VALIDATION,
                        source="state_registry",
                    ))
            except Exception as e:
                return Result.err(Error(
                    code="STATE_006",
                    message=f"模块状态守卫异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))

        return Result.ok(None)


# ═══════════════════════════════════════════════════════════════
# 状态注册表（核心）
# ═══════════════════════════════════════════════════════════════

class StateRegistry:
    """
    状态注册表 — 系统状态管理的核心组件。

    职责:
      1. 维护系统当前状态
      2. 管理所有模块的状态
      3. 验证状态转换合法性
      4. 记录状态转换历史
      5. 与 EventBus 集成（自动发布状态变更事件）
      6. 提供状态查询接口

    用法:
      registry = StateRegistry()

      # 系统状态转换
      registry.transition_system(SystemState.INITIALIZING, reason="Bootstrap start")

      # 注册模块
      registry.register_module("m01_strategy")

      # 模块状态转换
      registry.transition_module("m01_strategy", ModuleStatus.READY, reason="Init complete")

      # 查询
      registry.get_system_state()
      registry.get_module_state("m01_strategy")
      registry.get_all_modules_healthy()
    """

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        enable_event_publishing: bool = True,
        max_history: int = 1000,
    ):
        self._system_state: SystemState = SystemState.IDLE
        self._system_health: HealthLevel = HealthLevel.HEALTHY
        self._modules: Dict[str, ModuleState] = {}
        self._guard = StateGuard()
        self._event_bus = event_bus or get_global_event_bus()
        self._enable_events = enable_event_publishing
        self._max_history = max_history
        self._transition_history: deque = deque(maxlen=max_history)
        self._lock = threading.RLock()
        self._started_at: Optional[int] = None
        self._last_error: Optional[str] = None

    # ─── 系统状态 ───────────────────────────────────────────────

    @property
    def system_state(self) -> SystemState:
        return self._system_state

    @property
    def system_health(self) -> HealthLevel:
        return self._system_health

    def get_system_state(self) -> SystemState:
        return self._system_state

    def get_system_uptime_ms(self) -> int:
        if self._started_at:
            return int(time.time() * 1000) - self._started_at
        return 0

    def transition_system(
        self,
        to_state: SystemState,
        reason: str = "",
        triggered_by: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Result[StateTransition]:
        """
        系统状态转换。
        """
        with self._lock:
            from_state = self._system_state

            if from_state == to_state:
                return Result.ok(StateTransition(
                    entity_id="system",
                    entity_type="system",
                    from_state=from_state.value,
                    to_state=to_state.value,
                    reason="No-op (same state)",
                    success=True,
                ))

            # 守卫检查
            guard_result = self._guard.check_system_transition(from_state, to_state, reason)
            if guard_result.is_err():
                transition = StateTransition(
                    entity_id="system",
                    entity_type="system",
                    from_state=from_state.value,
                    to_state=to_state.value,
                    reason=reason,
                    triggered_by=triggered_by,
                    success=False,
                    error_message=guard_result.unwrap_err().message,
                )
                self._transition_history.append(transition)
                return Result.err(guard_result.unwrap_err())

            # 执行转换
            self._system_state = to_state

            if to_state == SystemState.RUNNING and from_state != SystemState.RUNNING:
                self._started_at = int(time.time() * 1000)
            elif to_state in (SystemState.STOPPED, SystemState.ERROR):
                self._started_at = None

            if to_state == SystemState.ERROR:
                self._system_health = HealthLevel.CRITICAL
                self._last_error = reason
            elif to_state == SystemState.RUNNING:
                self._system_health = HealthLevel.HEALTHY
                self._last_error = None

            # 记录历史
            transition = StateTransition(
                entity_id="system",
                entity_type="system",
                from_state=from_state.value,
                to_state=to_state.value,
                reason=reason,
                triggered_by=triggered_by,
                success=True,
                metadata=metadata or {},
            )
            self._transition_history.append(transition)

        # 发布事件
        if self._enable_events:
            try:
                event = Event(
                    event_type=EventType.SYSTEM_STARTED if to_state == SystemState.RUNNING
                              else EventType.SYSTEM_STOPPING if to_state == SystemState.STOPPING
                              else EventType.SYSTEM_ERROR if to_state == SystemState.ERROR
                              else EventType.SYSTEM_HEALTH_CHECK,
                    source="state_registry",
                    payload={
                        'from_state': from_state.value,
                        'to_state': to_state.value,
                        'reason': reason,
                        'triggered_by': triggered_by,
                    },
                )
                self._event_bus.publish(event)
            except Exception:
                pass

        return Result.ok(transition)

    # ─── 模块状态 ───────────────────────────────────────────────

    def register_module(
        self,
        module_name: str,
        initial_status: ModuleStatus = ModuleStatus.UNLOADED,
    ) -> Result[None]:
        """注册模块到状态注册表"""
        with self._lock:
            if module_name in self._modules:
                return Result.err(Error(
                    code="STATE_007",
                    message=f"模块已注册: {module_name}",
                    severity=ErrorSeverity.INFO,
                    category=ErrorCategory.VALIDATION,
                ))
            self._modules[module_name] = ModuleState(
                module_name=module_name,
                status=initial_status,
            )
        return Result.ok(None)

    def unregister_module(self, module_name: str) -> Result[None]:
        """注销模块"""
        with self._lock:
            if module_name not in self._modules:
                return Result.err(Error(
                    code="STATE_008",
                    message=f"模块未注册: {module_name}",
                    severity=ErrorSeverity.INFO,
                    category=ErrorCategory.VALIDATION,
                ))
            del self._modules[module_name]
        return Result.ok(None)

    def get_module_state(self, module_name: str) -> Optional[ModuleState]:
        """获取模块状态"""
        return self._modules.get(module_name)

    def get_all_module_states(self) -> Dict[str, ModuleState]:
        """获取所有模块状态"""
        with self._lock:
            return dict(self._modules)

    def transition_module(
        self,
        module_name: str,
        to_status: ModuleStatus,
        reason: str = "",
        triggered_by: str = "",
        error: Optional[Exception] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Result[StateTransition]:
        """模块状态转换"""
        with self._lock:
            if module_name not in self._modules:
                return Result.err(Error(
                    code="STATE_009",
                    message=f"模块未注册: {module_name}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                    source="state_registry",
                ))

            module_state = self._modules[module_name]
            from_status = module_state.status

            if from_status == to_status and not error:
                return Result.ok(StateTransition(
                    entity_id=module_name,
                    entity_type="module",
                    from_state=from_status.value,
                    to_state=to_status.value,
                    reason="No-op",
                    success=True,
                ))

            # 守卫检查
            guard_result = self._guard.check_module_transition(
                module_name, from_status, to_status, reason
            )
            if guard_result.is_err():
                transition = StateTransition(
                    entity_id=module_name,
                    entity_type="module",
                    from_state=from_status.value,
                    to_state=to_status.value,
                    reason=reason,
                    triggered_by=triggered_by,
                    success=False,
                    error_message=guard_result.unwrap_err().message,
                )
                self._transition_history.append(transition)
                return Result.err(guard_result.unwrap_err())

            # 执行转换
            transition = module_state.update_status(to_status, reason, error)

            # 更新系统健康度
            self._update_system_health_from_modules()

            # 记录历史
            transition.triggered_by = triggered_by
            transition.metadata = metadata or {}
            self._transition_history.append(transition)

        # 发布事件
        if self._enable_events:
            try:
                event = Event(
                    event_type=EventType.SYSTEM_HEALTH_CHECK,
                    source="state_registry",
                    payload={
                        'module': module_name,
                        'from_status': from_status.value,
                        'to_status': to_status.value,
                        'reason': reason,
                        'health': module_state.health.value,
                    },
                )
                self._event_bus.publish(event)
            except Exception:
                pass

        return Result.ok(transition)

    def set_module_health(
        self,
        module_name: str,
        health: HealthLevel,
        reason: str = "",
    ) -> Result[None]:
        """直接设置模块健康度"""
        with self._lock:
            if module_name not in self._modules:
                return Result.err(Error(
                    code="STATE_010",
                    message=f"模块未注册: {module_name}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))

            self._modules[module_name].health = health
            if health == HealthLevel.DEGRADED:
                self._modules[module_name].set_degraded(reason)
            elif health == HealthLevel.HEALTHY:
                self._modules[module_name].set_healthy()

            self._update_system_health_from_modules()
        return Result.ok(None)

    def set_module_custom_state(
        self, module_name: str, key: str, value: Any
    ) -> Result[None]:
        """设置模块自定义状态"""
        with self._lock:
            if module_name not in self._modules:
                return Result.err(Error(
                    code="STATE_011",
                    message=f"模块未注册: {module_name}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))
            self._modules[module_name].set_custom_state(key, value)
        return Result.ok(None)

    def _update_system_health_from_modules(self):
        """根据模块健康度更新系统健康度"""
        if not self._modules:
            return

        healths = [m.health for m in self._modules.values()]
        if HealthLevel.CRITICAL in healths:
            self._system_health = HealthLevel.CRITICAL
        elif HealthLevel.UNHEALTHY in healths:
            self._system_health = HealthLevel.UNHEALTHY
        elif HealthLevel.DEGRADED in healths:
            self._system_health = HealthLevel.DEGRADED
        else:
            self._system_health = HealthLevel.HEALTHY

    # ─── 批量查询 ───────────────────────────────────────────────

    def get_modules_by_status(self, status: ModuleStatus) -> List[str]:
        """获取指定状态的所有模块名"""
        with self._lock:
            return [name for name, m in self._modules.items() if m.status == status]

    def get_modules_by_health(self, health: HealthLevel) -> List[str]:
        """获取指定健康度的所有模块名"""
        with self._lock:
            return [name for name, m in self._modules.items() if m.health == health]

    def all_modules_ready(self) -> bool:
        """检查是否所有模块都已就绪"""
        with self._lock:
            if not self._modules:
                return False
            return all(
                m.status in (ModuleStatus.READY, ModuleStatus.RUNNING)
                for m in self._modules.values()
            )

    def all_modules_running(self) -> bool:
        """检查是否所有模块都在运行"""
        with self._lock:
            if not self._modules:
                return False
            return all(
                m.status == ModuleStatus.RUNNING
                for m in self._modules.values()
            )

    def any_module_error(self) -> bool:
        """检查是否有模块处于错误状态"""
        with self._lock:
            return any(m.status == ModuleStatus.ERROR for m in self._modules.values())

    def get_system_summary(self) -> dict:
        """获取系统状态摘要"""
        with self._lock:
            return {
                'system_state': self._system_state.value,
                'system_health': self._system_health.value,
                'uptime_ms': self.get_system_uptime_ms(),
                'total_modules': len(self._modules),
                'modules_by_status': {
                    status.value: len([m for m in self._modules.values() if m.status == status])
                    for status in ModuleStatus
                },
                'modules_by_health': {
                    health.value: len([m for m in self._modules.values() if m.health == health])
                    for health in HealthLevel
                },
                'last_error': self._last_error,
            }

    # ─── 历史查询 ───────────────────────────────────────────────

    def get_transition_history(
        self,
        entity_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[StateTransition]:
        """获取状态转换历史"""
        with self._lock:
            history = list(self._transition_history)

        if entity_id:
            history = [t for t in history if t.entity_id == entity_id]

        if limit:
            history = history[-limit:]

        return history

    def get_recent_transitions(self, count: int = 10) -> List[StateTransition]:
        """获取最近的状态转换"""
        with self._lock:
            return list(self._transition_history)[-count:]

    # ─── 守卫管理 ───────────────────────────────────────────────

    def register_system_guard(
        self, guard: Callable[[SystemState, SystemState, str], Optional[str]]
    ):
        """注册系统状态守卫"""
        self._guard.register_system_guard(guard)

    def register_module_guard(
        self, guard: Callable[[str, ModuleStatus, ModuleStatus, str], Optional[str]]
    ):
        """注册模块状态守卫"""
        self._guard.register_module_guard(guard)

    # ─── 重置 ───────────────────────────────────────────────────

    def reset(self):
        """重置注册表（测试用）"""
        with self._lock:
            self._system_state = SystemState.IDLE
            self._system_health = HealthLevel.HEALTHY
            self._modules.clear()
            self._transition_history.clear()
            self._started_at = None
            self._last_error = None


# ═══════════════════════════════════════════════════════════════
# 全局状态注册表实例
# ═══════════════════════════════════════════════════════════════

_global_state_registry = StateRegistry(enable_event_publishing=True)


def get_global_state_registry() -> StateRegistry:
    """获取全局状态注册表"""
    return _global_state_registry


def get_system_state() -> SystemState:
    """便捷函数：获取当前系统状态"""
    return _global_state_registry.system_state


def transition_system(
    to_state: SystemState, reason: str = "", triggered_by: str = ""
) -> Result[StateTransition]:
    """便捷函数：系统状态转换"""
    return _global_state_registry.transition_system(
        to_state, reason=reason, triggered_by=triggered_by
    )


def register_module(module_name: str) -> Result[None]:
    """便捷函数：注册模块"""
    return _global_state_registry.register_module(module_name)


def transition_module(
    module_name: str, to_status: ModuleStatus, reason: str = ""
) -> Result[StateTransition]:
    """便捷函数：模块状态转换"""
    return _global_state_registry.transition_module(
        module_name, to_status, reason=reason
    )
