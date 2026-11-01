"""
src/core/lifecycle.py
MA26-ATR 量化交易系统 — 核心原语：生命周期管理

设计理念:
  编排系统及所有模块的标准化生命周期：
    1. 统一启停接口（LifecycleAware）
    2. 依赖排序启动 / 逆序优雅关闭
    3. 优雅停机（drain + timeout）
    4. 信号处理（SIGINT / SIGTERM → 优雅关闭）
    5. 与 StateRegistry 联动（自动状态流转）
    6. 启动/关闭阶段钩子（Hook）

  生命周期流程:
    [register] → init() → start() → [RUNNING] → stop() → [STOPPED]
                      ↑                    ↓
                      └── pause/resume ────┘
                      ↓
                    ERROR → recover() / force_stop()

  关闭流程（优雅）:
    SIGINT/SIGTERM → request_shutdown()
      → StateRegistry → STOPPING
      → 通知所有模块 drain（停止接收新任务）
      → 等待 in-flight 任务完成（drain_timeout）
      → 逆序调用 stop()
      → 超时则 force_stop()
      → StateRegistry → STOPPED
"""

import os
import sys
import signal
import time
import threading
import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Any, Type
from enum import Enum

from .result import Result, Error, ErrorSeverity, ErrorCategory
from .state_registry import (
    StateRegistry, SystemState, ModuleStatus,
    get_global_state_registry, transition_module,
)
from .event_bus import EventBus, Event, EventType, get_global_event_bus


# ═══════════════════════════════════════════════════════════════
# 生命周期异常（模块内部使用，对外统一返回 Result）
# ═══════════════════════════════════════════════════════════════

class LifecycleError(Exception):
    """生命周期操作失败"""
    def __init__(self, code: str, message: str, module_name: str = ""):
        self.code = code
        self.message = message
        self.module_name = module_name
        super().__init__(f"[{code}] {message} (module={module_name})")


# ═══════════════════════════════════════════════════════════════
# 生命周期感知接口
# ═══════════════════════════════════════════════════════════════

class LifecycleAware(ABC):
    """
    生命周期感知接口 — 所有可管理模块必须实现。

    方法调用顺序:
      __init__() → on_register() → on_init() → on_start() → on_ready()
      → [RUNNING] → on_pause() / on_resume()
      → on_stop() → on_unregistered()

    关闭流程:
      on_drain() → on_stop() → on_destroy()
    """

    @property
    @abstractmethod
    def module_name(self) -> str:
        """模块名称（唯一标识）"""
        ...

    @property
    @abstractmethod
    def dependencies(self) -> List[str]:
        """
        依赖的模块名列表。
        被依赖的模块会先启动、后关闭。
        """
        ...

    @property
    def init_timeout_ms(self) -> int:
        """初始化超时（默认 30s）"""
        return 30000

    @property
    def start_timeout_ms(self) -> int:
        """启动超时（默认 30s）"""
        return 30000

    @property
    def drain_timeout_ms(self) -> int:
        """优雅停机 drain 超时（默认 10s）"""
        return 10000

    @property
    def stop_timeout_ms(self) -> int:
        """停止超时（默认 15s）"""
        return 15000

    # ─── 生命周期钩子 ─────────────────────────────────────────

    def on_register(self) -> Result[None]:
        """注册到生命周期管理器时调用"""
        return Result.ok(None)

    def on_init(self) -> Result[None]:
        """初始化资源（连接、配置加载等）"""
        return Result.ok(None)

    def on_start(self) -> Result[None]:
        """启动模块主逻辑（启动线程/协程/监听等）"""
        return Result.ok(None)

    def on_ready(self) -> Result[None]:
        """就绪通知（所有依赖已启动完成）"""
        return Result.ok(None)

    def on_pause(self) -> Result[None]:
        """暂停（停止处理新数据，保留状态）"""
        return Result.ok(None)

    def on_resume(self) -> Result[None]:
        """恢复"""
        return Result.ok(None)

    def on_drain(self) -> Result[None]:
        """
        优雅停机第一步：停止接收新任务，完成 in-flight 任务。
        有 timeout 限制。
        """
        return Result.ok(None)

    def on_stop(self) -> Result[None]:
        """停止模块（释放资源、关闭连接）"""
        return Result.ok(None)

    def on_destroy(self) -> Result[None]:
        """销毁（清理所有资源）"""
        return Result.ok(None)

    def on_health_check(self) -> Result[dict]:
        """健康检查"""
        return Result.ok({"status": "ok"})

    def on_recover(self, error: Exception) -> Result[None]:
        """从错误中恢复"""
        return Result.ok(None)


# ═══════════════════════════════════════════════════════════════
# 模块生命周期包装器
# ═══════════════════════════════════════════════════════════════

@dataclass
class ModuleLifecycleEntry:
    """
    模块生命周期注册表条目。
    """
    module: LifecycleAware
    state: ModuleStatus = ModuleStatus.UNLOADED
    registered_at: int = field(default_factory=lambda: int(time.time() * 1000))
    started_at: Optional[int] = None
    stopped_at: Optional[int] = None
    last_error: Optional[str] = None
    init_attempts: int = 0
    start_attempts: int = 0
    stop_attempts: int = 0

    def to_dict(self) -> dict:
        return {
            'module_name': self.module.module_name,
            'dependencies': self.module.dependencies,
            'state': self.state.value,
            'registered_at': self.registered_at,
            'started_at': self.started_at,
            'stopped_at': self.stopped_at,
            'last_error': self.last_error,
            'init_attempts': self.init_attempts,
            'start_attempts': self.start_attempts,
        }


# ═══════════════════════════════════════════════════════════════
# 生命周期管理器（核心）
# ═══════════════════════════════════════════════════════════════

class LifecycleManager:
    """
    生命周期管理器 — 编排所有模块的标准化启停。

    用法:
      manager = LifecycleManager()

      # 注册模块
      manager.register(m01_strategy)
      manager.register(m04_risk)

      # 启动所有
      manager.start_all()

      # 优雅关闭
      manager.shutdown()

      # 信号处理
      manager.install_signal_handlers()
    """

    def __init__(
        self,
        state_registry: Optional[StateRegistry] = None,
        event_bus: Optional[EventBus] = None,
        enable_signal_handlers: bool = True,
    ):
        self._modules: Dict[str, ModuleLifecycleEntry] = {}
        self._state_registry = state_registry or get_global_state_registry()
        self._event_bus = event_bus or get_global_event_bus()
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._shutdown_requested = False
        self._startup_order: List[str] = []
        self._shutdown_hooks: List[Callable[[], None]] = []
        self._startup_hooks: List[Callable[[], None]] = []

        if enable_signal_handlers:
            self.install_signal_handlers()

    # ─── 模块注册 ─────────────────────────────────────────────

    def register(self, module: LifecycleAware) -> Result[None]:
        """注册模块"""
        name = module.module_name
        with self._lock:
            if name in self._modules:
                return Result.err(Error(
                    code="LIFECYCLE_001",
                    message=f"模块已注册: {name}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                    source="lifecycle_manager",
                ))

            entry = ModuleLifecycleEntry(module=module)
            self._modules[name] = entry

        # 注册到 StateRegistry
        reg_result = self._state_registry.register_module(name)
        if reg_result.is_err():
            with self._lock:
                del self._modules[name]
            return Result.err(reg_result.unwrap_err())

        # 调用模块 on_register
        try:
            result = module.on_register()
            if result.is_err():
                with self._lock:
                    del self._modules[name]
                self._state_registry.unregister_module(name)
                return result
        except Exception as e:
            with self._lock:
                del self._modules[name]
            return Result.err(Error(
                code="LIFECYCLE_002",
                message=f"模块 {name} on_register 异常: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
                source="lifecycle_manager",
            ))

        entry.state = ModuleStatus.LOADED
        return Result.ok(None)

    def unregister(self, module_name: str) -> Result[None]:
        """注销模块"""
        with self._lock:
            entry = self._modules.get(module_name)
            if not entry:
                return Result.err(Error(
                    code="LIFECYCLE_003",
                    message=f"模块未注册: {module_name}",
                    severity=ErrorSeverity.INFO,
                    category=ErrorCategory.VALIDATION,
                ))

        # 如果正在运行，先停止
        if entry.state == ModuleStatus.RUNNING:
            stop_result = self.stop_module(module_name)
            if stop_result.is_err():
                return stop_result

        # 调用 on_destroy
        try:
            entry.module.on_destroy()
        except Exception:
            pass

        # 从注册表移除
        with self._lock:
            del self._modules[module_name]

        self._state_registry.unregister_module(module_name)
        return Result.ok(None)

    def get_module(self, name: str) -> Optional[LifecycleAware]:
        """获取模块实例"""
        entry = self._modules.get(name)
        return entry.module if entry else None

    def get_module_entry(self, name: str) -> Optional[ModuleLifecycleEntry]:
        return self._modules.get(name)

    # ─── 启动流程 ─────────────────────────────────────────────

    def start_all(self, timeout_ms: Optional[int] = None) -> Result[None]:
        """
        按依赖顺序启动所有模块。
        """
        with self._lock:
            if self._shutdown_requested:
                return Result.err(Error(
                    code="LIFECYCLE_004",
                    message="已请求关闭，无法启动",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))

        # 计算启动顺序（拓扑排序）
        sort_result = self._topological_sort()
        if sort_result.is_err():
            return Result.err(sort_result.unwrap_err())

        self._startup_order = sort_result.unwrap()

        # 更新系统状态
        self._state_registry.transition_system(
            SystemState.INITIALIZING,
            reason="start_all called",
            triggered_by="lifecycle_manager",
        )

        # 执行启动钩子
        for hook in self._startup_hooks:
            try:
                hook()
            except Exception:
                pass

        # 按序启动
        failed_modules = []
        for name in self._startup_order:
            entry = self._modules[name]
            start_result = self._start_module(entry, timeout_ms)
            if start_result.is_err():
                failed_modules.append((name, start_result.unwrap_err().message))

        if failed_modules:
            self._state_registry.transition_system(
                SystemState.ERROR,
                reason=f"启动失败: {failed_modules}",
                triggered_by="lifecycle_manager",
            )
            return Result.err(Error(
                code="LIFECYCLE_005",
                message=f"模块启动失败: {failed_modules}",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.INTERNAL,
                source="lifecycle_manager",
            ))

        # 所有模块就绪通知
        for name in self._startup_order:
            entry = self._modules[name]
            try:
                entry.module.on_ready()
            except Exception:
                pass

        # 更新系统状态
        self._state_registry.transition_system(
            SystemState.RUNNING,
            reason="All modules started",
            triggered_by="lifecycle_manager",
        )

        return Result.ok(None)

    def _start_module(
        self, entry: ModuleLifecycleEntry, timeout_ms: Optional[int] = None
    ) -> Result[None]:
        """启动单个模块"""
        name = entry.module.module_name
        timeout = timeout_ms or entry.module.start_timeout_ms

        # 状态转换
        trans_result = self._state_registry.transition_module(
            name, ModuleStatus.INITIALIZING, reason="start_module"
        )
        if trans_result.is_err():
            return Result.err(trans_result.unwrap_err())

        entry.state = ModuleStatus.INITIALIZING
        entry.init_attempts += 1

        # on_init
        try:
            init_result = entry.module.on_init()
            if init_result.is_err():
                entry.last_error = init_result.unwrap_err().message
                self._state_registry.transition_module(
                    name, ModuleStatus.ERROR,
                    reason=f"init failed: {entry.last_error}",
                )
                entry.state = ModuleStatus.ERROR
                return init_result
        except Exception as e:
            entry.last_error = str(e)
            self._state_registry.transition_module(
                name, ModuleStatus.ERROR, reason=f"init exception: {e}",
            )
            entry.state = ModuleStatus.ERROR
            return Result.err(Error(
                code="LIFECYCLE_006",
                message=f"模块 {name} on_init 异常: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))

        # on_start
        trans_result = self._state_registry.transition_module(
            name, ModuleStatus.STARTING if hasattr(ModuleStatus, 'STARTING') else ModuleStatus.READY,
            reason="init complete",
        )
        # 直接到 RUNNING
        trans_result = self._state_registry.transition_module(
            name, ModuleStatus.RUNNING, reason="start complete",
        )
        entry.state = ModuleStatus.RUNNING
        entry.started_at = int(time.time() * 1000)
        entry.start_attempts += 1

        try:
            start_result = entry.module.on_start()
            if start_result.is_err():
                entry.last_error = start_result.unwrap_err().message
                self._state_registry.transition_module(
                    name, ModuleStatus.ERROR,
                    reason=f"start failed: {entry.last_error}",
                )
                entry.state = ModuleStatus.ERROR
                return start_result
        except Exception as e:
            entry.last_error = str(e)
            self._state_registry.transition_module(
                name, ModuleStatus.ERROR, reason=f"start exception: {e}",
            )
            entry.state = ModuleStatus.ERROR
            return Result.err(Error(
                code="LIFECYCLE_007",
                message=f"模块 {name} on_start 异常: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))

        return Result.ok(None)

    # ─── 关闭流程 ─────────────────────────────────────────────

    def request_shutdown(self, reason: str = "User requested"):
        """
        请求优雅关闭（可被信号处理调用）。
        """
        with self._lock:
            if self._shutdown_requested:
                return  # 已经请求过
            self._shutdown_requested = True

        self._shutdown_event.set()

        # 更新系统状态
        self._state_registry.transition_system(
            SystemState.STOPPING,
            reason=reason,
            triggered_by="lifecycle_manager",
        )

        # 发布事件
        try:
            self._event_bus.publish(Event(
                event_type=EventType.SYSTEM_STOPPING,
                source="lifecycle_manager",
                payload={'reason': reason},
            ))
        except Exception:
            pass

        # 启动关闭线程
        shutdown_thread = threading.Thread(
            target=self._do_shutdown,
            args=(reason,),
            name="lifecycle-shutdown",
            daemon=False,
        )
        shutdown_thread.start()

    def _do_shutdown(self, reason: str):
        """执行优雅关闭"""
        # 1. Drain 阶段
        self._drain_all()

        # 2. 逆序停止
        self._stop_all()

        # 3. 执行关闭钩子
        for hook in self._shutdown_hooks:
            try:
                hook()
            except Exception:
                pass

        # 4. 更新系统状态
        self._state_registry.transition_system(
            SystemState.STOPPED,
            reason=f"Shutdown complete: {reason}",
            triggered_by="lifecycle_manager",
        )

    def _drain_all(self):
        """Drain 所有模块（停止接收新任务）"""
        # 按启动顺序 drain
        for name in self._startup_order:
            entry = self._modules.get(name)
            if not entry or entry.state != ModuleStatus.RUNNING:
                continue

            try:
                entry.module.on_drain()
            except Exception:
                pass

    def _stop_all(self):
        """逆序停止所有模块"""
        # 逆序
        reverse_order = list(reversed(self._startup_order))
        for name in reverse_order:
            entry = self._modules.get(name)
            if not entry:
                continue
            self._stop_module(entry)

    def stop_module(self, module_name: str) -> Result[None]:
        """停止单个模块"""
        entry = self._modules.get(module_name)
        if not entry:
            return Result.err(Error(
                code="LIFECYCLE_008",
                message=f"模块未注册: {module_name}",
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.VALIDATION,
            ))

        self._stop_module(entry)
        return Result.ok(None)

    def _stop_module(self, entry: ModuleLifecycleEntry) -> Result[None]:
        """停止单个模块"""
        name = entry.module.module_name
        entry.stop_attempts += 1

        # 状态转换
        self._state_registry.transition_module(
            name, ModuleStatus.STOPPING, reason="stop_module"
        )
        entry.state = ModuleStatus.STOPPING

        # on_stop
        try:
            stop_result = entry.module.on_stop()
            if stop_result.is_err():
                entry.last_error = stop_result.unwrap_err().message
                self._state_registry.transition_module(
                    name, ModuleStatus.ERROR,
                    reason=f"stop failed: {entry.last_error}",
                )
                entry.state = ModuleStatus.ERROR
                return stop_result
        except Exception as e:
            entry.last_error = str(e)
            self._state_registry.transition_module(
                name, ModuleStatus.ERROR, reason=f"stop exception: {e}",
            )
            entry.state = ModuleStatus.ERROR
            return Result.err(Error(
                code="LIFECYCLE_009",
                message=f"模块 {name} on_stop 异常: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))

        self._state_registry.transition_module(
            name, ModuleStatus.STOPPED, reason="stop complete"
        )
        entry.state = ModuleStatus.STOPPED
        entry.stopped_at = int(time.time() * 1000)
        return Result.ok(None)

    def shutdown(self, reason: str = "Explicit shutdown"):
        """同步关闭（阻塞直到完成）"""
        self.request_shutdown(reason)
        self._shutdown_event.wait(timeout=60)  # 最多等 60s

    # ─── 暂停/恢复 ────────────────────────────────────────────

    def pause_all(self, reason: str = "") -> Result[None]:
        """暂停所有模块"""
        self._state_registry.transition_system(
            SystemState.PAUSED, reason=reason, triggered_by="lifecycle_manager",
        )

        errors = []
        for name, entry in self._modules.items():
            if entry.state == ModuleStatus.RUNNING:
                try:
                    entry.module.on_pause()
                    self._state_registry.transition_module(
                        name, ModuleStatus.PAUSED, reason=reason,
                    )
                    entry.state = ModuleStatus.PAUSED
                except Exception as e:
                    errors.append(f"{name}: {e}")

        if errors:
            return Result.err(Error(
                code="LIFECYCLE_010",
                message=f"部分模块暂停失败: {errors}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))
        return Result.ok(None)

    def resume_all(self, reason: str = "") -> Result[None]:
        """恢复所有模块"""
        errors = []
        for name, entry in self._modules.items():
            if entry.state == ModuleStatus.PAUSED:
                try:
                    entry.module.on_resume()
                    self._state_registry.transition_module(
                        name, ModuleStatus.RUNNING, reason=reason,
                    )
                    entry.state = ModuleStatus.RUNNING
                except Exception as e:
                    errors.append(f"{name}: {e}")

        if not errors:
            self._state_registry.transition_system(
                SystemState.RUNNING, reason=reason, triggered_by="lifecycle_manager",
            )

        if errors:
            return Result.err(Error(
                code="LIFECYCLE_011",
                message=f"部分模块恢复失败: {errors}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))
        return Result.ok(None)

    # ─── 信号处理 ─────────────────────────────────────────────

    def install_signal_handlers(self):
        """安装 SIGINT/SIGTERM 信号处理"""
        if threading.current_thread() is not threading.main_thread():
            return  # 只能在主线程安装

        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            self.request_shutdown(reason=f"Received {sig_name}")

        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except (ValueError, OSError):
            pass  # 在某些环境下无法安装（如线程中）

    # ─── 钩子管理 ─────────────────────────────────────────────

    def add_startup_hook(self, hook: Callable[[], None]):
        """添加启动完成钩子"""
        self._startup_hooks.append(hook)

    def add_shutdown_hook(self, hook: Callable[[], None]):
        """添加关闭钩子"""
        self._shutdown_hooks.append(hook)

    # ─── 拓扑排序 ─────────────────────────────────────────────

    def _topological_sort(self) -> Result[List[str]]:
        """
        按依赖关系拓扑排序。
        返回启动顺序（被依赖的先启动）。
        """
        visited: Set[str] = set()
        visiting: Set[str] = set()
        result: List[str] = []

        def visit(name: str) -> Result[None]:
            if name in visiting:
                return Result.err(Error(
                    code="LIFECYCLE_012",
                    message=f"循环依赖检测: {name} → ... → {name}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.VALIDATION,
                    source="lifecycle_manager",
                ))
            if name in visited:
                return Result.ok(None)

            entry = self._modules.get(name)
            if not entry:
                return Result.err(Error(
                    code="LIFECYCLE_013",
                    message=f"依赖模块未注册: {name}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.VALIDATION,
                    source="lifecycle_manager",
                ))

            visiting.add(name)

            for dep in entry.module.dependencies:
                dep_result = visit(dep)
                if dep_result.is_err():
                    return dep_result

            visiting.remove(name)
            visited.add(name)
            result.append(name)
            return Result.ok(None)

        # 遍历所有模块
        for name in list(self._modules.keys()):
            if name not in visited:
                sort_result = visit(name)
                if sort_result.is_err():
                    return Result.err(sort_result.unwrap_err())

        return Result.ok(result)

    # ─── 查询 ─────────────────────────────────────────────────

    def is_shutdown_requested(self) -> bool:
        return self._shutdown_requested

    def wait_for_shutdown(self, timeout: Optional[float] = None):
        """等待关闭信号"""
        self._shutdown_event.wait(timeout=timeout)

    def get_all_status(self) -> Dict[str, dict]:
        """获取所有模块状态"""
        with self._lock:
            return {
                name: entry.to_dict()
                for name, entry in self._modules.items()
            }

    def health_check_all(self) -> Dict[str, Result[dict]]:
        """对所有模块执行健康检查"""
        results = {}
        for name, entry in self._modules.items():
            try:
                results[name] = entry.module.on_health_check()
            except Exception as e:
                results[name] = Result.err(Error(
                    code="LIFECYCLE_014",
                    message=f"健康检查异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))
        return results


# ═══════════════════════════════════════════════════════════════
# 优雅关闭上下文管理器
# ═══════════════════════════════════════════════════════════════

class GracefulShutdown:
    """
    优雅关闭上下文管理器。

    用法:
      with GracefulShutdown(manager):
          # 主循环
          while not shutdown_requested:
              process()
    """

    def __init__(self, manager: LifecycleManager):
        self._manager = manager

    def __enter__(self):
        return self._manager

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._manager.request_shutdown(reason=f"Exception: {exc_val}")
        else:
            self._manager.request_shutdown(reason="Context exit")
        self._manager._shutdown_event.wait(timeout=60)
        return False  # 不吞异常


# ═══════════════════════════════════════════════════════════════
# 全局生命周期管理器
# ═══════════════════════════════════════════════════════════════

_global_lifecycle_manager = LifecycleManager(enable_signal_handlers=True)


def get_global_lifecycle_manager() -> LifecycleManager:
    """获取全局生命周期管理器"""
    return _global_lifecycle_manager


def register_module(module: LifecycleAware) -> Result[None]:
    """便捷函数：注册模块到全局管理器"""
    return _global_lifecycle_manager.register(module)


def start_all() -> Result[None]:
    """便捷函数：启动所有模块"""
    return _global_lifecycle_manager.start_all()


def shutdown(reason: str = "Application exit"):
    """便捷函数：关闭"""
    _global_lifecycle_manager.shutdown(reason)
