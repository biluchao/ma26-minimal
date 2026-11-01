"""
tests/test_core.py
核心基础设施测试 — Result / CircuitBreaker / EventBus / TraceContext /
ConfigSnapshot / StateRegistry / Lifecycle / ModuleManifest

运行方式:
  python tests/test_core.py
  python -m unittest tests.test_core -v

依赖: 仅 Python 标准库 + src/core 下的模块
"""

import sys
import os
import time
import threading
import unittest
from typing import Optional

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core import (
    # result
    Result, Error, ErrorSeverity, ErrorCategory,
    # circuit_breaker
    CircuitBreaker, CircuitConfig, CircuitState,
    # event_bus
    EventBus, Event, EventType, IEventHandler,
    # trace_context
    TraceContext, TraceLevel, get_current_trace, set_current_trace,
    # config_snapshot
    ConfigSnapshot, ConfigSource,
    # state_registry
    StateRegistry, StateScope,
    # lifecycle
    SystemModule, ModuleState,
    # module_manifest
    ModuleManifest, InterfaceSpec, DependencySpec, VersionRange,
)


# ═══════════════════════════════════════════════════════════════
# 1. Result & Error 测试
# ═══════════════════════════════════════════════════════════════

class TestResult(unittest.TestCase):

    def test_ok_creation(self):
        r = Result.ok(42)
        self.assertTrue(r.is_ok())
        self.assertFalse(r.is_err())
        self.assertEqual(r.unwrap(), 42)

    def test_err_creation(self):
        err = Error("E001", "test error", ErrorSeverity.RECOVERABLE, "test_module")
        r = Result.err(err)
        self.assertFalse(r.is_ok())
        self.assertTrue(r.is_err())
        self.assertEqual(r.unwrap_err(), err)

    def test_unwrap_or(self):
        r_ok = Result.ok("hello")
        self.assertEqual(r_ok.unwrap_or("default"), "hello")

        err = Error("E002", "fail", ErrorSeverity.TRANSIENT, "test")
        r_err = Result.err(err)
        self.assertEqual(r_err.unwrap_or("default"), "default")

    def test_unwrap_err_raises_on_ok(self):
        r = Result.ok(1)
        with self.assertRaises(RuntimeError):
            r.unwrap_err()

    def test_unwrap_raises_on_err(self):
        err = Error("E003", "fail", ErrorSeverity.FATAL, "test")
        r = Result.err(err)
        with self.assertRaises(RuntimeError):
            r.unwrap()

    def test_map(self):
        r = Result.ok(10)
        mapped = r.map(lambda x: x * 2)
        self.assertTrue(mapped.is_ok())
        self.assertEqual(mapped.unwrap(), 20)

        err = Error("E004", "fail", ErrorSeverity.RECOVERABLE, "test")
        r_err = Result.err(err)
        mapped_err = r_err.map(lambda x: x * 2)
        self.assertTrue(mapped_err.is_err())

    def test_flat_map(self):
        r = Result.ok(5)
        fmapped = r.flat_map(lambda x: Result.ok(x + 3))
        self.assertTrue(fmapped.is_ok())
        self.assertEqual(fmapped.unwrap(), 8)

        fmapped_err = r.flat_map(lambda x: Result.err(
            Error("E005", "inner fail", ErrorSeverity.RECOVERABLE, "test")
        ))
        self.assertTrue(fmapped_err.is_err())

    def test_error_severity_values(self):
        self.assertEqual(ErrorSeverity.TRANSIENT.value, "transient")
        self.assertEqual(ErrorSeverity.RECOVERABLE.value, "recoverable")
        self.assertEqual(ErrorSeverity.DEGRADED.value, "degraded")
        self.assertEqual(ErrorSeverity.CRITICAL.value, "critical")
        self.assertEqual(ErrorSeverity.FATAL.value, "fatal")

    def test_error_category_values(self):
        self.assertEqual(ErrorCategory.NETWORK.value, "network")
        self.assertEqual(ErrorCategory.VALIDATION.value, "validation")
        self.assertEqual(ErrorCategory.BUSINESS.value, "business")


# ═══════════════════════════════════════════════════════════════
# 2. CircuitBreaker 测试
# ═══════════════════════════════════════════════════════════════

class TestCircuitBreaker(unittest.TestCase):

    def test_initial_state_closed(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=3, recovery_timeout=1))
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.allow_request())

    def test_failure_trip_to_open(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=3, recovery_timeout=1))
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertFalse(cb.allow_request())

    def test_success_resets_failures(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=3, recovery_timeout=1))
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.failure_count, 0)
        self.assertEqual(cb.state, CircuitState.CLOSED)

    def test_open_to_half_open_after_timeout(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=2, recovery_timeout=0.1))
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)

        time.sleep(0.15)
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

    def test_half_open_success_closes(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=2, recovery_timeout=0.1))
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        cb.allow_request()  # → HALF_OPEN
        cb.record_success()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(cb.failure_count, 0)

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker(CircuitConfig(failure_threshold=2, recovery_timeout=0.1))
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        cb.allow_request()  # → HALF_OPEN
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)


# ═══════════════════════════════════════════════════════════════
# 3. EventBus 测试
# ═══════════════════════════════════════════════════════════════

class MockHandler(IEventHandler):
    def __init__(self):
        self.events = []
        self.call_count = 0

    def handle(self, event: Event) -> None:
        self.events.append(event)
        self.call_count += 1


class TestEventBus(unittest.TestCase):

    def setUp(self):
        self.bus = EventBus()
        self.handler = MockHandler()

    def test_subscribe_and_publish(self):
        self.bus.subscribe(EventType.ALERT_SENT, self.handler)
        event = Event(EventType.ALERT_SENT, "test_source", {"msg": "hello"})
        self.bus.publish(event)
        self.assertEqual(self.handler.call_count, 1)
        self.assertEqual(self.handler.events[0].payload["msg"], "hello")

    def test_publish_without_handler(self):
        # 不应抛异常
        event = Event(EventType.SYSTEM_STARTING, "test", {})
        self.bus.publish(event)

    def test_multiple_handlers(self):
        handler2 = MockHandler()
        self.bus.subscribe(EventType.ALERT_SENT, self.handler)
        self.bus.subscribe(EventType.ALERT_SENT, handler2)
        self.bus.publish(Event(EventType.ALERT_SENT, "test", {"v": 1}))
        self.assertEqual(self.handler.call_count, 1)
        self.assertEqual(handler2.call_count, 1)

    def test_unsubscribe(self):
        self.bus.subscribe(EventType.ALERT_SENT, self.handler)
        self.bus.unsubscribe(EventType.ALERT_SENT, self.handler)
        self.bus.publish(Event(EventType.ALERT_SENT, "test", {}))
        self.assertEqual(self.handler.call_count, 0)

    def test_event_creation(self):
        event = Event(
            event_type=EventType.ORDER_FILLED,
            source="execution",
            payload={"order_id": "123"},
            trace_id="trace_001",
        )
        self.assertEqual(event.event_type, EventType.ORDER_FILLED)
        self.assertEqual(event.source, "execution")
        self.assertEqual(event.payload["order_id"], "123")
        self.assertEqual(event.trace_id, "trace_001")
        self.assertGreater(event.timestamp, 0)


# ═══════════════════════════════════════════════════════════════
# 4. TraceContext 测试
# ═══════════════════════════════════════════════════════════════

class TestTraceContext(unittest.TestCase):

    def setUp(self):
        # 清除之前的 trace
        import src.core.trace_context as tc
        tc._current_trace = None

    def test_create_trace(self):
        ctx = TraceContext.create("test_op", level=TraceLevel.INFO)
        self.assertIsNotNone(ctx.trace_id)
        self.assertEqual(ctx.operation, "test_op")
        self.assertEqual(ctx.level, TraceLevel.INFO)
        self.assertEqual(ctx.parent_id, "")

    def test_set_and_get_current(self):
        ctx = TraceContext.create("op1")
        set_current_trace(ctx)
        current = get_current_trace()
        self.assertIsNotNone(current)
        self.assertEqual(current.trace_id, ctx.trace_id)

    def test_child_span(self):
        parent = TraceContext.create("parent_op", level=TraceLevel.DEBUG)
        child = parent.child("child_op")
        self.assertEqual(child.parent_id, parent.span_id)
        self.assertEqual(child.trace_id, parent.trace_id)
        self.assertEqual(child.level, parent.level)

    def test_trace_level_values(self):
        self.assertEqual(TraceLevel.DEBUG.value, "debug")
        self.assertEqual(TraceLevel.INFO.value, "info")
        self.assertEqual(TraceLevel.WARN.value, "warn")
        self.assertEqual(TraceLevel.ERROR.value, "error")
        self.assertEqual(TraceLevel.FATAL.value, "fatal")


# ═══════════════════════════════════════════════════════════════
# 5. ConfigSnapshot 测试
# ═══════════════════════════════════════════════════════════════

class TestConfigSnapshot(unittest.TestCase):

    def test_create_and_get(self):
        cfg = ConfigSnapshot({
            "strategy": {
                "ma_period": 26,
                "atr_period": 14,
            },
            "risk": {
                "max_daily_loss": 0.05,
            },
            "simple_key": "hello",
        })
        self.assertEqual(cfg.get("simple_key"), "hello")
        self.assertEqual(cfg.get("strategy.ma_period"), 26)
        self.assertEqual(cfg.get("strategy.atr_period"), 14)
        self.assertEqual(cfg.get("risk.max_daily_loss"), 0.05)

    def test_get_with_default(self):
        cfg = ConfigSnapshot({"a": 1})
        self.assertEqual(cfg.get("b", 42), 42)
        self.assertEqual(cfg.get("a", 99), 1)

    def test_nested_missing_key(self):
        cfg = ConfigSnapshot({"x": {"y": 1}})
        self.assertIsNone(cfg.get("x.z"))
        self.assertEqual(cfg.get("x.z", "default"), "default")

    def test_empty_config(self):
        cfg = ConfigSnapshot({})
        self.assertIsNone(cfg.get("anything"))
        self.assertEqual(cfg.get("anything", 123), 123)

    def test_config_source_enum(self):
        self.assertEqual(ConfigSource.FILE.value, "file")
        self.assertEqual(ConfigSource.ENV.value, "env")
        self.assertEqual(ConfigSource.DEFAULT.value, "default")
        self.assertEqual(ConfigSource.RUNTIME.value, "runtime")


# ═══════════════════════════════════════════════════════════════
# 6. StateRegistry 测试
# ═══════════════════════════════════════════════════════════════

class TestStateRegistry(unittest.TestCase):

    def setUp(self):
        self.registry = StateRegistry()

    def test_write_and_read(self):
        self.registry.write("positions", "m05", {"BTCUSDT": {"qty": 0.5}})
        exists, data = self.registry.read("positions", "m05")
        self.assertTrue(exists)
        self.assertEqual(data["BTCUSDT"]["qty"], 0.5)

    def test_read_nonexistent(self):
        exists, data = self.registry.read("nonexistent", "m05")
        self.assertFalse(exists)
        self.assertIsNone(data)

    def test_scope_isolation(self):
        self.registry.write("key", "module_a", "value_a")
        self.registry.write("key", "module_b", "value_b")
        _, val_a = self.registry.read("key", "module_a")
        _, val_b = self.registry.read("key", "module_b")
        self.assertEqual(val_a, "value_a")
        self.assertEqual(val_b, "value_b")

    def test_delete(self):
        self.registry.write("temp", "m01", {"x": 1})
        self.registry.delete("temp", "m01")
        exists, _ = self.registry.read("temp", "m01")
        self.assertFalse(exists)

    def test_state_scope_enum(self):
        self.assertEqual(StateScope.MODULE.value, "module")
        self.assertEqual(StateScope.GLOBAL.value, "global")
        self.assertEqual(StateScope.SESSION.value, "session")


# ═══════════════════════════════════════════════════════════════
# 7. SystemModule (Lifecycle) 测试
# ═══════════════════════════════════════════════════════════════

class ConcreteTestModule(SystemModule):
    """具体模块实现用于测试"""

    def __init__(self, name="TestModule"):
        super().__init__()
        self._name = name
        self.configure_called = False
        self.start_called = False
        self.stop_called = False

    def configure(self, config):
        self.configure_called = True
        return Result.ok(None)

    def _do_start(self):
        self.start_called = True
        return Result.ok(None)

    def _do_stop(self):
        self.stop_called = True
        return Result.ok(None)

    def state_export(self):
        base = super().state_export()
        base["custom"] = "data"
        return base


class TestSystemModule(unittest.TestCase):

    def test_initial_state(self):
        mod = ConcreteTestModule()
        self.assertEqual(mod.state, ModuleState.INIT)

    def test_configure_transition(self):
        mod = ConcreteTestModule()
        cfg = ConfigSnapshot({"key": "value"})
        result = mod.configure(cfg)
        self.assertTrue(result.is_ok())
        self.assertTrue(mod.configure_called)
        self.assertEqual(mod.state, ModuleState.CONFIGURED)

    def test_start_transition(self):
        mod = ConcreteTestModule()
        mod.configure(ConfigSnapshot({}))
        result = mod.start()
        self.assertTrue(result.is_ok())
        self.assertTrue(mod.start_called)
        self.assertEqual(mod.state, ModuleState.RUNNING)

    def test_stop_transition(self):
        mod = ConcreteTestModule()
        mod.configure(ConfigSnapshot({}))
        mod.start()
        result = mod.stop()
        self.assertTrue(result.is_ok())
        self.assertTrue(mod.stop_called)
        self.assertEqual(mod.state, ModuleState.STOPPED)

    def test_double_start_idempotent(self):
        mod = ConcreteTestModule()
        mod.configure(ConfigSnapshot({}))
        mod.start()
        result = mod.start()  # 第二次 start
        self.assertTrue(result.is_ok())

    def test_stop_without_start(self):
        mod = ConcreteTestModule()
        mod.configure(ConfigSnapshot({}))
        result = mod.stop()
        self.assertTrue(result.is_ok())

    def test_state_export(self):
        mod = ConcreteTestModule()
        mod.configure(ConfigSnapshot({}))
        state = mod.state_export()
        self.assertIn("name", state)
        self.assertIn("state", state)
        self.assertIn("custom", state)

    def test_module_state_enum(self):
        self.assertEqual(ModuleState.INIT.value, "init")
        self.assertEqual(ModuleState.CONFIGURED.value, "configured")
        self.assertEqual(ModuleState.STARTING.value, "starting")
        self.assertEqual(ModuleState.RUNNING.value, "running")
        self.assertEqual(ModuleState.STOPPING.value, "stopping")
        self.assertEqual(ModuleState.STOPPED.value, "stopped")
        self.assertEqual(ModuleState.FAILED.value, "failed")


# ═══════════════════════════════════════════════════════════════
# 8. ModuleManifest 测试
# ═══════════════════════════════════════════════════════════════

class TestModuleManifest(unittest.TestCase):

    def test_create_manifest(self):
        manifest = ModuleManifest(
            name="M01_Strategy",
            version="1.0.0",
            description="MA26 Strategy Module",
        )
        self.assertEqual(manifest.name, "M01_Strategy")
        self.assertEqual(manifest.version, "1.0.0")

    def test_add_interface(self):
        manifest = ModuleManifest("M01", "1.0.0", "test")
        manifest.add_provides(InterfaceSpec(
            name="IStrategy",
            version="1.0.0",
            methods=["generate_signal", "update_kline"],
        ))
        self.assertEqual(len(manifest.provides), 1)
        self.assertEqual(manifest.provides[0].name, "IStrategy")

    def test_add_dependency(self):
        manifest = ModuleManifest("M01", "1.0.0", "test")
        manifest.add_requires(DependencySpec(
            name="M03_Kline",
            version_range=VersionRange(min_version="1.0.0", max_version="2.0.0"),
        ))
        self.assertEqual(len(manifest.requires), 1)
        self.assertEqual(manifest.requires[0].name, "M03_Kline")

    def test_version_range_check(self):
        vr = VersionRange(min_version="1.0.0", max_version="2.0.0")
        self.assertTrue(vr.satisfies("1.0.0"))
        self.assertTrue(vr.satisfies("1.5.0"))
        self.assertTrue(vr.satisfies("2.0.0"))
        self.assertFalse(vr.satisfies("0.9.0"))
        self.assertFalse(vr.satisfies("2.1.0"))

    def test_version_range_unbounded(self):
        vr = VersionRange(min_version="1.0.0")
        self.assertTrue(vr.satisfies("1.0.0"))
        self.assertTrue(vr.satisfies("3.0.0"))
        self.assertFalse(vr.satisfies("0.5.0"))

    def test_manifest_to_dict(self):
        manifest = ModuleManifest(
            name="M04_Risk",
            version="2.1.0",
            description="Risk Guard",
        )
        manifest.add_provides(InterfaceSpec("IRiskGuard", "1.0.0"))
        d = manifest.to_dict()
        self.assertEqual(d["name"], "M04_Risk")
        self.assertEqual(d["version"], "2.1.0")
        self.assertEqual(len(d["provides"]), 1)


# ═══════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════

def run_all_tests():
    """手动运行所有测试并打印结果"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestResult,
        TestCircuitBreaker,
        TestEventBus,
        TestTraceContext,
        TestConfigSnapshot,
        TestStateRegistry,
        TestSystemModule,
        TestModuleManifest,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print(f"测试结果: {'全部通过 ✅' if result.wasSuccessful() else '有失败 ❌'}")
    print(f"运行: {result.testsRun} 个测试")
    print(f"失败: {len(result.failures)} 个")
    print(f"错误: {len(result.errors)} 个")
    print("=" * 60)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
