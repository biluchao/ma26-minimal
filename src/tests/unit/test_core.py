"""tests/unit/test_core.py — Core infrastructure tests"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.core.result import Result, Error, ErrorSeverity, Errors
from src.core.event_bus import EventBus, Event, EventType
from src.core.config_snapshot import ConfigSnapshot, ConfigWatcher
from src.core.state_registry import StateRegistry
from src.core.trace_context import TraceManager
from src.core.module_manifest import ModuleManifest, DependencySpec, DependencyResolver


def test_result_ok_and_err():
    r = Result.ok(42)
    assert r.is_ok() and not r.is_err()
    assert r.unwrap() == 42
    assert r.unwrap_or(0) == 42

    e = Error("BOOM", "kaboom", ErrorSeverity.CRITICAL, "test")
    r2 = Result.err(e)
    assert r2.is_err()
    assert r2.error.code == "BOOM"
    assert r2.unwrap_or(99) == 99


def test_result_chain():
    r = Result.ok(10).map(lambda x: x * 2).and_then(lambda v: Result.ok(v + 1))
    assert r.unwrap() == 21
    r2 = Result.err(Error("X", "x", ErrorSeverity.RECOVERABLE))
    called = False
    def never(_): 
        global called
        called = True
        return Result.ok(0)
    r2.and_then(never)
    assert not called


def test_event_bus_publish_and_subscribe():
    bus = EventBus()
    seen = []
    bus.subscribe(EventType.BAR_RECEIVED, lambda e: seen.append(e))
    bus.publish(Event(type=EventType.BAR_RECEIVED, bar_index=5, data={'close': 100}))
    assert len(seen) == 1
    assert seen[0].bar_index == 5
    assert bus.get_stats()['delivered'] == 1


def test_event_bus_wildcard():
    bus = EventBus()
    seen = []
    bus.subscribe("*", lambda e: seen.append(e.type))
    bus.publish(Event(type=EventType.ORDER_FILLED, data={}))
    assert seen == [EventType.ORDER_FILLED]


def test_event_bus_priority():
    bus = EventBus()
    order = []
    bus.subscribe(EventType.BAR_RECEIVED, lambda e: order.append('b'), priority=100)
    bus.subscribe(EventType.BAR_RECEIVED, lambda e: order.append('a'), priority=10)
    bus.publish(Event(type=EventType.BAR_RECEIVED))
    assert order == ['a', 'b']


def test_event_bus_dead_letter():
    bus = EventBus()
    def boom(e):
        raise RuntimeError("handler error")
    bus.subscribe(EventType.BAR_RECEIVED, boom)
    bus.publish(Event(type=EventType.BAR_RECEIVED))
    assert len(bus.get_dead()) == 1


def test_config_snapshot_immutable_get():
    c = ConfigSnapshot({'risk': {'max_dd': 0.15, 'nested': {'v': 1}}, 'flat': 42})
    assert c.get('risk.max_dd') == 0.15
    assert c.get('risk.nested.v') == 1
    assert c.get('risk.missing', 'default') == 'default'
    assert c.get('risk.missing.sub', 'dv') == 'dv'
    assert c.get('flat') == 42
    assert c.version() == 0
    assert c.source() == 'init'
    assert len(c.hash()) == 16


def test_config_snapshot_diff():
    a = ConfigSnapshot({'a': 1, 'b': {'c': 1}})
    b = ConfigSnapshot({'a': 2, 'b': {'c': 1}})
    d = a.diff(b)
    assert 'a' in d
    assert 'b.c' not in d


def test_state_registry_single_writer():
    reg = StateRegistry()
    reg.declare('positions', 'M05_PositionBook', readers=['M06_EquityManager'])
    assert reg.write('positions', 'M05_PositionBook', {'qty': 1.0}) is True
    ok, val = reg.read('positions', 'M06_EquityManager')
    assert ok and val == {'qty': 1.0}
    assert reg.write('positions', 'M06_EquityManager', {'qty': 2.0}) is False
    assert reg.read('positions', 'M99_Unknown')[0] is False
    assert len(reg.get_violations()) == 2
    assert reg.validate() == []


def test_state_registry_snapshot():
    reg = StateRegistry()
    reg.declare('counter', 'M01')
    reg.write('counter', 'M01', 42)
    ok, ver, val = reg.snapshot('counter', 'M02')
    assert ok and ver == 1 and val == 42
    reg.write('counter', 'M01', 43)
    ok, ver2, _ = reg.snapshot('counter', 'M02')
    assert ver2 == 2


def test_trace_context_child():
    t1 = TraceManager()
    ctx = t1.new_trace(bar_index=7, source="test")
    assert ctx.bar_index == 7
    ctx2, span_id = ctx.child("op1", "M1")
    assert ctx2.trace_id == ctx.trace_id
    assert ctx2.parent_span_id == span_id


def test_trace_span_timing():
    tm = TraceManager()
    with tm.span("eval", "M1") as sp:
        assert sp.start_time > 0
    assert sp.end_time > 0
    assert sp.duration_ms() >= 0
    stats = tm.get_stats()
    assert stats['total_spans'] == 1


def test_manifest_version_matching():
    assert DependencySpec._match("4.1.0", ">=4.0.0,<5.0.0")
    assert not DependencySpec._match("5.0.0", ">=4.0.0,<5.0.0")
    assert DependencySpec._match("4.1.0", "*")
    assert DependencySpec._match("3.0.0", ">=2.0.0,<=4.0.0")
    assert not DependencySpec._match("1.0.0", ">=2.0.0")


def test_manifest_interface_compat():
    a = ModuleManifest(name="A", version="1.0.0", interfaces_provided=[{'name': 'IBroker', 'version': '1.0.0'}])
    b = ModuleManifest(name="B", version="1.5.0", interfaces_required=[{'name': 'IBroker', 'version': '1.0.0'}])
    ok, errs = DependencyResolver([a, b]).resolve()
    assert ok, errs


def test_manifest_major_break():
    a = ModuleManifest(name="A", version="1.0.0", interfaces_provided=[{'name': 'IBroker', 'version': '2.0.0'}])
    b = ModuleManifest(name="B", version="1.5.0", interfaces_required=[{'name': 'IBroker', 'version': '1.0.0'}])
    ok, errs = DependencyResolver([a, b]).resolve()
    assert not ok
    assert any("incompatible" in e for e in errs)


def test_manifest_cycle_detection():
    a = ModuleManifest(name="A", version="1.0.0", dependencies=[{'name': 'B', 'version_range': '>=1.0.0'}])
    b = ModuleManifest(name="B", version="1.0.0", dependencies=[{'name': 'C', 'version_range': '>=1.0.0'}])
    c = ModuleManifest(name="C", version="1.0.0", dependencies=[{'name': 'A', 'version_range': '>=1.0.0'}])
    ok, errs = DependencyResolver([a, b, c]).resolve()
    assert not ok
    assert any("Circular" in e for e in errs)


def test_topological_sort():
    a = ModuleManifest(name="A", version="1.0.0", dependencies=[{'name': 'B', 'version_range': '*'}])
    b = ModuleManifest(name="B", version="1.0.0", dependencies=[])
    order = DependencyResolver([a, b]).topological_sort()
    assert order.index('B') < order.index('A')


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓ {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {t.__name__}: {e}")
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {len(tests) - failed}/{len(tests)}")
    sys.exit(0 if failed == 0 else 1)

