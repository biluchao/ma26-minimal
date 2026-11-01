"""tests/unit/test_modules.py — Unit tests for business modules"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.core.result import ConfigSnapshot
from src.core.event_bus import EventBus
from src.core.trace_context import TraceManager
from src.core.alert_manager import AlertManager
from src.core.state_registry import StateRegistry
from src.m03_cache import BarCache
from src.m04_risk import RiskGuard
from src.m05_state import PositionBook
from src.m06_equity import EquityManager
from src.m07_execution import ExecutionManager
from src.m11_performance import PerformanceAnalyzer
from src.m12_snapshot import SnapshotManager
from src.m01_strategy import StrategyEngine, SignalRequest


def _infra():
    eb = EventBus()
    tm = TraceManager()
    sr = StateRegistry()
    am = AlertManager(use_color=False)
    return eb, tm, sr, am


def _inject(m, eb, tm, sr, am):
    m.inject(event_bus=eb, trace_manager=tm, state_registry=sr, alert_manager=am)


def _cfg(d=None):
    base = {
        'warmup': {'bars': 35},
        'strategy': {'ma_period': 26, 'atr_period': 14, 'trend_atr_mult': 0.5,
                     'tp_atr_mult': 3.0, 'stop_atr_mult': 2.0, 'max_size_pct': 0.25,
                     'step_size': 0.001, 'min_qty': 0.001, 'min_notional': 10.0,
                     'qty_decimals': 3, 'price_decimals': 2},
        'risk': {'max_position_pct': 0.95, 'max_single_loss_pct': 0.02,
                'max_daily_loss_pct': 0.05, 'max_drawdown_pct': 0.15,
                'max_open_orders': 5, 'max_slippage_bps': 50},
        'equity': {'initial_cash': 10000.0},
        'simulator': {'commission_bps': 10},
    }
    if d:
        base.update(d)
    return ConfigSnapshot(base)


def test_bar_cache_append_and_dedup():
    bc = BarCache(dedup_window=100)
    assert bc.append({'bar_index': 0, 'close': 1}).unwrap() is True
    assert bc.append({'bar_index': 0, 'close': 1}).unwrap() is False
    assert len(bc) == 1
    assert bc.get(0).unwrap()['close'] == 1
    assert bc.get_latest().unwrap()['close'] == 1
    assert bc.append({'bar_index': -1}).is_err()


def test_bar_cache_range_and_ready():
    bc = BarCache()
    for i in range(10):
        bc.append({'bar_index': i, 'close': 100 + i})
    r = bc.get_range(3, 6).unwrap()
    assert [b['bar_index'] for b in r] == [3, 4, 5, 6]
    assert not bc.is_ready(35)
    for i in range(10, 40):
        bc.append({'bar_index': i, 'close': 100 + i})
    assert bc.is_ready(35)


def test_ma26_and_atr():
    from src.m01_strategy.engine import MA26, ATR14
    ma = MA26(5)
    for v in [10, 20, 30, 40, 50]:
        ma.update(v)
    assert ma.is_ready() and ma.value() == 30.0
    atr = ATR14(3)
    atr.update(10, 8, 9)   # tr = 2
    atr.update(12, 10, 11) # tr = 2
    atr.update(14, 12, 13) # tr = 2
    assert atr.is_ready() and atr.value() == 2.0


def test_direction_machine_flips():
    from src.m01_strategy.engine import DirectionMachine
    dm = DirectionMachine(atr_mult=0.5)
    assert dm.direction == "FLAT"
    sig = dm.evaluate(11, 10, 2)      # close 11 > 10 + 1 → BULL
    assert sig and sig['to'] == "BULL"
    dm.evaluate(7, 10, 2)            # 7 < 10 - 1 → FLAT
    assert dm.direction == "FLAT"


def test_strategy_engine_no_signals_before_warmup():
    eng = StrategyEngine()
    eb, tm, sr, am = _infra()
    _inject(eng, eb, tm, sr, am)
    eng.configure(_cfg())
    eng.start()
    out = eng.on_bar({'bar_index': 0, 'close': 100, 'high': 101, 'low': 99, 'volume': 1})
    assert out.is_ok() and out.unwrap() == []
    state = eng.get_state()
    assert not state['ma_ready']


def test_strategy_engine_emits_signal():
    eng = StrategyEngine()
    eb, tm, sr, am = _infra()
    events = []
    eb.subscribe("signal.generated", lambda e: events.append(e))
    _inject(eng, eb, tm, sr, am)
    eng.configure(_cfg())
    eng.start()
    prev = 100.0
    for i in range(40):
        close = 100 + i * 2     # strong uptrend
        eng.on_bar({'bar_index': i, 'close': close, 'high': close + 1,
                   'low': close - 1, 'volume': 1})
    assert eng.get_state()['ma_ready']
    assert any(e.data.get('signal', {}).get('action', '').startswith('OPEN') for e in events), \
        [e.data for e in events]


def test_risk_guard_cooling_on_drawdown():
    rg = RiskGuard()
    eb, tm, sr, am = _infra()
    _inject(rg, eb, tm, sr, am)
    rg.configure(_cfg())
    rg.start()
    rg.reset_day(10000, "2026-01-01")
    res = rg.post_bar(8400, 10000, 10)   # 16% dd > 15%
    assert res.is_ok()
    assert rg.is_cooling()
    assert rg.get_state().cooling_reason


def test_risk_guard_daily_loss():
    rg = RiskGuard()
    eb, tm, sr, am = _infra()
    _inject(rg, eb, tm, sr, am)
    rg.configure(_cfg({'risk': {'max_daily_loss_pct': 0.02, 'max_drawdown_pct': 0.5,
                               'max_position_pct': 0.95, 'max_single_loss_pct': 0.02,
                               'max_open_orders': 5, 'max_slippage_bps': 50}}))
    rg.start()
    rg.reset_day(10000, "d")
    rg.post_bar(9790, 10000, 1)   # 2.1% loss > 2%
    assert rg.is_cooling()


def test_risk_guard_pre_trade_cooling_reject():
    rg = RiskGuard()
    eb, tm, sr, am = _infra()
    _inject(rg, eb, tm, sr, am)
    rg.configure(_cfg())
    rg.start()
    rg.force_cool("manual test", 0)
    r = rg.pre_trade(SignalRequest(action="OPEN_LONG", qty=0.1, price=100), None)
    assert r.is_err() and r.error.code == "RISK_COOLING"


def test_position_book_avg_price_add():
    pb = PositionBook()
    pb.start()
    r1 = pb.open_long(1.0, 100, 0, 0, 0)
    assert r1.unwrap().avg_price == 100.0
    r2 = pb.open_long(1.0, 120, 0, 0, 1)
    p = r2.unwrap()
    assert p.qty == 2.0 and abs(p.avg_price - 110.0) < 1e-9
    assert pb.revalue(130) == 40.0
    r3 = pb.reduce(1.0, 130)
    assert r3.is_ok()
    rpnl, rem = r3.unwrap()
    assert abs(rpnl - 20.0) < 1e-9 and abs(rem - 1.0) < 1e-9


def test_position_book_close():
    pb = PositionBook()
    pb.start()
    pb.open_short(1.0, 100, 0, 0, 0)
    assert pb.revalue(90) == 10.0
    r = pb.close()
    assert r.is_ok() and pb.get().qty == 0


def test_equity_manager_decimal_precision():
    em = EquityManager()
    eb, tm, sr, am = _infra()
    _inject(em, eb, tm, sr, am)
    em.configure(_cfg())
    em.start()
    cash = em.apply_fill(0.1, 30000, "BUY", 0.3, 1)
    assert cash.is_ok()
    assert em.cash() < 10000.0
    snap = em.revalue(31000, 0.1, 30000, "LONG", 1).unwrap()
    assert snap.total_equity > snap.cash
    assert snap.drawdown == 0.0


def test_execution_qty_rounding_and_notional():
    ex = ExecutionManager()
    eb, tm, sr, am = _infra()
    _inject(ex, eb, tm, sr, am)
    ex.configure(_cfg())
    ex.start()
    from src.core.trace_context import TraceContext
    ctx = TraceContext()
    r = ex.execute(SignalRequest(action="OPEN_LONG", qty=0.0001, price=30000), 30000, 1, ctx)
    assert r.is_err()   # below step_size after truncation
    r2 = ex.execute(SignalRequest(action="OPEN_LONG", qty=0.0005, price=30000), 30000, 1, ctx)
    assert r2.is_err()   # notional < 10
    r3 = ex.execute(SignalRequest(action="OPEN_LONG", qty=1.0, price=30000), 30000, 1, ctx)
    assert r3.is_ok() and r3.unwrap().qty == 1.0


def test_execution_slippage_applied():
    ex = ExecutionManager()
    eb, tm, sr, am = _infra()
    _inject(ex, eb, tm, sr, am)
    ex.configure(_cfg())
    ex.start()
    from src.core.trace_context import TraceContext
    ctx = TraceContext()
    f = ex.execute(SignalRequest(action="OPEN_LONG", qty=1.0, price=30000), 30000, 1, ctx).unwrap()
    assert f.price > 30000   # buy slips up


def test_performance_report_empty():
    p = PerformanceAnalyzer()
    p.start()
    r = p.report(10000, 10000)
    assert r.total_trades == 0 and r.win_rate == 0.0


def test_performance_report_with_trades():
    p = PerformanceAnalyzer()
    p.start()
    p.on_equity(10000, 0)
    p.on_equity(10100, 1)
    p.on_trade_open("LONG", 100, 1, 0)
    p.on_trade_close(110, 2)
    r = p.report(10100, 10100)
    assert r.total_trades == 1 and r.wins == 1 and r.win_rate == 1.0
    assert r.max_drawdown == 0.0


def test_snapshot_atomic_roundtrip():
    import tempfile, shutil
    d = tempfile.mkdtemp()
    try:
        sm = SnapshotManager(snapshot_dir=d)
        eb, tm, sr, am = _infra()
        _inject(sm, eb, tm, sr, am)
        sm.configure(_cfg())
        sm.start()
        sid = sm.save({'equity': 1234.5, 'qty': 0.5}, 42, "test").unwrap()
        assert sid
        data = sm.load(sid).unwrap()
        assert data['state']['equity'] == 1234.5
        assert data['parent_ref'] == ""
        # chain
        sid2 = sm.save({'equity': 2000}, 43, "test").unwrap()
        data2 = sm.load(sid2).unwrap()
        assert data2['parent_ref']
    finally:
        shutil.rmtree(d)


def test_snapshot_checksum_mismatch():
    import tempfile, shutil, json
    d = tempfile.mkdtemp()
    try:
        sm = SnapshotManager(snapshot_dir=d)
        eb, tm, sr, am = _infra()
        _inject(sm, eb, tm, sr, am)
        sm.configure(_cfg())
        sm.start()
        sid = sm.save({'x': 1}, 1).unwrap()
        path = sm.snapshot_dir / f"{sid}.json"
        with open(path) as f:
            body = f.read()
        # tamper
        body = body.replace('"x": 1', '"x": 999')
        with open(path, 'w') as f:
            f.write(body)
        r = sm.load(sid)
        assert r.is_err() and r.error.code == "SNAPSHOT_CHECKSUM_MISMATCH"
    finally:
        shutil.rmtree(d)


def test_snapshot_cleanup_keep():
    import tempfile, shutil
    d = tempfile.mkdtemp()
    try:
        sm = SnapshotManager(snapshot_dir=d)
        eb, tm, sr, am = _infra()
        _inject(sm, eb, tm, sr, am)
        sm.configure(_cfg({'state': {'snapshots_keep': 3}}))
        sm.start()
        for i in range(6):
            sm.save({'i': i}, i)
        files = list(sm.snapshot_dir.glob("*.json"))
        assert len(files) == 3
    finally:
        shutil.rmtree(d)


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓ {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback
            traceback.print_exc()
            print(f"  ✗ {t.__name__}: {e}")
    print(f"\n{'PASS' if failed == 0 else 'FAIL'}: {len(tests) - failed}/{len(tests)}")
    sys.exit(0 if failed == 0 else 1)

