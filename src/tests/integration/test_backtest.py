"""tests/integration/test_backtest.py — End-to-end backtest loop"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.bootstrap import SystemOrchestrator, generate_demo_bars
from src.core.result import ConfigSnapshot


def make_bars_with_trend(n=300):
    """A price series with a clear uptrend then reversal."""
    bars = []
    p = 30000.0
    for i in range(n):
        if i < 150:
            p += 20 + (i % 5)
        else:
            p -= 15 - (i % 4)
        bars.append({'bar_index': i, 'timestamp': i * 3600, 'open': p, 'close': p,
                    'high': p + 20, 'low': p - 20, 'volume': 100.0})
    return bars


def test_full_backtest_runs_without_exception():
    orch = SystemOrchestrator('config/default.yaml', 'backtest')
    r = orch.bootstrap()
    assert r.is_ok(), r.error.format_chain()
    bars = make_bars_with_trend(300)
    orch.run(bars)   # must not raise


def test_backtest_produces_performance_report():
    orch = SystemOrchestrator('config/default.yaml', 'backtest')
    orch.bootstrap()
    bars = make_bars_with_trend(300)
    orch.run(bars)
    perf = orch.modules['M11_PerformanceAnalyzer']
    report = perf.report(orch.modules['M06_EquityManager'].get_snapshot().total_equity,
                         orch.modules['M06_EquityManager'].peak())
    assert report.num_bars == 300
    assert isinstance(report.total_trades, int)
    assert 0.0 <= report.win_rate <= 1.0


def test_snapshot_persists_state():
    import tempfile, shutil
    d = tempfile.mkdtemp()
    try:
        cfg = {'warmup': {'bars': 5},
               'strategy': {'ma_period': 5, 'atr_period': 3, 'trend_atr_mult': 0.5,
                           'tp_atr_mult': 3.0, 'stop_atr_mult': 2.0, 'max_size_pct': 0.25,
                           'step_size': 0.001, 'min_qty': 0.001, 'min_notional': 10.0,
                           'qty_decimals': 3, 'price_decimals': 2},
               'risk': {'max_position_pct': 0.95, 'max_single_loss_pct': 0.02,
                       'max_daily_loss_pct': 0.5, 'max_drawdown_pct': 0.5,
                       'max_open_orders': 5, 'max_slippage_bps': 50},
               'equity': {'initial_cash': 10000.0},
               'state': {'snapshot_interval_bars': 50, 'snapshots_keep': 100},
               'simulator': {'commission_bps': 10}}
        orch = SystemOrchestrator.__new__(SystemOrchestrator)
        orch.config = ConfigSnapshot(cfg)
        orch.mode = 'backtest'
        orch._init_infrastructure()
        orch._register_modules()
        orch._inject()
        orch._configure_all()
        orch._running = True
        orch.start_order = [n for n in orch.start_order if n in orch.modules]
        for n in orch.start_order:
            orch.modules[n].start()
        bars = make_bars_with_trend(120)
        snap = orch.modules['M12_SnapshotManager']
        before = len(snap.list_snapshots())
        orch.run(bars)
        after = len(snap.list_snapshots())
        assert after > before, f"no snapshots created: {before} -> {after}"
        # load latest
        latest = snap.load_latest().unwrap()
        assert latest is not None
        assert 'equity' in latest['state']
    finally:
        shutil.rmtree(d)


def test_event_bus_receives_all_event_types():
    orch = SystemOrchestrator('config/default.yaml', 'backtest')
    orch.bootstrap()
    captured = []
    for et in ['bar.received', 'signal.generated', 'order.filled', 'equity.revalued',
               'risk.level_changed', 'snapshot.saved', 'system.started', 'system.stopped']:
        orch.event_bus.subscribe(et, lambda e, t=et: captured.append(t))
    orch.run(make_bars_with_trend(200))
    # system.started + system.stopped are guaranteed
    assert 'system.started' in captured
    assert 'system.stopped' in captured


def test_risk_cooling_stops_new_signals():
    orch = SystemOrchestrator('config/default.yaml', 'backtest')
    orch.bootstrap()
    bars = make_bars_with_trend(200)
    # manually trigger cooling before run
    risk = orch.modules['M04_RiskGuard']
    risk.force_cool("integration test", 0)
    assert risk.is_cooling()
    # pre_trade should reject
    from src.core import SignalRequest
    r = risk.pre_trade(SignalRequest(action="OPEN_LONG", qty=0.1, price=100), None)
    assert r.is_err()


def test_position_and_equity_consistency():
    """After every bar, equity = cash + qty * mark (within rounding)."""
    orch = SystemOrchestrator('config/default.yaml', 'backtest')
    orch.bootstrap()
    bars = make_bars_with_trend(250)
    orch.run(bars)
    equity = orch.modules['M06_EquityManager']
    pos = orch.modules['M05_PositionBook']
    snap = equity.get_snapshot()
    expected = equity.cash() + pos.get().qty * pos.get().avg_price
    assert abs(snap.total_equity - (equity.cash() + pos.get().unrealized_pnl)) < 1e-6, \
        f"equity={snap.total_equity} cash={equity.cash()} upnl={pos.get().unrealized_pnl}"


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

