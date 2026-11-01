"""bootstrap.py — System orchestrator: config → dependencies → lifecycle → main loop.

Usage:
    python src/bootstrap.py --mode backtest --config config/default.yaml
    python src/bootstrap.py --mode paper --config config/paper.yaml
    python src/bootstrap.py --mode live --config config/live.yaml
"""

import argparse
import sys
import time
import signal
from pathlib import Path

# ensure src/ is importable when run from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core import (ConfigSnapshot, ConfigWatcher, EventBus, TraceManager,
                  StateRegistry, DependencyResolver, ModuleManifest,
                  SystemModule, Result, Error, ErrorSeverity, EventType)
from core.alert_manager import AlertManager
from m01_strategy import StrategyEngine
from m03_cache import BarCache
from m04_risk import RiskGuard
from m05_state import PositionBook
from m06_equity import EquityManager
from m07_execution import ExecutionManager
from m11_performance import PerformanceAnalyzer
from m12_snapshot import SnapshotManager
from m13_tracer import Tracer


MODULE_CLASSES = {
    'M03_BarCache': BarCache,
    'M04_RiskGuard': RiskGuard,
    'M05_PositionBook': PositionBook,
    'M06_EquityManager': EquityManager,
    'M07_ExecutionManager': ExecutionManager,
    'M01_StrategyEngine': StrategyEngine,
    'M11_PerformanceAnalyzer': PerformanceAnalyzer,
    'M12_SnapshotManager': SnapshotManager,
    'M13_Tracer': None,  # instantiated separately
}


class SystemOrchestrator:
    def __init__(self, config_path: str, mode: str):
        self.config_path = config_path
        self.mode = mode
        self.config: Optional[ConfigSnapshot] = None
        self.event_bus: Optional[EventBus] = None
        self.trace_manager: Optional[TraceManager] = None
        self.state_registry: Optional[StateRegistry] = None
        self.alert_manager: Optional[AlertManager] = None
        self.tracer: Optional[Tracer] = None
        self.modules: dict = {}
        self.start_order: list = []
        self._running = False

    # ── pipeline ──
    def bootstrap(self) -> Result[None, Error]:
        r = self._load_config()
        if r.is_err():
            return r
        self._init_infrastructure()
        self._register_modules()
        r = self._configure_all()
        if r.is_err():
            return r
        self._inject()
        r = self._resolve_dependencies()
        if r.is_err():
            return r
        r = self._start_all()
        if r.is_err():
            self._emergency_stop()
            return r
        self.event_bus.publish_from("bootstrap", EventType.SYSTEM_STARTED, mode=self.mode)
        self._running = True
        return Result.ok(None)

    def _load_config(self) -> Result[None, Error]:
        try:
            import yaml
        except ImportError:
            return Result.err(Error("DEPENDENCY_MISSING", "PyYAML not installed. pip install pyyaml",
                                   ErrorSeverity.FATAL, "bootstrap"))
        path = Path(self.config_path)
        if not path.exists():
            return Result.err(Error("CONFIG_NOT_FOUND", f"{path} not found",
                                   ErrorSeverity.FATAL, "bootstrap"))
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except Exception as e:
            return Result.err(Error("CONFIG_PARSE_ERROR", str(e), ErrorSeverity.FATAL, "bootstrap"))

        if not isinstance(data, dict):
            return Result.err(Error("CONFIG_INVALID", "config root must be a mapping",
                                   ErrorSeverity.FATAL, "bootstrap"))

        # expand env vars like ${BINANCE_API_KEY}
        data = self._expand_env(data)
        self.config = ConfigSnapshot(data, version=1, source=str(path))
        return Result.ok(None)

    def _expand_env(self, obj):
        import os, re
        if isinstance(obj, dict):
            return {k: self._expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._expand_env(x) for x in obj]
        if isinstance(obj, str):
            m = re.fullmatch(r'\$\{([^}]+)\}', obj)
            if m:
                return os.environ.get(m.group(1), "")
            return obj
        return obj

    def _init_infrastructure(self):
        self.alert_manager = AlertManager(use_color=sys.stdout.isatty())
        self.tracer = Tracer(buffer_size=10000,
                             log_file="traces/engine.jsonl" if self.mode == 'live' else None)
        self.event_bus = EventBus(self.config.get('event_bus', {}))
        self.trace_manager = TraceManager(tracer=self.tracer)
        self.state_registry = StateRegistry(alert_manager=self.alert_manager)

    def _register_modules(self):
        order = ['M13_Tracer', 'M03_BarCache', 'M06_EquityManager', 'M05_PositionBook',
                 'M04_RiskGuard', 'M01_StrategyEngine', 'M07_ExecutionManager',
                 'M11_PerformanceAnalyzer', 'M12_SnapshotManager']
        for name in order:
            cls = MODULE_CLASSES.get(name)
            if cls is None:
                continue
            if name == 'M12_SnapshotManager':
                self.modules[name] = SnapshotManager(snapshot_dir=f"snapshots_{self.mode}")
            else:
                self.modules[name] = cls()
        self.start_order = order

    def _configure_all(self) -> Result[None, Error]:
        for name in self.start_order:
            m = self.modules.get(name)
            if m is None:
                continue
            r = m.configure(self.config)
            if r.is_err():
                return Result.err(Error("MODULE_CONFIG_FAILED",
                                       f"{name}: {r.error.format_chain()}",
                                       ErrorSeverity.FATAL, "bootstrap"))
        return Result.ok(None)

    def _inject(self):
        deps = {
            'event_bus': self.event_bus,
            'trace_manager': self.trace_manager,
            'state_registry': self.state_registry,
            'alert_manager': self.alert_manager,
        }
        for name in self.start_order:
            m = self.modules.get(name)
            if m:
                m.inject(**deps)

    def _resolve_dependencies(self) -> Result[None, Error]:
        """Lightweight cycle + presence check (manifests are inline)."""
        declared = {n: self.modules[n] for n in self.start_order if n in self.modules}
        present = set(declared)
        for name, m in declared.items():
            for dep in getattr(m, 'DEPENDENCIES', []):
                if dep not in present:
                    return Result.err(Error("DEPENDENCY_MISSING",
                                            f"{name} requires {dep} (not registered)",
                                            ErrorSeverity.CRITICAL, "bootstrap"))
        return Result.ok(None)

    def _start_all(self) -> Result[None, Error]:
        for name in self.start_order:
            m = self.modules.get(name)
            if m is None:
                continue
            r = m.start()
            if r.is_err():
                return Result.err(Error("MODULE_START_FAILED",
                                       f"{name}: {r.error.format_chain()}",
                                       ErrorSeverity.CRITICAL, "bootstrap"))
        return Result.ok(None)

    def _emergency_stop(self):
        for name in reversed(self.start_order):
            m = self.modules.get(name)
            if m and m.is_started:
                try:
                    m.stop()
                except Exception:
                    pass

    # ── lifecycle ──
    def run(self, bars: list = None):
        if bars is None:
            self.alert_manager.send("ERROR", "No bar source", "backtest requires --bars or a data feed")
            return
        import signal as _s
        _s.signal(_s.SIGINT, lambda *_: setattr(self, '_running', False))
        _s.signal(_s.SIGTERM, lambda *_: setattr(self, '_running', False))

        strategy = self.modules.get('M01_StrategyEngine')
        cache = self.modules['M03_BarCache']
        equity = self.modules['M06_EquityManager']
        risk = self.modules['M04_RiskGuard']
        position = self.modules['M05_PositionBook']
        execution = self.modules['M07_ExecutionManager']
        perf = self.modules['M11_PerformanceAnalyzer']
        snapshot = self.modules['M12_SnapshotManager']

        snap_interval = self.config.get('state.snapshot_interval_bars', 50)
        warmup = self.config.get('warmup.bars', 35)

        self.alert_manager.send("INFO", "Engine started",
                               f"mode={self.mode} bars={len(bars)} warmup={warmup}")

        for bar in bars:
            if not self._running:
                self.alert_manager.send("WARN", "Interrupted", "stopping on signal")
                break
            bi = bar['bar_index']

            cache.append(bar)
            if not cache.is_ready(warmup):
                continue

            with self.trace_manager.span("on_bar", "orchestrator") as span:
                # 1. strategy
                ctx = self.trace_manager.new_trace(bar_index=bi, source="on_bar")
                sig_result = strategy.on_bar(bar, ctx)
                signals = sig_result.unwrap_or([])

                # 2. execute signals
                for sig in signals:
                    risk_check = risk.pre_trade(sig, ctx)
                    if risk_check.is_err():
                        self.alert_manager.send("WARN", "Signal rejected",
                                                risk_check.error.message)
                        continue
                    mark = bar['close']
                    fill_result = execution.execute(sig, mark, bi, ctx)
                    if fill_result.is_err():
                        self.alert_manager.send("WARN", "Execution rejected",
                                                fill_result.error.message)
                        continue
                    fill = fill_result.unwrap()
                    if fill is None:
                        continue
                    equity.apply_fill(fill.qty, fill.price, "BUY" if "BUY" in sig.action else "SELL",
                                     fill.commission, bi, ctx)
                    # position + trade tracking
                    if "OPEN" in sig.action or "ADD" in sig.action:
                        if "LONG" in sig.action:
                            position.open_long(fill.qty, fill.price, 0.0, 0.0, bi)
                        perf.on_trade_open("LONG" if "LONG" in sig.action else "SHORT",
                                          fill.price, fill.qty, bi, ctx)
                    elif "CLOSE" in sig.action:
                        rpnl_r = position.reduce(fill.qty, fill.price)
                        if rpnl_r.is_ok():
                            rpnl, _ = rpnl_r.unwrap()
                            perf.on_trade_close(fill.price, bi, ctx)

                # 3. revalue
                pos = position.get()
                pos_upnl = position.revalue(bar['close'])
                eq_result = equity.revalue(bar['close'], pos.qty, pos.avg_price,
                                          pos.side, bi, ctx)
                if eq_result.is_ok():
                    snap_equity = eq_result.unwrap()
                    perf.on_equity(snap_equity.total_equity, bi, ctx)
                    risk_result = risk.post_bar(snap_equity.total_equity, equity.peak(), bi, ctx)

                # 4. snapshot
                if bi % snap_interval == 0:
                    state = self._collect_state()
                    snapshot.save(state, bi, source="periodic")

        self.shutdown()

    def _collect_state(self) -> dict:
        return {
            'bar_index': self.modules['M03_BarCache'].max_index,
            'equity': self.modules['M06_EquityManager'].state_export(),
            'position': self.modules['M05_PositionBook'].state_export(),
            'risk': self.modules['M04_RiskGuard'].state_export(),
            'strategy': self.modules['M01_StrategyEngine'].get_state(),
        }

    def shutdown(self) -> Result[None, Error]:
        self._running = False
        self.event_bus.publish_from("bootstrap", EventType.SYSTEM_STOPPING)
        for name in reversed(self.start_order):
            m = self.modules.get(name)
            if m and m.is_started:
                try:
                    m.stop()
                except Exception as e:
                    self.alert_manager.send("ERROR", f"Stop failed: {name}", str(e))
        self.event_bus.publish_from("bootstrap", EventType.SYSTEM_STOPPED)

        # final report
        equity = self.modules['M06_EquityManager']
        perf = self.modules['M11_PerformanceAnalyzer']
        snap = equity.get_snapshot()
        if snap:
            report = perf.report(snap.total_equity, snap.peak_equity)
            self.alert_manager.send("INFO", "Backtest complete",
                                    f"trades={report.total_trades} win={report.win_rate:.1%} "
                                    f"pf={report.profit_factor:.2f} mdd={report.max_drawdown:.2%} "
                                    f"sharpe={report.sharpe:.2f} pnl={report.total_pnl:.2f}")
        self.tracer.close()
        return Result.ok(None)


def main():
    parser = argparse.ArgumentParser(prog="ma26")
    parser.add_argument('--mode', choices=['backtest', 'paper', 'live'], default='backtest')
    parser.add_argument('--config', default='config/default.yaml')
    parser.add_argument('--bars', help='path to JSON bar file for backtest')
    args = parser.parse_args()

    orch = SystemOrchestrator(args.config, args.mode)
    r = orch.bootstrap()
    if r.is_err():
        print(f"FATAL: {r.error.format_chain()}", file=sys.stderr)
        sys.exit(1)

    bars = []
    if args.bars:
        import json
        with open(args.bars) as f:
            bars = json.load(f)
    elif args.mode == 'backtest':
        bars = generate_demo_bars(500)

    orch.run(bars)


def generate_demo_bars(n: int = 500) -> list:
    """Generate deterministic demo bars for smoke testing."""
    import math, hashlib
    bars = []
    price = 30000.0
    for i in range(n):
        seed = int(hashlib.md5(f"bar{i}".encode()).hexdigest(), 16)
        noise = ((seed % 2000) - 1000) / 1000.0 * 80
        drift = math.sin(i / 20.0) * 50
        close = max(100.0, price + noise + drift)
        high = close + abs(noise) * 0.5 + 10
        low = close - abs(noise) * 0.5 - 10
        bars.append({'bar_index': i, 'timestamp': i * 3600, 'open': price,
                    'high': high, 'low': low, 'close': close, 'volume': 1000.0})
        price = close
    return bars


if __name__ == '__main__':
    main()

