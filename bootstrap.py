"""
bootstrap.py
MA26-ATR 量化交易系统 — 系统启动编排器

职责:
  1. 解析命令行参数
  2. 加载 YAML 配置文件
  3. 初始化核心基础设施
  4. 注册并配置业务模块
  5. 根据运行模式启动对应流程
  6. 优雅关闭

运行方式:
  python bootstrap.py --mode backtest --config config/backtest.yaml
  python bootstrap.py --mode paper   --config config/paper.yaml
  python bootstrap.py --mode live    --config config/default.yaml
"""

import sys
import os
import argparse
import signal
import time
import yaml
import traceback
from typing import Optional

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from src.core import (
    Result, Error, ErrorSeverity, ErrorCategory,
    EventBus, Event, EventType,
    TraceContext, TraceLevel,
    ConfigSnapshot, StateRegistry,
    SystemModule, ModuleState,
)
from src.m01_strategy import MA26Strategy, StrategyConfig
from src.m03_kline import KlineCache
from src.m04_risk import RiskGuard, RiskConfig
from src.m05_position import PositionManager
from src.m06_equity import EquityTracker
from src.m07_execution import ExecutionManager
from src.m11_performance import PerformanceTracker
from src.m12_snapshot import SnapshotManager, SystemSnapshot
from src.m13_tracker import SignalTracker


# ═══════════════════════════════════════════════════════════════
# 系统编排器
# ═══════════════════════════════════════════════════════════════

class SystemOrchestrator:
    """
    系统编排器 — 管理所有模块的生命周期

    启动序列:
      load_config → init_modules → start_modules → run_mode → shutdown
    """

    def __init__(self, mode: str, config_path: str):
        self.mode = mode
        self.config_path = config_path
        self.config: Optional[ConfigSnapshot] = None
        self.raw_config: dict = {}

        # 核心基础设施
        self.event_bus = EventBus()
        self.state_registry = StateRegistry()

        # 业务模块（按依赖顺序排列）
        self.modules: list[SystemModule] = []
        self.kline: Optional[KlineCache] = None
        self.strategy: Optional[MA26Strategy] = None
        self.risk: Optional[RiskGuard] = None
        self.position: Optional[PositionManager] = None
        self.equity: Optional[EquityTracker] = None
        self.execution: Optional[ExecutionManager] = None
        self.performance: Optional[PerformanceTracker] = None
        self.snapshot_mgr: Optional[SnapshotManager] = None
        self.signal_tracker: Optional[SignalTracker] = None

        # 运行控制
        self.running = False
        self.shutdown_requested = False

        # 信号处理（优雅关闭）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 启动 trace
        self.trace = TraceContext.create("system_bootstrap", level=TraceLevel.INFO)
        TraceContext._current_trace = self.trace  # 线程局部存储

    def _signal_handler(self, signum, frame):
        """捕获 SIGINT/SIGTERM，触发优雅关闭"""
        print(f"\n\n[Signal] 收到信号 {signum}，请求优雅关闭...")
        self.shutdown_requested = True
        self.running = False

    # ─── 阶段1: 配置加载 ───────────────────────────────────────

    def load_config(self) -> Result[None]:
        """加载并校验 YAML 配置文件"""
        try:
            if not os.path.exists(self.config_path):
                return Result.err(Error(
                    code="BOOT_CFG_001",
                    message=f"配置文件不存在: {self.config_path}",
                    severity=ErrorSeverity.FATAL,
                    source="bootstrap",
                ))

            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.raw_config = yaml.safe_load(f)

            if not isinstance(self.raw_config, dict):
                return Result.err(Error(
                    code="BOOT_CFG_002",
                    message="配置文件格式错误：根节点必须是字典",
                    severity=ErrorSeverity.FATAL,
                    source="bootstrap",
                ))

            self.config = ConfigSnapshot(self.raw_config)
            print(f"  ✓ 配置加载成功: {self.config_path}")
            return Result.ok(None)

        except yaml.YAMLError as e:
            return Result.err(Error(
                code="BOOT_CFG_003",
                message=f"YAML 解析失败: {e}",
                severity=ErrorSeverity.FATAL,
                source="bootstrap",
            ))
        except Exception as e:
            return Result.err(Error(
                code="BOOT_CFG_004",
                message=f"配置加载异常: {e}",
                severity=ErrorSeverity.FATAL,
                source="bootstrap",
            ))

    # ─── 阶段2: 模块初始化 ──────────────────────────────────────

    def init_modules(self) -> Result[None]:
        """实例化所有模块并注入配置"""
        try:
            print("  [1/3] 实例化模块...")

            # ── 数据层 ──
            kline_cfg = self.raw_config.get('kline', {})
            self.kline = KlineCache(max_bars=kline_cfg.get('max_bars', 500))
            self.modules.append(self.kline)

            # ── 策略层 ──
            strategy_cfg = self.raw_config.get('strategy', {})
            self.strategy = MA26Strategy(StrategyConfig(
                ma_period=strategy_cfg.get('ma_period', 26),
                atr_period=strategy_cfg.get('atr_period', 14),
                band_multiplier=strategy_cfg.get('band_multiplier', 0.5),
                detach_multiplier=strategy_cfg.get('detach_multiplier', 0.3),
                min_trend_strength=strategy_cfg.get('min_trend_strength', 0.6),
            ))
            self.modules.append(self.strategy)

            # ── 风控层 ──
            risk_cfg = self.raw_config.get('risk', {})
            self.risk = RiskGuard(RiskConfig(
                max_daily_loss_pct=risk_cfg.get('max_daily_loss_pct', 0.05),
                max_single_loss_pct=risk_cfg.get('max_single_loss_pct', 0.02),
                max_drawdown_pct=risk_cfg.get('max_drawdown_pct', 0.20),
                max_slippage_bps=risk_cfg.get('max_slippage_bps', 50),
                max_position_value_usdt=risk_cfg.get('max_position_value_usdt', 10000.0),
                kill_switch_enabled=risk_cfg.get('kill_switch_enabled', True),
            ))
            self.modules.append(self.risk)

            # ── 仓位层 ──
            self.position = PositionManager()
            self.modules.append(self.position)

            # ── 权益层 ──
            initial_equity = self.raw_config.get('initial_equity', 100000.0)
            self.equity = EquityTracker(initial_equity=initial_equity)
            self.modules.append(self.equity)

            # ── 执行层 ──
            self.execution = ExecutionManager()
            self.modules.append(self.execution)

            # ── 绩效层 ──
            self.performance = PerformanceTracker()
            self.modules.append(self.performance)

            # ── 快照层 ──
            snapshot_dir = self.raw_config.get('snapshot_dir', './snapshots')
            os.makedirs(snapshot_dir, exist_ok=True)
            self.snapshot_mgr = SnapshotManager(snapshot_dir=snapshot_dir)
            self.modules.append(self.snapshot_mgr)

            # ── 信号追踪层 ──
            self.signal_tracker = SignalTracker()
            self.modules.append(self.signal_tracker)

            print(f"  ✓ {len(self.modules)} 个模块实例化完成")
            return Result.ok(None)

        except Exception as e:
            traceback.print_exc()
            return Result.err(Error(
                code="BOOT_INIT_001",
                message=f"模块实例化异常: {e}",
                severity=ErrorSeverity.FATAL,
                source="bootstrap",
            ))

    # ─── 阶段3: 模块启动 ────────────────────────────────────────

    def start_modules(self) -> Result[None]:
        """按序配置并启动所有模块"""
        try:
            print("  [2/3] 配置模块...")

            # 配置阶段
            for mod in self.modules:
                result = mod.configure(self.config)
                if result.is_err():
                    return Result.err(Error(
                        code="BOOT_START_001",
                        message=f"模块 {mod.__class__.__name__} 配置失败: "
                                f"{result.unwrap_err().message}",
                        severity=ErrorSeverity.FATAL,
                        source="bootstrap",
                    ))

            print("  [3/3] 启动模块...")
            # 启动阶段
            for mod in self.modules:
                result = mod.start()
                if result.is_err():
                    return Result.err(Error(
                        code="BOOT_START_002",
                        message=f"模块 {mod.__class__.__name__} 启动失败: "
                                f"{result.unwrap_err().message}",
                        severity=ErrorSeverity.FATAL,
                        source="bootstrap",
                    ))

            print(f"  ✓ 所有模块已启动 (状态: RUNNING)")
            return Result.ok(None)

        except Exception as e:
            traceback.print_exc()
            return Result.err(Error(
                code="BOOT_START_003",
                message=f"模块启动异常: {e}",
                severity=ErrorSeverity.FATAL,
                source="bootstrap",
            ))

    # ─── 阶段4: 运行模式分发 ────────────────────────────────────

    def run_backtest(self) -> Result[dict]:
        """回测模式：生成/加载数据 → 逐K线回放 → 输出结果"""
        try:
            print("\n" + "─" * 60)
            print("  📊 回测模式 (Backtest Mode)")
            print("─" * 60)

            # 延迟导入，避免循环依赖
            from tests.test_integration import BacktestEngine, BacktestDataGenerator

            # 创建回测引擎
            engine = BacktestEngine(self.raw_config)
            engine.configure_all(self.config)

            # 初始化快照管理器
            snapshot_dir = self.raw_config.get('snapshot_dir', './snapshots')
            os.makedirs(snapshot_dir, exist_ok=True)
            engine.snapshot_mgr = SnapshotManager(snapshot_dir=snapshot_dir)

            # ── 数据准备 ──
            data_source = self.raw_config.get('data_source', 'generated')
            n_bars = self.raw_config.get('n_bars', 100)
            start_price = self.raw_config.get('start_price', 50000.0)
            seed = self.raw_config.get('seed', 42)

            if data_source == 'generated':
                market_type = self.raw_config.get('market_type', 'trend')
                generator = BacktestDataGenerator(start_price=start_price, seed=seed)

                if market_type == 'trend':
                    bars = generator.generate_trend_market(
                        n_bars=n_bars,
                        trend_strength=self.raw_config.get('trend_strength', 0.003),
                    )
                elif market_type == 'ranging':
                    bars = generator.generate_ranging_market(
                        n_bars=n_bars,
                        amplitude=self.raw_config.get('amplitude', 0.01),
                    )
                else:
                    return Result.err(Error(
                        code="BOOT_BT_001",
                        message=f"未知市场类型: {market_type}",
                        severity=ErrorSeverity.RECOVERABLE,
                        source="bootstrap",
                    ))

                print(f"  数据源: 生成数据 ({market_type}, seed={seed})")

            elif data_source == 'csv':
                data_file = self.raw_config.get('data_file', '')
                if not data_file or not os.path.exists(data_file):
                    return Result.err(Error(
                        code="BOOT_BT_002",
                        message=f"CSV 数据文件不存在: {data_file}",
                        severity=ErrorSeverity.FATAL,
                        source="bootstrap",
                    ))
                # TODO: 实现 CSV 加载
                return Result.err(Error(
                    code="BOOT_BT_003",
                    message="CSV 数据加载尚未实现",
                    severity=ErrorSeverity.RECOVERABLE,
                    source="bootstrap",
                ))
            else:
                return Result.err(Error(
                    code="BOOT_BT_004",
                    message=f"未知数据源类型: {data_source}",
                    severity=ErrorSeverity.RECOVERABLE,
                    source="bootstrap",
                ))

            print(f"  K线数量: {len(bars)} 根 (3m)")
            print(f"  价格区间: {min(b.low for b in bars):.2f} ~ "
                  f"{max(b.high for b in bars):.2f}")

            # ── 执行回测 ──
            t0 = time.time()
            results = engine.run(bars)
            elapsed = time.time() - t0

            # ── 输出结果 ──
            print("\n" + "─" * 60)
            print("  📈 回测结果")
            print("─" * 60)
            print(f"  初始权益:     {results['initial_equity']:>14,.2f} USDT")
            print(f"  最终权益:     {results['final_equity']:>14,.2f} USDT")
            print(f"  总收益率:     {results['total_return']*100:>13,.2f} %")
            print(f"  信号生成:     {results['signals_generated']:>14} 个")
            print(f"  订单提交:     {results['orders_submitted']:>14} 个")
            print(f"  成交笔数:     {results['fills_received']:>14} 笔")
            print(f"  最大回撤:     {results['max_drawdown_pct']*100:>13,.2f} %")
            print(f"  权益曲线:     {results['equity_curve_length']:>14} 点")
            print(f"  执行耗时:     {elapsed:>14.3f} s")
            print("─" * 60)

            # ── 保存快照 ──
            snapshot = SystemSnapshot(
                snapshot_id=f"backtest_{int(time.time())}",
                timestamp=int(time.time() * 1000),
                equity=results['final_equity'],
                positions={
                    sym: {'qty': p.quantity, 'side': p.side.value}
                    for sym, p in results['positions_final'].items()
                },
                open_orders={},
                risk_state={},
                module_states={
                    m.__class__.__name__: m.state.value
                    for m in self.modules
                },
            )
            save_result = engine.snapshot_mgr.save(snapshot)
            if save_result.is_ok():
                print(f"  ✓ 快照已保存: {snapshot.snapshot_id}")

            return Result.ok(results)

        except Exception as e:
            traceback.print_exc()
            return Result.err(Error(
                code="BOOT_BT_999",
                message=f"回测运行异常: {e}",
                severity=ErrorSeverity.FATAL,
                source="bootstrap",
            ))

    def run_paper(self) -> Result[None]:
        """模拟盘模式（占位）"""
        print("\n" + "─" * 60)
        print("  📡 模拟盘模式 (Paper Trading)")
        print("─" * 60)
        print("  需要连接交易所 WebSocket 接收实时行情")
        print("  当前最小运行集尚未实现实时数据接入")
        print("  请使用 --mode backtest 进行策略验证")
        return Result.err(Error(
            code="BOOT_PAPER_001",
            message="模拟盘模式尚未实现",
            severity=ErrorSeverity.RECOVERABLE,
            source="bootstrap",
        ))

    def run_live(self) -> Result[None]:
        """实盘模式（占位）"""
        print("\n" + "─" * 60)
        print("  🔴 实盘模式 (Live Trading)")
        print("─" * 60)
        print("  需要完整的部署检查（M24 部署治理模块）")
        print("  当前最小运行集不包含实盘部署模块")
        print("  请先完成 M24 部署治理模块集成")
        return Result.err(Error(
            code="BOOT_LIVE_001",
            message="实盘模式尚未实现",
            severity=ErrorSeverity.RECOVERABLE,
            source="bootstrap",
        ))

    # ─── 主运行流程 ─────────────────────────────────────────────

    def run(self) -> int:
        """
        主入口：编排完整启动序列
        返回: 退出码 (0=成功, 1=失败)
        """
        print("╔" + "═" * 58 + "╗")
        print("║" + " " * 8 + "MA26-ATR 量化交易系统 v4.1.0-minimal" + " " * 16 + "║")
        print("║" + " " * 8 + f"Mode: {self.mode:<40}" + "║")
        print("╚" + "═" * 58 + "╝")

        # Step 1: 加载配置
        print("\n[Phase 1/4] 加载配置...")
        result = self.load_config()
        if result.is_err():
            print(f"  ✗ {result.unwrap_err().message}")
            return 1

        # Step 2: 初始化模块
        print("\n[Phase 2/4] 初始化模块...")
        result = self.init_modules()
        if result.is_err():
            print(f"  ✗ {result.unwrap_err().message}")
            return 1

        # Step 3: 启动模块
        print("\n[Phase 3/4] 启动模块...")
        result = self.start_modules()
        if result.is_err():
            print(f"  ✗ {result.unwrap_err().message}")
            return 1

        # Step 4: 运行
        print("\n[Phase 4/4] 运行系统...")
        self.running = True

        try:
            if self.mode == "backtest":
                result = self.run_backtest()
                if result.is_err():
                    print(f"  ✗ {result.unwrap_err().message}")
                    return 1
                print("\n  ✓ 回测完成")

            elif self.mode == "paper":
                self.run_paper()

            elif self.mode == "live":
                self.run_live()

            else:
                print(f"  ✗ 未知运行模式: {self.mode}")
                return 1

        except KeyboardInterrupt:
            print("\n\n[Interrupt] 用户中断")
        except Exception as e:
            print(f"\n  ✗ 运行异常: {e}")
            traceback.print_exc()
            return 1

        # 优雅关闭
        self.shutdown()
        return 0

    def shutdown(self):
        """反向停止所有模块"""
        print("\n" + "─" * 60)
        print("  🛑 系统关闭")
        print("─" * 60)

        self.running = False

        # 反向停止（依赖倒置原则）
        stopped = 0
        for mod in reversed(self.modules):
            try:
                result = mod.stop()
                if result.is_ok():
                    stopped += 1
            except Exception as e:
                print(f"  ⚠ {mod.__class__.__name__} 停止异常: {e}")

        print(f"  ✓ {stopped}/{len(self.modules)} 模块已停止")
        print("  系统安全退出")


# ═══════════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="MA26-ATR 量化交易系统 v4.1.0-minimal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python bootstrap.py -m backtest -c config/backtest.yaml
  python bootstrap.py -m backtest -c config/default.yaml
  python bootstrap.py -m paper   -c config/paper.yaml

运行模式:
  backtest  回测模式（生成数据或历史数据回放）
  paper     模拟盘模式（WebSocket 实时行情 + 模拟撮合）
  live      实盘模式（真实 API 交易，需 M24 部署模块）

当前最小运行集仅完整支持 backtest 模式。
        """,
    )

    parser.add_argument(
        "--mode", "-m",
        choices=["backtest", "paper", "live"],
        default="backtest",
        help="运行模式 (默认: backtest)",
    )

    parser.add_argument(
        "--config", "-c",
        default="config/default.yaml",
        help="配置文件路径 (默认: config/default.yaml)",
    )

    parser.add_argument(
        "--version", "-v",
        action="version",
        version="MA26-ATR Quant System v4.1.0-minimal",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    orchestrator = SystemOrchestrator(
        mode=args.mode,
        config_path=args.config,
    )

    exit_code = orchestrator.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
