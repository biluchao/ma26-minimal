"""
src/modules/execution/position_manager.py
MA26-ATR 量化交易系统 — 执行模块：仓位管理器

功能:
  1. 管理所有交易对的持仓状态（多/空/空仓）
  2. 消费 ORDER_REQUEST 事件（来自风控模块 m04_risk）
  3. 执行开仓/平仓/止损/止盈逻辑
  4. 模拟执行引擎（backtest/paper 模式）和实盘适配层（live 模式）
  5. 发布 TRADE_UPDATE 事件（成交通知 → 策略/风控）
  6. 发布 EQUITY_UPDATE 事件（权益更新 → 风控/监控）
  7. 发布 POSITION_UPDATE 事件（持仓变更 → 状态注册表）
  8. 实现 LifecycleAware 接口

执行模式:
  - backtest: 回测模式（按K线收盘价/下一根开盘价模拟成交）
  - paper:    模拟盘模式（按当前价格模拟成交，含滑点）
  - live:     实盘模式（调用交易所 API 下单）

数据流:
  RiskGuard (ORDER_REQUEST)
    → PositionManager._on_order_request()
    → 执行引擎（模拟/实盘）
    → 成交回调
    → TRADE_UPDATE → EventBus
    → EQUITY_UPDATE → EventBus
    → POSITION_UPDATE → StateRegistry
"""

import time
import threading
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum

from src.core.result import Result, Error, ErrorSeverity, ErrorCategory
from src.core.lifecycle import LifecycleAware
from src.core.event_bus import EventBus, Event, EventType, get_global_event_bus
from src.core.state_registry import get_global_state_registry, StateScope
from src.core.config import get_global_config_manager


# ═══════════════════════════════════════════════════════════════
# 枚举定义
# ═══════════════════════════════════════════════════════════════

class OrderSide(Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """订单类型"""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class ExecutionMode(Enum):
    """执行模式"""
    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class PositionSide(Enum):
    """持仓方向"""
    FLAT = 0       # 空仓
    LONG = 1       # 多仓
    SHORT = -1     # 空仓


# ═══════════════════════════════════════════════════════════════
# 数据类
# ═══════════════════════════════════════════════════════════════

@dataclass
class Order:
    """
    订单数据类。
    """
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: float = 0.0           # 限价单价格
    stop_price: float = 0.0      # 止损/止盈触发价
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    filled_price: float = 0.0    # 实际成交均价
    fee: float = 0.0
    fee_asset: str = ""
    created_at: int = 0
    updated_at: int = 0
    source: str = ""             # 来源模块
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'order_type': self.order_type.value,
            'quantity': self.quantity,
            'price': self.price,
            'stop_price': self.stop_price,
            'status': self.status.value,
            'filled_quantity': self.filled_quantity,
            'filled_price': self.filled_price,
            'fee': self.fee,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'source': self.source,
            'metadata': self.metadata,
        }


@dataclass
class Position:
    """
    持仓数据类。
    """
    symbol: str
    side: PositionSide = PositionSide.FLAT
    size: float = 0.0            # 持仓数量（正数为多，负数为空）
    entry_price: float = 0.0     # 开仓均价
    current_price: float = 0.0   # 当前市价
    unrealized_pnl: float = 0.0  # 未实现盈亏
    realized_pnl: float = 0.0    # 已实现盈亏
    margin: float = 0.0          # 占用保证金
    open_time: int = 0           # 开仓时间
    update_time: int = 0         # 最后更新时间
    orders: List[str] = field(default_factory=list)  # 关联订单ID

    def update_market_price(self, price: float):
        """更新市价并计算未实现盈亏"""
        self.current_price = price
        self.update_time = int(time.time() * 1000)
        if self.size != 0 and self.entry_price > 0:
            if self.size > 0:
                self.unrealized_pnl = (price - self.entry_price) * self.size
            else:
                self.unrealized_pnl = (self.entry_price - price) * abs(self.size)

    def is_flat(self) -> bool:
        return self.size == 0

    def is_long(self) -> bool:
        return self.size > 0

    def is_short(self) -> bool:
        return self.size < 0

    def market_value(self) -> float:
        """持仓市值"""
        return abs(self.size) * self.current_price

    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'side': self.side.name,
            'size': self.size,
            'entry_price': self.entry_price,
            'current_price': self.current_price,
            'unrealized_pnl': self.unrealized_pnl,
            'realized_pnl': self.realized_pnl,
            'margin': self.margin,
            'market_value': self.market_value(),
            'open_time': self.open_time,
            'update_time': self.update_time,
        }


@dataclass
class Fill:
    """
    成交记录。
    """
    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    fee: float
    fee_asset: str
    timestamp: int
    pnl: float = 0.0            # 该笔成交的已实现盈亏
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'fill_id': self.fill_id,
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'quantity': self.quantity,
            'price': self.price,
            'fee': self.fee,
            'fee_asset': self.fee_asset,
            'timestamp': self.timestamp,
            'pnl': self.pnl,
        }


# ═══════════════════════════════════════════════════════════════
# 执行引擎接口
# ═══════════════════════════════════════════════════════════════

class ExecutionEngine:
    """
    执行引擎抽象基类。
    不同模式（backtest/paper/live）有不同实现。
    """

    def __init__(self, mode: ExecutionMode):
        self.mode = mode
        self._slippage_bps: float = 0.0  # 滑点（基点）
        self._fee_rate: float = 0.001     # 手续费率（0.1%）

    def set_slippage(self, bps: float):
        """设置滑点（基点：1bp = 0.01%）"""
        self._slippage_bps = bps

    def set_fee_rate(self, rate: float):
        """设置手续费率"""
        self._fee_rate = rate

    def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        current_price: float,
        metadata: dict = None,
    ) -> Tuple[Fill, Order]:
        """
        执行市价单。
        返回 (Fill, Order)。
        """
        raise NotImplementedError

    def execute_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        limit_price: float,
        metadata: dict = None,
    ) -> Order:
        """
        执行限价单。
        返回 Order（可能立即成交或挂单）。
        """
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> bool:
        """取消订单"""
        raise NotImplementedError

    def get_current_price(self, symbol: str) -> float:
        """获取当前价格（模拟/实盘）"""
        raise NotImplementedError


class BacktestExecutionEngine(ExecutionEngine):
    """
    回测执行引擎。
    按信号触发时的K线价格执行（含滑点模拟）。
    """

    def __init__(self):
        super().__init__(ExecutionMode.BACKTEST)
        self._price_provider: Optional[Callable[[str], float]] = None

    def set_price_provider(self, provider: Callable[[str], float]):
        """设置价格提供者（通常从 KlineCache 获取）"""
        self._price_provider = provider

    def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        current_price: float = 0.0,
        metadata: dict = None,
    ) -> Tuple[Fill, Order]:
        """回测市价单执行"""
        # 获取执行价格
        if current_price <= 0 and self._price_provider:
            current_price = self._price_provider(symbol)
        if current_price <= 0:
            current_price = metadata.get('price', 0) if metadata else 0

        # 滑点模拟
        slippage = current_price * self._slippage_bps / 10000
        if side == OrderSide.BUY:
            exec_price = current_price + slippage
        else:
            exec_price = current_price - slippage

        # 手续费
        fee = exec_price * quantity * self._fee_rate
        fee_asset = "USDT"

        # 生成订单
        order_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        order = Order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            filled_price=exec_price,
            filled_quantity=quantity,
            status=OrderStatus.FILLED,
            fee=fee,
            fee_asset=fee_asset,
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )

        # 生成成交
        fill_id = str(uuid.uuid4())
        fill = Fill(
            fill_id=fill_id,
            order_id=order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=exec_price,
            fee=fee,
            fee_asset=fee_asset,
            timestamp=now,
            metadata=metadata or {},
        )

        return fill, order

    def execute_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        limit_price: float,
        metadata: dict = None,
    ) -> Order:
        """回测限价单"""
        order_id = str(uuid.uuid4())
        now = int(time.time() * 1000)

        # 回测中限价单立即按限价成交（简化处理）
        fee = limit_price * quantity * self._fee_rate

        return Order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=limit_price,
            filled_price=limit_price,
            filled_quantity=quantity,
            status=OrderStatus.FILLED,
            fee=fee,
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )

    def cancel_order(self, order_id: str) -> bool:
        return True

    def get_current_price(self, symbol: str) -> float:
        if self._price_provider:
            return self._price_provider(symbol)
        return 0.0


class PaperExecutionEngine(ExecutionEngine):
    """
    模拟盘执行引擎。
    与回测类似但使用实时 WebSocket 价格。
    """

    def __init__(self):
        super().__init__(ExecutionMode.PAPER)
        self._price_provider: Optional[Callable[[str], float]] = None

    def set_price_provider(self, provider: Callable[[str], float]):
        self._price_provider = provider

    def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        current_price: float = 0.0,
        metadata: dict = None,
    ) -> Tuple[Fill, Order]:
        """模拟盘市价单"""
        if current_price <= 0 and self._price_provider:
            current_price = self._price_provider(symbol)
        if current_price <= 0:
            current_price = metadata.get('price', 0) if metadata else 0

        # 滑点更大（模拟盘模拟真实市场冲击）
        slippage = current_price * self._slippage_bps / 10000
        if side == OrderSide.BUY:
            exec_price = current_price + slippage
        else:
            exec_price = current_price - slippage

        fee = exec_price * quantity * self._fee_rate

        order_id = str(uuid.uuid4())
        now = int(time.time() * 1000)

        order = Order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            filled_price=exec_price,
            filled_quantity=quantity,
            status=OrderStatus.FILLED,
            fee=fee,
            fee_asset="USDT",
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )

        fill = Fill(
            fill_id=str(uuid.uuid4()),
            order_id=order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=exec_price,
            fee=fee,
            fee_asset="USDT",
            timestamp=now,
            metadata=metadata or {},
        )

        return fill, order

    def execute_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        limit_price: float,
        metadata: dict = None,
    ) -> Order:
        order_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        fee = limit_price * quantity * self._fee_rate

        return Order(
            order_id=order_id,
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=limit_price,
            filled_price=limit_price,
            filled_quantity=quantity,
            status=OrderStatus.FILLED,
            fee=fee,
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )

    def cancel_order(self, order_id: str) -> bool:
        return True

    def get_current_price(self, symbol: str) -> float:
        if self._price_provider:
            return self._price_provider(symbol)
        return 0.0


class LiveExecutionEngine(ExecutionEngine):
    """
    实盘执行引擎。
    实际调用交易所 API（此处为框架骨架，需对接 ccxt/binance-sdk）。
    """

    def __init__(self, api_key: str = "", api_secret: str = ""):
        super().__init__(ExecutionMode.LIVE)
        self._api_key = api_key
        self._api_secret = api_secret
        self._client = None  # 交易所客户端（ccxt 等）
        self._pending_orders: Dict[str, Order] = {}

    def connect(self) -> Result[None]:
        """连接交易所"""
        # TODO: 初始化 ccxt/binance 客户端
        # self._client = ccxt.binance({...})
        return Result.ok(None)

    def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        current_price: float = 0.0,
        metadata: dict = None,
    ) -> Tuple[Fill, Order]:
        """实盘市价单"""
        if not self._client:
            raise RuntimeError("Exchange client not connected")

        # TODO: 调用交易所 API
        # order = self._client.create_market_buy_order(symbol, quantity)
        raise NotImplementedError("Live execution requires exchange API integration")

    def execute_limit_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: float,
        limit_price: float,
        metadata: dict = None,
    ) -> Order:
        """实盘限价单"""
        if not self._client:
            raise RuntimeError("Exchange client not connected")

        # TODO: 调用交易所 API
        raise NotImplementedError("Live execution requires exchange API integration")

    def cancel_order(self, order_id: str) -> bool:
        # TODO: 调用交易所 API 取消订单
        raise NotImplementedError

    def get_current_price(self, symbol: str) -> float:
        # TODO: 从 WebSocket 或 REST 获取实时价格
        raise NotImplementedError


# ═══════════════════════════════════════════════════════════════
# 仓位管理器主类
# ═══════════════════════════════════════════════════════════════

class PositionManager(LifecycleAware):
    """
    仓位管理器模块。

    职责:
      1. 维护所有交易对的持仓状态
      2. 接收 ORDER_REQUEST 事件，调用执行引擎
      3. 成交后更新持仓、发布事件、计算盈亏
      4. 支持多模式切换（backtest/paper/live）
    """

    def __init__(self):
        # 执行模式
        self._mode = ExecutionMode.PAPER
        self._execution_engine: Optional[ExecutionEngine] = None

        # 持仓状态
        self._positions: Dict[str, Position] = {}  # symbol → Position
        self._orders: Dict[str, Order] = {}         # order_id → Order
        self._fills: List[Fill] = []

        # 权益追踪
        self._initial_equity: float = 100000.0
        self._current_equity: float = 100000.0
        self._available_balance: float = 100000.0

        # 统计
        self._total_trades = 0
        self._winning_trades = 0
        self._total_fees = 0.0
        self._total_realized_pnl = 0.0

        # 依赖
        self._event_bus: Optional[EventBus] = None
        self._config_mgr = None
        self._state_registry = None

        # 线程安全
        self._lock = threading.RLock()
        self._running = False

    # ─── LifecycleAware 接口 ─────────────────────────────────

    @property
    def module_name(self) -> str:
        return "m03_execution"

    @property
    def dependencies(self) -> List[str]:
        return ["core_config", "core_event_bus", "core_state"]

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
        """初始化仓位管理器"""
        # 配置
        try:
            self._config_mgr = get_global_config_manager()
        except Exception:
            pass

        if self._config_mgr:
            self._initial_equity = self._config_mgr.get_float(
                "initial_equity", self._initial_equity
            )
            mode_str = self._config_mgr.get_str("mode", "paper")
            self._mode = ExecutionMode(mode_str)

        self._current_equity = self._initial_equity
        self._available_balance = self._initial_equity

        # 事件总线
        try:
            self._event_bus = get_global_event_bus()
        except Exception:
            pass

        # 状态注册
        try:
            self._state_registry = get_global_state_registry()
        except Exception:
            pass

        # 初始化执行引擎
        self._init_execution_engine()

        return Result.ok(None)

    def on_start(self) -> Result[None]:
        """启动"""
        self._running = True

        # 订阅订单请求
        if self._event_bus:
            self._event_bus.subscribe(
                EventType.ORDER_REQUEST,
                self._on_order_request,
            )
            # 订阅K线更新（用于更新未实现盈亏）
            self._event_bus.subscribe(
                EventType.KLINE_UPDATE,
                self._on_kline_update_for_pnl,
            )

        return Result.ok(None)

    def on_drain(self) -> Result[None]:
        """优雅停机"""
        self._running = False
        return Result.ok(None)

    def on_stop(self) -> Result[None]:
        """停止"""
        self._running = False

        if self._event_bus:
            try:
                self._event_bus.unsubscribe(
                    EventType.ORDER_REQUEST, self._on_order_request
                )
            except Exception:
                pass

        return Result.ok(None)

    def on_health_check(self) -> Result[dict]:
        with self._lock:
            return Result.ok({
                'running': self._running,
                'mode': self._mode.value,
                'total_positions': len(self._positions),
                'active_positions': sum(
                    1 for p in self._positions.values() if not p.is_flat()
                ),
                'total_orders': len(self._orders),
                'total_fills': len(self._fills),
                'current_equity': self._current_equity,
                'available_balance': self._available_balance,
                'total_realized_pnl': self._total_realized_pnl,
                'total_fees': self._total_fees,
                'total_trades': self._total_trades,
                'win_rate': (
                    self._winning_trades / self._total_trades
                    if self._total_trades > 0 else 0
                ),
            })

    # ─── 执行引擎初始化 ───────────────────────────────────────

    def _init_execution_engine(self):
        """根据模式初始化执行引擎"""
        if self._mode == ExecutionMode.BACKTEST:
            self._execution_engine = BacktestExecutionEngine()
        elif self._mode == ExecutionMode.PAPER:
            self._execution_engine = PaperExecutionEngine()
        elif self._mode == ExecutionMode.LIVE:
            api_key = ""
            api_secret = ""
            if self._config_mgr:
                api_key = self._config_mgr.get_str("exchange.api_key", "")
                api_secret = self._config_mgr.get_str("exchange.api_secret", "")
            self._execution_engine = LiveExecutionEngine(api_key, api_secret)
        else:
            self._execution_engine = PaperExecutionEngine()

        # 设置滑点和手续费
        if self._config_mgr and self._execution_engine:
            slippage = self._config_mgr.get_float("execution.slippage_bps", 50)
            fee_rate = self._config_mgr.get_float("execution.fee_rate", 0.001)
            self._execution_engine.set_slippage(slippage)
            self._execution_engine.set_fee_rate(fee_rate)

    # ─── 事件处理 ─────────────────────────────────────────────

    def _on_order_request(self, event: Event):
        """
        处理订单请求（来自风控模块）。
        核心执行入口。
        """
        if not self._running or not self._execution_engine:
            return

        payload = event.payload
        signal_type = payload.get('signal_type', '')
        symbol = payload.get('symbol', '')
        price = float(payload.get('price', 0))
        suggested_size = float(payload.get('suggested_size', 0))

        if not symbol or suggested_size <= 0:
            return

        try:
            if signal_type in ('buy',):
                self._open_long(symbol, suggested_size, price, payload)
            elif signal_type in ('sell',):
                self._open_short(symbol, suggested_size, price, payload)
            elif signal_type in ('close_long', 'stop_loss', 'take_profit'):
                # 判断是平多还是平空
                position = self._get_position(symbol)
                if position.is_long():
                    self._close_long(symbol, price, payload)
                elif position.is_short():
                    self._close_short(symbol, price, payload)
                elif signal_type == 'close_short':
                    self._close_short(symbol, price, payload)
            elif signal_type == 'close_short':
                self._close_short(symbol, price, payload)

        except Exception as e:
            # 发布执行失败事件
            if self._event_bus:
                self._event_bus.publish(Event(
                    event_type=EventType.ERROR,
                    source="m03_execution",
                    payload={
                        'error': str(e),
                        'symbol': symbol,
                        'signal_type': signal_type,
                    },
                ))

    def _on_kline_update_for_pnl(self, event: Event):
        """用最新K线价格更新未实现盈亏"""
        payload = event.payload
        symbol = payload.get('symbol', '')
        close = float(payload.get('close', 0))

        if symbol and close > 0:
            self._update_position_price(symbol, close)

    # ─── 开仓/平仓逻辑 ────────────────────────────────────────

    def _open_long(self, symbol: str, size: float, price: float, metadata: dict):
        """开多仓"""
        fill, order = self._execution_engine.execute_market_order(
            symbol, OrderSide.BUY, size, price, metadata
        )

        with self._lock:
            position = self._get_or_create_position(symbol)

            # 更新持仓
            old_size = position.size
            if old_size >= 0:
                # 加多
                new_size = old_size + size
                if new_size > 0:
                    position.entry_price = (
                        (old_size * position.entry_price + size * fill.price)
                        / new_size
                    )
                position.size = new_size
                position.side = PositionSide.LONG
            else:
                # 平空 + 开多
                cover_size = min(abs(old_size), size)
                remaining_size = size - cover_size

                # 平空部分
                if cover_size > 0:
                    pnl = (position.entry_price - fill.price) * cover_size
                    self._total_realized_pnl += pnl
                    if pnl > 0:
                        self._winning_trades += 1

                if remaining_size > 0:
                    position.size = remaining_size
                    position.entry_price = fill.price
                    position.side = PositionSide.LONG
                else:
                    position.size += cover_size
                    if position.size == 0:
                        position.side = PositionSide.FLAT
                        position.entry_price = 0

            position.update_market_price(fill.price)
            if position.open_time == 0:
                position.open_time = fill.timestamp

            # 记录
            self._orders[order.order_id] = order
            self._fills.append(fill)
            self._total_trades += 1
            self._total_fees += fill.fee
            self._update_equity()

        # 发布事件
        self._publish_trade_update(fill, order, position)

    def _open_short(self, symbol: str, size: float, price: float, metadata: dict):
        """开空仓"""
        fill, order = self._execution_engine.execute_market_order(
            symbol, OrderSide.SELL, size, price, metadata
        )

        with self._lock:
            position = self._get_or_create_position(symbol)

            old_size = position.size
            if old_size <= 0:
                # 加空
                new_size = old_size - size
                if new_size < 0:
                    position.entry_price = (
                        (abs(old_size) * position.entry_price + size * fill.price)
                        / abs(new_size)
                    )
                position.size = new_size
                position.side = PositionSide.SHORT
            else:
                # 平多 + 开空
                cover_size = min(old_size, size)
                remaining_size = size - cover_size

                if cover_size > 0:
                    pnl = (fill.price - position.entry_price) * cover_size
                    self._total_realized_pnl += pnl
                    if pnl > 0:
                        self._winning_trades += 1

                if remaining_size > 0:
                    position.size = -remaining_size
                    position.entry_price = fill.price
                    position.side = PositionSide.SHORT
                else:
                    position.size -= cover_size
                    if position.size == 0:
                        position.side = PositionSide.FLAT
                        position.entry_price = 0

            position.update_market_price(fill.price)
            if position.open_time == 0:
                position.open_time = fill.timestamp

            self._orders[order.order_id] = order
            self._fills.append(fill)
            self._total_trades += 1
            self._total_fees += fill.fee
            self._update_equity()

        self._publish_trade_update(fill, order, position)

    def _close_long(self, symbol: str, price: float, metadata: dict):
        """平多仓"""
        position = self._get_position(symbol)
        if position.size <= 0:
            return

        close_size = position.size
        fill, order = self._execution_engine.execute_market_order(
            symbol, OrderSide.SELL, close_size, price, metadata
        )

        with self._lock:
            # 计算盈亏
            pnl = (fill.price - position.entry_price) * close_size
            self._total_realized_pnl += pnl
            if pnl > 0:
                self._winning_trades += 1

            # 重置持仓
            position.size = 0
            position.side = PositionSide.FLAT
            position.entry_price = 0
            position.unrealized_pnl = 0
            position.open_time = 0

            self._orders[order.order_id] = order
            self._fills.append(fill)
            self._total_fees += fill.fee
            self._update_equity()

        self._publish_trade_update(fill, order, position, pnl)

    def _close_short(self, symbol: str, price: float, metadata: dict):
        """平空仓"""
        position = self._get_position(symbol)
        if position.size >= 0:
            return

        close_size = abs(position.size)
        fill, order = self._execution_engine.execute_market_order(
            symbol, OrderSide.BUY, close_size, price, metadata
        )

        with self._lock:
            pnl = (position.entry_price - fill.price) * close_size
            self._total_realized_pnl += pnl
            if pnl > 0:
                self._winning_trades += 1

            position.size = 0
            position.side = PositionSide.FLAT
            position.entry_price = 0
            position.unrealized_pnl = 0
            position.open_time = 0

            self._orders[order.order_id] = order
            self._fills.append(fill)
            self._total_fees += fill.fee
            self._update_equity()

        self._publish_trade_update(fill, order, position, pnl)

    # ─── 辅助方法 ─────────────────────────────────────────────

    def _get_or_create_position(self, symbol: str) -> Position:
        """获取或创建持仓"""
        if symbol not in self._positions:
            self._positions[symbol] = Position(symbol=symbol)
        return self._positions[symbol]

    def _get_position(self, symbol: str) -> Position:
        """获取持仓（不存在则返回空持仓）"""
        return self._positions.get(symbol, Position(symbol=symbol))

    def _update_position_price(self, symbol: str, price: float):
        """更新持仓市价"""
        with self._lock:
            if symbol in self._positions:
                self._positions[symbol].update_market_price(price)
                self._update_equity()

    def _update_equity(self):
        """更新总权益"""
        total_unrealized = sum(
            p.unrealized_pnl for p in self._positions.values()
        )
        self._current_equity = (
            self._initial_equity
            + self._total_realized_pnl
            + total_unrealized
        )
        self._available_balance = self._current_equity

    def _publish_trade_update(self, fill: Fill, order: Order, position: Position, pnl: float = 0.0):
        """发布成交更新事件"""
        if not self._event_bus:
            return

        # TRADE_UPDATE
        self._event_bus.publish(Event(
            event_type=EventType.TRADE_UPDATE,
            source="m03_execution",
            payload={
                'symbol': fill.symbol,
                'side': fill.side.value,
                'size': fill.quantity,
                'price': fill.price,
                'fee': fill.fee,
                'pnl': pnl,
                'order_id': fill.order_id,
                'fill_id': fill.fill_id,
                'timestamp': fill.timestamp,
                'position': position.to_dict(),
            },
        ))

        # EQUITY_UPDATE
        self._event_bus.publish(Event(
            event_type=EventType.EQUITY_UPDATE,
            source="m03_execution",
            payload={
                'equity': self._current_equity,
                'available': self._available_balance,
                'realized_pnl': self._total_realized_pnl,
                'unrealized_pnl': sum(p.unrealized_pnl for p in self._positions.values()),
                'timestamp': int(time.time() * 1000),
            },
        ))

        # POSITION_UPDATE → StateRegistry
        if self._state_registry:
            try:
                self._state_registry.set(
                    f"position.{fill.symbol}",
                    position.to_dict(),
                    scope=StateScope.RUNTIME,
                )
            except Exception:
                pass

    # ─── 公共接口 ─────────────────────────────────────────────

    def get_position(self, symbol: str) -> Optional[dict]:
        """获取持仓快照"""
        with self._lock:
            if symbol not in self._positions:
                return None
            return self._positions[symbol].to_dict()

    def get_all_positions(self) -> List[dict]:
        """获取所有持仓"""
        with self._lock:
            return [p.to_dict() for p in self._positions.values()]

    def get_equity(self) -> float:
        """获取当前权益"""
        with self._lock:
            return self._current_equity

    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._lock:
            return {
                'mode': self._mode.value,
                'total_trades': self._total_trades,
                'winning_trades': self._winning_trades,
                'win_rate': (
                    self._winning_trades / self._total_trades
                    if self._total_trades > 0 else 0
                ),
                'total_realized_pnl': self._total_realized_pnl,
                'total_fees': self._total_fees,
                'current_equity': self._current_equity,
                'initial_equity': self._initial_equity,
                'return_pct': (
                    (self._current_equity - self._initial_equity) / self._initial_equity
                    if self._initial_equity > 0 else 0
                ),
                'active_positions': sum(
                    1 for p in self._positions.values() if not p.is_flat()
                ),
                'total_orders': len(self._orders),
                'total_fills': len(self._fills),
            }

    def reset(self):
        """重置所有状态"""
        with self._lock:
            self._positions.clear()
            self._orders.clear()
            self._fills.clear()
            self._current_equity = self._initial_equity
            self._available_balance = self._initial_equity
            self._total_trades = 0
            self._winning_trades = 0
            self._total_fees = 0.0
            self._total_realized_pnl = 0.0


# ═══════════════════════════════════════════════════════════════
# 全局单例
# ═══════════════════════════════════════════════════════════════

_global_position_manager: Optional[PositionManager] = None
_global_pm_lock = threading.Lock()


def get_global_position_manager() -> PositionManager:
    """获取全局 PositionManager 单例"""
    global _global_position_manager
    if _global_position_manager is None:
        with _global_pm_lock:
            if _global_position_manager is None:
                _global_position_manager = PositionManager()
    return _global_position_manager


def reset_global_position_manager():
    """重置全局单例（测试用）"""
    global _global_position_manager
    with _global_pm_lock:
        if _global_position_manager:
            _global_position_manager.reset()
        _global_position_manager = None
