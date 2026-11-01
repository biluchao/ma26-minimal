"""
src/core/event_bus.py
MA26-ATR 量化交易系统 — 核心原语：事件总线

设计理念:
  事件驱动架构的核心组件，实现模块间完全解耦。
  所有模块通过 EventBus 进行异步通信，禁止模块间直接调用。

  特性:
    - 类型安全的事件订阅/发布
    - 同步 + 异步双模式发布
    - 通配符订阅 (* 监听所有事件)
    - 事件优先级 (HIGH / NORMAL / LOW)
    - 线程安全
    - Handler 异常隔离（单个 handler 崩溃不影响其他）
    - 死信队列（发布失败的事件暂存）
    - 事件过滤和中间件链

  事件流:
    Publisher → EventBus.publish() → [Filter] → [Middleware] → Handler(s)
"""

import time
import threading
import queue
import traceback
from enum import Enum, auto
from typing import (
    Callable, Optional, Any, Dict, List, Set, Tuple, 
    Union, Protocol, runtime_checkable
)
from dataclasses import dataclass, field
from collections import defaultdict, deque

from .result import Result, Error, ErrorSeverity, ErrorCategory


# ═══════════════════════════════════════════════════════════════
# 事件优先级
# ═══════════════════════════════════════════════════════════════

class EventPriority(Enum):
    """事件处理优先级"""
    HIGH = 0      # 风控、熔断、紧急停止
    NORMAL = 1    # 策略信号、订单状态
    LOW = 2       # 日志、统计、心跳


# ═══════════════════════════════════════════════════════════════
# 事件类型枚举
# ═══════════════════════════════════════════════════════════════

class EventType(Enum):
    """
    系统事件类型全集。
    
    命名规范:
      [模块缩写]_[动作]_[状态]
    """

    # ─── 系统级 ───
    SYSTEM_STARTED = "SYSTEM_STARTED"
    SYSTEM_STOPPING = "SYSTEM_STOPPING"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    SYSTEM_HEALTH_CHECK = "SYSTEM_HEALTH_CHECK"

    # ─── 数据/K线 ───
    TICK_RECEIVED = "TICK_RECEIVED"
    KLINE_CLOSED = "KLINE_CLOSED"
    KLINE_UPDATED = "KLINE_UPDATED"
    DATA_GAP_DETECTED = "DATA_GAP_DETECTED"

    # ─── 策略 ───
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    SIGNAL_EXPIRED = "SIGNAL_EXPIRED"
    TREND_CHANGED = "TREND_CHANGED"

    # ─── 风控 ───
    RISK_CHECK_PASSED = "RISK_CHECK_PASSED"
    RISK_CHECK_REJECTED = "RISK_CHECK_REJECTED"
    RISK_COOLING_TRIGGERED = "RISK_COOLING_TRIGGERED"
    RISK_KILL_SWITCH = "RISK_KILL_SWITCH"

    # ─── 执行 ───
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_PARTIALLY_FILLED = "ORDER_PARTIALLY_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_EXPIRED = "ORDER_EXPIRED"

    # ─── 仓位 ───
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_CLOSED = "POSITION_CLOSED"
    POSITION_UPDATED = "POSITION_UPDATED"
    POSITION_LIQUIDATED = "POSITION_LIQUIDATED"

    # ─── 权益 ───
    EQUITY_UPDATED = "EQUITY_UPDATED"
    EQUITY_ALERT = "EQUITY_ALERT"

    # ─── 绩效 ───
    TRADE_CLOSED = "TRADE_CLOSED"
    PNL_REALIZED = "PNL_REALIZED"
    PNL_UNREALIZED = "PNL_UNREALIZED"

    # ─── 快照 ───
    SNAPSHOT_SAVED = "SNAPSHOT_SAVED"
    SNAPSHOT_RESTORED = "SNAPSHOT_RESTORED"

    # ─── 信号追踪 ───
    SIGNAL_ACKNOWLEDGED = "SIGNAL_ACKNOWLEDGED"
    SIGNAL_TIMEOUT = "SIGNAL_TIMEOUT"

    # ─── 告警 ───
    ALERT_SENT = "ALERT_SENT"
    ALERT_ACKNOWLEDGED = "ALERT_ACKNOWLEDGED"

    # ─── 通配符 ───
    ALL = "*"  # 监听所有事件


# ═══════════════════════════════════════════════════════════════
# 事件类
# ═══════════════════════════════════════════════════════════════

@dataclass
class Event:
    """
    事件对象 — 系统中所有通信的载体。
    """
    event_type: EventType
    source: str                               # 发布者模块名
    payload: Any = None                       # 事件载荷
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    priority: EventPriority = EventPriority.NORMAL
    event_id: str = field(default="")
    trace_id: str = field(default="")
    parent_event_id: str = field(default="")
    ttl: int = field(default=3)               # 最大转发次数（防止事件风暴）
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.event_id:
            self.event_id = f"{self.event_type.value}_{self.timestamp}_{id(self)}"

    def is_expired(self) -> bool:
        """检查事件是否超过 TTL"""
        return self.ttl <= 0

    def decrement_ttl(self) -> 'Event':
        """递减 TTL，返回新事件（不可变）"""
        new_event = Event(
            event_type=self.event_type,
            source=self.source,
            payload=self.payload,
            timestamp=self.timestamp,
            priority=self.priority,
            event_id=self.event_id,
            trace_id=self.trace_id,
            parent_event_id=self.event_id,  # 新事件以当前为父
            ttl=self.ttl - 1,
            metadata=dict(self.metadata),
        )
        return new_event

    def to_dict(self) -> dict:
        return {
            'event_id': self.event_id,
            'event_type': self.event_type.value,
            'source': self.source,
            'timestamp': self.timestamp,
            'priority': self.priority.value,
            'trace_id': self.trace_id,
            'parent_event_id': self.parent_event_id,
            'ttl': self.ttl,
            'payload_type': type(self.payload).__name__ if self.payload else None,
        }

    def __repr__(self) -> str:
        return (f"Event(type={self.event_type.value}, "
                f"src={self.source}, id={self.event_id[:16]}...)")


# ═══════════════════════════════════════════════════════════════
# 事件过滤器
# ═══════════════════════════════════════════════════════════════

@runtime_checkable
class EventFilter(Protocol):
    """事件过滤器协议"""
    def filter(self, event: Event) -> bool:
        """返回 True 表示事件通过，False 表示拦截"""
        ...


class SourceFilter:
    """按源模块过滤"""
    def __init__(self, allowed_sources: Set[str]):
        self.allowed_sources = allowed_sources

    def filter(self, event: Event) -> bool:
        return event.source in self.allowed_sources


class PayloadFilter:
    """按载荷条件过滤"""
    def __init__(self, predicate: Callable[[Any], bool]):
        self.predicate = predicate

    def filter(self, event: Event) -> bool:
        return self.predicate(event.payload)


# ═══════════════════════════════════════════════════════════════
# 事件中间件
# ═══════════════════════════════════════════════════════════════

class EventMiddleware:
    """
    事件中间件基类。
    子类可重写 before_publish / after_deliver 来拦截或修改事件。
    """

    def before_publish(self, event: Event) -> Optional[Event]:
        """
        事件发布前调用。
        返回 None 表示拦截事件（不发布）。
        返回修改后的 Event 或原 Event。
        """
        return event

    def after_deliver(self, event: Event, handler: Callable, 
                      success: bool, error: Optional[Exception]):
        """事件投递后调用（用于监控/日志）"""
        pass


# ═══════════════════════════════════════════════════════════════
# 订阅者包装
# ═══════════════════════════════════════════════════════════════

@dataclass
class Subscriber:
    """订阅者信息"""
    handler: Callable[[Event], Any]
    priority: EventPriority = EventPriority.NORMAL
    filters: List[EventFilter] = field(default_factory=list)
    is_async: bool = False
    max_retries: int = 0
    timeout_ms: int = 5000
    subscriber_id: str = field(default="")
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))

    def __post_init__(self):
        if not self.subscriber_id:
            self.subscriber_id = f"sub_{id(self.handler)}_{self.created_at}"

    def should_handle(self, event: Event) -> bool:
        """检查是否应该处理该事件"""
        # 优先级过滤
        if event.priority.value > self.priority.value:
            return False
        # 过滤器链
        for f in self.filters:
            try:
                if not f.filter(event):
                    return False
            except Exception:
                return False
        return True

    def __hash__(self) -> int:
        return hash(self.subscriber_id)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Subscriber):
            return False
        return self.subscriber_id == other.subscriber_id


# ═══════════════════════════════════════════════════════════════
# 死信队列
# ═══════════════════════════════════════════════════════════════

class DeadLetterQueue:
    """
    死信队列 — 存储处理失败或超时的事件。
    """

    def __init__(self, max_size: int = 1000):
        self._max_size = max_size
        self._queue: deque = deque(maxlen=max_size)
        self._lock = threading.Lock()

    def put(self, event: Event, error: Exception, handler_id: str = ""):
        """放入死信"""
        with self._lock:
            entry = {
                'event': event,
                'error': str(error),
                'error_type': type(error).__name__,
                'handler_id': handler_id,
                'failed_at': int(time.time() * 1000),
                'traceback': traceback.format_exc(),
            }
            self._queue.append(entry)

    def get_all(self) -> List[dict]:
        """获取所有死信"""
        with self._lock:
            return list(self._queue)

    def pop_all(self) -> List[dict]:
        """取出所有死信（清空队列）"""
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
            return items

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def clear(self):
        with self._lock:
            self._queue.clear()


# ═══════════════════════════════════════════════════════════════
# 事件总线主类
# ═══════════════════════════════════════════════════════════════

class EventBus:
    """
    事件总线 — 系统通信中枢。

    用法:
      bus = EventBus()

      # 订阅
      bus.subscribe(EventType.SIGNAL_GENERATED, on_signal)
      bus.subscribe(EventType.ALL, on_any_event)  # 通配符

      # 发布（同步）
      bus.publish(Event(EventType.TICK_RECEIVED, "kline", payload=tick))

      # 发布（异步）
      bus.publish_async(Event(EventType.TICK_RECEIVED, "kline", payload=tick))

      # 取消订阅
      bus.unsubscribe(EventType.SIGNAL_GENERATED, on_signal)

      # 关闭
      bus.shutdown()
    """

    def __init__(
        self,
        name: str = "default",
        async_mode: bool = False,
        max_queue_size: int = 10000,
        dead_letter_enabled: bool = True,
    ):
        self._name = name
        self._async_mode = async_mode
        self._running = False

        # 订阅表: event_type → {priority → [Subscriber]}
        self._subscribers: Dict[EventType, Dict[int, List[Subscriber]]] = defaultdict(
            lambda: defaultdict(list)
        )
        # 通配符订阅
        self._wildcard_subscribers: Dict[int, List[Subscriber]] = defaultdict(list)

        self._lock = threading.RLock()

        # 异步模式
        self._async_queue: Optional[queue.PriorityQueue] = None
        self._worker_threads: List[threading.Thread] = []
        self._worker_count = 2

        # 中间件链
        self._middlewares: List[EventMiddleware] = []

        # 死信队列
        self._dlq = DeadLetterQueue() if dead_letter_enabled else None

        # 统计
        self._stats = {
            'published_total': 0,
            'published_sync': 0,
            'published_async': 0,
            'delivered_total': 0,
            'delivery_failures': 0,
            'filtered_total': 0,
            'dead_letters': 0,
            'start_time': None,
        }

        # 启动
        self._running = True
        self._stats['start_time'] = int(time.time() * 1000)

        if async_mode:
            self._start_async_workers(max_queue_size)

    # ─── 订阅管理 ───────────────────────────────────────────────

    def subscribe(
        self,
        event_type: Union[EventType, str],
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filters: Optional[List[EventFilter]] = None,
        is_async: bool = False,
        max_retries: int = 0,
        timeout_ms: int = 5000,
    ) -> str:
        """
        订阅事件。
        返回 subscriber_id，用于取消订阅。
        """
        if isinstance(event_type, str):
            if event_type == "*":
                event_type = EventType.ALL
            else:
                # 尝试从字符串解析
                try:
                    event_type = EventType(event_type)
                except ValueError:
                    raise ValueError(f"未知事件类型: {event_type}")

        subscriber = Subscriber(
            handler=handler,
            priority=priority,
            filters=filters or [],
            is_async=is_async,
            max_retries=max_retries,
            timeout_ms=timeout_ms,
        )

        with self._lock:
            if event_type == EventType.ALL:
                self._wildcard_subscribers[priority.value].append(subscriber)
            else:
                self._subscribers[event_type][priority.value].append(subscriber)

        return subscriber.subscriber_id

    def unsubscribe(
        self,
        event_type: Union[EventType, str],
        handler: Callable[[Event], Any],
    ) -> bool:
        """取消订阅"""
        if isinstance(event_type, str):
            if event_type == "*":
                event_type = EventType.ALL
            else:
                try:
                    event_type = EventType(event_type)
                except ValueError:
                    return False

        with self._lock:
            removed = False
            if event_type == EventType.ALL:
                for priority_subs in self._wildcard_subscribers.values():
                    for sub in list(priority_subs):
                        if sub.handler == handler:
                            priority_subs.remove(sub)
                            removed = True
            else:
                if event_type in self._subscribers:
                    for priority_subs in self._subscribers[event_type].values():
                        for sub in list(priority_subs):
                            if sub.handler == handler:
                                priority_subs.remove(sub)
                                removed = True
            return removed

    def unsubscribe_by_id(self, subscriber_id: str) -> bool:
        """通过 subscriber_id 取消订阅"""
        with self._lock:
            for priority_subs in self._wildcard_subscribers.values():
                for sub in list(priority_subs):
                    if sub.subscriber_id == subscriber_id:
                        priority_subs.remove(sub)
                        return True
            for type_subs in self._subscribers.values():
                for priority_subs in type_subs.values():
                    for sub in list(priority_subs):
                        if sub.subscriber_id == subscriber_id:
                            priority_subs.remove(sub)
                            return True
        return False

    def get_subscriber_count(self, event_type: Optional[EventType] = None) -> int:
        """获取订阅者数量"""
        with self._lock:
            if event_type:
                if event_type == EventType.ALL:
                    return sum(len(s) for s in self._wildcard_subscribers.values())
                return sum(
                    len(s) for s in self._subscribers.get(event_type, {}).values()
                )
            total = sum(len(s) for s in self._wildcard_subscribers.values())
            for type_subs in self._subscribers.values():
                total += sum(len(s) for s in type_subs.values())
            return total

    # ─── 中间件 ─────────────────────────────────────────────────

    def add_middleware(self, middleware: EventMiddleware):
        """添加中间件"""
        self._middlewares.append(middleware)

    def remove_middleware(self, middleware: EventMiddleware):
        """移除中间件"""
        if middleware in self._middlewares:
            self._middlewares.remove(middleware)

    # ─── 发布 ───────────────────────────────────────────────────

    def publish(self, event: Event) -> Result[None]:
        """
        同步发布事件。
        立即投递给所有匹配的订阅者。
        """
        if not self._running:
            return Result.err(Error(
                code="EB_001",
                message="EventBus 已关闭",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
                source="event_bus",
            ))

        # 中间件: before_publish
        for mw in self._middlewares:
            try:
                event = mw.before_publish(event)
                if event is None:
                    return Result.ok(None)  # 被中间件拦截
            except Exception:
                pass

        self._stats['published_total'] += 1
        self._stats['published_sync'] += 1

        # 检查 TTL
        if event.is_expired():
            return Result.err(Error(
                code="EB_TTL",
                message=f"事件 TTL 已耗尽: {event.event_id}",
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.INTERNAL,
                source="event_bus",
            ))

        # 收集匹配的订阅者
        matched = self._match_subscribers(event)

        if not matched:
            return Result.ok(None)

        # 按优先级排序投递
        matched.sort(key=lambda s: s.priority.value)

        for subscriber in matched:
            if not subscriber.should_handle(event):
                self._stats['filtered_total'] += 1
                continue

            self._deliver(event, subscriber)

        return Result.ok(None)

    def publish_async(self, event: Event) -> Result[None]:
        """
        异步发布事件。
        放入队列由工作线程处理。
        """
        if not self._running:
            return Result.err(Error(
                code="EB_001",
                message="EventBus 已关闭",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
                source="event_bus",
            ))

        if not self._async_mode or not self._async_queue:
            # 回退到同步发布
            return self.publish(event)

        # 中间件: before_publish
        for mw in self._middlewares:
            try:
                event = mw.before_publish(event)
                if event is None:
                    return Result.ok(None)
            except Exception:
                pass

        self._stats['published_total'] += 1
        self._stats['published_async'] += 1

        # 放入优先级队列
        priority = event.priority.value
        self._async_queue.put((priority, time.time(), event))

        return Result.ok(None)

    def _match_subscribers(self, event: Event) -> List[Subscriber]:
        """匹配事件对应的订阅者"""
        matched = []

        with self._lock:
            # 精确类型匹配
            if event.event_type in self._subscribers:
                for priority_subs in self._subscribers[event.event_type].values():
                    matched.extend(priority_subs)

            # 通配符匹配
            for priority_subs in self._wildcard_subscribers.values():
                matched.extend(priority_subs)

        return matched

    def _deliver(self, event: Event, subscriber: Subscriber):
        """投递事件给单个订阅者"""
        try:
            if subscriber.is_async:
                # 异步投递（提交到线程池）
                threading.Thread(
                    target=self._invoke_handler,
                    args=(event, subscriber),
                    daemon=True,
                ).start()
            else:
                # 同步投递
                self._invoke_handler(event, subscriber)

        except Exception as e:
            self._stats['delivery_failures'] += 1
            if self._dlq:
                self._dlq.put(event, e, subscriber.subscriber_id)

    def _invoke_handler(self, event: Event, subscriber: Subscriber):
        """调用 handler"""
        success = False
        error = None

        try:
            result = subscriber.handler(event)
            self._stats['delivered_total'] += 1
            success = True
            return result
        except Exception as e:
            error = e
            self._stats['delivery_failures'] += 1

            # 重试逻辑
            if subscriber.max_retries > 0:
                for attempt in range(subscriber.max_retries):
                    try:
                        time.sleep(0.1 * (attempt + 1))
                        subscriber.handler(event)
                        self._stats['delivered_total'] += 1
                        success = True
                        error = None
                        break
                    except Exception as retry_e:
                        error = retry_e

            # 死信
            if not success and self._dlq:
                self._dlq.put(event, error, subscriber.subscriber_id)
                self._stats['dead_letters'] += 1

        finally:
            # 中间件: after_deliver
            for mw in self._middlewares:
                try:
                    mw.after_deliver(event, subscriber.handler, success, error)
                except Exception:
                    pass

    # ─── 异步工作线程 ───────────────────────────────────────────

    def _start_async_workers(self, max_queue_size: int):
        """启动异步工作线程"""
        self._async_queue = queue.PriorityQueue(maxsize=max_queue_size)

        for i in range(self._worker_count):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"EventBus-Worker-{i}",
                daemon=True,
            )
            t.start()
            self._worker_threads.append(t)

    def _worker_loop(self):
        """工作线程主循环"""
        while self._running:
            try:
                if self._async_queue is None:
                    break

                # 阻塞获取（1秒超时，检查 shutdown）
                try:
                    priority, enqueued_at, event = self._async_queue.get(
                        timeout=1.0
                    )
                except queue.Empty:
                    continue

                # 同步投递
                self.publish(event)

            except Exception:
                pass

    # ─── 管理操作 ───────────────────────────────────────────────

    def clear_all_subscribers(self):
        """清空所有订阅"""
        with self._lock:
            self._subscribers.clear()
            self._wildcard_subscribers.clear()

    def get_dead_letters(self) -> List[dict]:
        """获取死信队列"""
        if self._dlq:
            return self._dlq.get_all()
        return []

    def clear_dead_letters(self):
        """清空死信队列"""
        if self._dlq:
            self._dlq.clear()

    def get_stats(self) -> dict:
        """获取统计信息"""
        return dict(self._stats)

    def reset_stats(self):
        """重置统计"""
        for key in self._stats:
            if isinstance(self._stats[key], int):
                self._stats[key] = 0
        self._stats['start_time'] = int(time.time() * 1000)

    def shutdown(self, timeout: float = 5.0):
        """
        优雅关闭事件总线。
        等待异步队列处理完毕。
        """
        self._running = False

        # 等待工作线程完成
        if self._worker_threads:
            deadline = time.time() + timeout
            for t in self._worker_threads:
                remaining = deadline - time.time()
                if remaining > 0:
                    t.join(timeout=remaining)

        # 清空队列
        if self._async_queue:
            while not self._async_queue.empty():
                try:
                    self._async_queue.get_nowait()
                except queue.Empty:
                    break

        self._worker_threads.clear()

    def __repr__(self) -> str:
        return (f"EventBus(name={self._name!r}, async={self._async_mode}, "
                f"subscribers={self.get_subscriber_count()}, "
                f"published={self._stats['published_total']})")


# ═══════════════════════════════════════════════════════════════
# 全局事件总线实例
# ═══════════════════════════════════════════════════════════════

_global_event_bus = EventBus(name="global", async_mode=True)


def get_global_event_bus() -> EventBus:
    """获取全局事件总线"""
    return _global_event_bus


def publish(event_type: EventType, source: str, payload: Any = None, **kwargs) -> Result[None]:
    """便捷函数：向全局事件总线发布事件"""
    event = Event(event_type=event_type, source=source, payload=payload, **kwargs)
    return _global_event_bus.publish(event)


def subscribe(event_type: Union[EventType, str], handler: Callable[[Event], Any], **kwargs) -> str:
    """便捷函数：向全局事件总线订阅事件"""
    return _global_event_bus.subscribe(event_type, handler, **kwargs)
