"""
src/core/trace_context.py
MA26-ATR 量化交易系统 — 核心原语：追踪上下文

设计理念:
  借鉴 W3C Trace Context (RFC 9464) 和 OpenTelemetry 模型。
  为每次交易决策、订单执行提供端到端的调用链追踪。

  核心概念:
    Trace   — 一次完整的交易决策链路（从信号生成到成交确认）
    Span    — 链路中的一个环节（如策略计算、风控检查、订单提交）
    Context — 跨模块传播的不可变追踪上下文

  传播方式:
    - Event.trace_id / Event.parent_event_id 携带
    - 模块间通过 TraceContext 对象传递
    - 线程本地存储维护当前活跃 Span

  格式:
    TraceId  — 32位十六进制 (128-bit UUID)
    SpanId   — 16位十六进制 (64-bit random)
    ParentId — 16位十六进制 (父 SpanId)
"""

import uuid
import threading
import time
import random
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Generator
from contextlib import contextmanager


# ═══════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════

def _generate_trace_id() -> str:
    """生成 32 位十六进制 TraceId (128-bit)"""
    return uuid.uuid4().hex


def _generate_span_id() -> str:
    """生成 16 位十六进制 SpanId (64-bit)"""
    return uuid.uuid4().hex[:16]


# ═══════════════════════════════════════════════════════════════
# 追踪标志
# ═══════════════════════════════════════════════════════════════

class TraceFlags:
    """追踪标志位"""
    NONE = 0x00
    SAMPLED = 0x01      # 是否采样（记录详细数据）

    @classmethod
    def is_sampled(cls, flags: int) -> bool:
        return (flags & cls.SAMPLED) == cls.SAMPLED

    @classmethod
    def set_sampled(cls, flags: int) -> int:
        return flags | cls.SAMPLED


# ═══════════════════════════════════════════════════════════════
# 追踪上下文
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class TraceContext:
    """
    不可变追踪上下文。

    跨模块、跨线程传播追踪信息。
    每次创建子 Span 时生成新的 TraceContext（span_id 更新，trace_id 不变）。
    """
    trace_id: str                       # 全局唯一追踪 ID
    span_id: str                        # 当前 Span ID
    parent_span_id: Optional[str]       # 父 Span ID
    flags: int = TraceFlags.SAMPLED     # 追踪标志
    state: Dict[str, str] = field(default_factory=dict)  # 厂商扩展状态

    @classmethod
    def root(cls, trace_id: Optional[str] = None) -> 'TraceContext':
        """创建根上下文（无父 Span）"""
        return cls(
            trace_id=trace_id or _generate_trace_id(),
            span_id=_generate_span_id(),
            parent_span_id=None,
        )

    def child(self, span_id: Optional[str] = None) -> 'TraceContext':
        """创建子上下文（新 Span，继承 trace_id）"""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=span_id or _generate_span_id(),
            parent_span_id=self.span_id,
            flags=self.flags,
            state=dict(self.state),
        )

    def is_sampled(self) -> bool:
        """是否采样"""
        return TraceFlags.is_sampled(self.flags)

    def with_state(self, key: str, value: str) -> 'TraceContext':
        """返回带新状态的上下文（不可变）"""
        new_state = dict(self.state)
        new_state[key] = value
        return TraceContext(
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            flags=self.flags,
            state=new_state,
        )

    def to_dict(self) -> dict:
        return {
            'trace_id': self.trace_id,
            'span_id': self.span_id,
            'parent_span_id': self.parent_span_id,
            'flags': self.flags,
            'sampled': self.is_sampled(),
            'state': self.state,
        }

    def to_w3c_header(self) -> str:
        """
        序列化为 W3C traceparent 格式:
        version-trace_id-parent_id-flags
        """
        version = "00"
        flags_hex = f"{self.flags:02x}"
        return f"{version}-{self.trace_id}-{self.span_id}-{flags_hex}"

    @classmethod
    def from_w3c_header(cls, header: str) -> Optional['TraceContext']:
        """从 W3C traceparent 格式解析"""
        try:
            parts = header.split('-')
            if len(parts) != 4:
                return None
            version, trace_id, span_id, flags_hex = parts
            return cls(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=None,
                flags=int(flags_hex, 16),
            )
        except Exception:
            return None

    def __repr__(self) -> str:
        return (f"TraceContext(trace={self.trace_id[:8]}..., "
                f"span={self.span_id}, parent={self.parent_span_id})")


# ═══════════════════════════════════════════════════════════════
# Span（追踪跨度）
# ═══════════════════════════════════════════════════════════════

class SpanStatus(Enum):
    """Span 状态"""
    OK = "OK"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


@dataclass
class Span:
    """
    追踪跨度 — 表示一个工作单元。

    生命周期:
      start() → 执行操作 → end() / record_exception()
    """
    name: str
    context: TraceContext
    parent_context: Optional[TraceContext] = None
    start_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    end_time_ms: Optional[int] = None
    status: SpanStatus = SpanStatus.OK
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    exception: Optional[Exception] = None
    source_module: str = ""

    def end(self, status: Optional[SpanStatus] = None):
        """结束 Span"""
        self.end_time_ms = int(time.time() * 1000)
        if status:
            self.status = status

    def duration_ms(self) -> Optional[int]:
        """获取持续时间"""
        if self.end_time_ms:
            return self.end_time_ms - self.start_time_ms
        return None

    def record_exception(self, exc: Exception):
        """记录异常"""
        self.exception = exc
        self.status = SpanStatus.ERROR
        self.events.append({
            'name': 'exception',
            'timestamp': int(time.time() * 1000),
            'exception_type': type(exc).__name__,
            'exception_msg': str(exc),
        })

    def set_attribute(self, key: str, value: Any):
        """设置属性"""
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """添加 Span 内事件"""
        self.events.append({
            'name': name,
            'timestamp': int(time.time() * 1000),
            'attributes': attributes or {},
        })

    def is_finished(self) -> bool:
        return self.end_time_ms is not None

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'trace_id': self.context.trace_id,
            'span_id': self.context.span_id,
            'parent_span_id': self.context.parent_span_id,
            'source_module': self.source_module,
            'start_time_ms': self.start_time_ms,
            'end_time_ms': self.end_time_ms,
            'duration_ms': self.duration_ms(),
            'status': self.status.value,
            'attributes': self.attributes,
            'events': self.events,
            'has_exception': self.exception is not None,
        }


# ═══════════════════════════════════════════════════════════════
# 当前追踪上下文（线程本地）
# ═══════════════════════════════════════════════════════════════

class CurrentTraceContext:
    """
    线程本地当前追踪上下文管理。

    使用 threading.local() 维护每个线程的独立追踪栈。
    """

    _local = threading.local()

    @classmethod
    def get(cls) -> Optional[TraceContext]:
        """获取当前线程的活跃追踪上下文"""
        stack = getattr(cls._local, 'stack', None)
        if stack and len(stack) > 0:
            return stack[-1]
        return None

    @classmethod
    def get_trace_id(cls) -> Optional[str]:
        """获取当前 trace_id"""
        ctx = cls.get()
        return ctx.trace_id if ctx else None

    @classmethod
    def push(cls, context: TraceContext):
        """压入上下文栈"""
        if not hasattr(cls._local, 'stack'):
            cls._local.stack = []
        cls._local.stack.append(context)

    @classmethod
    def pop(cls) -> Optional[TraceContext]:
        """弹出上下文栈"""
        stack = getattr(cls._local, 'stack', None)
        if stack and len(stack) > 0:
            return stack.pop()
        return None

    @classmethod
    def clear(cls):
        """清空上下文栈"""
        if hasattr(cls._local, 'stack'):
            cls._local.stack.clear()

    @classmethod
    @contextmanager
    def scope(cls, context: TraceContext) -> Generator[None, None, None]:
        """
        上下文管理器：在作用域内设置当前追踪上下文。

        用法:
          with CurrentTraceContext.scope(ctx):
              do_work()  # 内部可通过 CurrentTraceContext.get() 获取 ctx
        """
        cls.push(context)
        try:
            yield
        finally:
            cls.pop()


# ═══════════════════════════════════════════════════════════════
# Span 追踪器（创建和管理 Span）
# ═══════════════════════════════════════════════════════════════

class Tracer:
    """
    Span 追踪器 — 创建和管理 Span 的生命周期。

    用法:
      tracer = Tracer("m01_strategy")

      # 方式1: 上下文管理器
      with tracer.span("calculate_signal", trace_id="abc123") as span:
          signal = compute()
          span.set_attribute("signal_type", signal.type)

      # 方式2: 手动管理
      span = tracer.start_span("risk_check")
      try:
          risk_guard.check()
          span.end()
      except Exception as e:
          span.record_exception(e)
          span.end(SpanStatus.ERROR)

      # 方式3: 从父上下文创建子 Span
      child_ctx = tracer.child_context(parent_ctx)
    """

    def __init__(self, module_name: str, sampler_rate: float = 1.0):
        self._module_name = module_name
        self._sampler_rate = sampler_rate  # 采样率 0.0-1.0
        self._spans: List[Span] = []       # 已完成的 Span 记录
        self._lock = threading.Lock()

    @property
    def module_name(self) -> str:
        return self._module_name

    def should_sample(self) -> bool:
        """是否采样（基于采样率）"""
        return random.random() < self._sampler_rate

    def start_span(
        self,
        name: str,
        parent_context: Optional[TraceContext] = None,
        trace_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Span:
        """
        创建并开始一个 Span。

        如果提供了 parent_context，则创建子上下文。
        否则从 CurrentTraceContext 获取当前上下文。
        """
        # 确定父上下文
        if parent_context is None:
            parent_context = CurrentTraceContext.get()

        # 创建 Span 上下文
        if parent_context:
            ctx = parent_context.child()
        elif trace_id:
            ctx = TraceContext.root(trace_id=trace_id)
        else:
            ctx = TraceContext.root()

        # 采样决策
        if not ctx.is_sampled():
            if not self.should_sample():
                ctx = TraceContext(
                    trace_id=ctx.trace_id,
                    span_id=ctx.span_id,
                    parent_span_id=ctx.parent_span_id,
                    flags=TraceFlags.NONE,
                )

        span = Span(
            name=name,
            context=ctx,
            parent_context=parent_context,
            source_module=self._module_name,
        )

        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, v)

        return span

    def end_span(self, span: Span, status: Optional[SpanStatus] = None):
        """结束 Span 并记录"""
        if not span.is_finished():
            span.end(status)

        with self._lock:
            self._spans.append(span)

    @contextmanager
    def span(
        self,
        name: str,
        parent_context: Optional[TraceContext] = None,
        trace_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Generator[Span, None, None]:
        """
        上下文管理器方式创建 Span。

        用法:
          with tracer.span("process_order") as span:
              ...
        """
        span = self.start_span(
            name=name,
            parent_context=parent_context,
            trace_id=trace_id,
            attributes=attributes,
        )

        # 设置到当前上下文
        token = CurrentTraceContext.push(span.context)

        try:
            yield span
        except Exception as e:
            span.record_exception(e)
            self.end_span(span, SpanStatus.ERROR)
            raise
        else:
            self.end_span(span, SpanStatus.OK)
        finally:
            CurrentTraceContext.pop()

    def child_context(
        self,
        parent_context: Optional[TraceContext] = None,
    ) -> TraceContext:
        """创建子追踪上下文"""
        if parent_context is None:
            parent_context = CurrentTraceContext.get()

        if parent_context:
            return parent_context.child()
        return TraceContext.root()

    def get_completed_spans(self) -> List[Span]:
        """获取所有已完成的 Span"""
        with self._lock:
            return list(self._spans)

    def get_spans_by_trace(self, trace_id: str) -> List[Span]:
        """按 trace_id 获取 Span 列表"""
        with self._lock:
            return [s for s in self._spans if s.context.trace_id == trace_id]

    def clear_spans(self):
        """清空 Span 记录"""
        with self._lock:
            self._spans.clear()

    def to_dict(self) -> dict:
        with self._lock:
            return {
                'module': self._module_name,
                'sampler_rate': self._sampler_rate,
                'completed_spans': len(self._spans),
                'spans': [s.to_dict() for s in self._spans[-50:]],  # 最近50个
            }


# ═══════════════════════════════════════════════════════════════
# 追踪上下文传播器（与 Event 集成）
# ═══════════════════════════════════════════════════════════════

class TracePropagator:
    """
    追踪上下文传播器 — 在 Event 和模块间注入/提取追踪上下文。

    与 EventBus 集成:
      - 发布 Event 时注入当前 TraceContext
      - 订阅者处理 Event 时提取 TraceContext
    """

    @staticmethod
    def inject_to_event(event: 'Event', context: Optional[TraceContext] = None):
        """
        将追踪上下文注入到 Event 中。
        """
        if context is None:
            context = CurrentTraceContext.get()

        if context:
            event.trace_id = context.trace_id
            event.metadata['span_id'] = context.span_id
            event.metadata['parent_span_id'] = context.parent_span_id

    @staticmethod
    def extract_from_event(event: 'Event') -> Optional[TraceContext]:
        """
        从 Event 中提取追踪上下文。
        """
        if not event.trace_id:
            return None

        span_id = event.metadata.get('span_id', _generate_span_id())
        parent_span_id = event.metadata.get('parent_span_id')

        return TraceContext(
            trace_id=event.trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
        )

    @staticmethod
    def create_child_from_event(
        event: 'Event',
        module_name: str = "",
    ) -> Optional[TraceContext]:
        """
        从 Event 创建子追踪上下文。
        """
        parent_ctx = TracePropagator.extract_from_event(event)
        if not parent_ctx:
            return None

        child_ctx = parent_ctx.child()
        if module_name:
            child_ctx = child_ctx.with_state('source_module', module_name)
        return child_ctx


# ═══════════════════════════════════════════════════════════════
# 全局 Tracer 注册表
# ═══════════════════════════════════════════════════════════════

class TracerRegistry:
    """
    全局 Tracer 注册表 — 每个模块一个 Tracer。
    """

    __slots__ = ('_tracers', '_lock')

    def __init__(self):
        self._tracers: Dict[str, Tracer] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        module_name: str,
        sampler_rate: float = 1.0,
    ) -> Tracer:
        """获取或创建模块的 Tracer"""
        with self._lock:
            if module_name not in self._tracers:
                self._tracers[module_name] = Tracer(module_name, sampler_rate)
            return self._tracers[module_name]

    def get(self, module_name: str) -> Optional[Tracer]:
        return self._tracers.get(module_name)

    def list_all(self) -> List[str]:
        return list(self._tracers.keys())

    def get_all_spans_by_trace(self, trace_id: str) -> Dict[str, List[Span]]:
        """获取所有模块中指定 trace_id 的 Span"""
        result = {}
        with self._lock:
            for name, tracer in self._tracers.items():
                spans = tracer.get_spans_by_trace(trace_id)
                if spans:
                    result[name] = spans
        return result

    def clear_all(self):
        """清空所有 Tracer 的 Span"""
        with self._lock:
            for tracer in self._tracers.values():
                tracer.clear_spans()


# ═══════════════════════════════════════════════════════════════
# 全局实例和便捷函数
# ═══════════════════════════════════════════════════════════════

_global_tracer_registry = TracerRegistry()


def get_tracer(module_name: str, sampler_rate: float = 1.0) -> Tracer:
    """获取模块的 Tracer"""
    return _global_tracer_registry.get_or_create(module_name)


def get_current_trace_id() -> Optional[str]:
    """获取当前线程的 trace_id"""
    return CurrentTraceContext.get_trace_id()


def get_current_trace_context() -> Optional[TraceContext]:
    """获取当前线程的追踪上下文"""
    return CurrentTraceContext.get()


def start_trace(
    module_name: str,
    span_name: str,
    trace_id: Optional[str] = None,
) -> tuple[Tracer, Span]:
    """便捷函数：开始一个新的追踪"""
    tracer = get_tracer(module_name)
    span = tracer.start_span(span_name, trace_id=trace_id)
    return tracer, span


def trace_span(module_name: str, span_name: str, **kwargs):
    """装饰器：为函数添加追踪 Span"""
    def decorator(fn):
        def wrapper(*args, **kw):
            tracer = get_tracer(module_name)
            with tracer.span(span_name, **kwargs):
                return fn(*args, **kw)
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper
    return decorator
