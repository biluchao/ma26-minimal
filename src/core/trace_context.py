"""core/trace_context.py — Trace context with auto-propagating ContextVar"""

import uuid
import time
from dataclasses import dataclass, field
from typing import Optional, Any
from contextvars import ContextVar


@dataclass
class TraceSpan:
    span_id: str
    parent_span_id: str
    trace_id: str
    operation: str
    module: str
    start_time: int
    end_time: int = 0
    tags: dict = field(default_factory=dict)
    error: Optional[str] = None

    def duration_ms(self) -> int:
        return self.end_time - self.start_time


@dataclass
class TraceContext:
    trace_id: str = ""
    parent_span_id: str = ""
    root_span_id: str = ""
    bar_index: int = 0
    source: str = ""
    tags: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.trace_id:
            self.trace_id = str(uuid.uuid4())[:16]

    def child(self, operation: str, module: str):
        new = TraceContext(
            trace_id=self.trace_id,
            parent_span_id=str(uuid.uuid4())[:12],
            root_span_id=self.root_span_id or self.trace_id,
            bar_index=self.bar_index, source=self.source,
            tags=dict(self.tags),
        )
        return new, new.parent_span_id

    def with_tag(self, k: str, v: Any) -> 'TraceContext':
        new = TraceContext(trace_id=self.trace_id, parent_span_id=self.parent_span_id,
                          root_span_id=self.root_span_id, bar_index=self.bar_index,
                          source=self.source, tags=dict(self.tags))
        new.tags[k] = v
        return new

    def with_bar(self, bar_index: int) -> 'TraceContext':
        new = TraceContext(trace_id=self.trace_id, parent_span_id=self.parent_span_id,
                          root_span_id=self.root_span_id, source=self.source,
                          tags=dict(self.tags))
        new.bar_index = bar_index
        return new


_current: ContextVar[Optional[TraceContext]] = ContextVar('trace', default=None)


class TraceManager:
    def __init__(self, tracer=None):
        self.tracer = tracer
        self._spans: dict = {}
        self._stats = {'total_spans': 0, 'error_spans': 0}

    def new_trace(self, bar_index: int = 0, source: str = "", **tags) -> TraceContext:
        ctx = TraceContext(bar_index=bar_index, source=source, tags=dict(tags))
        _current.set(ctx)
        return ctx

    def current(self) -> Optional[TraceContext]:
        return _current.get()

    def set(self, ctx: TraceContext):
        _current.set(ctx)

    def span(self, operation: str, module: str, parent: Optional[TraceContext] = None):
        return TraceSpanContext(self, operation, module, parent)

    def _start(self, operation, module, parent):
        ctx = parent or self.current()
        if ctx is None:
            ctx = self.new_trace()
        new_ctx, span_id = ctx.child(operation, module)
        _current.set(new_ctx)
        span = TraceSpan(span_id=span_id, parent_span_id=ctx.parent_span_id,
                         trace_id=ctx.trace_id, operation=operation, module=module,
                         start_time=int(time.time() * 1000))
        return span, new_ctx

    def _end(self, span, error):
        span.end_time = int(time.time() * 1000)
        span.error = error
        self._spans[span.span_id] = span
        self._stats['total_spans'] += 1
        if error:
            self._stats['error_spans'] += 1
        if self.tracer:
            self.tracer.log('span', trace_id=span.trace_id, span_id=span.span_id,
                           operation=span.operation, module=span.module,
                           duration_ms=span.duration_ms(), error=error or '')

    def get_tree(self, trace_id: str) -> dict:
        spans = [s for s in self._spans.values() if s.trace_id == trace_id]
        spans.sort(key=lambda s: s.start_time)
        tree = {}
        for s in spans:
            tree[s.span_id] = {'op': s.operation, 'mod': s.module,
                              'dur': s.duration_ms(), 'err': s.error, 'children': []}
        root = None
        for s in spans:
            if s.parent_span_id in tree:
                tree[s.parent_span_id]['children'].append(tree[s.span_id])
            else:
                root = tree[s.span_id]
        return root or {}

    def get_stats(self) -> dict:
        total = self._stats['total_spans']
        return {'total_spans': total, 'error_spans': self._stats['error_spans'],
                'unique_traces': len(set(s.trace_id for s in self._spans.values())),
                'avg_duration_ms': (sum(s.duration_ms() for s in self._spans.values()) / max(total, 1))}


class TraceSpanContext:
    def __init__(self, mgr: TraceManager, op: str, mod: str, parent=None):
        self.mgr, self.op, self.mod, self.parent = mgr, op, mod, parent
        self.span = None

    def __enter__(self):
        self.span, _ = self.mgr._start(self.op, self.mod, self.parent)
        return self.span

    def __exit__(self, exc_type, exc, tb):
        self.mgr._end(self.span, str(exc) if exc else None)
        return False

