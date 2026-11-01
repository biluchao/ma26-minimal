"""core/event_bus.py — Event bus with type + payload"""

from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, List, Any
from collections import defaultdict, deque
import time
import uuid


class EventType:
    # lifecycle
    SYSTEM_STARTING = "system.starting"
    SYSTEM_STARTED = "system.started"
    SYSTEM_STOPPING = "system.stopping"
    SYSTEM_STOPPED = "system.stopped"
    MODULE_STARTED = "module.started"
    MODULE_STOPPED = "module.stopped"
    # data
    BAR_RECEIVED = "bar.received"
    BAR_QUALITY_CHECKED = "bar.quality_checked"
    BAR_PROCESSED = "bar.processed"
    DATA_GAP_DETECTED = "data.gap_detected"
    # signal
    SIGNAL_GENERATED = "signal.generated"
    SIGNAL_ARBITRATED = "signal.arbitrated"
    SIGNAL_APPROVED = "signal.approved"
    SIGNAL_REJECTED = "signal.rejected"
    SIGNAL_DEDUPED = "signal.deduped"
    # order
    ORDER_SUBMITTED = "order.submitted"
    ORDER_FILLED = "order.filled"
    ORDER_PARTIAL_FILLED = "order.partial_filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    # risk
    RISK_CHECK_PASSED = "risk.check_passed"
    RISK_CHECK_FAILED = "risk.check_failed"
    RISK_COOLING_TRIGGERED = "risk.cooling_triggered"
    RISK_COOLING_ENDED = "risk.cooling_ended"
    RISK_LEVEL_CHANGED = "risk.level_changed"
    DAILY_LOSS_LIMIT_HIT = "risk.daily_loss_limit_hit"
    # equity / trade
    EQUITY_REVALUED = "equity.revalued"
    SNAPSHOT_SAVED = "snapshot.saved"
    SNAPSHOT_LOADED = "snapshot.loaded"
    TRADE_OPENED = "trade.opened"
    TRADE_CLOSED = "trade.closed"
    STOP_LOSS_EXECUTED = "stop_loss.executed"
    TAKE_PROFIT_EXECUTED = "take_profit.executed"
    # system
    ALERT_SENT = "alert.sent"
    CONFIG_CHANGED = "config.changed"
    ERROR_OCCURRED = "error.occurred"
    CIRCUIT_TRIPPED = "circuit.tripped"
    MODE_CHANGED = "mode.changed"


@dataclass
class Event:
    type: str
    timestamp: int = 0
    trace_id: str = ""
    bar_index: int = 0
    source: str = ""
    data: dict = field(default_factory=dict)
    event_id: str = ""

    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = int(time.time() * 1000)
        if not self.event_id:
            self.event_id = str(uuid.uuid4())[:12]


EventHandler = Callable[[Event], None]


class EventBus:
    """Minimal event bus — sync dispatch, priority subscription, dead-letter."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self._handlers: Dict[str, List] = defaultdict(list)
        self._wildcard: List = []
        self._persist_types: set = set(self.config.get('persist_event_types', [
            EventType.ORDER_FILLED, EventType.RISK_COOLING_TRIGGERED,
            EventType.TRADE_CLOSED, EventType.ERROR_OCCURRED,
        ]))
        self._persist_q: deque = deque(maxlen=self.config.get('persist_queue_size', 10000))
        self._dead: List[Event] = []
        self._stats = {'published': 0, 'delivered': 0, 'failed': 0}

    def subscribe(self, event_type: str, handler: EventHandler, priority: int = 100):
        if event_type == "*":
            self._wildcard.append((priority, handler))
            self._wildcard.sort(key=lambda x: x[0])
        else:
            self._handlers[event_type].append((priority, handler))
            self._handlers[event_type].sort(key=lambda x: x[0])

    def unsubscribe(self, event_type: str, handler: EventHandler):
        if event_type in self._handlers:
            self._handlers[event_type] = [(p, h) for p, h in self._handlers[event_type] if h != handler]

    def publish(self, event: Event) -> bool:
        self._stats['published'] += 1
        if event.type in self._persist_types:
            self._persist_q.append(event)

        handlers = []
        handlers.extend(self._handlers.get(event.type, []))
        for pattern, hs in self._handlers.items():
            if '*' in pattern and event.type.startswith(pattern.rstrip('*').rstrip('.')):
                handlers.extend(hs)
        handlers.extend(self._wildcard)
        handlers.sort(key=lambda x: x[0])

        for _, h in handlers:
            try:
                h(event)
                self._stats['delivered'] += 1
            except Exception:
                self._stats['failed'] += 1
                self._dead.append(event)
        return True

    def publish_from(self, source: str, event_type: str, trace_id: str = "",
                     bar_index: int = 0, **data) -> bool:
        return self.publish(Event(type=event_type, trace_id=trace_id,
                                  bar_index=bar_index, source=source, data=data))

    def replay(self, from_bar: int, types: Optional[List[str]] = None) -> int:
        n = 0
        for e in self._persist_q:
            if e.bar_index >= from_bar and (types is None or e.type in types):
                self.publish(e)
                n += 1
        return n

    def get_stats(self) -> dict:
        return dict(self._stats)

    def get_dead(self) -> list:
        return list(self._dead)

