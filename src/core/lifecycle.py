"""core/lifecycle.py — Unified module lifecycle contract"""

import time
from abc import ABC, abstractmethod
from contextlib import nullcontext

from .result import Result, Error, ErrorSeverity
from .config_snapshot import ConfigSnapshot
from .event_bus import EventBus, EventType
from .trace_context import TraceContext, TraceManager
from .state_registry import StateRegistry


class SystemModule(ABC):
    """
    Base class for all modules.

    Lifecycle: __init__ → configure → start → [running] → stop → [stopped]
    All subclasses must set: MODULE_NAME, MODULE_VERSION, DEPENDENCIES
    """

    MODULE_NAME: str = ""
    MODULE_VERSION: str = "0.0.0"
    DEPENDENCIES: list = []

    def __init__(self):
        self._config: Optional[ConfigSnapshot] = None
        self._event_bus: Optional[EventBus] = None
        self._trace_manager: Optional[TraceManager] = None
        self._state_registry: Optional[StateRegistry] = None
        self._alert_manager = None
        self._started = False
        self._stopped = False
        self._start_time = 0
        self._stop_time = 0

    # ── lifecycle ──
    def configure(self, config: ConfigSnapshot) -> Result[None, Error]:
        self._config = config
        return Result.ok(None)

    def start(self) -> Result[None, Error]:
        if self._started:
            return Result.ok(None)
        with self._trace("module.start", self.MODULE_NAME):
            r = self._do_start()
            if r.is_err():
                return r
        self._started = True
        self._start_time = int(time.time() * 1000)
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.MODULE_STARTED,
                                         module=self.MODULE_NAME, version=self.MODULE_VERSION)
        return Result.ok(None)

    def stop(self) -> Result[None, Error]:
        if self._stopped:
            return Result.ok(None)
        with self._trace("module.stop", self.MODULE_NAME):
            r = self._do_stop()
            if r.is_err():
                return r
        self._stopped = True
        self._stop_time = int(time.time() * 1000)
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.MODULE_STOPPED,
                                         module=self.MODULE_NAME)
        return Result.ok(None)

    @abstractmethod
    def _do_start(self) -> Result[None, Error]:
        pass

    @abstractmethod
    def _do_stop(self) -> Result[None, Error]:
        pass

    # ── state export/import ──
    def state_export(self) -> dict:
        return {'module': self.MODULE_NAME, 'version': self.MODULE_VERSION,
                'started': self._started, 'start_time': self._start_time}

    def state_import(self, state: dict) -> Result[None, Error]:
        return Result.ok(None)

    # ── health ──
    def health_check(self) -> dict:
        return {
            'module': self.MODULE_NAME,
            'status': 'HEALTHY' if self._started and not self._stopped else 'UNHEALTHY',
            'details': {'started': self._started, 'stopped': self._stopped,
                        'uptime_ms': self.uptime_ms()},
            'timestamp': int(time.time() * 1000),
        }

    # ── injection ──
    def inject(self, **deps):
        self._event_bus = deps.get('event_bus')
        self._trace_manager = deps.get('trace_manager')
        self._state_registry = deps.get('state_registry')
        self._alert_manager = deps.get('alert_manager')

    def _trace(self, operation, module=None):
        if self._trace_manager:
            return self._trace_manager.span(operation, module or self.MODULE_NAME)
        return nullcontext()

    # ── properties ──
    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    def uptime_ms(self) -> int:
        if not self._started:
            return 0
        end = self._stop_time if self._stopped else int(time.time() * 1000)
        return end - self._start_time

    @property
    def config(self) -> Optional[ConfigSnapshot]:
        return self._config

