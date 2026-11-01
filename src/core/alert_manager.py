"""core/alert_manager.py — Alert manager (stdout + callback)"""

import time
from collections import deque
from threading import RLock

from .interfaces import AlertMessage, AlertLevel
from .event_bus import EventType


class AlertManager:
    """
    Central alert dispatcher.
    - ring buffer of recent alerts
    - prints to stdout (colorized by level)
    - optional callback for external sinks (Telegram / email / webhook)
    """

    COLORS = {
        'DEBUG': '\033[90m',
        'INFO': '\033[36m',
        'WARN': '\033[33m',
        'ERROR': '\033[31m',
        'URGENT': '\033[1;31m',
    }
    RESET = '\033[0m'

    def __init__(self, buffer_size: int = 1000, use_color: bool = True):
        self._buf = deque(maxlen=buffer_size)
        self._lock = RLock()
        self._callbacks = []
        self._use_color = use_color

    def add_sink(self, callback):
        self._callbacks.append(callback)

    def send(self, level: str, title: str, message: str, ctx: dict = None) -> bool:
        alert = AlertMessage(level=level, title=title, message=message,
                            context=ctx or {}, timestamp=int(time.time() * 1000))
        with self._lock:
            self._buf.append(alert)
        self._print(alert)
        for cb in self._callbacks:
            try:
                cb(alert)
            except Exception:
                pass
        return True

    def _print(self, a: AlertMessage):
        ts = time.strftime('%H:%M:%S', time.gmtime(a.timestamp / 1000))
        color = self.COLORS.get(a.level, '') if self._use_color else ''
        reset = self.RESET if self._use_color else ''
        print(f"{color}[{ts}] [{a.level}] {a.title}: {a.message}{reset}")
        if a.context:
            print(f"{color}  ctx={a.context}{reset}")

    def tail(self, n: int = 50, min_level: str = "INFO") -> list:
        order = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'URGENT']
        min_i = order.index(min_level) if min_level in order else 0
        with self._lock:
            return [a for a in list(self._buf)[-n:] if order.index(a.level) >= min_i]

    def get_stats(self) -> dict:
        with self._lock:
            stats = {}
            for a in self._buf:
                stats[a.level] = stats.get(a.level, 0) + 1
            return stats

