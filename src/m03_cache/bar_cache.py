"""m03_cache/bar_cache.py — Bar cache with dedup window"""

import time
from collections import deque
from threading import RLock

from core import SystemModule, Result, Error, ErrorSeverity, EventType, Event


class BarCache(SystemModule):
    """Stores bars by index; dedup window prevents duplicate writes."""

    MODULE_NAME = "M03_BarCache"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = []

    def __init__(self, dedup_window: int = 5000, cleanup_interval: int = 1000):
        super().__init__()
        self.dedup_window = dedup_window
        self.cleanup_interval = cleanup_interval
        self._bars: dict = {}
        self._order: list = []
        self._seen: set = set()
        self._lock = RLock()
        self._cleanup_counter = 0
        self._max_index = -1

    def _do_start(self) -> Result[None, Error]:
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def append(self, bar: dict) -> Result[bool, Error]:
        """
        Append a bar. Returns Ok(True) if new, Ok(False) if duplicate.
        `bar` must contain: bar_index, timestamp, open, high, low, close, volume
        """
        bi = bar.get('bar_index')
        if bi is None:
            return Result.err(Error("BAR_NO_INDEX", "bar_index required", ErrorSeverity.RECOVERABLE, self.MODULE_NAME))
        if not isinstance(bi, int) or bi < 0:
            return Result.err(Error("BAR_BAD_INDEX", f"bad bar_index {bi}", ErrorSeverity.RECOVERABLE, self.MODULE_NAME))

        key = (bi, bar.get('timestamp', 0))
        with self._lock:
            if key in self._seen:
                return Result.ok(False)
            if bi <= self._max_index and (bi, self._bars[bi]['timestamp'] if bi in self._bars else 0) == key[1]:
                self._seen.add(key)
                return Result.ok(False)

            self._bars[bi] = dict(bar)
            self._order.append(bi)
            self._seen.add(key)
            if bi > self._max_index:
                self._max_index = bi

            self._cleanup_counter += 1
            if self._cleanup_counter >= self.cleanup_interval:
                self._cleanup_counter = 0
                self._cleanup()

        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.BAR_RECEIVED,
                                         bar_index=bi, timestamp=bar.get('timestamp', 0),
                                         close=bar.get('close'))
        return Result.ok(True)

    def get(self, bar_index: int) -> Result[dict, Error]:
        with self._lock:
            bar = self._bars.get(bar_index)
            if bar is None:
                return Result.err(Error("BAR_NOT_FOUND", f"bar {bar_index} not cached",
                                       ErrorSeverity.TRANSIENT, self.MODULE_NAME))
            return Result.ok(dict(bar))

    def get_latest(self) -> Result[dict, Error]:
        with self._lock:
            if not self._order:
                return Result.err(Error("BAR_CACHE_EMPTY", "no bars in cache", ErrorSeverity.TRANSIENT, self.MODULE_NAME))
            return Result.ok(dict(self._bars[self._order[-1]]))

    def get_range(self, start: int, end: int) -> Result[list, Error]:
        with self._lock:
            out = []
            for bi in range(start, end + 1):
                if bi in self._bars:
                    out.append(dict(self._bars[bi]))
            return Result.ok(out)

    def __getitem__(self, bar_index: int) -> dict:
        with self._lock:
            return dict(self._bars[bar_index])

    def __len__(self) -> int:
        with self._lock:
            return len(self._order)

    def __contains__(self, bar_index: int) -> bool:
        with self._lock:
            return bar_index in self._bars

    @property
    def max_index(self) -> int:
        with self._lock:
            return self._max_index

    def is_ready(self, warmup_bars: int) -> bool:
        with self._lock:
            return len(self._order) >= warmup_bars

    def state_export(self) -> dict:
        base = super().state_export()
        with self._lock:
            base.update({'max_index': self._max_index, 'count': len(self._order)})
        return base

    def _cleanup(self):
        if len(self._order) <= self.dedup_window:
            return
        excess = len(self._order) - self.dedup_window
        for _ in range(excess):
            bi = self._order.pop(0)
            self._bars.pop(bi, None)
        self._seen = {(bi, self._bars[bi]['timestamp']) for bi in self._order if bi in self._bars}

