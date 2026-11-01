"""m13_tracer/tracer.py — Structured, async-safe tracer"""

import time
import json
import threading
from collections import deque
from typing import Optional


class Tracer:
    """
    Minimal structured tracer.
    - ring buffer (no unbounded growth)
    - levels: DEBUG/INFO/WARN/ERROR/URGENT
    - JSONL file sink (atomic)
    """

    LEVELS = {'DEBUG': 10, 'INFO': 20, 'WARN': 30, 'ERROR': 40, 'URGENT': 50}

    def __init__(self, buffer_size: int = 10000, log_file: Optional[str] = None,
                 min_level: str = "INFO"):
        self._buf = deque(maxlen=buffer_size)
        self._lock = threading.Lock()
        self._file = None
        if log_file:
            try:
                self._file = open(log_file, 'a', encoding='utf-8')
            except Exception:
                self._file = None
        self._min_level = self.LEVELS.get(min_level, 20)
        self._counts = {lvl: 0 for lvl in self.LEVELS}

    def log(self, event: str, level: str = "INFO", **kwargs):
        lv = self.LEVELS.get(level, 20)
        if lv < self._min_level:
            return
        entry = {
            'ts': int(time.time() * 1000),
            'level': level,
            'event': event,
            'data': kwargs,
        }
        with self._lock:
            self._buf.append(entry)
            self._counts[level] += 1
            if self._file:
                try:
                    self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    self._file.flush()
                except Exception:
                    pass

    def warn(self, event, **kw): self.log(event, "WARN", **kw)
    def error(self, event, **kw): self.log(event, "ERROR", **kw)
    def urgent(self, event, **kw): self.log(event, "URGENT", **kw)
    def info(self, event, **kw): self.log(event, "INFO", **kw)
    def debug(self, event, **kw): self.log(event, "DEBUG", **kw)

    def tail(self, n: int = 100, min_level: str = "DEBUG") -> list:
        lv = self.LEVELS.get(min_level, 0)
        with self._lock:
            return [e for e in list(self._buf)[-n:] if self.LEVELS.get(e['level'], 0) >= lv]

    def get_stats(self) -> dict:
        with self._lock:
            return dict(self._counts)

    def close(self):
        with self._lock:
            if self._file:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None

    def __del__(self):
        self.close()

