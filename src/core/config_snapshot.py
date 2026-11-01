"""core/config_snapshot.py — Immutable config snapshot"""

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Optional, Callable


@dataclass
class ConfigChangeEvent:
    version: int
    changes: dict
    timestamp: int
    source: str


class ConfigSnapshot:
    """Immutable config snapshot. Once created, never mutates."""

    def __init__(self, data: dict, version: int = 0, source: str = "init"):
        self._data = data
        self._version = version
        self._source = source
        self._hash = self._calc_hash()

    def get(self, path: str, default: Any = None) -> Any:
        parts = path.split('.')
        cur = self._data
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return default
        return cur

    def get_section(self, section: str) -> dict:
        return dict(self._data.get(section, {}))

    def version(self) -> int:
        return self._version

    def hash(self) -> str:
        return self._hash

    def source(self) -> str:
        return self._source

    def as_dict(self) -> dict:
        return dict(self._data)

    def _calc_hash(self) -> str:
        s = json.dumps(self._data, sort_keys=True, default=str)
        return hashlib.sha256(s.encode()).hexdigest()[:16]

    def diff(self, other: 'ConfigSnapshot') -> dict:
        out = {}
        self._collect(self._data, other._data, "", out)
        return out

    def _collect(self, a, b, path, out):
        if isinstance(a, dict) and isinstance(b, dict):
            for k in set(list(a.keys()) + list(b.keys())):
                self._collect(a.get(k), b.get(k), f"{path}.{k}" if path else k, out)
        elif a != b:
            out[path] = (a, b)


class ConfigWatcher:
    def __init__(self):
        self._watchers: dict = {}
        self._current: Optional[ConfigSnapshot] = None

    def watch(self, path: str, callback: Callable[[ConfigChangeEvent], None]):
        if path not in self._watchers:
            self._watchers[path] = []
        self._watchers[path].append(callback)

    def apply(self, new_data: dict, source: str = "hot_reload") -> ConfigSnapshot:
        new = ConfigSnapshot(new_data,
                             version=(self._current.version() + 1) if self._current else 1,
                             source=source)
        changes = self._current.diff(new) if self._current else {"*": (None, new)}
        event = ConfigChangeEvent(version=new.version(), changes=changes,
                                 timestamp=int(time.time() * 1000), source=source)
        for path, cbs in self._watchers.items():
            if path == "*" or path in changes:
                for cb in cbs:
                    try:
                        cb(event)
                    except Exception:
                        pass
        self._current = new
        return new

    def current(self) -> Optional[ConfigSnapshot]:
        return self._current

