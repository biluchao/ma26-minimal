"""core/state_registry.py — Shared state ownership registry"""

import time
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from threading import RLock


@dataclass
class StateDeclaration:
    name: str
    owner_module: str
    readers: list = field(default_factory=list)
    description: str = ""
    state_type: str = "dict"
    version: int = 0
    last_modified_bar: int = 0
    last_modified_time: int = 0


@dataclass
class StateAccessViolation:
    state_name: str
    accessor: str
    operation: str
    timestamp: int
    reason: str


class StateRegistry:
    """
    Single-writer / multi-reader state registry.
    Owner is the only writer; readers get notified on change.
    """

    def __init__(self, alert_manager=None):
        self.alert_manager = alert_manager
        self._decl: dict = {}
        self._states: dict = {}
        self._locks: dict = {}
        self._subs: dict = {}
        self._violations: list = []
        self._glock = RLock()

    def declare(self, name: str, owner: str, readers: Optional[list] = None,
                state_type: str = "dict", description: str = "") -> StateDeclaration:
        with self._glock:
            if name in self._decl:
                raise ValueError(
                    f"State '{name}' already declared by '{self._decl[name].owner_module}', "
                    f"cannot redeclare by '{owner}'")
            d = StateDeclaration(name=name, owner_module=owner, readers=readers or [],
                                description=description, state_type=state_type)
            self._decl[name] = d
            self._locks[name] = RLock()
            self._states[name] = None
            return d

    def write(self, name: str, module: str, value: Any, bar_index: int = 0) -> bool:
        with self._glock:
            d = self._decl.get(name)
            if not d:
                self._violate(name, module, "WRITE", "state not declared")
                return False
            if d.owner_module != module:
                self._violate(name, module, "WRITE",
                              f"'{module}' is not owner '{d.owner_module}'")
                return False
            with self._locks[name]:
                self._states[name] = value
                d.version += 1
                d.last_modified_bar = bar_index
                d.last_modified_time = int(time.time() * 1000)
            for cb in self._subs.get(name, []):
                try:
                    cb(d.version, value)
                except Exception:
                    pass
            return True

    def read(self, name: str, module: str) -> tuple:
        with self._glock:
            d = self._decl.get(name)
            if not d:
                self._violate(name, module, "READ", "state not declared")
                return False, None
            if d.owner_module != module and module not in d.readers:
                self._violate(name, module, "READ",
                              f"'{module}' is neither owner nor reader")
                return False, None
            with self._locks[name]:
                return True, self._states.get(name)

    def snapshot(self, name: str, module: str) -> tuple:
        with self._glock:
            d = self._decl.get(name)
            if not d:
                return False, 0, None
            with self._locks[name]:
                return True, d.version, self._states.get(name)

    def subscribe(self, name: str, callback: Callable[[int, Any], None]) -> bool:
        self._subs.setdefault(name, []).append(callback)
        return True

    def _violate(self, name, accessor, op, reason):
        v = StateAccessViolation(name, accessor, op, int(time.time() * 1000), reason)
        self._violations.append(v)
        if self.alert_manager:
            self.alert_manager.send("URGENT", "State Access Violation",
                                    f"{op} on '{name}' by '{accessor}': {reason}")

    def get_declarations(self) -> dict:
        return dict(self._decl)

    def get_violations(self) -> list:
        return list(self._violations)

    def validate(self) -> list:
        issues = []
        for name, d in self._decl.items():
            if not d.owner_module:
                issues.append(f"State '{name}' has no owner")
            if d.version == 0 and self._states.get(name) is not None:
                issues.append(f"State '{name}' has value but version=0")
        return issues

