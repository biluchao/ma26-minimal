"""m12_snapshot/manager.py — Atomic snapshot save/load with checksum + chain"""

import os
import json
import time
import uuid
import hashlib
from pathlib import Path
from threading import RLock
from typing import Optional

from core import SystemModule, Result, Error, ErrorSeverity, EventType


class SnapshotManager(SystemModule):
    """
    Atomic snapshot persistence.
    - write to .tmp then rename (atomic on POSIX)
    - SHA-256 checksum per file
    - parent_ref chain (each snapshot references previous)
    - auto-cleanup: keeps only latest N
    """

    MODULE_NAME = "M12_SnapshotManager"
    MODULE_VERSION = "4.1.0"
    DEPENDENCIES = []

    def __init__(self, snapshot_dir: str = "snapshots"):
        super().__init__()
        self.snapshot_dir = Path(snapshot_dir)
        self._keep = 100
        self._parent_hash = ""
        self._lock = RLock()

    def configure(self, config) -> Result[None, Error]:
        super().configure(config)
        s = config.get_section('state')
        self._keep = s.get('snapshots_keep', 100)
        return Result.ok(None)

    def _do_start(self) -> Result[None, Error]:
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        # secure permissions
        try:
            os.chmod(self.snapshot_dir, 0o700)
        except Exception:
            pass
        return Result.ok(None)

    def _do_stop(self) -> Result[None, Error]:
        return Result.ok(None)

    def save(self, state: dict, bar_index: int, source: str = "") -> Result[str, Error]:
        snap_id = f"snap_{bar_index:08d}_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        payload = {
            'bar_index': bar_index,
            'source': source,
            'timestamp': int(time.time() * 1000),
            'parent_ref': self._parent_hash,
            'state': state,
        }
        body = json.dumps(payload, separators=(',', ':'), default=str)
        h = hashlib.sha256(body.encode()).hexdigest()
        payload['checksum'] = h
        body = json.dumps(payload, separators=(',', ':'), default=str)

        tmp = self.snapshot_dir / f"{snap_id}.tmp"
        final = self.snapshot_dir / f"{snap_id}.json"
        try:
            with open(tmp, 'w') as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.rename(tmp, final)
            os.chmod(final, 0o600)
        except OSError as e:
            return Result.err(Error("SNAPSHOT_WRITE_FAILED", str(e),
                                   ErrorSeverity.CRITICAL, self.MODULE_NAME))
        self._parent_hash = h

        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.SNAPSHOT_SAVED,
                                         snapshot_id=snap_id, bar_index=bar_index)
        self._cleanup()
        return Result.ok(snap_id)

    def load(self, snap_id: str) -> Result[dict, Error]:
        path = self.snapshot_dir / f"{snap_id}.json"
        try:
            with open(path) as f:
                data = json.load(f)
        except FileNotFoundError:
            return Result.err(Error("SNAPSHOT_NOT_FOUND", snap_id,
                                   ErrorSeverity.CRITICAL, self.MODULE_NAME))
        except json.JSONDecodeError as e:
            return Result.err(Error("SNAPSHOT_CORRUPTED", str(e),
                                   ErrorSeverity.CRITICAL, self.MODULE_NAME))

        # verify checksum
        stored = data.pop('checksum', None)
        body = json.dumps(data, separators=(',', ':'), default=str)
        if stored and hashlib.sha256(body.encode()).hexdigest() != stored:
            return Result.err(Error("SNAPSHOT_CHECKSUM_MISMATCH", snap_id,
                                   ErrorSeverity.CRITICAL, self.MODULE_NAME))
        if self._event_bus:
            self._event_bus.publish_from(self.MODULE_NAME, EventType.SNAPSHOT_LOADED,
                                         snapshot_id=snap_id)
        return Result.ok(data)

    def load_latest(self) -> Result[Optional[dict], Error]:
        files = sorted(self.snapshot_dir.glob("*.json"))
        if not files:
            return Result.ok(None)
        latest = files[-1]
        return self.load(latest.stem)

    def list_snapshots(self) -> list:
        return sorted(p.name for p in self.snapshot_dir.glob("*.json"))

    def _cleanup(self):
        files = sorted(self.snapshot_dir.glob("*.json"))
        for old in files[:-self._keep]:
            try:
                old.unlink()
            except OSError:
                pass

    def state_export(self) -> dict:
        base = super().state_export()
        base.update({'snapshot_dir': str(self.snapshot_dir), 'keep': self._keep,
                    'count': len(list(self.snapshot_dir.glob("*.json")))})
        return base

