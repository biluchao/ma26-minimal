"""
src/core/config_snapshot.py
MA26-ATR 量化交易系统 — 核心原语：配置快照

设计理念:
  配置快照用于保存系统任意时刻的完整配置状态，支持：
    1. 系统启动时的配置固化（审计/回溯）
    2. 参数热更新的版本管理（回滚能力）
    3. 回测复现（精确还原当时的参数组合）
    4. 多环境配置对比（diff）

  核心概念:
    ConfigSnapshot  — 某一时刻的完整配置冻结副本
    SnapshotDiff    — 两个快照之间的差异描述
    SnapshotManager — 快照的存储/加载/版本链管理

  存储格式:
    snapshots/
    ├── {run_id}_v001.yaml      # 初始配置
    ├── {run_id}_v002.yaml      # 热更新后
    ├── {run_id}_v003.yaml      # 再次更新
    └── {run_id}_manifest.json  # 版本清单
"""

import os
import json
import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from enum import Enum
from pathlib import Path

import yaml

from .result import Result, Error, ErrorSeverity, ErrorCategory


# ═══════════════════════════════════════════════════════════════
# 快照来源
# ═══════════════════════════════════════════════════════════════

class SnapshotSource(Enum):
    """快照来源"""
    STARTUP = "STARTUP"           # 系统启动时自动创建
    HOT_RELOAD = "HOT_RELOAD"     # 热更新产生
    MANUAL = "MANUAL"             # 手动触发
    ROLLBACK = "ROLLBACK"         # 回滚产生
    SCHEDULED = "SCHEDULED"       # 定时快照


class SnapshotFormat(Enum):
    """快照存储格式"""
    YAML = "yaml"
    JSON = "json"
    RAW_DICT = "dict"


# ═══════════════════════════════════════════════════════════════
# 配置快照
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ConfigSnapshot:
    """
    不可变配置快照。

    包含系统某一时刻的完整配置状态。
    """
    snapshot_id: str                          # 唯一标识
    run_id: str                              # 运行实例 ID
    version: int                             # 版本号（同一 run_id 内递增）
    source: SnapshotSource                   # 创建来源
    created_at: int                          # 创建时间戳 (ms)
    created_by: str                          # 创建者（模块名或 "user"）
    config_data: Dict[str, Any]              # 完整配置字典
    parent_snapshot_id: Optional[str] = None # 父快照 ID（版本链）
    description: str = ""                    # 描述
    tags: Dict[str, str] = field(default_factory=dict)
    checksum: str = ""                       # 配置数据的 SHA256

    def __post_init__(self):
        # 自动计算校验和
        if not self.checksum and self.config_data:
            object.__setattr__(self, 'checksum', self._compute_checksum())

    def _compute_checksum(self) -> str:
        """计算配置数据的 SHA256"""
        serialized = json.dumps(self.config_data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        按路径获取配置值。
        路径格式: "strategy.ma_period" 或 "risk.max_daily_loss_pct"
        """
        keys = key_path.split('.')
        value = self.config_data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def has_changed_from(self, other: 'ConfigSnapshot') -> bool:
        """检查与另一个快照相比配置是否变化"""
        return self.checksum != other.checksum

    def to_dict(self) -> dict:
        return {
            'snapshot_id': self.snapshot_id,
            'run_id': self.run_id,
            'version': self.version,
            'source': self.source.value,
            'created_at': self.created_at,
            'created_by': self.created_by,
            'parent_snapshot_id': self.parent_snapshot_id,
            'description': self.description,
            'tags': self.tags,
            'checksum': self.checksum,
            'config_data': self.config_data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ConfigSnapshot':
        """从字典恢复"""
        return cls(
            snapshot_id=data['snapshot_id'],
            run_id=data['run_id'],
            version=data['version'],
            source=SnapshotSource(data['source']),
            created_at=data['created_at'],
            created_by=data['created_by'],
            config_data=data['config_data'],
            parent_snapshot_id=data.get('parent_snapshot_id'),
            description=data.get('description', ''),
            tags=data.get('tags', {}),
            checksum=data.get('checksum', ''),
        )

    def save_to_file(self, directory: str, fmt: SnapshotFormat = SnapshotFormat.YAML) -> Result[str]:
        """保存到文件"""
        try:
            os.makedirs(directory, exist_ok=True)
            if fmt == SnapshotFormat.YAML:
                filepath = os.path.join(directory, f"{self.run_id}_v{self.version:03d}.yaml")
                with open(filepath, 'w', encoding='utf-8') as f:
                    yaml.safe_dump(self.to_dict(), f, default_flow_style=False, allow_unicode=True)
            elif fmt == SnapshotFormat.JSON:
                filepath = os.path.join(directory, f"{self.run_id}_v{self.version:03d}.json")
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
            else:
                return Result.err(Error(
                    code="CFG_001",
                    message=f"不支持的快照格式: {fmt}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))
            return Result.ok(filepath)
        except Exception as e:
            return Result.err(Error(
                code="CFG_002",
                message=f"保存快照失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    @classmethod
    def load_from_file(cls, filepath: str) -> Result['ConfigSnapshot']:
        """从文件加载"""
        try:
            path = Path(filepath)
            if not path.exists():
                return Result.err(Error(
                    code="CFG_003",
                    message=f"快照文件不存在: {filepath}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))

            if path.suffix in ('.yaml', '.yml'):
                with open(path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
            elif path.suffix == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                return Result.err(Error(
                    code="CFG_004",
                    message=f"不支持的文件格式: {path.suffix}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))

            return Result.ok(cls.from_dict(data))
        except Exception as e:
            return Result.err(Error(
                code="CFG_005",
                message=f"加载快照失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    def __repr__(self) -> str:
        return (f"ConfigSnapshot(id={self.snapshot_id[:16]}..., "
                f"run={self.run_id}, v={self.version}, "
                f"src={self.source.value}, checksum={self.checksum[:8]}...)")


# ═══════════════════════════════════════════════════════════════
# 快照差异
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ConfigChange:
    """单个配置变更"""
    key_path: str
    old_value: Any
    new_value: Any
    change_type: str  # "added" | "removed" | "modified"

    def __repr__(self) -> str:
        if self.change_type == "added":
            return f"+ {self.key_path} = {self.new_value}"
        elif self.change_type == "removed":
            return f"- {self.key_path} (was {self.old_value})"
        else:
            return f"~ {self.key_path}: {self.old_value} → {self.new_value}"


@dataclass(frozen=True)
class SnapshotDiff:
    """
    两个配置快照之间的差异。
    """
    from_snapshot_id: str
    to_snapshot_id: str
    changes: List[ConfigChange] = field(default_factory=list)
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))

    def is_empty(self) -> bool:
        return len(self.changes) == 0

    def summary(self) -> dict:
        added = sum(1 for c in self.changes if c.change_type == "added")
        removed = sum(1 for c in self.changes if c.change_type == "removed")
        modified = sum(1 for c in self.changes if c.change_type == "modified")
        return {
            'total_changes': len(self.changes),
            'added': added,
            'removed': removed,
            'modified': modified,
        }

    def get_changes_by_prefix(self, prefix: str) -> List[ConfigChange]:
        """按前缀筛选变更"""
        return [c for c in self.changes if c.key_path.startswith(prefix)]

    def to_dict(self) -> dict:
        return {
            'from_snapshot_id': self.from_snapshot_id,
            'to_snapshot_id': self.to_snapshot_id,
            'created_at': self.created_at,
            'summary': self.summary(),
            'changes': [
                {
                    'key_path': c.key_path,
                    'old_value': c.old_value,
                    'new_value': c.new_value,
                    'change_type': c.change_type,
                }
                for c in self.changes
            ],
        }

    def __repr__(self) -> str:
        s = self.summary()
        return (f"SnapshotDiff({self.from_snapshot_id[:8]}... → "
                f"{self.to_snapshot_id[:8]}..., "
                f"changes={s['total_changes']} "
                f"(+{s['added']} ~{s['modified']} -{s['removed']}))")


# ═══════════════════════════════════════════════════════════════
# 快照差异计算器
# ═══════════════════════════════════════════════════════════════

class SnapshotDiffer:
    """
    计算两个配置快照之间的差异。
    """

    @staticmethod
    def diff(
        old: ConfigSnapshot,
        new: ConfigSnapshot,
        ignore_paths: Optional[Set[str]] = None,
    ) -> SnapshotDiff:
        """
        计算两个快照之间的差异。
        """
        ignore_paths = ignore_paths or set()
        changes = []

        # 递归比较
        old_flat = SnapshotDiffer._flatten(old.config_data)
        new_flat = SnapshotDiffer._flatten(new.config_data)

        # 检查被修改和删除的键
        for key, old_val in old_flat.items():
            if key in ignore_paths:
                continue
            if key not in new_flat:
                if not any(key.startswith(ig) for ig in ignore_paths):
                    changes.append(ConfigChange(
                        key_path=key,
                        old_value=old_val,
                        new_value=None,
                        change_type="removed",
                    ))
            elif new_flat[key] != old_val:
                if not any(key.startswith(ig) for ig in ignore_paths):
                    changes.append(ConfigChange(
                        key_path=key,
                        old_value=old_val,
                        new_value=new_flat[key],
                        change_type="modified",
                    ))

        # 检查新增的键
        for key, new_val in new_flat.items():
            if key in ignore_paths:
                continue
            if key not in old_flat:
                if not any(key.startswith(ig) for ig in ignore_paths):
                    changes.append(ConfigChange(
                        key_path=key,
                        old_value=None,
                        new_value=new_val,
                        change_type="added",
                    ))

        return SnapshotDiff(
            from_snapshot_id=old.snapshot_id,
            to_snapshot_id=new.snapshot_id,
            changes=changes,
        )

    @staticmethod
    def _flatten(d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """将嵌套字典展平为 key.path → value"""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(SnapshotDiffer._flatten(v, new_key, sep))
            else:
                items[new_key] = v
        return items


# ═══════════════════════════════════════════════════════════════
# 快照管理器
# ═══════════════════════════════════════════════════════════════

class SnapshotManager:
    """
    配置快照管理器 — 负责快照的创建、存储、版本链管理。

    用法:
      manager = SnapshotManager(snapshot_dir="./snapshots/backtest")

      # 创建快照
      snapshot = manager.create(
          config_data=loaded_config,
          run_id="backtest_20240101",
          source=SnapshotSource.STARTUP,
      )

      # 热更新创建新版本
      new_snapshot = manager.create_from_existing(
          old_snapshot=snapshot,
          config_data=new_config,
          source=SnapshotSource.HOT_RELOAD,
      )

      # 对比
      diff = manager.diff(snapshot, new_snapshot)

      # 回滚
      rollback = manager.rollback_to(new_snapshot.snapshot_id)
    """

    def __init__(self, snapshot_dir: str = "./snapshots"):
        self._snapshot_dir = snapshot_dir
        self._snapshots: Dict[str, ConfigSnapshot] = {}      # snapshot_id → snapshot
        self._version_chain: Dict[str, List[str]] = {}        # run_id → [snapshot_id, ...]
        self._lock = threading.RLock()
        self._next_version: Dict[str, int] = {}              # run_id → next_version

        # 确保目录存在
        os.makedirs(snapshot_dir, exist_ok=True)

    @property
    def snapshot_dir(self) -> str:
        return self._snapshot_dir

    def create(
        self,
        config_data: Dict[str, Any],
        run_id: str,
        source: SnapshotSource,
        created_by: str = "system",
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        parent_snapshot_id: Optional[str] = None,
        auto_save: bool = True,
        save_format: SnapshotFormat = SnapshotFormat.YAML,
    ) -> Result[ConfigSnapshot]:
        """
        创建新快照。
        """
        with self._lock:
            # 版本号递增
            if run_id not in self._next_version:
                self._next_version[run_id] = 1
            version = self._next_version[run_id]

            snapshot_id = f"{run_id}_v{version:03d}_{int(time.time() * 1000)}"

            snapshot = ConfigSnapshot(
                snapshot_id=snapshot_id,
                run_id=run_id,
                version=version,
                source=source,
                created_at=int(time.time() * 1000),
                created_by=created_by,
                config_data=config_data,
                parent_snapshot_id=parent_snapshot_id,
                description=description,
                tags=tags or {},
            )

            # 存储
            self._snapshots[snapshot_id] = snapshot
            if run_id not in self._version_chain:
                self._version_chain[run_id] = []
            self._version_chain[run_id].append(snapshot_id)

            # 版本号递增
            self._next_version[run_id] += 1

        # 自动保存
        if auto_save:
            save_result = snapshot.save_to_file(self._snapshot_dir, save_format)
            if save_result.is_err():
                return Result.err(save_result.unwrap_err())

        return Result.ok(snapshot)

    def create_from_existing(
        self,
        old_snapshot: ConfigSnapshot,
        config_data: Dict[str, Any],
        source: SnapshotSource,
        created_by: str = "system",
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        auto_save: bool = True,
    ) -> Result[ConfigSnapshot]:
        """
        基于现有快照创建新版本。
        """
        return self.create(
            config_data=config_data,
            run_id=old_snapshot.run_id,
            source=source,
            created_by=created_by,
            description=description,
            tags=tags,
            parent_snapshot_id=old_snapshot.snapshot_id,
            auto_save=auto_save,
        )

    def get(self, snapshot_id: str) -> Optional[ConfigSnapshot]:
        """获取快照"""
        return self._snapshots.get(snapshot_id)

    def get_latest(self, run_id: str) -> Optional[ConfigSnapshot]:
        """获取某个 run_id 的最新快照"""
        with self._lock:
            if run_id not in self._version_chain:
                return None
            chain = self._version_chain[run_id]
            if not chain:
                return None
            latest_id = chain[-1]
            return self._snapshots.get(latest_id)

    def get_version(self, run_id: str, version: int) -> Optional[ConfigSnapshot]:
        """获取指定版本"""
        with self._lock:
            if run_id not in self._version_chain:
                return None
            for sid in self._version_chain[run_id]:
                snap = self._snapshots.get(sid)
                if snap and snap.version == version:
                    return snap
            return None

    def get_chain(self, run_id: str) -> List[ConfigSnapshot]:
        """获取版本链"""
        with self._lock:
            if run_id not in self._version_chain:
                return []
            return [
                self._snapshots[sid]
                for sid in self._version_chain[run_id]
                if sid in self._snapshots
            ]

    def rollback_to(self, snapshot_id: str) -> Result[ConfigSnapshot]:
        """
        回滚到指定快照（创建一个新快照，parent 指向目标）。
        """
        target = self._snapshots.get(snapshot_id)
        if not target:
            return Result.err(Error(
                code="CFG_006",
                message=f"目标快照不存在: {snapshot_id}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        # 创建回滚快照
        return self.create(
            config_data=target.config_data,
            run_id=target.run_id,
            source=SnapshotSource.ROLLBACK,
            created_by="rollback",
            description=f"Rollback to {snapshot_id}",
            parent_snapshot_id=snapshot_id,
        )

    def diff(
        self,
        from_id: str,
        to_id: str,
        ignore_paths: Optional[Set[str]] = None,
    ) -> Result[SnapshotDiff]:
        """
        计算两个快照之间的差异。
        """
        from_snap = self._snapshots.get(from_id)
        to_snap = self._snapshots.get(to_id)

        if not from_snap:
            return Result.err(Error(
                code="CFG_007", message=f"源快照不存在: {from_id}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))
        if not to_snap:
            return Result.err(Error(
                code="CFG_008", message=f"目标快照不存在: {to_id}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        diff_result = SnapshotDiffer.diff(from_snap, to_snap, ignore_paths)
        return Result.ok(diff_result)

    def diff_latest(self, run_id: str) -> Result[SnapshotDiff]:
        """对比最新两个版本"""
        chain = self.get_chain(run_id)
        if len(chain) < 2:
            return Result.err(Error(
                code="CFG_009",
                message=f"版本链不足2个，无法对比: {run_id}",
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.CONFIG,
            ))
        return self.diff(chain[-2].snapshot_id, chain[-1].snapshot_id)

    def load_from_disk(self, run_id: str) -> Result[int]:
        """
        从磁盘加载某个 run_id 的所有快照。
        """
        loaded = 0
        try:
            for filename in os.listdir(self._snapshot_dir):
                if not (filename.startswith(run_id) and
                        filename.endswith(('.yaml', '.yml', '.json'))):
                    continue

                filepath = os.path.join(self._snapshot_dir, filename)
                result = ConfigSnapshot.load_from_file(filepath)
                if result.is_ok():
                    snap = result.unwrap()
                    with self._lock:
                        self._snapshots[snap.snapshot_id] = snap
                        if snap.run_id not in self._version_chain:
                            self._version_chain[snap.run_id] = []
                        if snap.snapshot_id not in self._version_chain[snap.run_id]:
                            self._version_chain[snap.run_id].append(snap.snapshot_id)
                        # 更新版本号
                        if snap.run_id not in self._next_version:
                            self._next_version[snap.run_id] = snap.version + 1
                        elif snap.version >= self._next_version[snap.run_id]:
                            self._next_version[snap.run_id] = snap.version + 1
                    loaded += 1

            # 按版本排序
            with self._lock:
                for rid in self._version_chain:
                    self._version_chain[rid].sort(
                        key=lambda sid: self._snapshots[sid].version
                    )

            return Result.ok(loaded)
        except Exception as e:
            return Result.err(Error(
                code="CFG_010",
                message=f"从磁盘加载快照失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    def save_manifest(self, run_id: str) -> Result[str]:
        """
        保存版本清单（manifest.json）。
        """
        chain = self.get_chain(run_id)
        if not chain:
            return Result.err(Error(
                code="CFG_011",
                message=f"没有快照可保存: {run_id}",
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.CONFIG,
            ))

        manifest = {
            'run_id': run_id,
            'snapshot_count': len(chain),
            'created_at': int(time.time() * 1000),
            'snapshots': [
                {
                    'snapshot_id': s.snapshot_id,
                    'version': s.version,
                    'source': s.source.value,
                    'created_at': s.created_at,
                    'created_by': s.created_by,
                    'parent_snapshot_id': s.parent_snapshot_id,
                    'description': s.description,
                    'checksum': s.checksum,
                }
                for s in chain
            ],
        }

        try:
            manifest_path = os.path.join(self._snapshot_dir, f"{run_id}_manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
            return Result.ok(manifest_path)
        except Exception as e:
            return Result.err(Error(
                code="CFG_012",
                message=f"保存清单失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    def list_run_ids(self) -> List[str]:
        """列出所有 run_id"""
        with self._lock:
            return list(self._version_chain.keys())

    def delete_snapshot(self, snapshot_id: str) -> Result[None]:
        """删除快照"""
        snap = self._snapshots.get(snapshot_id)
        if not snap:
            return Result.err(Error(
                code="CFG_013",
                message=f"快照不存在: {snapshot_id}",
                severity=ErrorSeverity.INFO,
                category=ErrorCategory.CONFIG,
            ))

        with self._lock:
            # 从内存移除
            del self._snapshots[snapshot_id]
            if snap.run_id in self._version_chain:
                self._version_chain[snap.run_id].remove(snapshot_id)

        # 从磁盘删除
        try:
            for ext in ('.yaml', '.json'):
                filepath = os.path.join(self._snapshot_dir, f"{snap.run_id}_v{snap.version:03d}{ext}")
                if os.path.exists(filepath):
                    os.remove(filepath)
        except Exception:
            pass

        return Result.ok(None)

    def clear_all(self):
        """清空所有快照"""
        with self._lock:
            self._snapshots.clear()
            self._version_chain.clear()
            self._next_version.clear()

    def stats(self) -> dict:
        """统计信息"""
        with self._lock:
            total_snapshots = len(self._snapshots)
            total_runs = len(self._version_chain)
            snapshots_per_run = {
                rid: len(chain) for rid, chain in self._version_chain.items()
            }
        return {
            'total_snapshots': total_snapshots,
            'total_runs': total_runs,
            'snapshots_per_run': snapshots_per_run,
            'snapshot_dir': self._snapshot_dir,
        }


# ═══════════════════════════════════════════════════════════════
# 配置校验器
# ═══════════════════════════════════════════════════════════════

class ConfigValidator:
    """
    配置校验器 — 确保快照中的配置参数合法。
    """

    # 参数范围约束
    CONSTRAINTS = {
        'initial_equity': (1000.0, 10000000.0),
        'strategy.ma_period': (5, 200),
        'strategy.atr_period': (5, 100),
        'strategy.band_multiplier': (0.1, 5.0),
        'strategy.detach_multiplier': (0.05, 2.0),
        'strategy.min_trend_strength': (0.1, 1.0),
        'risk.max_daily_loss_pct': (0.001, 0.5),
        'risk.max_single_loss_pct': (0.001, 0.2),
        'risk.max_drawdown_pct': (0.05, 0.8),
        'risk.max_slippage_bps': (1, 1000),
        'risk.max_position_value_usdt': (100.0, 1000000.0),
        'kline.max_bars': (50, 10000),
    }

    @classmethod
    def validate(cls, snapshot: ConfigSnapshot) -> Result[List[str]]:
        """
        校验配置快照。
        返回 Ok([warning_messages]) — 警告不阻断，错误通过 Err 返回。
        """
        warnings = []
        errors = []

        for key_path, (min_val, max_val) in cls.CONSTRAINTS.items():
            value = snapshot.get(key_path)
            if value is None:
                warnings.append(f"参数缺失: {key_path}")
                continue
            if not isinstance(value, (int, float)):
                errors.append(f"参数类型错误 {key_path}: expected number, got {type(value).__name__}")
                continue
            if value < min_val or value > max_val:
                errors.append(
                    f"参数越界 {key_path}: {value} "
                    f"(允许范围 [{min_val}, {max_val}])"
                )

        if errors:
            return Result.err(Error(
                code="CFG_VALIDATE_ERR",
                message=f"配置校验失败: {'; '.join(errors)}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
                source="config_validator",
                context={'errors': errors, 'warnings': warnings},
            ))

        return Result.ok(warnings)


# ═══════════════════════════════════════════════════════════════
# 全局管理器实例
# ═══════════════════════════════════════════════════════════════

_global_snapshot_manager = SnapshotManager(snapshot_dir="./snapshots")


def get_global_snapshot_manager() -> SnapshotManager:
    """获取全局快照管理器"""
    return _global_snapshot_manager


def create_startup_snapshot(
    config_data: Dict[str, Any],
    run_id: str,
    snapshot_dir: str = "./snapshots",
) -> Result[ConfigSnapshot]:
    """
    便捷函数：创建系统启动快照。
    """
    manager = SnapshotManager(snapshot_dir)
    return manager.create(
        config_data=config_data,
        run_id=run_id,
        source=SnapshotSource.STARTUP,
        created_by="bootstrap",
        description="System startup configuration",
        auto_save=True,
    )
