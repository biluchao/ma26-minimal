"""
src/core/config.py
MA26-ATR 量化交易系统 — 核心原语：配置管理

设计理念:
  1. 多源配置加载（YAML 文件 + 环境变量覆盖）
  2. 深度合并策略（base config + environment override，后者优先）
  3. 点分路径访问（如 "strategy.ma_period" → config['strategy']['ma_period']）
  4. 环境变量映射（BINANCE__STRATEGY__MA_PERIOD → strategy.ma_period）
  5. 配置校验（类型/范围/必填，基于 ConfigValidator）
  6. 与 SnapshotManager 集成（每次变更生成版本化快照，支持回滚）
  7. 与 EventBus 集成（发布 CONFIG_UPDATED / CONFIG_ROLLED_BACK 事件）
  8. 实现 LifecycleAware（可被 LifecycleManager 统一编排）

  配置加载流程:
    load([default.yaml, backtest.yaml])
      → 深度合并 → 环境变量覆盖 → 校验 → 生成快照 → 发布事件 → READY
"""

import os
import re
import copy
import threading
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from pathlib import Path
import yaml

from .result import Result, Error, ErrorSeverity, ErrorCategory
from .lifecycle import LifecycleAware
from .event_bus import EventBus, Event, EventType, get_global_event_bus
from .config_snapshot import (
    SnapshotManager, SnapshotDiffer, ConfigValidator,
    get_global_snapshot_manager,
)
from .state_registry import get_global_state_registry


# ═══════════════════════════════════════════════════════════════
# 配置异常
# ═══════════════════════════════════════════════════════════════

class ConfigError(Exception):
    """配置操作失败"""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


# ═══════════════════════════════════════════════════════════════
# 深度合并工具
# ═══════════════════════════════════════════════════════════════

def deep_merge(base: dict, override: dict, path: str = "") -> dict:
    """
    深度合并两个字典。override 中的值覆盖 base 中的值。
    列表直接替换，不做合并。
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        current_path = f"{path}.{key}" if path else key
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, current_path)
        else:
            result[key] = copy.deepcopy(value)
    return result


def deep_get(config: dict, key_path: str, default: Any = None) -> Any:
    """点分路径获取值"""
    parts = key_path.split('.')
    current = config
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def deep_set(config: dict, key_path: str, value: Any) -> Result[None]:
    """点分路径设置值（原地修改）"""
    parts = key_path.split('.')
    if not parts:
        return Result.err(Error(
            code="CONFIG_001",
            message="空键路径",
            severity=ErrorSeverity.RECOVERABLE,
            category=ErrorCategory.VALIDATION,
        ))

    current = config
    for i, part in enumerate(parts[:-1]):
        if part not in current:
            current[part] = {}
        elif not isinstance(current[part], dict):
            return Result.err(Error(
                code="CONFIG_002",
                message=f"路径 {'.'.join(parts[:i+1])} 不是字典，无法设置 {key_path}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.VALIDATION,
            ))
        current = current[part]

    current[parts[-1]] = value
    return Result.ok(None)


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """将嵌套字典展平为点分路径 → 值的字典"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


# ═══════════════════════════════════════════════════════════════
# 环境变量覆盖
# ═══════════════════════════════════════════════════════════════

def apply_env_overrides(
    config: dict,
    prefix: str = "BINANCE",
    separator: str = "__",
) -> List[str]:
    """
    将环境变量映射到配置字典。
    规则: BINANCE__STRATEGY__MA_PERIOD=50 → config['strategy']['ma_period']=50
    返回值: 被覆盖的键路径列表
    """
    overridden = []
    prefix_upper = prefix.upper()
    prefix_len = len(prefix_upper) + len(separator)

    for env_key, env_value in os.environ.items():
        if not env_key.startswith(f"{prefix_upper}{separator}"):
            continue

        # 去掉前缀，按 separator 分割
        key_path = env_key[prefix_len:].lower().replace(separator, '.')
        # 尝试解析值类型
        parsed_value = _parse_env_value(env_value)

        result = deep_set(config, key_path, parsed_value)
        if result.is_ok():
            overridden.append(key_path)

    return overridden


def _parse_env_value(value: str) -> Any:
    """尝试解析环境变量值为合适的 Python 类型"""
    # 布尔
    if value.lower() in ('true', 'false', 'yes', 'no', '1', '0'):
        return value.lower() in ('true', 'yes', '1')
    # 整数
    try:
        if '.' not in value:
            return int(value)
    except ValueError:
        pass
    # 浮点数
    try:
        return float(value)
    except ValueError:
        pass
    # 列表 (逗号分隔)
    if ',' in value:
        parts = [p.strip() for p in value.split(',')]
        # 尝试解析每个元素
        parsed_parts = []
        for p in parts:
            if p.lower() in ('true', 'false'):
                parsed_parts.append(p.lower() == 'true')
            else:
                try:
                    parsed_parts.append(int(p))
                except ValueError:
                    try:
                        parsed_parts.append(float(p))
                    except ValueError:
                        parsed_parts.append(p)
        return parsed_parts
    # 字符串
    return value


# ═══════════════════════════════════════════════════════════════
# 配置管理器（核心）
# ═══════════════════════════════════════════════════════════════

class ConfigManager(LifecycleAware):
    """
    配置管理器 — 系统的配置中心。

    职责:
      1. 从 YAML 文件加载配置
      2. 多文件深度合并
      3. 环境变量覆盖
      4. 配置校验
      5. 点分路径读写
      6. 变更快照（与 SnapshotManager 集成）
      7. 配置变更事件（与 EventBus 集成）

    用法:
      config_mgr = ConfigManager(
          file_paths=["config/default.yaml", "config/backtest.yaml"],
          env_prefix="BINANCE",
      )
      config_mgr.on_init()
      config_mgr.on_start()

      # 读取
      ma_period = config_mgr.get("strategy.ma_period", 26)

      # 写入（会生成快照并发布事件）
      config_mgr.set("strategy.ma_period", 50)

      # 重新加载
      config_mgr.reload()
    """

    def __init__(
        self,
        file_paths: Optional[List[str]] = None,
        env_prefix: str = "BINANCE",
        auto_env_override: bool = True,
        snapshot_enabled: bool = True,
        event_bus: Optional[EventBus] = None,
        snapshot_manager: Optional[SnapshotManager] = None,
    ):
        self._file_paths: List[str] = file_paths or []
        self._env_prefix = env_prefix
        self._auto_env_override = auto_env_override
        self._snapshot_enabled = snapshot_enabled
        self._config: Dict[str, Any] = {}
        self._raw_config: Dict[str, Any] = {}       # 合并后、env覆盖前的原始配置
        self._lock = threading.RLock()
        self._initialized = False
        self._event_bus = event_bus
        self._snapshot_manager = snapshot_manager
        self._env_overridden_keys: List[str] = []

    # ─── LifecycleAware 接口 ─────────────────────────────────

    @property
    def module_name(self) -> str:
        return "core_config"

    @property
    def dependencies(self) -> List[str]:
        return []  # 核心模块，无依赖

    @property
    def init_timeout_ms(self) -> int:
        return 10000

    def on_init(self) -> Result[None]:
        """加载并合并配置文件"""
        if not self._file_paths:
            return Result.err(Error(
                code="CONFIG_003",
                message="未指定配置文件路径",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIG,
            ))

        # 加载并合并
        load_result = self.load(self._file_paths)
        if load_result.is_err():
            return load_result

        # 环境变量覆盖
        if self._auto_env_override:
            self._env_overridden_keys = apply_env_overrides(
                self._config, prefix=self._env_prefix
            )

        # 校验
        validate_result = self._validate()
        if validate_result.is_err():
            return validate_result

        # 生成初始快照
        if self._snapshot_enabled:
            self._create_snapshot("initial_load")

        self._initialized = True
        return Result.ok(None)

    def on_start(self) -> Result[None]:
        """启动（配置管理器无需后台线程）"""
        return Result.ok(None)

    def on_stop(self) -> Result[None]:
        """停止"""
        with self._lock:
            self._config.clear()
            self._raw_config.clear()
            self._initialized = False
        return Result.ok(None)

    def on_health_check(self) -> Result[dict]:
        return Result.ok({
            "initialized": self._initialized,
            "config_keys": list(self._config.keys()),
            "file_paths": self._file_paths,
            "env_overridden_count": len(self._env_overridden_keys),
        })

    # ─── 配置加载 ─────────────────────────────────────────────

    def load(self, file_paths: List[str]) -> Result[None]:
        """
        加载多个配置文件并深度合并。
        后面的文件覆盖前面的。
        """
        merged = {}

        for fp in file_paths:
            path = Path(fp)
            if not path.exists():
                return Result.err(Error(
                    code="CONFIG_004",
                    message=f"配置文件不存在: {fp}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIG,
                ))

            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    if data is None:
                        data = {}
                    if not isinstance(data, dict):
                        return Result.err(Error(
                            code="CONFIG_005",
                            message=f"配置文件格式错误（不是字典）: {fp}",
                            severity=ErrorSeverity.CRITICAL,
                            category=ErrorCategory.CONFIG,
                        ))
                    merged = deep_merge(merged, data)
            except yaml.YAMLError as e:
                return Result.err(Error(
                    code="CONFIG_006",
                    message=f"YAML 解析失败 {fp}: {e}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIG,
                ))
            except Exception as e:
                return Result.err(Error(
                    code="CONFIG_007",
                    message=f"加载配置文件失败 {fp}: {e}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.CONFIG,
                ))

        with self._lock:
            self._raw_config = copy.deepcopy(merged)
            self._config = merged
            self._file_paths = file_paths

        return Result.ok(None)

    def reload(self) -> Result[None]:
        """重新加载配置"""
        if not self._file_paths:
            return Result.err(Error(
                code="CONFIG_008",
                message="无配置文件路径，无法重载",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        # 保存旧配置用于对比
        old_config = copy.deepcopy(self._config)

        # 重新加载
        load_result = self.load(self._file_paths)
        if load_result.is_err():
            return load_result

        # 环境变量覆盖
        if self._auto_env_override:
            self._env_overridden_keys = apply_env_overrides(
                self._config, prefix=self._env_prefix
            )

        # 校验
        validate_result = self._validate()
        if validate_result.is_err():
            # 回滚
            with self._lock:
                self._config = old_config
            return validate_result

        # 生成快照
        if self._snapshot_enabled:
            snapshot_id = self._create_snapshot("reload")

        # 发布变更事件
        self._publish_change_event(old_config, self._config, "reload")

        return Result.ok(None)

    # ─── 配置访问 ─────────────────────────────────────────────

    def get(self, key_path: str, default: Any = None) -> Any:
        """点分路径获取配置值"""
        with self._lock:
            return deep_get(self._config, key_path, default)

    def get_int(self, key_path: str, default: int = 0) -> int:
        """获取整数值"""
        value = self.get(key_path, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def get_float(self, key_path: str, default: float = 0.0) -> float:
        """获取浮点数值"""
        value = self.get(key_path, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def get_bool(self, key_path: str, default: bool = False) -> bool:
        """获取布尔值"""
        value = self.get(key_path, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1', 'on')
        return bool(value) if value is not None else default

    def get_str(self, key_path: str, default: str = "") -> str:
        """获取字符串值"""
        value = self.get(key_path, default)
        return str(value) if value is not None else default

    def get_dict(self, key_path: str, default: Optional[dict] = None) -> dict:
        """获取字典值"""
        value = self.get(key_path, default or {})
        return value if isinstance(value, dict) else (default or {})

    def get_list(self, key_path: str, default: Optional[list] = None) -> list:
        """获取列表值"""
        value = self.get(key_path, default or [])
        return value if isinstance(value, list) else (default or [])

    def set(self, key_path: str, value: Any, create_snapshot: bool = True) -> Result[None]:
        """设置配置值"""
        with self._lock:
            old_value = deep_get(self._config, key_path)
            result = deep_set(self._config, key_path, value)
            if result.is_err():
                return result

        # 生成快照
        if create_snapshot and self._snapshot_enabled:
            self._create_snapshot(f"set:{key_path}")

        # 发布变更事件
        self._publish_single_change_event(key_path, old_value, value)

        return Result.ok(None)

    def get_all(self) -> dict:
        """获取完整配置副本"""
        with self._lock:
            return copy.deepcopy(self._config)

    def get_flattened(self) -> dict:
        """获取展平的配置（点分路径 → 值）"""
        with self._lock:
            return flatten_dict(self._config)

    # ─── 校验 ─────────────────────────────────────────────────

    def _validate(self) -> Result[None]:
        """校验当前配置"""
        # 使用 ConfigValidator 做基础校验
        validator = ConfigValidator()

        # 校验数值范围（常见参数）
        validations = [
            ("initial_equity", lambda v: isinstance(v, (int, float)) and v > 0),
            ("strategy.ma_period", lambda v: isinstance(v, int) and 5 <= v <= 500),
            ("strategy.atr_period", lambda v: isinstance(v, int) and 5 <= v <= 200),
            ("strategy.band_multiplier", lambda v: isinstance(v, (int, float)) and 0.1 <= v <= 10.0),
            ("strategy.detach", lambda v: isinstance(v, (int, float)) and 0.0 <= v <= 1.0),
            ("strategy.trend_strength", lambda v: isinstance(v, (int, float)) and 0.0 <= v <= 1.0),
            ("risk.daily_loss_limit", lambda v: isinstance(v, (int, float)) and 0 < v <= 1),
            ("risk.max_position_size", lambda v: isinstance(v, (int, float)) and v > 0),
            ("risk.max_drawdown", lambda v: isinstance(v, (int, float)) and 0 < v <= 1),
            ("execution.slippage_bps", lambda v: isinstance(v, (int, float)) and v >= 0),
        ]

        errors = []
        for key_path, check_fn in validations:
            value = self.get(key_path)
            if value is not None and not check_fn(value):
                errors.append(f"{key_path}={value} 校验失败")

        if errors:
            return Result.err(Error(
                code="CONFIG_009",
                message=f"配置校验失败: {'; '.join(errors)}",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIG,
                context={'errors': errors},
            ))

        return Result.ok(None)

    def validate_with_schema(self, schema: Dict[str, Dict[str, Any]]) -> Result[List[str]]:
        """
        使用自定义 schema 校验。
        schema 格式: {key_path: {"type": str, "min": num, "max": num, "required": bool}}
        """
        warnings = []
        errors = []

        for key_path, rules in schema.items():
            value = self.get(key_path)
            required = rules.get('required', False)

            if value is None:
                if required:
                    errors.append(f"缺少必填参数: {key_path}")
                continue

            # 类型检查
            expected_type = rules.get('type')
            if expected_type:
                type_map = {
                    'int': int, 'float': (int, float), 'str': str,
                    'bool': bool, 'list': list, 'dict': dict,
                }
                expected = type_map.get(expected_type)
                if expected and not isinstance(value, expected):
                    errors.append(f"{key_path} 类型错误: 期望 {expected_type}")

            # 范围检查
            if isinstance(value, (int, float)):
                if 'min' in rules and value < rules['min']:
                    errors.append(f"{key_path}={value} 小于最小值 {rules['min']}")
                if 'max' in rules and value > rules['max']:
                    errors.append(f"{key_path}={value} 大于最大值 {rules['max']}")

        if errors:
            return Result.err(Error(
                code="CONFIG_010",
                message=f"Schema 校验失败: {'; '.join(errors)}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        return Result.ok(warnings)

    # ─── 快照管理 ─────────────────────────────────────────────

    def _create_snapshot(self, reason: str) -> Optional[str]:
        """创建配置快照"""
        if not self._snapshot_enabled:
            return None

        try:
            if self._snapshot_manager is None:
                self._snapshot_manager = get_global_snapshot_manager()

            snapshot_id = self._snapshot_manager.create_snapshot(
                config=copy.deepcopy(self._config),
                reason=reason,
            )
            return snapshot_id
        except Exception:
            return None

    def rollback(self, snapshot_id: Optional[str] = None) -> Result[None]:
        """回滚到指定快照（默认上一个）"""
        if not self._snapshot_enabled:
            return Result.err(Error(
                code="CONFIG_011",
                message="快照功能未启用",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        try:
            if self._snapshot_manager is None:
                self._snapshot_manager = get_global_snapshot_manager()

            # 获取快照
            if snapshot_id:
                snapshot = self._snapshot_manager.get_snapshot(snapshot_id)
            else:
                snapshot = self._snapshot_manager.get_latest_snapshot()

            if not snapshot:
                return Result.err(Error(
                    code="CONFIG_012",
                    message="无可用快照",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))

            # 恢复
            old_config = copy.deepcopy(self._config)
            with self._lock:
                self._config = copy.deepcopy(snapshot.config)

            # 创建回滚快照
            self._create_snapshot(f"rollback_from_{snapshot.snapshot_id[:8]}")

            # 发布事件
            self._publish_change_event(old_config, self._config, "rollback")

            return Result.ok(None)
        except Exception as e:
            return Result.err(Error(
                code="CONFIG_013",
                message=f"回滚失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    def list_snapshots(self) -> list:
        """列出所有快照"""
        if not self._snapshot_enabled:
            return []
        if self._snapshot_manager is None:
            self._snapshot_manager = get_global_snapshot_manager()
        return self._snapshot_manager.list_snapshots()

    # ─── 事件发布 ─────────────────────────────────────────────

    def _publish_change_event(self, old_config: dict, new_config: dict, reason: str):
        """发布配置变更事件"""
        if self._event_bus is None:
            try:
                self._event_bus = get_global_event_bus()
            except Exception:
                return

        try:
            # 计算差异
            differ = SnapshotDiffer()
            old_flat = flatten_dict(old_config)
            new_flat = flatten_dict(new_config)
            changes = differ.diff_dicts(old_flat, new_flat)

            self._event_bus.publish(Event(
                event_type=EventType.CONFIG_UPDATED,
                source="config_manager",
                payload={
                    'reason': reason,
                    'changes': changes,
                    'old_config_keys': list(old_config.keys()),
                    'new_config_keys': list(new_config.keys()),
                },
            ))
        except Exception:
            pass

    def _publish_single_change_event(self, key_path: str, old_value: Any, new_value: Any):
        """发布单个配置项变更事件"""
        if self._event_bus is None:
            try:
                self._event_bus = get_global_event_bus()
            except Exception:
                return

        try:
            self._event_bus.publish(Event(
                event_type=EventType.CONFIG_UPDATED,
                source="config_manager",
                payload={
                    'reason': 'single_set',
                    'changes': [{
                        'path': key_path,
                        'old': old_value,
                        'new': new_value,
                    }],
                },
            ))
        except Exception:
            pass

    # ─── 信息 ─────────────────────────────────────────────────

    def get_file_paths(self) -> List[str]:
        return list(self._file_paths)

    def get_env_overridden_keys(self) -> List[str]:
        return list(self._env_overridden_keys)

    def is_initialized(self) -> bool:
        return self._initialized

    def to_dict(self) -> dict:
        return {
            'file_paths': self._file_paths,
            'env_prefix': self._env_prefix,
            'auto_env_override': self._auto_env_override,
            'snapshot_enabled': self._snapshot_enabled,
            'initialized': self._initialized,
            'env_overridden_keys': self._env_overridden_keys,
            'config_keys': list(self._config.keys()) if self._initialized else [],
        }


# ═══════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════

_global_config_manager: Optional[ConfigManager] = None
_global_config_lock = threading.Lock()


def get_global_config_manager(
    file_paths: Optional[List[str]] = None,
    env_prefix: str = "BINANCE",
) -> ConfigManager:
    """获取全局配置管理器（单例）"""
    global _global_config_manager
    if _global_config_manager is None:
        with _global_config_lock:
            if _global_config_manager is None:
                _global_config_manager = ConfigManager(
                    file_paths=file_paths,
                    env_prefix=env_prefix,
                )
    return _global_config_manager


def init_config(
    file_paths: List[str],
    env_prefix: str = "BINANCE",
    auto_env_override: bool = True,
) -> Result[None]:
    """初始化全局配置管理器"""
    global _global_config_manager
    with _global_config_lock:
        _global_config_manager = ConfigManager(
            file_paths=file_paths,
            env_prefix=env_prefix,
            auto_env_override=auto_env_override,
        )
    return _global_config_manager.on_init()


def get_config(key_path: str, default: Any = None) -> Any:
    """便捷函数：从全局配置管理器获取值"""
    mgr = get_global_config_manager()
    return mgr.get(key_path, default)


def set_config(key_path: str, value: Any) -> Result[None]:
    """便捷函数：设置全局配置"""
    mgr = get_global_config_manager()
    return mgr.set(key_path, value)


def reload_config() -> Result[None]:
    """便捷函数：重载配置"""
    mgr = get_global_config_manager()
    return mgr.reload()
