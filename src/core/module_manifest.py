"""
src/core/module_manifest.py
MA26-ATR 量化交易系统 — 核心原语：模块清单

设计理念:
  模块清单是系统的"组件目录"，集中声明所有可用模块的元数据：
    1. 模块身份（名称、版本、描述、作者）
    2. 依赖声明（启动顺序、版本约束）
    3. 配置 Schema（参数名、类型、范围、默认值）
    4. 能力标签（数据源/策略/风控/执行/监控）
    5. 生命周期类入口
    6. 从 YAML/JSON 文件加载（支持动态发现）

  与 LifecycleManager 的关系:
    ManifestRegistry 提供模块元数据 → LifecycleManager 负责启停编排

  与 StateRegistry 的关系:
    ManifestRegistry 注册模块 → StateRegistry 追踪运行时状态

  模块发现流程:
    manifest.yaml → ModuleManifest → 校验 → 实例化 → 注册到 LifecycleManager
"""

import os
import importlib
import inspect
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Type, Any, Callable
from enum import Enum
from pathlib import Path

import yaml

from .result import Result, Error, ErrorSeverity, ErrorCategory
from .lifecycle import LifecycleAware, LifecycleManager, get_global_lifecycle_manager
from .state_registry import (
    StateRegistry, ModuleStatus, get_global_state_registry,
)
from .config_snapshot import ConfigValidator


# ═══════════════════════════════════════════════════════════════
# 模块能力标签
# ═══════════════════════════════════════════════════════════════

class ModuleCapability(Enum):
    """
    模块能力标签 — 描述模块的功能分类。
    """
    DATA_SOURCE = "data_source"           # 数据源（行情/订单簿/交易）
    STRATEGY = "strategy"                # 策略（信号生成）
    RISK = "risk"                        # 风控（限额/熔断/检查）
    EXECUTION = "execution"              # 执行（订单/成交）
    PORTFOLIO = "portfolio"              # 组合管理（持仓/盈亏）
    MONITOR = "monitor"                  # 监控（日志/指标/告警）
    BACKTEST = "backtest"                # 回测引擎
    BROKER = "broker"                    # 经纪商接口
    UTILITY = "utility"                  # 工具（时间/计算/序列化）
    GATEWAY = "gateway"                  # 外部网关


class ModuleTier(Enum):
    """
    模块层级 — 影响启动优先级。
    """
    CORE = "core"                       # 核心（配置/状态/事件总线）
    INFRA = "infra"                     # 基础设施（数据/日志/监控）
    BUSINESS = "business"               # 业务逻辑（策略/风控/执行）
    EXTERNAL = "external"               # 外部依赖（交易所/数据库）


# ═══════════════════════════════════════════════════════════════
# 配置参数描述
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ConfigParam:
    """
    配置参数描述 — 声明模块所需的配置项。
    """
    name: str                           # 参数名（如 "ma_period"）
    param_type: str                     # 类型（int/float/str/bool/list/dict）
    required: bool = True               # 是否必填
    default: Any = None                 # 默认值
    min_value: Optional[float] = None   # 最小值
    max_value: Optional[float] = None   # 最大值
    choices: Optional[List[Any]] = None # 可选值列表
    description: str = ""               # 描述
    path: str = ""                      # 在配置中的完整路径（如 "strategy.ma_period"）

    def validate(self, value: Any) -> Result[None]:
        """校验参数值"""
        # 类型检查
        type_map = {
            'int': int, 'float': (int, float), 'str': str,
            'bool': bool, 'list': list, 'dict': dict,
        }
        expected = type_map.get(self.param_type)
        if expected and not isinstance(value, expected):
            return Result.err(Error(
                code="MANIFEST_001",
                message=f"参数 {self.name} 类型错误: 期望 {self.param_type}, 实际 {type(value).__name__}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.VALIDATION,
            ))

        # 范围检查
        if isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                return Result.err(Error(
                    code="MANIFEST_002",
                    message=f"参数 {self.name} 值 {value} 小于最小值 {self.min_value}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))
            if self.max_value is not None and value > self.max_value:
                return Result.err(Error(
                    code="MANIFEST_003",
                    message=f"参数 {self.name} 值 {value} 大于最大值 {self.max_value}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))

        # 可选值检查
        if self.choices is not None and value not in self.choices:
            return Result.err(Error(
                code="MANIFEST_004",
                message=f"参数 {self.name} 值 {value} 不在可选列表中: {self.choices}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.VALIDATION,
            ))

        return Result.ok(None)

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'type': self.param_type,
            'required': self.required,
            'default': self.default,
            'min': self.min_value,
            'max': self.max_value,
            'choices': self.choices,
            'description': self.description,
            'path': self.path,
        }


# ═══════════════════════════════════════════════════════════════
# 依赖约束
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class DependencyConstraint:
    """
    依赖约束 — 声明对另一个模块的依赖及版本要求。
    """
    module_name: str                    # 依赖的模块名
    min_version: Optional[str] = None   # 最低版本
    max_version: Optional[str] = None   # 最高版本
    required: bool = True               # 是否必须
    capability_match: Optional[ModuleCapability] = None  # 能力匹配（不指定具体模块名）

    def to_dict(self) -> dict:
        return {
            'module': self.module_name,
            'min_version': self.min_version,
            'max_version': self.max_version,
            'required': self.required,
            'capability': self.capability_match.value if self.capability_match else None,
        }


# ═══════════════════════════════════════════════════════════════
# 模块清单
# ═══════════════════════════════════════════════════════════════

@dataclass
class ModuleManifest:
    """
    模块清单 — 描述一个模块的完整元数据。

    可以从 YAML 文件加载，也可以代码创建。
    """
    # 身份
    name: str                           # 模块名（唯一，如 "m01_strategy"）
    version: str                        # 版本（语义化，如 "1.2.0"）
    description: str = ""               # 描述
    author: str = ""                    # 作者
    tier: ModuleTier = ModuleTier.BUSINESS
    capabilities: List[ModuleCapability] = field(default_factory=list)

    # 入口
    entry_class: Optional[str] = None   # 生命周期类全路径（如 "src.modules.strategy.MA26Strategy"）
    entry_module: Optional[str] = None  # 模块路径（如 "src.modules.strategy"）

    # 依赖
    dependencies: List[DependencyConstraint] = field(default_factory=list)

    # 配置
    config_params: List[ConfigParam] = field(default_factory=list)
    config_section: str = ""            # 配置文件中对应的 section（如 "strategy"）

    # 运行时
    auto_start: bool = True             # 是否自动启动
    singleton: bool = True              # 是否单例
    max_instances: int = 1              # 最大实例数

    # 资源
    cpu_weight: float = 1.0             # CPU 权重（用于资源分配）
    memory_mb: Optional[int] = None     # 预估内存占用

    # 扩展
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.config_section:
            object.__setattr__(self, 'config_section', self.name)

    def get_capability_names(self) -> List[str]:
        return [c.value for c in self.capabilities]

    def get_dependency_names(self) -> List[str]:
        return [d.module_name for d in self.dependencies if not d.capability_match]

    def get_config_param(self, name: str) -> Optional[ConfigParam]:
        for p in self.config_params:
            if p.name == name:
                return p
        return None

    def validate_config(self, config_section: dict) -> Result[List[str]]:
        """
        校验配置段是否满足本模块的 ConfigParam 要求。
        返回 Ok([warnings]) 或 Err。
        """
        warnings = []
        errors = []

        for param in self.config_params:
            value = config_section.get(param.name)

            if value is None:
                if param.required and param.default is None:
                    errors.append(f"缺少必填参数: {param.name}")
                    continue
                elif param.default is not None:
                    value = param.default
                    warnings.append(f"参数 {param.name} 使用默认值: {param.default}")
            else:
                # 校验参数值
                val_result = param.validate(value)
                if val_result.is_err():
                    errors.append(val_result.unwrap_err().message)

        if errors:
            return Result.err(Error(
                code="MANIFEST_005",
                message=f"模块 {self.name} 配置校验失败: {'; '.join(errors)}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
                context={'errors': errors, 'warnings': warnings},
            ))

        return Result.ok(warnings)

    def load_entry_class(self) -> Result[Type[LifecycleAware]]:
        """
        动态加载入口类。
        """
        if not self.entry_class:
            return Result.err(Error(
                code="MANIFEST_006",
                message=f"模块 {self.name} 未指定 entry_class",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        try:
            # 解析模块路径和类名
            if '.' in self.entry_class:
                parts = self.entry_class.split('.')
                module_path = '.'.join(parts[:-1])
                class_name = parts[-1]
            else:
                if not self.entry_module:
                    return Result.err(Error(
                        code="MANIFEST_007",
                        message=f"模块 {self.name} 需要 entry_module 或完整 entry_class 路径",
                        severity=ErrorSeverity.RECOVERABLE,
                        category=ErrorCategory.CONFIG,
                    ))
                module_path = self.entry_module
                class_name = self.entry_class

            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)

            if not inspect.isclass(cls) or not issubclass(cls, LifecycleAware):
                return Result.err(Error(
                    code="MANIFEST_008",
                    message=f"入口类 {self.entry_class} 不是 LifecycleAware 的子类",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))

            return Result.ok(cls)
        except Exception as e:
            return Result.err(Error(
                code="MANIFEST_009",
                message=f"加载入口类失败 {self.entry_class}: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))

    def instantiate(self, **kwargs) -> Result[LifecycleAware]:
        """
        实例化模块。
        """
        cls_result = self.load_entry_class()
        if cls_result.is_err():
            return Result.err(cls_result.unwrap_err())

        cls = cls_result.unwrap()
        try:
            instance = cls(**kwargs)
            return Result.ok(instance)
        except Exception as e:
            return Result.err(Error(
                code="MANIFEST_010",
                message=f"实例化模块 {self.name} 失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.INTERNAL,
            ))

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'version': self.version,
            'description': self.description,
            'author': self.author,
            'tier': self.tier.value,
            'capabilities': self.get_capability_names(),
            'entry_class': self.entry_class,
            'entry_module': self.entry_module,
            'dependencies': [d.to_dict() for d in self.dependencies],
            'config_params': [p.to_dict() for p in self.config_params],
            'config_section': self.config_section,
            'auto_start': self.auto_start,
            'singleton': self.singleton,
            'max_instances': self.max_instances,
            'cpu_weight': self.cpu_weight,
            'memory_mb': self.memory_mb,
            'tags': self.tags,
            'metadata': self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Result['ModuleManifest']:
        """从字典创建"""
        try:
            # 解析 capabilities
            caps = []
            for c in data.get('capabilities', []):
                try:
                    caps.append(ModuleCapability(c))
                except ValueError:
                    pass

            # 解析 tier
            tier = ModuleTier(data.get('tier', 'business'))

            # 解析 dependencies
            deps = []
            for d in data.get('dependencies', []):
                deps.append(DependencyConstraint(
                    module_name=d.get('module', ''),
                    min_version=d.get('min_version'),
                    max_version=d.get('max_version'),
                    required=d.get('required', True),
                    capability_match=ModuleCapability(d['capability']) if d.get('capability') else None,
                ))

            # 解析 config_params
            params = []
            for p in data.get('config_params', []):
                params.append(ConfigParam(
                    name=p['name'],
                    param_type=p.get('type', 'str'),
                    required=p.get('required', True),
                    default=p.get('default'),
                    min_value=p.get('min'),
                    max_value=p.get('max'),
                    choices=p.get('choices'),
                    description=p.get('description', ''),
                    path=p.get('path', ''),
                ))

            manifest = cls(
                name=data['name'],
                version=data.get('version', '0.0.0'),
                description=data.get('description', ''),
                author=data.get('author', ''),
                tier=tier,
                capabilities=caps,
                entry_class=data.get('entry_class'),
                entry_module=data.get('entry_module'),
                dependencies=deps,
                config_params=params,
                config_section=data.get('config_section', ''),
                auto_start=data.get('auto_start', True),
                singleton=data.get('singleton', True),
                max_instances=data.get('max_instances', 1),
                cpu_weight=data.get('cpu_weight', 1.0),
                memory_mb=data.get('memory_mb'),
                tags=data.get('tags', {}),
                metadata=data.get('metadata', {}),
            )
            return Result.ok(manifest)
        except Exception as e:
            return Result.err(Error(
                code="MANIFEST_011",
                message=f"解析模块清单失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    @classmethod
    def from_yaml_file(cls, filepath: str) -> Result['ModuleManifest']:
        """从 YAML 文件加载"""
        try:
            path = Path(filepath)
            if not path.exists():
                return Result.err(Error(
                    code="MANIFEST_012",
                    message=f"清单文件不存在: {filepath}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))

            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            if isinstance(data, list):
                # 如果文件包含多个清单，取第一个
                if not data:
                    return Result.err(Error(
                        code="MANIFEST_013",
                        message=f"清单文件为空: {filepath}",
                        severity=ErrorSeverity.RECOVERABLE,
                        category=ErrorCategory.CONFIG,
                    ))
                data = data[0]

            return cls.from_dict(data)
        except Exception as e:
            return Result.err(Error(
                code="MANIFEST_014",
                message=f"加载清单文件失败 {filepath}: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

    def save_to_file(self, filepath: str) -> Result[None]:
        """保存到 YAML 文件"""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.safe_dump(self.to_dict(), f, default_flow_style=False, allow_unicode=True)
            return Result.ok(None)
        except Exception as e:
            return Result.err(Error(
                code="MANIFEST_015",
                message=f"保存清单文件失败: {e}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))


# ═══════════════════════════════════════════════════════════════
# 清单注册表
# ═══════════════════════════════════════════════════════════════

class ManifestRegistry:
    """
    模块清单注册表 — 管理所有模块的元数据。

    职责:
      1. 注册/注销模块清单
      2. 依赖解析与循环检测
      3. 按能力/层级查询模块
      4. 与 LifecycleManager 协作（自动注册和启动）
      5. 从目录批量加载清单文件

    用法:
      registry = ManifestRegistry()

      # 注册清单
      manifest = ModuleManifest(
          name="m01_strategy",
          version="1.0.0",
          capabilities=[ModuleCapability.STRATEGY],
          entry_class="src.modules.strategy.MA26Strategy",
          dependencies=[DependencyConstraint("m04_risk")],
      )
      registry.register(manifest)

      # 从目录加载
      registry.load_from_directory("./modules")

      # 获取启动顺序
      order = registry.resolve_startup_order()

      # 实例化并注册到 LifecycleManager
      for name in order:
          manifest = registry.get(name)
          instance = manifest.instantiate()
          lifecycle_manager.register(instance)
    """

    def __init__(
        self,
        lifecycle_manager: Optional[LifecycleManager] = None,
        state_registry: Optional[StateRegistry] = None,
        auto_register_lifecycle: bool = False,
    ):
        self._manifests: Dict[str, ModuleManifest] = {}
        self._lock = threading.RLock()
        self._lifecycle_manager = lifecycle_manager
        self._state_registry = state_registry
        self._auto_register = auto_register_lifecycle
        self._loaded_paths: List[str] = []

    def register(self, manifest: ModuleManifest) -> Result[None]:
        """注册模块清单"""
        with self._lock:
            if manifest.name in self._manifests:
                return Result.err(Error(
                    code="MANIFEST_016",
                    message=f"模块清单已存在: {manifest.name}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.VALIDATION,
                ))
            self._manifests[manifest.name] = manifest

        return Result.ok(None)

    def unregister(self, module_name: str) -> Result[None]:
        """注销模块清单"""
        with self._lock:
            if module_name not in self._manifests:
                return Result.err(Error(
                    code="MANIFEST_017",
                    message=f"模块清单不存在: {module_name}",
                    severity=ErrorSeverity.INFO,
                    category=ErrorCategory.VALIDATION,
                ))
            del self._manifests[module_name]
        return Result.ok(None)

    def get(self, module_name: str) -> Optional[ModuleManifest]:
        """获取模块清单"""
        return self._manifests.get(module_name)

    def get_all(self) -> Dict[str, ModuleManifest]:
        """获取所有清单"""
        with self._lock:
            return dict(self._manifests)

    def get_by_capability(self, capability: ModuleCapability) -> List[ModuleManifest]:
        """按能力查询"""
        with self._lock:
            return [m for m in self._manifests.values() if capability in m.capabilities]

    def get_by_tier(self, tier: ModuleTier) -> List[ModuleManifest]:
        """按层级查询"""
        with self._lock:
            return [m for m in self._manifests.values() if m.tier == tier]

    def get_auto_start_modules(self) -> List[ModuleManifest]:
        """获取所有自动启动的模块"""
        with self._lock:
            return [m for m in self._manifests.values() if m.auto_start]

    # ─── 依赖解析 ─────────────────────────────────────────────

    def resolve_startup_order(self) -> Result[List[str]]:
        """
        解析启动顺序（拓扑排序）。
        被依赖的模块先启动。
        """
        visited: Set[str] = set()
        visiting: Set[str] = set()
        result: List[str] = []

        def visit(name: str) -> Result[None]:
            if name in visiting:
                return Result.err(Error(
                    code="MANIFEST_018",
                    message=f"循环依赖检测: {name}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.VALIDATION,
                ))
            if name in visited:
                return Result.ok(None)

            manifest = self._manifests.get(name)
            if not manifest:
                return Result.err(Error(
                    code="MANIFEST_019",
                    message=f"依赖模块未注册: {name}",
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.VALIDATION,
                ))

            visiting.add(name)

            for dep in manifest.dependencies:
                if dep.capability_match:
                    # 按能力匹配：找到具有该能力的模块
                    capable = self.get_by_capability(dep.capability_match)
                    if not capable:
                        if dep.required:
                            return Result.err(Error(
                                code="MANIFEST_020",
                                message=f"未找到具有能力 {dep.capability_match.value} 的模块",
                                severity=ErrorSeverity.CRITICAL,
                                category=ErrorCategory.VALIDATION,
                            ))
                    else:
                        for m in capable:
                            dep_result = visit(m.name)
                            if dep_result.is_err():
                                return dep_result
                else:
                    dep_result = visit(dep.module_name)
                    if dep_result.is_err():
                        if dep.required:
                            return dep_result

            visiting.remove(name)
            visited.add(name)
            result.append(name)
            return Result.ok(None)

        with self._lock:
            module_names = list(self._manifests.keys())

        for name in module_names:
            if name not in visited:
                sort_result = visit(name)
                if sort_result.is_err():
                    return Result.err(sort_result.unwrap_err())

        return Result.ok(result)

    def validate_dependencies(self) -> Result[List[str]]:
        """
        校验所有依赖是否满足。
        返回 Ok([warnings]) 或 Err。
        """
        warnings = []
        errors = []

        with self._lock:
            for name, manifest in self._manifests.items():
                for dep in manifest.dependencies:
                    if dep.capability_match:
                        capable = self.get_by_capability(dep.capability_match)
                        if not capable and dep.required:
                            errors.append(
                                f"{name} 需要能力 {dep.capability_match.value}，但未找到提供该能力的模块"
                            )
                    else:
                        if dep.module_name not in self._manifests and dep.required:
                            errors.append(
                                f"{name} 依赖 {dep.module_name}，但未注册"
                            )

        if errors:
            return Result.err(Error(
                code="MANIFEST_021",
                message=f"依赖校验失败: {'; '.join(errors)}",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.VALIDATION,
                context={'errors': errors, 'warnings': warnings},
            ))

        return Result.ok(warnings)

    # ─── 批量加载 ─────────────────────────────────────────────

    def load_from_directory(
        self,
        directory: str,
        pattern: str = "manifest.yaml",
    ) -> Result[int]:
        """
        从目录批量加载清单文件。
        支持 directory/manifest.yaml 或 directory/*/manifest.yaml。
        """
        loaded = 0
        errors = []

        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                return Result.err(Error(
                    code="MANIFEST_022",
                    message=f"目录不存在: {directory}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.CONFIG,
                ))

            # 查找所有匹配的文件
            if pattern in ("manifest.yaml", "manifest.yml"):
                # 先查根目录
                for ext in ('.yaml', '.yml'):
                    root_manifest = dir_path / f"manifest{ext}"
                    if root_manifest.exists():
                        result = ModuleManifest.from_yaml_file(str(root_manifest))
                        if result.is_ok():
                            self.register(result.unwrap())
                            loaded += 1
                        else:
                            errors.append(result.unwrap_err().message)

                # 查子目录
                for subdir in dir_path.iterdir():
                    if subdir.is_dir():
                        for ext in ('.yaml', '.yml'):
                            manifest_file = subdir / f"manifest{ext}"
                            if manifest_file.exists():
                                result = ModuleManifest.from_yaml_file(str(manifest_file))
                                if result.is_ok():
                                    self.register(result.unwrap())
                                    loaded += 1
                                else:
                                    errors.append(result.unwrap_err().message)
            else:
                # glob 模式
                for filepath in dir_path.glob(pattern):
                    if filepath.is_file():
                        result = ModuleManifest.from_yaml_file(str(filepath))
                        if result.is_ok():
                            self.register(result.unwrap())
                            loaded += 1
                        else:
                            errors.append(result.unwrap_err().message)

            self._loaded_paths.append(directory)
        except Exception as e:
            errors.append(str(e))

        if loaded == 0 and errors:
            return Result.err(Error(
                code="MANIFEST_023",
                message=f"未加载任何清单: {'; '.join(errors)}",
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.CONFIG,
            ))

        return Result.ok(loaded)

    # ─── 实例化与注册到 LifecycleManager ──────────────────────

    def instantiate_and_register_all(
        self,
        config: Optional[Dict[str, Any]] = None,
        lifecycle_manager: Optional[LifecycleManager] = None,
    ) -> Result[Dict[str, LifecycleAware]]:
        """
        实例化所有自动启动的模块并注册到 LifecycleManager。
        """
        lm = lifecycle_manager or self._lifecycle_manager
        if not lm:
            lm = get_global_lifecycle_manager()

        # 解析启动顺序
        order_result = self.resolve_startup_order()
        if order_result.is_err():
            return Result.err(order_result.unwrap_err())

        order = order_result.unwrap()
        instances: Dict[str, LifecycleAware] = {}

        for name in order:
            manifest = self._manifests.get(name)
            if not manifest or not manifest.auto_start:
                continue

            # 校验配置
            if config and manifest.config_section in config:
                config_section = config[manifest.config_section]
                val_result = manifest.validate_config(config_section)
                if val_result.is_err():
                    return Result.err(val_result.unwrap_err())

            # 实例化
            inst_result = manifest.instantiate()
            if inst_result.is_err():
                return Result.err(inst_result.unwrap_err())

            instance = inst_result.unwrap()

            # 注册到 LifecycleManager
            reg_result = lm.register(instance)
            if reg_result.is_err():
                return Result.err(reg_result.unwrap_err())

            instances[name] = instance

        return Result.ok(instances)

    # ─── 统计 ─────────────────────────────────────────────────

    def stats(self) -> dict:
        """统计信息"""
        with self._lock:
            by_tier = {}
            by_capability = {}
            for m in self._manifests.values():
                by_tier[m.tier.value] = by_tier.get(m.tier.value, 0) + 1
                for cap in m.capabilities:
                    by_capability[cap.value] = by_capability.get(cap.value, 0) + 1

            return {
                'total_modules': len(self._manifests),
                'auto_start': sum(1 for m in self._manifests.values() if m.auto_start),
                'by_tier': by_tier,
                'by_capability': by_capability,
                'loaded_paths': self._loaded_paths,
            }


# ═══════════════════════════════════════════════════════════════
# 内置模块清单（系统核心模块）
# ═══════════════════════════════════════════════════════════════

def create_core_manifests() -> List[ModuleManifest]:
    """
    创建系统核心模块的清单。
    这些模块由 bootstrap 自动注册，不需要 manifest.yaml。
    """
    return [
        ModuleManifest(
            name="core_config",
            version="4.1.0",
            description="Configuration management",
            author="system",
            tier=ModuleTier.CORE,
            capabilities=[ModuleCapability.UTILITY],
            entry_class="src.core.config.ConfigManager",
            auto_start=True,
        ),
        ModuleManifest(
            name="core_state",
            version="4.1.0",
            description="System state registry",
            author="system",
            tier=ModuleTier.CORE,
            capabilities=[ModuleCapability.UTILITY],
            entry_class="src.core.state_registry.StateRegistry",
            auto_start=True,
        ),
        ModuleManifest(
            name="core_event_bus",
            version="4.1.0",
            description="Event bus",
            author="system",
            tier=ModuleTier.CORE,
            capabilities=[ModuleCapability.UTILITY],
            entry_class="src.core.event_bus.EventBus",
            auto_start=True,
        ),
        ModuleManifest(
            name="core_lifecycle",
            version="4.1.0",
            description="Lifecycle management",
            author="system",
            tier=ModuleTier.CORE,
            capabilities=[ModuleCapability.UTILITY],
            entry_class="src.core.lifecycle.LifecycleManager",
            auto_start=True,
        ),
    ]


# ═══════════════════════════════════════════════════════════════
# 全局清单注册表
# ═══════════════════════════════════════════════════════════════

_global_manifest_registry = ManifestRegistry(auto_register_lifecycle=False)


def get_global_manifest_registry() -> ManifestRegistry:
    """获取全局清单注册表"""
    return _global_manifest_registry


def register_manifest(manifest: ModuleManifest) -> Result[None]:
    """便捷函数：注册模块清单"""
    return _global_manifest_registry.register(manifest)


def get_manifest(module_name: str) -> Optional[ModuleManifest]:
    """便捷函数：获取模块清单"""
    return _global_manifest_registry.get(module_name)
