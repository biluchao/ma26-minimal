"""
src/core/result.py
MA26-ATR 量化交易系统 — 核心原语：Result 类型与错误处理

设计理念:
  借鉴 Rust 的 Result<T, E> 模式，强制调用方显式处理成功/失败分支。
  所有模块的公共方法均返回 Result，禁止抛出异常跨越模块边界。

  Result.ok(value)   → Ok(value)
  Result.err(error)  → Err(error)

  result.is_ok() / result.is_err()
  result.unwrap() / result.unwrap_err() / result.unwrap_or(default)
  result.map(fn) / result.map_err(fn) / result.and_then(fn)

  Error 包含:
    code        — 机器可读错误码 (如 "RISK_001")
    message     — 人类可读描述
    severity    — 严重程度 (INFO / RECOVERABLE / FATAL)
    category    — 错误分类 (VALIDATION / NETWORK / INTERNAL / ...)
    source      — 产生错误的模块名
    timestamp   — 错误发生时间 (ms)
    context     — 附加上下文字典
"""

import time
from enum import Enum, auto
from typing import (
    Generic, TypeVar, Callable, Optional, Any, Union, Tuple
)

T = TypeVar('T')        # 成功值类型
E = TypeVar('E')        # 错误值类型（通常为 Error）


# ═══════════════════════════════════════════════════════════════
# 错误严重程度
# ═══════════════════════════════════════════════════════════════

class ErrorSeverity(Enum):
    """错误严重程度枚举"""
    INFO = "INFO"                       # 信息级，不影响运行
    RECOVERABLE = "RECOVERABLE"         # 可恢复，系统可继续运行
    FATAL = "FATAL"                     # 致命，需停止模块或系统


# ═══════════════════════════════════════════════════════════════
# 错误分类
# ═══════════════════════════════════════════════════════════════

class ErrorCategory(Enum):
    """错误分类枚举"""
    VALIDATION = "VALIDATION"           # 参数校验失败
    CONFIG = "CONFIG"                   # 配置错误
    NETWORK = "NETWORK"                 # 网络/连接问题
    EXCHANGE = "EXCHANGE"               # 交易所 API 错误
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"  # 数据不足
    RISK_VIOLATION = "RISK_VIOLATION"   # 风控拒绝
    INTERNAL = "INTERNAL"               # 内部逻辑错误
    TIMEOUT = "TIMEOUT"                 # 超时
    RATE_LIMIT = "RATE_LIMIT"           # 频率限制
    UNKNOWN = "UNKNOWN"                 # 未知错误


# ═══════════════════════════════════════════════════════════════
# Error 类
# ═══════════════════════════════════════════════════════════════

class Error:
    """
    系统统一错误类型。

    所有模块产生的错误必须封装为 Error 对象，通过 Result.err() 返回。
    """

    __slots__ = (
        'code', 'message', 'severity', 'category',
        'source', 'timestamp', 'context', '_frozen',
    )

    def __init__(
        self,
        code: str,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.RECOVERABLE,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        source: str = "",
        timestamp: Optional[int] = None,
        context: Optional[dict] = None,
    ):
        self.code = code
        self.message = message
        self.severity = severity
        self.category = category
        self.source = source
        self.timestamp = timestamp if timestamp is not None else int(time.time() * 1000)
        self.context = context if context is not None else {}
        self._frozen = False

    def freeze(self) -> 'Error':
        """冻结错误对象，防止后续修改（用于不可变传递）"""
        self._frozen = True
        return self

    def with_context(self, **kwargs) -> 'Error':
        """返回带附加上下文的新 Error（不修改原对象）"""
        new_ctx = dict(self.context)
        new_ctx.update(kwargs)
        return Error(
            code=self.code,
            message=self.message,
            severity=self.severity,
            category=self.category,
            source=self.source,
            timestamp=self.timestamp,
            context=new_ctx,
        )

    def is_fatal(self) -> bool:
        return self.severity == ErrorSeverity.FATAL

    def is_recoverable(self) -> bool:
        return self.severity == ErrorSeverity.RECOVERABLE

    def to_dict(self) -> dict:
        return {
            'code': self.code,
            'message': self.message,
            'severity': self.severity.value,
            'category': self.category.value,
            'source': self.source,
            'timestamp': self.timestamp,
            'context': self.context,
        }

    def __repr__(self) -> str:
        return (
            f"Error(code={self.code!r}, msg={self.message!r}, "
            f"sev={self.severity.value}, cat={self.category.value}, "
            f"src={self.source!r})"
        )

    def __str__(self) -> str:
        return f"[{self.severity.value}] {self.code} @ {self.source}: {self.message}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Error):
            return False
        return (
            self.code == other.code
            and self.message == other.message
            and self.severity == other.severity
            and self.category == other.category
            and self.source == other.source
        )

    def __hash__(self) -> int:
        return hash((self.code, self.severity, self.category, self.source))


# ═══════════════════════════════════════════════════════════════
# Result 类
# ═══════════════════════════════════════════════════════════════

class Result(Generic[T]):
    """
    统一结果类型。

    两种状态:
      Ok(value)   — 成功，携带值
      Err(error)  — 失败，携带 Error 对象

    用法:
      result = module.do_something()
      if result.is_ok():
          value = result.unwrap()
      else:
          error = result.unwrap_err()
    """

    __slots__ = ('_is_ok', '_value', '_error')

    def __init__(self, is_ok: bool, value: Any = None, error: Any = None):
        self._is_ok = is_ok
        self._value = value
        self._error = error

    # ─── 构造函数 ───────────────────────────────────────────────

    @classmethod
    def ok(cls, value: Any) -> 'Result':
        """创建成功结果"""
        return cls(is_ok=True, value=value, error=None)

    @classmethod
    def err(cls, error: Union[Error, str]) -> 'Result':
        """
        创建失败结果。
        如果传入字符串，自动包装为 Error(UNKNOWN)。
        """
        if isinstance(error, str):
            error = Error(
                code="ERR_000",
                message=error,
                severity=ErrorSeverity.RECOVERABLE,
                category=ErrorCategory.UNKNOWN,
            )
        return cls(is_ok=False, value=None, error=error)

    # ─── 状态查询 ───────────────────────────────────────────────

    def is_ok(self) -> bool:
        return self._is_ok

    def is_err(self) -> bool:
        return not self._is_ok

    # ─── 值提取 ─────────────────────────────────────────────────

    def unwrap(self) -> T:
        """
        提取成功值。
        如果是 Err，抛出 RuntimeError。
        """
        if self._is_ok:
            return self._value
        raise RuntimeError(f"尝试 unwrap Err: {self._error}")

    def unwrap_err(self) -> Error:
        """
        提取错误值。
        如果是 Ok，抛出 RuntimeError。
        """
        if not self._is_ok:
            return self._error
        raise RuntimeError(f"尝试 unwrap_err Ok: {self._value}")

    def unwrap_or(self, default: T) -> T:
        """如果是 Ok 返回值，否则返回 default"""
        if self._is_ok:
            return self._value
        return default

    def unwrap_or_else(self, fn: Callable[[Error], T]) -> T:
        """如果是 Ok 返回值，否则调用 fn(error)"""
        if self._is_ok:
            return self._value
        return fn(self._error)

    # ─── 链式操作 ───────────────────────────────────────────────

    def map(self, fn: Callable[[T], Any]) -> 'Result':
        """
        如果是 Ok，对值应用 fn 并返回新 Result。
        如果是 Err，直接传递错误。
        """
        if self._is_ok:
            try:
                return Result.ok(fn(self._value))
            except Exception as e:
                return Result.err(Error(
                    code="MAP_001",
                    message=f"map 操作异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))
        return self

    def map_err(self, fn: Callable[[Error], Error]) -> 'Result':
        """
        如果是 Err，对错误应用 fn 并返回新 Result。
        如果是 Ok，直接传递值。
        """
        if not self._is_ok:
            try:
                return Result.err(fn(self._error))
            except Exception as e:
                return Result.err(Error(
                    code="MAP_002",
                    message=f"map_err 操作异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))
        return self

    def and_then(self, fn: Callable[[T], 'Result']) -> 'Result':
        """
        如果是 Ok，对值应用 fn（fn 必须返回 Result）。
        如果是 Err，直接传递错误。
        """
        if self._is_ok:
            try:
                return fn(self._value)
            except Exception as e:
                return Result.err(Error(
                    code="AND_001",
                    message=f"and_then 操作异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))
        return self

    def or_else(self, fn: Callable[[Error], 'Result']) -> 'Result':
        """
        如果是 Err，对错误应用 fn（fn 必须返回 Result）。
        如果是 Ok，直接传递值。
        """
        if not self._is_ok:
            try:
                return fn(self._error)
            except Exception as e:
                return Result.err(Error(
                    code="OR_001",
                    message=f"or_else 操作异常: {e}",
                    severity=ErrorSeverity.RECOVERABLE,
                    category=ErrorCategory.INTERNAL,
                ))
        return self

    # ─── 组合操作 ───────────────────────────────────────────────

    def flatten(self) -> 'Result':
        """
        展平嵌套 Result（Result[Result[T]] → Result[T]）
        """
        if self._is_ok and isinstance(self._value, Result):
            return self._value
        return self

    @classmethod
    def combine(cls, results: list['Result']) -> 'Result[list]':
        """
        合并多个 Result。
        全部成功返回 Ok([values])，任一失败返回第一个 Err。
        """
        values = []
        for r in results:
            if r.is_err():
                return r
            values.append(r.unwrap())
        return Result.ok(values)

    @classmethod
    def collect(cls, results: list['Result']) -> 'Result[list]':
        """
        收集所有 Result，无论成功失败。
        返回 Ok([(i, Ok(v))] + [(i, Err(e))]) 的分离列表。
        """
        successes = []
        failures = []
        for i, r in enumerate(results):
            if r.is_ok():
                successes.append((i, r.unwrap()))
            else:
                failures.append((i, r.unwrap_err()))
        return Result.ok({'successes': successes, 'failures': failures})

    # ─── 序列化 ─────────────────────────────────────────────────

    def to_dict(self) -> dict:
        if self._is_ok:
            val = self._value
            if hasattr(val, 'to_dict'):
                val = val.to_dict()
            elif isinstance(val, (list, tuple)):
                val = [v.to_dict() if hasattr(v, 'to_dict') else v for v in val]
            return {'status': 'ok', 'value': val}
        else:
            return {'status': 'err', 'error': self._error.to_dict()}

    def __repr__(self) -> str:
        if self._is_ok:
            return f"Result.Ok({self._value!r})"
        return f"Result.Err({self._error!r})"

    def __bool__(self) -> bool:
        """允许 if result: 语法"""
        return self._is_ok

    def __iter__(self):
        """
        支持解包语法:
          if result:
              value, = result
        """
        if self._is_ok:
            yield self._value
        else:
            raise StopIteration


# ═══════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════

def ok(value: T) -> Result[T]:
    """便捷函数：创建 Ok Result"""
    return Result.ok(value)


def err(message: str, code: str = "ERR_000", source: str = "") -> Result:
    """便捷函数：从字符串创建 Err Result"""
    return Result.err(Error(
        code=code,
        message=message,
        severity=ErrorSeverity.RECOVERABLE,
        category=ErrorCategory.UNKNOWN,
        source=source,
    ))


def fatal(message: str, code: str = "FATAL_000", source: str = "") -> Result:
    """便捷函数：创建致命错误 Result"""
    return Result.err(Error(
        code=code,
        message=message,
        severity=ErrorSeverity.FATAL,
        category=ErrorCategory.INTERNAL,
        source=source,
    ))


def try_catch(fn: Callable, *args, **kwargs) -> Result:
    """
    将可能抛异常的函数调用包装为 Result。
    """
    try:
        return Result.ok(fn(*args, **kwargs))
    except Exception as e:
        return Result.err(Error(
            code="TRY_001",
            message=f"异常捕获: {e}",
            severity=ErrorSeverity.RECOVERABLE,
            category=ErrorCategory.INTERNAL,
            context={'args': str(args), 'kwargs': str(kwargs)},
        ))
