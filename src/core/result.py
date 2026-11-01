"""core/result.py — Result[T, Error] 错误传播协议"""

from enum import IntEnum
from dataclasses import dataclass
from typing import Generic, TypeVar, Optional, Callable

T = TypeVar('T')
E = TypeVar('E')


class ErrorSeverity(IntEnum):
    TRANSIENT = 1
    RECOVERABLE = 2
    DEGRADED = 3
    CRITICAL = 4
    FATAL = 5


@dataclass
class Error:
    code: str
    message: str
    severity: ErrorSeverity = ErrorSeverity.RECOVERABLE
    source: str = ""
    context: dict = None
    timestamp: int = 0

    def __post_init__(self):
        if self.context is None:
            self.context = {}

    def is_retryable(self) -> bool:
        return self.severity == ErrorSeverity.TRANSIENT

    def is_fatal(self) -> bool:
        return self.severity in (ErrorSeverity.CRITICAL, ErrorSeverity.FATAL)

    def format_chain(self) -> str:
        return f"[{self.severity.name}] {self.source}: {self.code} - {self.message}"


class Errors:
    @staticmethod
    def nan_detected(source: str, ctx: str = "") -> Error:
        return Error("NAN_DETECTED", f"NaN in {ctx}", ErrorSeverity.RECOVERABLE, source)

    @staticmethod
    def division_by_zero(source: str, ctx: str = "") -> Error:
        return Error("DIVISION_BY_ZERO", f"Division by zero in {ctx}",
                     ErrorSeverity.RECOVERABLE, source)

    @staticmethod
    def invalid_config(source: str, field: str, msg: str) -> Error:
        return Error("INVALID_CONFIG", f"{field}: {msg}", ErrorSeverity.FATAL, source)

    @staticmethod
    def state_mismatch(source: str, msg: str) -> Error:
        return Error("STATE_MISMATCH", msg, ErrorSeverity.CRITICAL, source)

    @staticmethod
    def insufficient_funds(source: str, needed: float, avail: float) -> Error:
        return Error("INSUFFICIENT_FUNDS", f"need {needed} have {avail}",
                     ErrorSeverity.RECOVERABLE, source)

    @staticmethod
    def order_rejected(source: str, reason: str) -> Error:
        return Error("ORDER_REJECTED", reason, ErrorSeverity.RECOVERABLE, source)

    @staticmethod
    def future_data(source: str, bar: int, current: int) -> Error:
        return Error("FUTURE_DATA", f"bar {bar} > current {current}", ErrorSeverity.FATAL, source)

    @staticmethod
    def dependency_missing(source: str, dep: str) -> Error:
        return Error("DEPENDENCY_MISSING", dep, ErrorSeverity.CRITICAL, source)


@dataclass
class Result(Generic[T, E]):
    value: Optional[T] = None
    error: Optional[E] = None

    @classmethod
    def ok(cls, value: T) -> 'Result':
        return cls(value=value)

    @classmethod
    def err(cls, error: E) -> 'Result':
        return cls(error=error)

    def is_ok(self) -> bool:
        return self.error is None

    def is_err(self) -> bool:
        return self.error is not None

    def unwrap(self) -> T:
        if self.error:
            raise RuntimeError(f"Unwrap on error: {self.error.format_chain()}")
        return self.value

    def unwrap_or(self, default: T) -> T:
        return self.value if self.error is None else default

    def map(self, fn: Callable[[T], 'U']) -> 'Result[U, E]':
        from typing import TypeVar
        U = TypeVar('U')
        if self.error:
            return Result.err(self.error)  # type: ignore[return-value]
        try:
            return Result.ok(fn(self.value))
        except Exception as e:
            return Result.err(Error("MAP_ERROR", str(e), ErrorSeverity.RECOVERABLE))  # type: ignore[return-value]

    def and_then(self, fn):
        if self.error:
            return Result.err(self.error)  # type: ignore[return-value]
        return fn(self.value)

    def or_else(self, fn):
        if self.error:
            return fn(self.error)
        return self

