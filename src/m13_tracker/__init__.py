"""
src/m13_tracker/__init__.py
M13 — 信号追踪模块

信号去重 / 信号→订单血缘链 / 重复拦截 / 信号生命周期
"""

from .signal_tracker import (
    SignalTracker,
    SignalRecord,
    SignalLineage,
    DedupResult,
    SignalState,
    TraceNode,
)

__all__ = [
    "SignalTracker",
    "SignalRecord",
    "SignalLineage",
    "DedupResult",
    "SignalState",
    "TraceNode",
]
