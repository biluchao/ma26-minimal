"""
tests/__init__.py
测试包 — MA26 最小运行集验证

三层测试:
  test_core.py         → 核心原语（Result/EventBus/Trace/Config/State）
  test_modules.py      → 业务模块逻辑（Strategy/Risk/Position/Execution）
  test_integration.py  → 端到端回测闭环
"""

import sys
import os

# 确保项目根目录在 sys.path 中
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

__version__ = "4.1.0-minimal-test"
