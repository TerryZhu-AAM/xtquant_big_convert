"""单文件直跑自举 (BMG4-07): pytest tests/xx.py 冷启动时无兄弟模块先注入 src 路径.

各测试文件历史上各自 ``sys.path.insert(ROOT/src)``, 漏网文件
(test_xtquant_shim_import.py / test_no_BUG_20260901_ghost_execution.py) 单文件跑
即 ModuleNotFoundError — 全套件跑时被先导入的兄弟模块喂饱 (隔离运行假红, 与
「锁被喂饱假绿」同族的收集形态)。conftest 统一自举, 既有文件内的 insert 幂等无害。
"""
import os
import sys

_SRC = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"
)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
