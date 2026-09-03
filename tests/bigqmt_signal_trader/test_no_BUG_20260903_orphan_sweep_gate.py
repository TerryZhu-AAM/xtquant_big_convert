"""[BUG-20260903-multiproc-sweep] 孤儿订阅清扫进程身份门 · 默认关·显式开 (BMG3-04 闭环锁).

背景 (2026-09-03 实弹事故):
  孤儿清扫判据 = 「seq 不在本进程 _quote_callbacks」, 但测试/preflight 进程同样经真
  .env 构造桥客户端并武装同一清扫 — 它们的 callbacks 为空 → 把后端进程的活跃订阅当
  孤儿 hdel (当日 19:03 后端写入 4 条真实订阅, 19:06-19:21 间被无日志静默清空,
  quote_events 通道 25s 0 事件 → 实时 tick 断供). 清扫 docstring 自认「多后端进程
  并存属未支持部署形态, 跨进程保护不设防」.

修复 (根因 = 只有「订阅唯一属主」进程才有资格清孤儿):
  武装点 _start_quote_listener 抽出 _arm_orphan_sweep, 由
  _bool_value(os.environ["BIGQMT_ORPHAN_SWEEP"], False) 门控 — 默认关, 只有后端主进程
  (app/main.py setdefault 开) 会武装; 测试/preflight 进程不再武装. 直接调
  _sweep_orphan_subscriptions 的显式路径不受影响 (既有语义).

本锁 (防未来回退):
  - 行为测: 默认环境 (无变量) 武装零 Timer.
  - 行为测: 显式 falsey ("0"/"false"/"") 仍零武装.
  - 行为测: 显式 "1" 恰武装一次且 daemon.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import xtquant_compat
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

_ENV_KEY = "BIGQMT_ORPHAN_SWEEP"


class _FakeTimer:
    started = False

    def __init__(self, interval, function):
        _FakeTimer.instances.append(self)

    def start(self):
        self.started = True


_FakeTimer.instances = []


def _make_host():
    """__new__ 轻量宿主: 只备齐 _start_quote_listener 触及的属性, 事件循环替身为 no-op."""
    host = BigQmtXtData.__new__(BigQmtXtData)
    host._quote_event_thread = None
    host._quote_event_running = False
    host._quote_event_loop = lambda: None
    return host


class OrphanSweepGateTest(unittest.TestCase):
    def setUp(self):
        _FakeTimer.instances = []
        self._saved = os.environ.pop(_ENV_KEY, None)

    def tearDown(self):
        if self._saved is not None:
            os.environ[_ENV_KEY] = self._saved
        else:
            os.environ.pop(_ENV_KEY, None)

    def _arm(self):
        host = _make_host()
        with mock.patch.object(xtquant_compat.threading, "Timer", _FakeTimer):
            host._start_quote_listener()

    def test_default_env_arms_nothing(self):
        """无变量 (测试/未改造进程的默认态) → 零 Timer 武装. 红证: 修复前武装即红."""
        self._arm()
        self.assertEqual(_FakeTimer.instances, [])

    def test_falsey_env_still_off(self):
        for falsey in ("0", "false", ""):
            with self.subTest(falsey=falsey):
                os.environ[_ENV_KEY] = falsey
                self._arm()
                self.assertEqual(_FakeTimer.instances, [])

    def test_explicit_opt_in_arms_once_as_daemon(self):
        os.environ[_ENV_KEY] = "1"
        self._arm()
        self.assertEqual(len(_FakeTimer.instances), 1)
        self.assertTrue(_FakeTimer.instances[0].started)


if __name__ == "__main__":
    unittest.main()
