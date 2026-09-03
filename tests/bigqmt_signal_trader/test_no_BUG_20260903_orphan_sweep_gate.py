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


class _FakeRedisHash:
    """hgetall/hlen/hdel 最小假 redis — field 以 bytes 回读 (生产 redis 形态)."""

    def __init__(self, fields):
        self._fields = dict(fields)
        self.deleted = []

    def hgetall(self, key):
        return {k.encode(): v.encode() for k, v in self._fields.items()}

    def hlen(self, key):
        return len(self._fields)

    def hdel(self, key, *fields):
        for f in fields:
            f = f.decode() if isinstance(f, (bytes, bytearray)) else f
            self._fields.pop(f, None)
            self.deleted.append(f)


def _make_sweep_host(callbacks, self_seqs=()):
    host = BigQmtXtData.__new__(BigQmtXtData)
    host.client = mock.MagicMock()
    host.client.account_id = "66632458"
    host._redis_fake = _FakeRedisHash(
        {"1788435004202": "{}", "1788435004203": "{}", "1788435004204": "{}"}
    )
    host.client._redis.return_value = host._redis_fake
    host._quote_callbacks = callbacks
    host._self_seqs = set(self_seqs)
    return host


class OrphanSweepCriterionTest(unittest.TestCase):
    """[BUG-20260903-sweep-keyspace / BMG4-01] 清扫判据键空间归一 + 属主自写豁免.

    红证 (修前 HEAD 双红):
    - int 键活跃订阅被删: _quote_callbacks 键是内存 int seq (_next_seq), redis
      回读 field 是 str, 直接 membership 永真 — 2026-09-03 19:04:58 实弹: 后端
      自己的清扫删 64 条含自身在订订阅 → 当日 tick 断供真凶 (BMG3-04「跨进程
      互删」主归因据此修正, WinSW out.log 19:04:58 WARNING 铁证)。
    - 本进程自写无 callback 订阅 (常驻指数 subscribe_quote 两参形态) 被删 —
      每次重启 T+120s 复删, 大盘闸指数 1m 断流 (BMG4-01)。
    """

    def test_own_active_int_key_subscription_spared(self):
        host = _make_sweep_host({1788435004202: mock.MagicMock()})
        host._sweep_orphan_subscriptions()
        self.assertNotIn(
            "1788435004202", host._redis_fake.deleted,
            "本进程在订 seq (int 键) 被判孤儿 = 自删活跃订阅 — 19:04:58 事故形态",
        )
        self.assertIn("1788435004203", host._redis_fake.deleted)

    def test_own_callbackless_subscription_spared(self):
        host = _make_sweep_host({}, self_seqs={"1788435004204"})
        host._sweep_orphan_subscriptions()
        self.assertNotIn(
            "1788435004204", host._redis_fake.deleted,
            "本进程自写无 callback 订阅 (常驻指数) 被删 = 每次重启后指数 1m 断流",
        )
        self.assertIn("1788435004202", host._redis_fake.deleted)

    def test_foreign_residue_still_swept(self):
        host = _make_sweep_host({}, self_seqs={"1788435004202"})
        host._sweep_orphan_subscriptions()
        self.assertEqual(
            sorted(host._redis_fake.deleted), ["1788435004203", "1788435004204"],
            "跨进程残渣必须照清 — 豁免面仅限本进程自写/在订",
        )


class SameCodeCleanupOwnershipTest(unittest.TestCase):
    """[BMG4-04] hlen>10 同码清理挂属主门 — 非属主进程不得 hdel 他人活跃同码 seq.

    触发路径 (BMG4-B S-2): hlen>10 × 探针码∈在订池 × 时序重叠 — preflight 订
    探针码时同码清理会把后端活跃 seq 静默 hdel (该路径成功时无日志, 与清扫不同)。
    """

    def _make_subscribe_host(self, env_value=None):
        if env_value is None:
            os.environ.pop(_ENV_KEY, None)
        else:
            os.environ[_ENV_KEY] = env_value
        host = BigQmtXtData.__new__(BigQmtXtData)
        host.client = mock.MagicMock()
        host.client.account_id = "66632458"
        host.client.save_quote_subscription.return_value = True
        fields = {}
        for i in range(10):
            fields[str(100 + i)] = '{"stock_code": "600028.SH", "seq": %d}' % (100 + i)
        fields["999"] = '{"stock_code": "600028.SH", "seq": 999}'  # 外来活跃 seq
        host._redis_fake = _FakeRedisHash(fields)
        host.client._redis.return_value = host._redis_fake
        host._subscribe_seq = 1788435000000
        host._code_to_seq = {}
        host._self_seqs = set()
        host._quote_callbacks = {}
        host._quote_event_thread = None
        return host

    def tearDown(self):
        os.environ.pop(_ENV_KEY, None)

    def test_ungated_process_spares_foreign_same_code_seqs(self):
        host = self._make_subscribe_host(env_value=None)
        host.subscribe_quote("600028.SH", period="1m")
        self.assertEqual(
            host._redis_fake.deleted, [],
            "非属主进程 subscribe 不得触发同码 hdel — 会静默删掉他人活跃 seq",
        )
        self.assertIn("999", host._redis_fake._fields)

    def test_owner_process_still_cleans_same_code(self):
        host = self._make_subscribe_host(env_value="1")
        host.subscribe_quote("600028.SH", period="1m")
        self.assertIn("999", host._redis_fake.deleted)
        self.assertIn("100", host._redis_fake.deleted)
        self.assertNotIn("999", host._redis_fake._fields)


if __name__ == "__main__":
    unittest.main()
