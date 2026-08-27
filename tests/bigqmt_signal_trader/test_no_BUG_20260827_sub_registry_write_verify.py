"""[BUG-20260827-quote-sub-registry-silent-loss] 订阅注册表静默丢失·写后回读校验 + fail-loud 守卫.

背景 (2026-08-27 盘中事故取证):
  000983.SZ 在 bigqmt:quote_subscriptions:{account} 哈希中完全缺失, 推送泵按哈希遍历
  → 全天零帧; 而其 subscribe_quote 的初始快照回调 12:36:14 送达过 (callback_stats
  received=2) → save_quote_subscription 曾被调用且已定局. 失败留痕只有 QMT/backend
  进程 stdout 一行 print, 当日后端 out.log 被 SizeBased 滚动覆盖、大QMT侧
  python/logs/bigqmt.log 为 0 字节 (该路径不走 logging_setup) → 成因三选一
  (hset双失败 / 清理竞争 hdel / hdel 路径吞异常) 事后不可裁决.

修复三层:
  L1: save_quote_subscription hset 后立即 hget 回读并按 JSON 语义比对 — 写入丢失/
      值残缺一律视同失败, 吃满既有 2× retry 预算; 最终失败走 log.error(持久化) +
      print(stdout) 双通道, 返回 False.
  L2: subscribe_quote 收到 _saved is not True → raise RuntimeError — 现有唯一生产
      调用方 gateway_provider._subscribe_impl / admin_subscribe / etf_tick_router
      全部已有 per-code try/except → failed 列表 → 黑名单 TTL → TTL 过期差集补订,
      缺口由既有的收敛回路自愈 (BUG-20260813 家族闭环).
  L3: raise 必须发生在任何客户端半状态 (_code_to_seq / _quote_callbacks) 写入之前 —
      无哈希条目的 seq 不允许在反向索引/回调表中留下孤儿.

守卫 (本测试, 防 A 机 git clone 旧版回退):
  - 行为测: 静默丢键 Redis / 值残缺 Redis → save False 且吃满重试.
  - 行为测: save False 时 subscribe_quote 必 raise 且零客户端半状态.
  - 结构钉: 回读调用在场 + raise 在场 (字符串级, 与 0813 守卫同款).
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtData


class _SilentDropRedis:
    """hset 表面成功但 field 不落盘 (模拟瞬时故障的另一种形态: ACK 后丢写)."""

    def __init__(self):
        self.hset_calls = 0
        self.hget_calls = 0
        self.store = {}

    def hset(self, key, field, value):
        self.hset_calls += 1
        return 1  # 表面成功, 但 store 不写

    def hget(self, key, field):
        self.hget_calls += 1
        return self.store.get(field)

    def hdel(self, *a, **kw):
        pass

    def hgetall(self, key):
        return dict(self.store)

    def hlen(self, key):
        return len(self.store)


class _TruncatedValueRedis(_SilentDropRedis):
    """hset 落盘但值被截断 (回读与写入语义不一致)."""

    def __init__(self):
        super().__init__()
        self.raw_value = None

    def hset(self, key, field, value):
        self.hset_calls += 1
        self.raw_value = value
        self.store[field] = value[: max(len(value) // 2, 1)]  # 残缺值
        return 1


class _GoodRedis(_SilentDropRedis):
    def hset(self, key, field, value):
        self.hset_calls += 1
        self.store[field] = value if isinstance(value, str) else value.decode("utf-8")
        return 1


class _VerifyClient(BigQmtRpcClient):
    def __init__(self, redis_stub):
        self.account_id = "test-acct"
        self._stub = redis_stub

    def _redis(self):
        return self._stub


class SaveWriteVerifyTests(unittest.TestCase):
    """L1: save_quote_subscription 写后回读."""

    def test_silent_drop_key_returns_false_and_exhausts_retry(self):
        stub = _SilentDropRedis()
        client = _VerifyClient(stub)
        with mock.patch("bigqmt_signal_trader.xtquant_compat.log") as fake_log:
            result = client.save_quote_subscription(
                42, {"seq": 42, "stock_code": "000983.SZ", "period": "1m"}, active=True
            )
        self.assertFalse(result)
        self.assertEqual(stub.hset_calls, 2)      # 吃满既有 2× 重试预算
        self.assertEqual(stub.hget_calls, 2)      # 每次尝试都有回读
        fake_log.error.assert_called()            # 失败必须进持久化 logger (INV-4)

    def test_truncated_value_returns_false(self):
        stub = _TruncatedValueRedis()
        client = _VerifyClient(stub)
        with mock.patch("bigqmt_signal_trader.xtquant_compat.log"):
            result = client.save_quote_subscription(
                43, {"seq": 43, "stock_code": "000983.SZ", "period": "1m"}, active=True
            )
        self.assertFalse(result)

    def test_consistent_write_returns_true(self):
        stub = _GoodRedis()
        client = _VerifyClient(stub)
        result = client.save_quote_subscription(
            44, {"seq": 44, "stock_code": "600103.SH", "period": "1m"}, active=True
        )
        self.assertTrue(result)
        stored = json.loads(stub.store["44"])
        self.assertEqual(stored["stock_code"], "600103.SH")

    def test_hset_raising_still_returns_false_after_retries(self):
        """既有 BUG-20260813 行为不回退: hset 抛异常路径保持 2× 尝试 + False."""
        stub = _SilentDropRedis()

        class RaisingRedis(_SilentDropRedis):
            def hset(self, *a, **kw):
                self.hset_calls += 1
                raise ConnectionError("simulated")

        raising = RaisingRedis()
        client = _VerifyClient(raising)
        with mock.patch("bigqmt_signal_trader.xtquant_compat.log") as fake_log:
            result = client.save_quote_subscription(
                45, {"seq": 45, "stock_code": "600103.SH"}, active=True
            )
        self.assertFalse(result)
        self.assertEqual(raising.hset_calls, 2)
        fake_log.error.assert_called()


class _SaveFailClient:
    """其余协议面照常、唯独 save 失败的最小 client 替身."""

    def __init__(self):
        self.account_id = "acct"
        self.redis = FakeEvents()
        self.save_called = 0

    def _redis(self):
        return self.redis

    def save_quote_subscription(self, seq, payload, active=True):
        self.save_called += 1
        return False

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        self.redis.events.append((event_type, payload))
        return {"event_type": event_type}


class FakeEvents:
    def __init__(self):
        self.events = []
        self.hashes = {}

    def publish_event(self, event_type, payload):
        self.events.append((event_type, payload))


class SubscribeQuoteFailLoudTests(unittest.TestCase):
    """L2/L3: save 失败 → raise, 且客户端零半状态."""

    def test_subscribe_quote_raises_when_save_fails(self):
        client = _SaveFailClient()
        xtdata = BigQmtXtData(client)
        cb_hits = []
        with self.assertRaises(RuntimeError):
            xtdata.subscribe_quote("000983.SZ", period="1m", callback=cb_hits.append)
        self.assertEqual(client.save_called, 1)
        # L3: 异常必须先于任何半状态写入
        self.assertEqual(xtdata._code_to_seq, {})
        self.assertEqual(xtdata._quote_callbacks, {})
        self.assertEqual(cb_hits, [])
        self.assertEqual(client.redis.events, [])

    def test_subscribe_quote_success_path_unaffected(self):
        """save 正常时不 raise (防过度拦截) — 用真实 save + Good redis."""
        stub = _GoodRedis()
        client = _VerifyClient(stub)
        xtdata = BigQmtXtData(client)
        seq = xtdata.subscribe_quote("600103.SH", period="1d", callback=None)
        self.assertEqual(xtdata._code_to_seq.get("600103.SH"), seq)
        self.assertIn(str(seq), stub.store)


class StructuralGuardsTests(unittest.TestCase):
    """结构钉: 读源码防回退 (A 机 git clone 旧版)."""

    @classmethod
    def _read_bridge(cls) -> str:
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "xtquant_compat.py")
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    def test_save_has_readback_verification(self):
        src = self._read_bridge()
        self.assertIn('hget(key, str(seq))', src)

    def test_subscribe_quote_raises_on_save_false(self):
        src = self._read_bridge()
        self.assertIn("_saved is not True", src)
        self.assertIn("raise RuntimeError", src)

    def test_no_silent_pass_around_registry_delete(self):
        """hdel 分支禁止 except: pass 全吞 (INV-4)."""
        src = self._read_bridge()
        self.assertNotIn("except Exception:\n                pass\n            return True", src)


if __name__ == "__main__":
    unittest.main()
