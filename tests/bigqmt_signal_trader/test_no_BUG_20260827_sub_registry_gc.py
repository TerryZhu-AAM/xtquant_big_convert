"""[BUG-20260827-sub-registry-gc] INV-3 注册表卫生·消费端保活回收+审计 守卫.

背景 (2026-08-27 R1 取证):
  订阅哈希 66 条中 45 条为 08-10~08-27 晨间历史遗留 (含 601678.SH 同码 3 条),
  无人能回收 — 清理只在"有人再订同码"时触发且仅同码。泵每秒全量遍历这些条目
  白耗 RPC 与推流带宽 (午后实测 601678.SH 出 84 帧), 过期 seq 帧还持续轰炸
  backend 路由层。INV-3: 注册表是有界资源, 陈年条目必须可回收且回收有审计痕迹。

设计 (两文件闭环, 全部桥子仓内):
  - 保活戳: 客户端 _dispatch_*_event 成功处理该 seq 帧后节流写
    ``bigqmt:quote_subs_seen:{acct}`` (field=seq, value=now_ms, 每 seq ≥60s 一次)
    → 消费端死亡 ⇒ 戳停更。
  - 回收器 quote_events.reap_stale_subscriptions(env 门控 BIGQMT_QUOTE_SUB_GC,
    默认关 = 零行为变更直至 ops 双端部署后显式开启):
      条目被回收须同时满足: 服务端首次观测已过 GRACE 且 (无戳 或 戳龄 > KEEP_TTL);
    动作 = hdel + tombstone zadd(时间戳可排序, 保留审计) + 返回回收清单供调用方
    清理 pump 内存去重/心跳孤儿键。
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_events import reap_stale_subscriptions

NOW_MS = 1_800_000_000_000


class FakeRegistryRedis:
    """hgetall/hdel/zadd 最小替身, 记录审计动作."""

    def __init__(self, subs=None, seen=None):
        self.subs = dict(subs or {})          # {field: json_str}
        self.seen = dict(seen or {})
        self.deleted = []
        self.tombstones = []                  # [(key, member, score)]

    # 注册表哈希
    def hgetall(self, key):
        return dict(self.subs)

    def hlen(self, key):
        return len(self.subs)

    def hdel(self, key, *fields):
        self.deleted.extend((key, f) for f in fields)
        for f in fields:
            self.subs.pop(f, None)

    # 保活戳 / tombstone
    def hget(self, key, field):
        return self.seen.get(field)

    def zadd(self, key, mapping):
        for member, score in mapping.items():
            self.tombstones.append((key, member, score))

    def publish(self, channel, payload):
        pass


def _payload(seq, code="601678.SH"):
    import json

    return json.dumps({"seq": seq, "stock_code": code, "period": "1m"})


class ReapGateTests(unittest.TestCase):
    def test_gate_off_is_strict_noop(self):
        r = FakeRegistryRedis({"old": _payload("old")})
        stats = reap_stale_subscriptions(
            r, "acct", now_ms=NOW_MS, keep_ttl_ms=1000, grace_ms=1000,
            observe_map={}, enabled=False,
        )
        self.assertEqual(stats["reaped"], [])
        self.assertEqual(r.deleted, [])
        self.assertEqual(r.tombstones, [])

    def test_fresh_seen_stamp_keeps_entry(self):
        r = FakeRegistryRedis(
            {"s1": _payload("s1")},
            seen={"s1": NOW_MS - 5_000},
            )
        stats = reap_stale_subscriptions(
            r, "acct", now_ms=NOW_MS, keep_ttl_ms=60_000, grace_ms=60_000,
            observe_map={"s1": NOW_MS - 120_000}, enabled=True,
        )
        self.assertEqual(stats["reaped"], [])
        self.assertEqual(r.subs.get("s1"), _payload("s1"))

    def test_stale_stamp_reaped_with_tombstone_audit(self):
        r = FakeRegistryRedis(
            {"dead": _payload("dead", "600989.SH")},
            seen={"dead": NOW_MS - 400_000},
        )
        stats = reap_stale_subscriptions(
            r, "acct", now_ms=NOW_MS, keep_ttl_ms=300_000, grace_ms=300_000,
            observe_map={"dead": NOW_MS - 400_000}, enabled=True,
        )
        self.assertEqual(stats["reaped"], ["dead"])
        self.assertIn(("bigqmt:quote_subscriptions:acct", "dead"), r.deleted)
        self.assertEqual(len(r.tombstones), 1)
        key, member, score = r.tombstones[0]
        self.assertEqual(key, "bigqmt:quote_subs_tombstone:acct")
        self.assertEqual(member, "dead")
        self.assertEqual(score, NOW_MS)

    def test_grace_protects_unstamped_new_observation(self):
        """旧客户端未升级不发戳 + 新观测条目 → 宽限期保护, 绝不误收."""
        r = FakeRegistryRedis({"new": _payload("new", "000983.SZ")})
        stats = reap_stale_subscriptions(
            r, "acct", now_ms=NOW_MS, keep_ttl_ms=300_000, grace_ms=3_600_000,
            observe_map={"new": NOW_MS - 60_000},   # 刚发现 60s
            enabled=True,
        )
        self.assertEqual(stats["reaped"], [])
        self.assertIn("new", r.subs)


class KeepaliveWriterTests(unittest.TestCase):
    """客户端保活节流写: 同 seq 60s 内至多一次."""

    def test_should_write_keepalive_throttled(self):
        from bigqmt_signal_trader.quote_events import should_write_keepalive

        self.assertTrue(should_write_keepalive(None, now_ms=NOW_MS, min_interval_ms=60_000))
        self.assertFalse(should_write_keepalive(NOW_MS - 30_000, now_ms=NOW_MS, min_interval_ms=60_000))
        self.assertTrue(should_write_keepalive(NOW_MS - 61_000, now_ms=NOW_MS, min_interval_ms=60_000))


class StrategyCallSitePins(unittest.TestCase):
    def test_strategy_pins(self):
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_strategy.py")
        src = open(path, encoding="utf-8").read()
        self.assertIn("BIGQMT_QUOTE_SUB_GC", src)
        self.assertIn("reap_stale_subscriptions", src)
        self.assertIn("_last_quote_bar_time.pop", src)   # 孤儿键同步回收

    def test_strategy_imports_os_for_gc_gate(self):
        """[对抗复审抓漏] GC 门用 os.environ — 模块顶部必须 import os.

        py_compile 不查未绑定全局名; 缺 import os 会让泵首个周期 NameError
        (被 adjust 外层宽捕获吞 → 行情推送全停)。AST 钉防回退。
        """
        import ast

        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_strategy.py")
        tree = ast.parse(open(path, encoding="utf-8").read())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertIn("os", imported)

    def test_client_keeps_alive_on_dispatch(self):
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "xtquant_compat.py")
        src = open(path, encoding="utf-8").read()
        # dispatch 路径必须写保活戳 (别名导入: 字面量键名在 quote_events 定义)
        self.assertIn("_write_keepalive", src)
        self.assertIn("_should_write_keepalive", src)
        qe_path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "quote_events.py")
        qe_src = open(qe_path, encoding="utf-8").read()
        self.assertIn("quote_subs_seen", qe_src)


if __name__ == "__main__":
    unittest.main()
