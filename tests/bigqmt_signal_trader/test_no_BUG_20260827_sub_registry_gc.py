"""[BUG-20260827-sub-registry-gc] INV-3 注册表卫生·持久化账本回收+爆炸半径闸门 守卫.

背景 (2026-08-27 R1 取证 + 同日对抗复审 DEF-1 定案):
  订阅哈希 66 条中 45 条为 08-10~08-27 晨间历史遗留 (含 601678.SH 同码 3 条),
  无人能回收 — 清理只在"有人再订同码"时触发且仅同码。泵每秒全量遍历这些条目
  白耗 RPC 与推流带宽 (午后实测 601678.SH 出 84 帧)。INV-3: 注册表是有界资源,
  陈年条目必须可回收且回收有审计痕迹。

[对抗复审 DEF-1 三重悖论 → 本重设计锁面]
  原实现缺陷 (红证 μR: observe_map 空 ⇒ reap 永空, 而条目明明已积 91 天):
    P1-a 首见账本是策略进程内存 dict — 泵每次重启宽限锚归零, 现实节律下 GC
         恒空转 (装置失效);
    P1-b >72h 长活进程 + 单端升级时 [戳缺失] 全部成立 = 一拍全池合法回收;
    P1-c keep_ttl 配置 int(cfg*1000) 类型混淆 (字符串配置静默摆烂)。
  重设计三件套 (本套件全部咬合):
    L-durable: 首见账本持久化 bigqmt:quote_subs_firstseen:{acct}, 泵重启不重置宽限
               (跨调用/重启等价行为测);
    L-cap:     单周期回收量闸门 (默认注册表 10%, ≥5), 全池级误收机制上不可能一拍
               发生, 超出候选顺延并计 deferred;
    L-coerce:  coerce_seconds_to_ms 纯函数强转秒配置 (数字字符串 OK / 非法回退默认),
               strategy 不再裸 int(cfg*1000)。
  附带: tombstone member="seq|stock_code" 可溯源可恢复; zset 自修剪到最近 2000 笔;
        ledger 回收同步 hdel; 残缺 JSON 条目同样进候选 (label=<unreadable>)。

契约演进声明 (相对首版): reap_stale_subscriptions 删除 observe_map 形参 —
账本单源收敛到 Redis, 不再接受进程内存注入 (两来源即无真相)。
"""
from __future__ import annotations

import ast
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_events import (
    TOMBSTONE_MAX_ENTRIES,
    coerce_seconds_to_ms,
    firstseen_key,
    reap_stale_subscriptions,
)

NOW_MS = 1_800_000_000_000


class FakeRegistryRedis:
    """按 key 语义分库的最小替身, 记录全部审计动作."""

    def __init__(self, subs=None, seen=None, firstseen=None):
        self.subs = dict(subs or {})            # quote_subscriptions {field: json}
        self.seen = dict(seen or {})            # quote_subs_seen      {seq: ms_str}
        self.firstseen = dict(firstseen or {})  # quote_subs_firstseen {seq: ms_str}
        self.deleted = []                       # [(key, field)]
        self.tombstones = []                    # [(key, member, score)]
        self.fail_keys = set()

    @staticmethod
    def _which(key):
        if ":quote_subscriptions:" in key:
            return "subs"
        if ":quote_subs_seen:" in key:
            return "seen"
        if ":quote_subs_firstseen:" in key:
            return "firstseen"
        return None

    def _store(self, which):
        return getattr(self, which) if which else {}

    # ── 读 ──
    def hgetall(self, key):
        if key in self.fail_keys:
            raise ConnectionError("simulated redis failure on %s" % key)
        which = self._which(key)
        return dict(self._store(which))

    def hget(self, key, field):
        if key in self.fail_keys:
            raise ConnectionError("simulated")
        return self._store(self._which(key)).get(field)

    def hlen(self, key):
        return len(self._store(self._which(key)))

    # ── 写 ──
    def hdel(self, key, *fields):
        store = self._store(self._which(key))
        for f in fields:
            f = f.decode() if isinstance(f, (bytes, bytearray)) else str(f)
            self.deleted.append((key, f))
            store.pop(f, None)

    def hset(self, key, value=None, mapping=None):
        store = self._store(self._which(key))
        assert mapping is not None, "本仓 GC 只用 hset(mapping=) 形态"
        for f, v in mapping.items():
            store[f] = v

    def zadd(self, key, mapping):
        for member, score in mapping.items():
            self.tombstones.append((key, member, score))

    def zremrangebyrank(self, key, start, end):
        pass  # 替身不做真裁剪; 结构钉验证调用在场即可

    def publish(self, channel, payload):
        pass


def _payload(seq, code="601678.SH"):
    return json.dumps({"seq": seq, "stock_code": code, "period": "1m"})


def _reap(redis_stub, **kw):
    defaults = dict(
        now_ms=NOW_MS,
        keep_ttl_ms=300_000,
        grace_ms=300_000,
        enabled=True,
    )
    defaults.update(kw)
    return reap_stale_subscriptions(redis_stub, "acct", **defaults)


class ReapGateTests(unittest.TestCase):
    def test_gate_off_is_strict_noop(self):
        r = FakeRegistryRedis({"old": _payload("old")})
        stats = _reap(r, enabled=False)
        self.assertEqual(stats["reaped"], [])
        self.assertEqual(stats["deferred"], 0)
        self.assertEqual(r.deleted, [])
        self.assertEqual(r.tombstones, [])

    def test_fresh_seen_stamp_keeps_entry(self):
        r = FakeRegistryRedis(
            {"s1": _payload("s1")},
            seen={"s1": str(NOW_MS - 5_000)},
            firstseen={"s1": str(NOW_MS - 120_000)},
        )
        stats = _reap(r)
        self.assertEqual(stats["reaped"], [])
        self.assertIn("s1", r.subs)

    def test_stale_stamp_reaped_with_traceable_tombstone_and_ledger_cleanup(self):
        r = FakeRegistryRedis(
            {"dead": _payload("dead", "600989.SH")},
            seen={"dead": str(NOW_MS - 400_000)},
            firstseen={"dead": str(NOW_MS - 400_000)},
        )
        stats = _reap(r)
        self.assertEqual(stats["reaped"], ["dead"])
        subs_key = "bigqmt:quote_subscriptions:acct"
        ledger_key = firstseen_key("acct")
        self.assertIn((subs_key, "dead"), r.deleted)
        self.assertIn((ledger_key, "dead"), r.deleted)   # 账本条目同步清 (有界自我约束)
        self.assertEqual(len(r.tombstones), 1)
        key, member, score = r.tombstones[0]
        self.assertEqual(key, "bigqmt:quote_subs_tombstone:acct")
        self.assertEqual(member, "dead|600989.SH")       # member 可溯源: seq|code
        self.assertEqual(score, NOW_MS)

    def test_grace_protects_unstamped_new_observation_and_persists_first_seen(self):
        """旧客户端未升级不发戳 + 新观测条目 → 宽限期保护; 且首见入 Redis 账本."""
        r = FakeRegistryRedis({"new": _payload("new", "000983.SZ")})
        stats = _reap(r, grace_ms=3_600_000)
        self.assertEqual(stats["reaped"], [])
        self.assertIn("new", r.subs)
        self.assertEqual(r.firstseen.get("new"), str(NOW_MS))  # 持久化而非进程内

    def test_restart_does_not_reset_grace_anchor__durable_ledger_reap(self):
        """[DEF-1 核心红证反转] 泵重启 (新进程、同一 Redis) 后宽限锚不归零.

        旧行为 (μR 已演示): observe_map 进程内 ⇒ 重启后 reap 恒 []。
        新契约: 第 1 次调用登记首见进 Redis 账本; 「重启」后再次调用, 同一 redis
        存储上的陈旧条目凭账本判龄即可回收 — 无任何 caller 态参与。
        """
        t0 = NOW_MS - 91 * 86_400_000  # 设定「91 天前」为初始观测点
        r = FakeRegistryRedis({"legacy": _payload("legacy", "601678.SH")},
                              firstseen={"legacy": str(t0)})
        # 第一次调用: 未过宽限? 91d >> 300s 宽限, 直接满足条件一; 戳缺失 → 应回收
        s1 = _reap(r)
        self.assertEqual(s1["reaped"], ["legacy"])
        # 「重启后再来一轮」: 注册表里换了个新 seq (模拟补订), 记账本轮刚见到
        r2 = FakeRegistryRedis({"fresh": _payload("fresh", "600103.SH")})
        s2a = _reap(r2, now_ms=NOW_MS)                      # 首见本轮登记, 保护
        self.assertEqual(s2a["reaped"], [])
        self.assertTrue(r2.firstseen.get("fresh"))
        # 用一个全新 caller 实例/进程语境重放同一存储: 宽限未过仍受保护 (不误收)
        r3 = FakeRegistryRedis(
            {"fresh": _payload("fresh", "600103.SH")},
            firstseen=dict(r2.firstseen),                   # ← 持久化账本的威力
        )
        s3 = _reap(r3)
        self.assertEqual(s3["reaped"], [])
        # 时间前进到账本首见过期且始终无戳 → 可回收 (旧设计永远到不了这里)
        s4 = _reap(r3, now_ms=NOW_MS + 4 * 86_400_000)
        self.assertEqual(s4["reaped"], ["fresh"])

    def test_cap_limits_single_cycle_blast_radius(self):
        """[DEF-1 修复点2] 20 条全候选 + cap=3 → 恰回收最老 3 条, 其余顺延."""
        subs = {}
        firstseen = {}
        for i in range(20):
            seq = "old%02d" % i
            subs[seq] = _payload(seq, "600989.SH")
            # i 越大越年轻: old00 = 30 天前, old19 = 11 天前 (全部 > 300s 宽限)
            firstseen[seq] = str(NOW_MS - (30 - i) * 86_400_000)
        r = FakeRegistryRedis(subs, firstseen=firstseen)
        stats = _reap(r, max_reap=3)
        self.assertEqual(len(stats["reaped"]), 3)
        self.assertEqual(stats["deferred"], 17)
        self.assertEqual(len(r.tombstones), 3)
        self.assertEqual(len(r.subs), 17)
        # 最老优先: 回收的是 old00..old02
        for seq in ("old00", "old01", "old02"):
            self.assertIn(seq, stats["reaped"])

    def test_default_cap_is_ratio_based(self):
        """max_reap 缺省 → max(5, 10% of registry)."""
        subs = {("s%02d" % i): _payload("s%02d" % i) for i in range(20)}
        fs = {k: str(NOW_MS - 10 * 86_400_000) for k in subs}
        r = FakeRegistryRedis(subs, firstseen=fs)
        stats = _reap(r)  # 20*0.10=2 < 5 → floor 5
        self.assertEqual(len(stats["reaped"]), 5)

    def test_corrupt_json_entry_is_reap_candidate_with_unreadable_label(self):
        r = FakeRegistryRedis(
            {"corrupt": "{not-json"},
            firstseen={"corrupt": str(NOW_MS - 400_000)},
        )
        stats = _reap(r)
        self.assertEqual(stats["reaped"], ["corrupt"])
        member = r.tombstones[0][1]
        self.assertTrue(member.startswith("corrupt|"))
        self.assertIn("<unreadable>", member)


class CoerceSecondsTests(unittest.TestCase):
    """[DEF-1 修复点3] 配置类型混淆治根 — 字符串/非法值不再让 GC 静默摆烂."""

    def test_matrix(self):
        self.assertEqual(coerce_seconds_to_ms(None, 259_200.0), 259_200_000)
        self.assertEqual(coerce_seconds_to_ms("", 259_200.0), 259_200_000)
        self.assertEqual(coerce_seconds_to_ms(72, 999_999.0), 72_000)
        self.assertEqual(coerce_seconds_to_ms("86400", 1.0), 86_400_000)   # 字符串可用
        self.assertEqual(coerce_seconds_to_ms("abc", 50.0), 50_000)        # 非法回退
        self.assertEqual(coerce_seconds_to_ms(-5, 50.0), 50_000)           # 负值回退
        self.assertEqual(coerce_seconds_to_ms(0, 50.0), 50_000)            # 0 回退 (原 or 兜底语义保持)

    def test_redis_failure_conservative_abort(self):
        r = FakeRegistryRedis({"x": _payload("x")})
        r.fail_keys.add("bigqmt:quote_subs_firstseen:acct")
        stats = _reap(r)
        self.assertEqual(stats["reaped"], [])
        self.assertIn("x", r.subs)


class KeepaliveWriterTests(unittest.TestCase):
    """客户端保活节流写: 同 seq 60s 内至多一次."""

    def test_should_write_keepalive_throttled(self):
        from bigqmt_signal_trader.quote_events import should_write_keepalive

        self.assertTrue(should_write_keepalive(None, now_ms=NOW_MS, min_interval_ms=60_000))
        self.assertFalse(should_write_keepalive(NOW_MS - 30_000, now_ms=NOW_MS, min_interval_ms=60_000))
        self.assertTrue(should_write_keepalive(NOW_MS - 61_000, now_ms=NOW_MS, min_interval_ms=60_000))


class StrategyCallSitePins(unittest.TestCase):
    @staticmethod
    def _strategy_src():
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_strategy.py")
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    def test_strategy_pins(self):
        src = self._strategy_src()
        self.assertIn("BIGQMT_QUOTE_SUB_GC", src)
        self.assertIn("reap_stale_subscriptions", src)
        self.assertIn("_last_quote_bar_time.pop", src)   # 孤儿键同步回收
        self.assertIn("coerce_seconds_to_ms", src)       # 配置强转经纯函数
        self.assertIn("sub_gc_grace_sec", src)           # 宽限亦走配置口
        # DEF-1 反指: 进程内存账本与旧裸乘法形态禁止回潮
        self.assertNotIn("_quote_sub_observe_map", src)
        self.assertNotIn('int(quote_config.get("sub_gc_keep_ttl_sec"', src)

    def test_quote_events_pins_durable_ledger_and_trim(self):
        qe_path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "quote_events.py")
        with open(qe_path, encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn("bigqmt:quote_subs_firstseen:", src)          # 持久化账本
        self.assertIn("zremrangebyrank", src)                        # 审计面自修剪
        self.assertNotIn("observe_map", src)                         # 旧形参禁潮

    def test_strategy_imports_os_for_gc_gate(self):
        """[对抗复审抓漏] GC 门用 os.environ — 模块顶部必须 import os (AST 钉)."""
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
        self.assertIn("_write_keepalive", src)
        self.assertIn("_should_write_keepalive", src)
        qe_path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "quote_events.py")
        qe_src = open(qe_path, encoding="utf-8").read()
        self.assertIn("quote_subs_seen", qe_src)

    def test_tombstone_budget_constant_bounded(self):
        # 审计面自身也受「有界资源」不变量约束
        self.assertIsInstance(TOMBSTONE_MAX_ENTRIES, int)
        self.assertGreater(TOMBSTONE_MAX_ENTRIES, 0)


if __name__ == "__main__":
    unittest.main()
