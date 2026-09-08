"""[R25-03] registry_deactivate_orphans 单元测试 (600354 断流案横扫修复).

孤儿判定: stock_code ∉ needed 且 seq 不在本 boot _code_to_seq 活跃值集 →
物理 hdel (save_quote_subscription active=False 路径); 本 boot 活跃 seq 无条件
保护 (fail-toward-keep); 已停用/畸形条目不动; redis 故障计 errors 不中断.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

HASH_KEY = "bigqmt:quote_subscriptions:8890541985"


class FakeRedis:
    def __init__(self, entries):
        self.entries = dict(entries)
        self.deleted = []

    def hgetall(self, key):
        assert key == HASH_KEY
        return self.entries

    def hdel(self, key, field):
        self.deleted.append(field)
        self.entries.pop(field, None)


class FakeClient:
    def __init__(self, entries):
        self.account_id = "8890541985"
        self.redis = FakeRedis(entries)

    def _redis(self):
        return self.redis

    def save_quote_subscription(self, seq, payload, active=True):
        if not active:
            self.redis.hdel(HASH_KEY, str(seq))
            return True
        return True


def make_xtdata(entries, code_to_seq=None):
    x = BigQmtXtData.__new__(BigQmtXtData)
    x.client = FakeClient(entries)
    x._code_to_seq = dict(code_to_seq or {})
    return x


def entry(seq, code, active=None):
    p = {"seq": seq, "stock_code": code, "period": "1m"}
    if active is not None:
        p["active"] = active
    return {str(seq).encode(): json.dumps(p).encode()}


class TestRegistryDeactivateOrphans:
    def test_orphan_removed_needed_and_live_protected(self):
        """孤儿删 / needed 留 / 本 boot 活跃 seq 无条件保护 (fail-toward-keep)."""
        entries = {}
        entries.update(entry(1, "000983.SZ"))  # 孤儿 (08-27 残留) → 删
        entries.update(entry(2, "600354.SH"))  # needed 口径内 → 留
        entries.update(entry(3, "000300.SH"))  # 非 needed 但本 boot 活跃 → 留
        x = make_xtdata(entries, code_to_seq={"000300.SH": 3})
        r = x.registry_deactivate_orphans({"600354.SH"})
        assert r == {"deactivated": ["000983.SZ"], "errors": 0}
        assert x.client.redis.deleted == ["1"]

    def test_already_inactive_skipped(self):
        """已带 active=False 的条目跳过 (幂等, 二次清扫零动作)."""
        x = make_xtdata(entry(1, "000983.SZ", active=False))
        assert x.registry_deactivate_orphans(set())["deactivated"] == []

    def test_malformed_entries_left_untouched(self):
        """非 JSON / 无 stock_code 条目不动 — 泵侧本就跳过, 防洗掉唯一可解析态."""
        entries = {
            b"9": b"not-json",
            b"8": json.dumps({"period": "1m", "seq": 8}).encode(),
        }
        x = make_xtdata(entries)
        r = x.registry_deactivate_orphans(set())
        assert r["deactivated"] == []
        assert x.client.redis.deleted == []

    def test_redis_error_counted_not_raised(self):
        """hgetall 故障 → errors=1 不抛 (观测面契约, caller 侧另有 warning)."""
        x = make_xtdata({})
        x.client._redis = lambda: (_ for _ in ()).throw(RuntimeError("down"))
        r = x.registry_deactivate_orphans({"600354.SH"})
        assert r == {"deactivated": [], "errors": 1}

    def test_save_failure_counted(self):
        """save_quote_subscription 返 False (写删校验失败) 计 errors 不中断."""
        x = make_xtdata(entry(1, "000983.SZ"))
        x.client.save_quote_subscription = lambda *a, **k: False
        r = x.registry_deactivate_orphans(set())
        assert r["deactivated"] == [] and r["errors"] == 1
