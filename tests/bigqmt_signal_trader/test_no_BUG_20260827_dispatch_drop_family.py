"""[BUG-20260827-dispatch-drop-counters / maxlen-unify / db-source-parity] P2 家族守卫.

INV-4: 路由层任何丢弃都必须可观测 (曾经纯静默 return/pass — 08-12 事故靠 88 条
堆积刷屏才暴露, 形态不可再版)。附带锁:
  - 流 cap 单一真相源: 客户端 publish_event 与服务端 _publish 同源 QUOTE_STREAM_MAXLEN;
  - Redis db 默认值双源一致: redis_common.build_redis_client 兜底 == 客户端
    load_client_config 兜底 (BUG-P0-20260810 注释钦定 db=0 是合法选择, 默认分裂
    会静默跨库 RPC 无人消费)。
"""
from __future__ import annotations

import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_events import (
    EVENT_HEARTBEAT,
    QUOTE_STREAM_MAXLEN,
)
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class _MinimalClient:
    def __init__(self):
        self.account_id = "acct"


def _xtdata_with_cb(seq=11):
    x = BigQmtXtData(_MinimalClient())
    hits = []

    def cb(payload):
        hits.append(payload)

    x._quote_callbacks[seq] = cb
    return x, hits


class DispatchDropCounterTests(unittest.TestCase):
    def test_malformed_event_counted(self):
        x, _ = _xtdata_with_cb()
        before = x.get_dispatch_drop_stats()["malformed_event"]
        x._dispatch_quote_event("{not json")
        x._dispatch_quote_event(None if False else "[1,2]")  # 非 dict 但合法 json
        after = x.get_dispatch_drop_stats()
        self.assertEqual(after["malformed_event"], before + 2)

    def test_unknown_type_counted_and_subscribe_type_neutral(self):
        x, _ = _xtdata_with_cb()
        before = x.get_dispatch_drop_stats()["unknown_type"]
        x._dispatch_quote_event(json.dumps({"event_type": "subscribe_quote", "seq": 1}))
        self.assertEqual(x.get_dispatch_drop_stats()["unknown_type"], before + 1)

    def test_seq_miss_without_fallback_counted(self):
        x, hits = _xtdata_with_cb(seq=999)   # 注册的是别的 seq
        before = x.get_dispatch_drop_stats()["seq_miss_no_fallback"]
        x._dispatch_quote_event(json.dumps({
            "event_type": "quote", "seq": 111, "stock_code": "600103.SH",
            "period": "1m", "close": 3.82,
        }))
        self.assertEqual(x.get_dispatch_drop_stats()["seq_miss_no_fallback"], before + 1)
        self.assertEqual(hits, [])

    def test_callback_exception_counted(self):
        x = BigQmtXtData(_MinimalClient())

        def boom(_payload):
            raise ValueError("cb broke")

        x._quote_callbacks[11] = boom
        before = x.get_dispatch_drop_stats()["callback_exception"]
        x._dispatch_quote_event(json.dumps({
            "event_type": "quote", "seq": 11, "stock_code": "600103.SH",
            "period": "1m", "close": 3.82,
        }))
        self.assertEqual(x.get_dispatch_drop_stats()["callback_exception"], before + 1)

    def test_heartbeat_route_does_not_pollute_quote_counters(self):
        x, _ = _xtdata_with_cb(seq=11)
        base = x.get_dispatch_drop_stats()
        handler_events = []
        x.set_quote_heartbeat_handler(handler_events.append)
        x._dispatch_quote_event(json.dumps(normalize_hb()))
        stats = x.get_dispatch_drop_stats()
        for kind in ("malformed_event", "unknown_type", "seq_miss_no_fallback"):
            self.assertEqual(stats[kind], base[kind], kind)


def normalize_hb():
    from bigqmt_signal_trader.quote_events import normalize_heartbeat_event

    return normalize_heartbeat_event(seq=11, stock_code="600103.SH",
                                     account_id="acct", last_price=3.82)


class StreamMaxlenUnifyTests(unittest.TestCase):
    def test_client_and_server_share_single_source(self):
        """客户端 publish_event 必须引用共享常量而非字面量 1000."""
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader", "xtquant_compat.py")
        src = open(path, encoding="utf-8").read()
        self.assertIn("maxlen=_QUOTE_STREAM_MAXLEN", src)
        self.assertNotIn("maxlen=1000", src)

    def test_default_value_is_2000_configurable_by_env(self):
        # 模块导入时已固化: 值必须与服务端默认 2000 一致; env 路径经 reload 生效。
        from bigqmt_signal_trader import quote_events as qe

        self.assertEqual(qe.QUOTE_STREAM_MAXLEN, 2000)
        raw = open(os.path.join(ROOT, "src", "bigqmt_signal_trader", "quote_events.py"),
                   encoding="utf-8").read()
        self.assertIn('BIGQMT_QUOTE_EVENT_MAXLEN', raw)


class DbDefaultParityTests(unittest.TestCase):
    def test_bridge_and_adapters_db_fallbacks_identical(self):
        redis_common = open(
            os.path.join(ROOT, "src", "bigqmt_signal_trader", "adapters", "redis_common.py"),
            encoding="utf-8").read()
        compat = open(
            os.path.join(ROOT, "src", "bigqmt_signal_trader", "xtquant_compat.py"),
            encoding="utf-8").read()
        # 两处兜底 db 必须同值 (当前均 5; 运行时经 BIGQMT_REDIS_DB env 统一为真实值)
        self.assertIn('_db_value = 5', redis_common)
        self.assertIn('BIGQMT_REDIS_DB', redis_common)
        self.assertIn('BIGQMT_REDIS_DB', compat)
        self.assertIn('_env_int("BIGQMT_REDIS_DB", 5)', compat)


class DropStatsObservabilityTests(unittest.TestCase):
    """[对抗复审 DEF-2 收口] 计数必须有出口: WARN 节流落日志 + facade 透传."""

    def test_warn_emitted_every_20th_increment_with_snapshot(self):
        from unittest import mock

        x = BigQmtXtData(_MinimalClient())
        with mock.patch("bigqmt_signal_trader.xtquant_compat.log") as fake_log:
            for _ in range(19):
                x._dispatch_quote_event("{not json")
            fake_log.warning.assert_not_called()          # 阈值内静默 (热路径零 IO)
            x._dispatch_quote_event("{not json")          # 第 20 次 → 触发
            fake_log.warning.assert_called_once()
            args = fake_log.warning.call_args[0]
            self.assertIn("dispatch-drops", args[0])
            snapshot = args[3]
            self.assertEqual(snapshot["malformed_event"], 20)

    def test_facade_passthrough_exists(self):
        """shim facade 必须有同名透传 — 后端 getattr 守卫调用面."""
        facade_path = os.path.join(ROOT, "src", "xtquant", "xtdata.py")
        src = open(facade_path, encoding="utf-8").read()
        self.assertIn("def get_dispatch_drop_stats", src)
        self.assertIn("_compat.xtdata.get_dispatch_drop_stats()", src)
        sys.path.insert(0, os.path.join(ROOT, "src"))
        try:
            import importlib
            import xtquant.xtdata as facade  # noqa: import 已在仓内

            importlib.reload(facade)
            stats = facade.get_dispatch_drop_stats()
            self.assertIsInstance(stats, dict)
            self.assertIn("malformed_event", stats)
        finally:
            sys.path.remove(os.path.join(ROOT, "src"))

    def test_redis_common_logs_effective_db_at_boot(self):
        """[DEF-6] 启动期生效 db 留痕行在场 — A/B 双源不对称漂移可一眼确诊."""
        redis_common = open(
            os.path.join(ROOT, "src", "bigqmt_signal_trader", "adapters", "redis_common.py"),
            encoding="utf-8").read()
        self.assertIn("build_redis_client host=%s port=%s db=%d", redis_common)


if __name__ == "__main__":
    unittest.main()
