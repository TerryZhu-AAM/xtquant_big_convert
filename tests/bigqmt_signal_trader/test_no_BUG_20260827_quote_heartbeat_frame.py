"""[BUG-20260827-quote-heartbeat-frame] INV-2 活性与变化解耦 守卫.

背景 (600103.SH 2026-08-27 全天封板实测):
  pump 按 close 去重 = 价格变才推。封死涨停后 lastPrice 恒 3.82 → 重启清空去重
  缓存后推出首批几帧即永久静默。数据面 (get_full_tick) 恒新鲜、流通道恒通,
  后端健康判定却只能看到"没有帧"→ 无法三分「断流/未订阅/无变化」。

修复语义 (本文件锁):
  L1 quote_events: EVENT_HEARTBEAT 帧型 + 纯函数 should_send_heartbeat +
     normalize_heartbeat_event (liveness-only, 零 OHLCV 字段)。
  L2 strategy pump: dedup skip 分支到期发心跳 (_last_quote_heartbeat_at 按 seq 记账,
     间隔配置化默认 30s)。— 结构钉锁源码, QMT 进程内逻辑不可单测。
  L3 xtquant_compat: heartbeat 帧独立路由到 set_quote_heartbeat_handler 钩子;
     绝不调用 _quote_callbacks 里的 tick 回调 (零伪造行情面); 老消费端对未知
     event_type 的忽略分支保留 (双端非锁定序升级安全)。
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
    EVENT_QUOTE,
    normalize_heartbeat_event,
    should_send_heartbeat,
)
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class HeartbeatPureFunctionTests(unittest.TestCase):
    def test_never_sent_fires_immediately(self):
        self.assertTrue(should_send_heartbeat(None, now_ts=1000.0, interval_seconds=30))

    def test_interval_elapsed_and_not_elapsed(self):
        self.assertTrue(should_send_heartbeat(970.0, now_ts=1000.0, interval_seconds=30))
        self.assertFalse(should_send_heartbeat(999.9, now_ts=1000.0, interval_seconds=30))

    def test_zero_or_negative_interval_disables(self):
        self.assertFalse(should_send_heartbeat(None, now_ts=1000.0, interval_seconds=0))


class HeartbeatEventShapeTests(unittest.TestCase):
    def test_event_type_is_heartbeat_without_bar_fields(self):
        ev = normalize_heartbeat_event(seq=7, stock_code="600103.SH",
                                       account_id="acct", last_price=3.82)
        self.assertEqual(ev["event_type"], EVENT_HEARTBEAT)
        self.assertNotEqual(ev["event_type"], EVENT_QUOTE)
        self.assertEqual(ev["stock_code"], "600103.SH")
        self.assertEqual(ev["last_price"], 3.82)
        for forbidden in ("open", "high", "low", "close", "volume", "amount"):
            self.assertNotIn(forbidden, ev)   # liveness-only, 无行情字段可被误当 tick
        self.assertIn("created_at_ts", ev)


class _MinimalClient:
    def __init__(self):
        self.account_id = "acct"


class HeartbeatDispatchRoutingTests(unittest.TestCase):
    """L3: 心跳与 tick 回调严格分流."""

    def _xtdata(self):
        return BigQmtXtData(_MinimalClient())

    @staticmethod
    def _hb_event(seq=7, code="600103.SH"):
        return json.dumps(normalize_heartbeat_event(
            seq=seq, stock_code=code, account_id="acct", last_price=3.82))

    def test_heartbeat_routes_to_handler_not_tick_callback(self):
        x = self._xtdata()
        cb_hits, hb_hits = [], []
        x._quote_callbacks[7] = cb_hits.append      # 同 seq 注册了 tick 回调
        x.set_quote_heartbeat_handler(hb_hits.append)

        x._dispatch_quote_event(self._hb_event(seq=7))

        self.assertEqual(len(hb_hits), 1)
        self.assertEqual(hb_hits[0]["event_type"], EVENT_HEARTBEAT)
        self.assertEqual(cb_hits, [])               # 关键: tick 回调零触发

    def test_quote_still_routes_to_tick_callback(self):
        x = self._xtdata()
        cb_hits, hb_hits = [], []
        x._quote_callbacks[8] = cb_hits.append
        x.set_quote_heartbeat_handler(hb_hits.append)
        x._dispatch_quote_event(json.dumps({
            "event_type": EVENT_QUOTE, "seq": 8, "stock_code": "000983.SZ",
            "period": "1m", "close": 7.54, "bar_time": "",
        }))
        self.assertEqual(len(cb_hits), 1)
        self.assertEqual(hb_hits, [])

    def test_no_handler_registered_is_silent_noop(self):
        x = self._xtdata()
        x._dispatch_quote_event(self._hb_event())   # 应无异常

    def test_handler_exception_swallowed_not_fatal(self):
        x = self._xtdata()

        def boom(_ev):
            raise RuntimeError("observer broke")

        x.set_quote_heartbeat_handler(boom)
        x._dispatch_quote_event(self._hb_event())   # 异常吞掉, 监视线程不死

    def test_unknown_event_type_still_ignored(self):
        """老 filter 不回退 — 双端非锁定序升级的安全底座."""
        x = self._xtdata()
        hb_hits = []
        x.set_quote_heartbeat_handler(hb_hits.append)
        x._dispatch_quote_event(json.dumps({"event_type": "subscribe_quote", "seq": 1}))
        x._dispatch_quote_event("not-json-at-all")
        self.assertEqual(hb_hits, [])


class StrategyPumpStructuralPins(unittest.TestCase):
    """L2 结构钉: QMT 侧 adjust 进程内逻辑无法单测, 锁源码关键锚点防回退."""

    @classmethod
    def _read_strategy(cls) -> str:
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_strategy.py")
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    def test_dedup_branch_contains_heartbeat_publish(self):
        src = self._read_strategy()
        # 去重分支内必须有心跳判定 + 发布 + 记账三件套
        self.assertIn("_last_quote_heartbeat_at.get(str(seq_val))", src)
        self.assertIn("should_send_heartbeat", src)
        self.assertIn("normalize_heartbeat_event", src)

    def test_heartbeat_interval_configurable_with_default(self):
        src = self._read_strategy()
        self.assertIn("heartbeat_interval_seconds", src)
        self.assertIn("_QUOTE_HEARTBEAT_INTERVAL_SEC", src)


if __name__ == "__main__":
    unittest.main()
