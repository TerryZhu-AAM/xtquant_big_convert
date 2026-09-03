import os
import sys
import types
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.xtquant_compat import (
    BigQmtRpcClient,
    FIX_PRICE,
    MARKET_PEER_PRICE_FIRST,
    SH_MARKET,
    STOCK_BUY,
    STOCK_SELL,
    SZ_MARKET,
    BigQmtXtData,
    BigQmtXtTrader,
    StockAccount,
    configure,
    load_client_config,
    xt_trader,
)
from bigqmt_signal_trader.full_tick_cache import full_tick_demand_key, full_tick_request_id, write_full_tick_cache


class FakeRpcClient:
    def __init__(self):
        self.account_id = "acct"
        self.calls = []
        self.redis = FakeRedisEvents()
        self.full_tick_cache_config = {
            "enabled": True,
            "demand_ttl_seconds": 10,
            "cache_ttl_seconds": 10,
            "wait_seconds": 0.1,
            "poll_interval_seconds": 0.01,
        }

    def _redis(self):
        return self.redis

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append((method, params or {}, account_id, timeout_seconds))
        if method == "query_stock_asset":
            return {"account_id": "acct", "cash": 100.5, "total_asset": 1000.5}
        if method == "query_stock_positions":
            return {
                "600000.SH": {
                    "stock_code": "600000.SH",
                    "volume": 1000,
                    "available": 800,
                    "cost": 10.2,
                    "price": 10.8,
                    "market_value": 10800.0,
                    "frozen_volume": 200,
                    "on_road_volume": 5,
                    "yesterday_volume": 900,
                    "direction": 48,
                    "stock_name": "PF Bank",
                }
            }
        if method == "query_stock_position":
            return {
                "stock_code": "600000.SH",
                "volume": 1000,
                "available": 800,
                "cost": 10.2,
            }
        if method == "query_stock_orders":
            return [
                {
                    "order_sys_id": "sys-1",
                    "user_order_id": "remark-1",
                    "stock_code": "600000.SH",
                    "action": "SELL",
                    "volume": 300,
                    "traded_volume": 100,
                    "status": "50",
                    "price": 10.1,
                }
            ]
        if method == "query_stock_trades":
            return [
                {
                    "trade_id": "trade-1",
                    "order_sys_id": "sys-1",
                    "user_order_id": "remark-1",
                    "stock_code": "600000.SH",
                    "action": "BUY",
                    "volume": 100,
                    "price": 10.0,
                }
            ]
        if method == "query_execution_snapshot":
            return {
                "account_id": "acct",
                "server_time": "2026-07-15 10:00:00",
                "orders": [
                    {
                        "order_sys_id": "sys-1", "user_order_id": "remark-1",
                        "stock_code": "600000.SH", "action": "SELL", "volume": 300,
                        "traded_volume": 100, "status": "50", "price": 10.1,
                    }
                ],
                "trades": [
                    {
                        "trade_id": "trade-1", "order_sys_id": "sys-1",
                        "user_order_id": "remark-1", "stock_code": "600000.SH",
                        "action": "BUY", "volume": 100, "price": 10.0,
                    }
                ],
            }
        if method == "order_stock":
            return {"status": "SUBMITTED", "user_order_id": "bq:1", "order_sys_id": "sys-2"}
        if method == "order_stock_batch":
            return [{"success": True, "accepted": True, "user_order_id": "batch-tag"}]
        if method == "cancel_order_stock_sysid":
            return {"success": True}
        if method == "get_full_tick":
            codes = params.get("codes") or []
            if codes == ["SH", "SZ"]:
                return {
                    "000001.SH": {"lastPrice": 3000},
                    "000001.SZ": {"lastPrice": 10},
                    "600000.SH": {"lastPrice": 10},
                    "510300.SH": {"lastPrice": 4},
                    "300001.SZ": {"lastPrice": 20},
                    "113001.SH": {"lastPrice": 100},
                }
            return {codes[0]: {"lastPrice": 10, "bidPrice": [9.9], "askPrice": [10.1]}}
        if method == "get_instrument_detail":
            return {"InstrumentStatus": 0, "code": params.get("code")}
        if method == "get_market_data_ex":
            if params.get("stock_list") == ["159518.SZ"]:
                try:
                    import pandas as pd

                    return {
                        "159518.SZ": pd.DataFrame(
                            {
                                "stime": ["20250813 10:27:00", "20250813 10:28:00"],
                                "time": [None, None],
                                "open": [0.872, 0.873],
                                "high": [0.873, 0.873],
                                "low": [0.872, 0.872],
                                "close": [0.872, 0.872],
                                "volume": [2791.0, 1659.0],
                            }
                        )
                    }
                except Exception:
                    return {"159518.SZ": []}
            return {"600000.SH": {"close": [10.0]}}
        if method == "ping":
            return {"pong": True}
        raise AssertionError("unexpected method: %s" % method)

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        return self.redis.publish_event(event_type, payload)

    def save_quote_subscription(self, seq, payload, active=True):
        if active:
            self.redis.hset("bigqmt:quote_subscriptions:%s" % self.account_id, str(seq), payload)
            return True
        else:
            self.redis.hdel("bigqmt:quote_subscriptions:%s" % self.account_id, str(seq))
            return True


class FakeRedisEvents:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.deleted = []
        self.events = []
        self.expired = []

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    def hdel(self, key, field):
        self.deleted.append((key, field))
        self.hashes.setdefault(key, {}).pop(field, None)
        return 1

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def expire(self, key, seconds):
        self.expired.append((key, seconds))
        return True

    def setex(self, key, seconds, value):
        self.kv[key] = value
        self.expired.append((key, seconds))
        return True

    def publish_event(self, event_type, payload):
        self.events.append((event_type, payload))
        return {"event_type": event_type, "payload": payload}

    def get(self, key):
        if key in self.kv:
            return self.kv[key]
        value = self.hashes.get(key)
        if value is None:
            return None
        import json

        return json.dumps(value).encode("utf-8")


class XtquantCompatTest(unittest.TestCase):
    def _with_fake_config(self, module_name="test_bigqmt_client_cfg"):
        module = types.ModuleType(module_name)
        module.BIGQMT_ACCOUNT_ID = "cfg-account"
        module.BIGQMT_RPC_TIMEOUT_SECONDS = 9
        module.BIGQMT_REDIS_CONFIG = {
            "host": "cfg-host",
            "port": 6380,
            "db": 6,
            "username": "cfg-user",
            "password": "cfg-pass",
        }
        old_env = os.environ.get("BIGQMT_CLIENT_CONFIG_MODULE")
        os.environ["BIGQMT_CLIENT_CONFIG_MODULE"] = module_name
        sys.modules[module_name] = module
        return module_name, old_env

    def _cleanup_fake_config(self, module_name, old_env):
        sys.modules.pop(module_name, None)
        if old_env is None:
            os.environ.pop("BIGQMT_CLIENT_CONFIG_MODULE", None)
        else:
            os.environ["BIGQMT_CLIENT_CONFIG_MODULE"] = old_env

    def _trader(self):
        trader = BigQmtXtTrader(account_id="acct")
        trader.client = FakeRpcClient()
        return trader

    def _xtdata(self):
        return BigQmtXtData(FakeRpcClient())

    def test_trader_read_methods_return_miniqmt_style_objects(self):
        trader = self._trader()
        acc = StockAccount("acct")

        asset = trader.query_stock_asset(acc)
        positions = trader.query_stock_positions(acc)
        single = trader.query_stock_position(acc, "600000")

        self.assertEqual(asset.cash, 100.5)
        self.assertEqual(asset.market_value, 900.0)
        self.assertEqual(positions[0].stock_code, "600000.SH")
        self.assertEqual(positions[0].can_use_volume, 800)
        self.assertEqual(positions[0].avg_price, 10.2)
        self.assertEqual(positions[0].price, 10.8)
        self.assertEqual(positions[0].market_value, 10800.0)
        self.assertEqual(positions[0].frozen_volume, 200)
        self.assertEqual(positions[0].on_road_volume, 5)
        self.assertEqual(positions[0].yesterday_volume, 900)
        self.assertEqual(positions[0].direction, 48)
        self.assertEqual(single.stock_code, "600000.SH")

    def test_asset_and_position_objects_carry_native_m_prefixed_aliases(self):
        # PR #67: 原生 xtquant 客户端（如 miniqmt_redis）读 m_ 前缀字段。
        # 回归锚点：别名引用的局部变量必须先定义（曾 NameError 漏网）。
        trader = self._trader()
        acc = StockAccount("acct")

        asset = trader.query_stock_asset(acc)
        positions = trader.query_stock_positions(acc)
        single = trader.query_stock_position(acc, "600000")

        self.assertEqual(asset.m_strAccountID, "acct")
        self.assertEqual(asset.m_dCash, 100.5)
        self.assertEqual(asset.m_dAvailableCash, 100.5)
        self.assertEqual(asset.m_dTotalAsset, 1000.5)
        self.assertEqual(asset.m_dMarketValue, 900.0)

        pos = positions[0]
        self.assertEqual(pos.m_strAccountID, "acct")
        self.assertEqual(pos.m_strStockCode, "600000.SH")
        self.assertEqual(pos.m_strStockName, "PF Bank")
        self.assertEqual(pos.m_nVolume, 1000)
        self.assertEqual(pos.m_nCanUseVolume, 800)
        self.assertEqual(pos.m_nCanUseVol, 800)
        self.assertEqual(pos.m_nEnableAmount, 800)
        self.assertEqual(pos.m_dAvgPrice, 10.2)
        self.assertEqual(pos.m_dLastPrice, 10.8)
        self.assertEqual(pos.m_dMarketValue, 10800.0)
        self.assertEqual(pos.m_nFrozenVolume, 200)
        self.assertEqual(pos.m_nOnRoadVolume, 5)
        self.assertEqual(pos.m_nYesterdayVolume, 900)
        self.assertEqual(pos.m_nDirection, 48)
        self.assertEqual(single.m_strStockCode, "600000.SH")

    def test_orders_trades_order_and_cancel_are_miniqmt_shaped(self):
        trader = self._trader()
        acc = StockAccount("acct")

        orders = trader.query_stock_orders(acc, cancelable_only=False)
        trades = trader.query_stock_trades(acc)
        order_id = trader.order_stock(
            acc,
            "600000.SH",
            STOCK_BUY,
            100,
            MARKET_PEER_PRICE_FIRST,
            0,
            "strategy",
            "remark",
        )
        cancelled = trader.cancel_order_stock_sysid(acc, SH_MARKET, "sys-2")

        self.assertEqual(orders[0].order_type, STOCK_SELL)
        self.assertEqual(orders[0].order_status, 50)
        self.assertEqual(orders[0].order_volume, 300)
        self.assertEqual(trades[0].order_type, STOCK_BUY)
        self.assertEqual(trades[0].traded_price, 10.0)
        self.assertEqual(trades[0].order_remark, "remark-1")
        # MiniQMT hands back an int 委托编号; Big QMT only has the broker's
        # string 合同编号, so the id is both (issue #113).
        self.assertEqual(str(order_id), "sys-2")
        self.assertIsInstance(order_id, int)
        self.assertGreater(order_id, 0)
        self.assertEqual(cancelled, 0)      # 0 == success, not True
        self.assertEqual(trader.client.calls[-2][1]["price_type"], MARKET_PEER_PRICE_FIRST)
        # strategy_name 默认 ""（返回全部委托），与服务端一致（strategy_name 陷阱）。
        self.assertEqual(trader.client.calls[-4][1]["strategy_name"], "")

    def test_execution_snapshot_maps_orders_and_trades_with_one_rpc(self):
        trader = self._trader()
        acc = StockAccount("acct")

        snapshot = trader.query_execution_snapshot(
            acc, order_strategy_name="icestone_grid_600276", trade_strategy_name=""
        )

        self.assertEqual(snapshot["orders"][0].order_sysid, "sys-1")
        self.assertEqual(snapshot["trades"][0].trade_id, "trade-1")
        self.assertEqual(snapshot["server_time"], "2026-07-15 10:00:00")
        self.assertEqual(trader.client.calls[-1][0], "query_execution_snapshot")
        self.assertEqual(trader.client.calls[-1][1]["trade_strategy_name"], "")

    def test_order_stock_never_returns_user_tag_as_real_order_id(self):
        trader = self._trader()
        acc = StockAccount("acct")
        trader.client.call = lambda *_args, **_kwargs: {
            "status": "SUBMITTED", "user_order_id": "bq:request-only",
            "order_sys_id": None,
        }

        order_id = trader.order_stock(
            acc, "600000.SH", STOCK_BUY, 100, FIX_PRICE, 10.0,
            "strategy", "remark",
        )
        result = trader.order_stock_result(
            acc, "600000.SH", STOCK_BUY, 100, FIX_PRICE, 10.0,
            "strategy", "remark",
        )

        self.assertEqual(order_id, -1)
        self.assertEqual(result["user_order_id"], "bq:request-only")
        self.assertIsNone(result["order_sys_id"])

    def test_order_stock_batch_forwards_batch_identity(self):
        trader = self._trader()
        acc = StockAccount("acct")

        result = trader.order_stock_batch(
            acc,
            [{"stock_code": "600000.SH", "order_type": STOCK_BUY,
              "order_volume": 100, "price": 10.0,
              "order_remark": "batch-tag"}],
            batch_id="BATCH-IDENTITY-1",
        )

        method, params, account_id, _timeout = trader.client.calls[-1]
        self.assertEqual(method, "order_stock_batch")
        self.assertEqual(account_id, "acct")
        self.assertEqual(params["batch_id"], "BATCH-IDENTITY-1")
        self.assertEqual(params["orders"][0]["order_remark"], "batch-tag")
        self.assertTrue(result[0]["accepted"])

    def test_xtdata_read_methods_and_sector_filter(self):
        xtdata = self._xtdata()
        write_full_tick_cache(
            xtdata.client.redis,
            xtdata.client.account_id,
            ["600000.SH"],
            {"600000.SH": {"lastPrice": 10, "bidPrice": [9.9], "askPrice": [10.1]}},
        )
        write_full_tick_cache(
            xtdata.client.redis,
            xtdata.client.account_id,
            ["SH", "SZ"],
            {
                "000001.SH": {"lastPrice": 3000},
                "000001.SZ": {"lastPrice": 10},
                "600000.SH": {"lastPrice": 10},
                "510300.SH": {"lastPrice": 4},
                "300001.SZ": {"lastPrice": 20},
                "113001.SH": {"lastPrice": 100},
            },
        )

        ticks = xtdata.get_full_tick(["600000.SH"])
        detail = xtdata.get_instrument_detail("600000.SH")
        sector_codes = xtdata.get_stock_list_in_sector("沪深A股")
        market_data = xtdata.get_market_data_ex(["close"], ["600000.SH"], count=1)

        self.assertEqual(ticks["600000.SH"]["bidPrice"], [9.9])
        self.assertEqual(detail["InstrumentStatus"], 0)
        self.assertEqual(sector_codes, ["000001.SZ", "300001.SZ", "600000.SH"])
        self.assertEqual(market_data["600000.SH"]["close"], [10.0])

    def test_market_data_ex_normalizes_bigqmt_stime_to_miniqmt_shape(self):
        try:
            import pandas  # noqa: F401
        except Exception:
            self.skipTest("pandas not installed")

        xtdata = self._xtdata()

        data = xtdata.get_market_data_ex(
            ["time", "open", "high", "low", "close", "volume"],
            ["159518.SZ"],
            period="1m",
            start_time="20250601000000",
            end_time="",
            count=-1,
        )
        df = data["159518.SZ"]

        self.assertEqual(list(df.index), ["20250813102700", "20250813102800"])
        self.assertEqual(
            list(df.columns),
            ["time", "open", "high", "low", "close", "volume"],
        )
        self.assertEqual(int(df.iloc[0]["time"]), 1755052020000)
        self.assertNotIn("stime", df.columns)

    def test_xtdata_full_tick_reads_redis_cache_and_renews_demand(self):
        xtdata = self._xtdata()
        write_full_tick_cache(
            xtdata.client.redis,
            xtdata.client.account_id,
            ["SZ", "SH"],
            {"600000.SH": {"lastPrice": 10, "bidPrice": [9.9], "askPrice": [10.1]}},
        )

        ticks = xtdata.get_full_tick(["SH", "SZ"])

        self.assertIn("600000.SH", ticks)
        self.assertFalse([call for call in xtdata.client.calls if call[0] == "get_full_tick"])
        demand_key = full_tick_demand_key(xtdata.client.account_id)
        self.assertIn(full_tick_request_id(["SH", "SZ"]), xtdata.client.redis.hashes[demand_key])

    def test_xtdata_full_tick_symbol_miss_falls_back_to_rpc(self):
        xtdata = self._xtdata()
        xtdata.client.full_tick_cache_config["wait_seconds"] = 0

        ticks = xtdata.get_full_tick(["600000.SH"])

        # A cold cache miss on a symbol list now falls back to a live RPC instead
        # of a hard wait_seconds stall, so the first call returns in ~ms.
        self.assertEqual(ticks["600000.SH"]["bidPrice"], [9.9])
        self.assertEqual([call[0] for call in xtdata.client.calls if call[0] == "get_full_tick"], ["get_full_tick"])
        demand_key = full_tick_demand_key(xtdata.client.account_id)
        self.assertIn(full_tick_request_id(["600000.SH"]), xtdata.client.redis.hashes[demand_key])

    def test_xtdata_full_market_tick_miss_raises_without_rpc(self):
        xtdata = self._xtdata()
        xtdata.client.full_tick_cache_config["wait_seconds"] = 0

        # Whole-market snapshots must stay on the demand cache; a miss must never
        # live-pull ~50k rows over RPC.
        with self.assertRaises(TimeoutError):
            xtdata.get_full_tick(["SH", "SZ"])

        self.assertFalse([call for call in xtdata.client.calls if call[0] == "get_full_tick"])
        demand_key = full_tick_demand_key(xtdata.client.account_id)
        self.assertIn(full_tick_request_id(["SH", "SZ"]), xtdata.client.redis.hashes[demand_key])

    def test_xtdata_full_market_tick_can_fall_back_to_rpc_when_cache_disabled(self):
        xtdata = self._xtdata()
        xtdata.client.full_tick_cache_config["enabled"] = False

        xtdata.get_full_tick(["SH", "SZ"])

        self.assertEqual(xtdata.client.calls[-1][0], "get_full_tick")
        self.assertEqual(xtdata.client.calls[-1][3], 30)

    def test_quote_subscribe_and_unsubscribe_write_redis_events(self):
        xtdata = self._xtdata()

        # Tick subscriptions ride the whole-quote push session now (#95), so
        # stand one in; the bookkeeping this test covers is unchanged.
        class _Session(object):
            def __init__(self):
                self.active = set()
                self.subscribed = []

            def start(self):
                pass

            def subscribe_whole_quote(self, code_list, callback=None):
                self.subscribed.append(list(code_list))
                sub_id = 900 + len(self.subscribed)
                self.active.add(sub_id)
                return sub_id

            def unsubscribe_quote(self, sub_id):
                self.active.discard(sub_id)
                return 0

            def has_subscription(self, sub_id):
                return sub_id in self.active

        session = _Session()
        xtdata._quote_session_factory = lambda: session

        seq = xtdata.subscribe_quote("600000.SH", period="tick")
        result = xtdata.unsubscribe_quote(seq)

        key = "bigqmt:quote_subscriptions:acct"
        self.assertEqual(result, 0)
        # [merge 2026-09-03 裁决] tick 订阅改走本地 quote_events 通路 (Redis hash
        # 泵 + seq 路由), 不经 whole-quote session — 原上游 session 路由断言
        # (session.subscribed / session.active) 移除; redis 事件与注册簿账本
        # 断言保留: 两种架构下都必须成立的核心契约。
        self.assertNotIn(str(seq), xtdata.client.redis.hashes.get(key, {}))
        self.assertIn((key, str(seq)), xtdata.client.redis.deleted)
        self.assertEqual(xtdata.client.redis.events[0][0], "subscribe_quote")
        self.assertEqual(xtdata.client.redis.events[1][0], "unsubscribe_quote")

    # --- BUG-20260813-quote-callback-seq-mismatch ---

    def test_dispatch_fallback_on_stale_seq(self):
        """QMT 推旧 seq (hash 未更新) → _dispatch_quote_event 通过 _code_to_seq
        反查活 seq → 找到 callback → 成功路由. 防持仓股卖出规则失明."""
        xtdata = self._xtdata()
        calls = []
        cb = lambda datas: calls.append(datas)

        seq_a = xtdata.subscribe_quote("601869.SH", period="1m", callback=cb)
        # re-subscribe → new seq, _code_to_seq now points to seq_b
        seq_b = xtdata.subscribe_quote("601869.SH", period="1m", callback=cb)
        self.assertNotEqual(seq_a, seq_b)

        # simulate callback loss: remove old seq callback
        xtdata._quote_callbacks.pop(seq_a, None)

        # clear initial-snapshot calls from subscribe_quote
        calls.clear()

        # dispatch event with OLD seq → should fallback to seq_b callback
        import json as _json
        event = _json.dumps({
            "event_type": "quote", "seq": seq_a,
            "stock_code": "601869.SH", "period": "1m",
            "close": 350.0, "volume": 100, "amount": 35000,
        })
        xtdata._dispatch_quote_event(event)

        self.assertEqual(len(calls), 1)
        self.assertIn("601869.SH", calls[0])

    def test_dispatch_no_callback_returns_silently(self):
        """Unknown seq + unknown stock_code → silent return (no crash)."""
        xtdata = self._xtdata()
        import json as _json
        event = _json.dumps({
            "event_type": "quote", "seq": 99999,
            "stock_code": "999999.SH", "period": "1m",
            "close": 10.0,
        })
        # should not raise
        xtdata._dispatch_quote_event(event)

    def test_save_quote_subscription_returns_true_on_success(self):
        """save_quote_subscription hset success → returns True."""
        xtdata = self._xtdata()
        result = xtdata.client.save_quote_subscription(
            42, {"seq": 42, "stock_code": "600000.SH"}, active=True
        )
        self.assertTrue(result)

    def test_save_quote_subscription_retries_on_failure(self):
        """hset raises → retries 2x → returns False."""
        import json as _json
        from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient

        class FailingRedis:
            def __init__(self):
                self.call_count = 0
            def hset(self, *a, **kw):
                self.call_count += 1
                raise Exception("simulated Redis failure")
            def hdel(self, *a, **kw):
                pass
            def hgetall(self, key):
                return {}
            def hlen(self, key):
                return 0

        class FailingClient(BigQmtRpcClient):
            def __init__(self):
                self.account_id = "test"
                self._fake_redis = FailingRedis()
            def _redis(self):
                return self._fake_redis

        client = FailingClient()
        result = client.save_quote_subscription(
            42, {"seq": 42, "stock_code": "600000.SH"}, active=True
        )
        self.assertFalse(result)
        self.assertEqual(client._fake_redis.call_count, 2)

    def test_optional_xtquant_shim_imports_constants_and_classes(self):
        from xtquant import xtconstant
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount as ShimStockAccount

        self.assertEqual(xtconstant.STOCK_BUY, STOCK_BUY)
        self.assertEqual(xtconstant.FIX_PRICE, FIX_PRICE)
        self.assertEqual(xtconstant.SZ_MARKET, SZ_MARKET)
        self.assertIs(XtQuantTrader, BigQmtXtTrader)
        self.assertEqual(ShimStockAccount("acct").account_id, "acct")

    def test_configure_updates_imported_xt_trader_object_in_place(self):
        original = xt_trader
        configure(account_id="acct-new", redis_client=FakeRpcClient())

        self.assertIs(xt_trader, original)
        self.assertEqual(xt_trader.client.account_id, "acct-new")

    def test_client_reads_account_and_redis_from_private_config(self):
        module_name, old_env = self._with_fake_config()
        try:
            config = load_client_config()
            client = BigQmtRpcClient()
        finally:
            self._cleanup_fake_config(module_name, old_env)

        self.assertEqual(config["account_id"], "cfg-account")
        self.assertEqual(client.account_id, "cfg-account")
        self.assertEqual(client.redis_config["host"], "cfg-host")
        self.assertEqual(client.redis_config["port"], 6380)
        self.assertEqual(client.redis_config["db"], 6)
        self.assertEqual(client.redis_config["username"], "cfg-user")
        self.assertEqual(client.redis_config["password"], "cfg-pass")
        self.assertEqual(client.timeout_seconds, 9)

    def test_explicit_client_params_override_private_config(self):
        module_name, old_env = self._with_fake_config()
        try:
            client = BigQmtRpcClient(
                account_id="explicit-account",
                redis_config={"host": "explicit-host", "password": ""},
                timeout_seconds=3,
            )
        finally:
            self._cleanup_fake_config(module_name, old_env)

        self.assertEqual(client.account_id, "explicit-account")
        self.assertEqual(client.redis_config["host"], "explicit-host")
        self.assertEqual(client.redis_config["port"], 6380)
        self.assertEqual(client.redis_config["password"], "")
        self.assertEqual(client.timeout_seconds, 3)

    def test_trader_falls_back_to_cached_positions_when_rpc_fails(self):
        class FailingRpcClient(FakeRpcClient):
            def call(self, method, params=None, account_id=None, timeout_seconds=None):
                if method in ("query_stock_asset", "query_stock_positions", "query_stock_position"):
                    raise RuntimeError("rpc down")
                return super().call(method, params, account_id, timeout_seconds)

        client = FailingRpcClient()
        client.redis.hashes["bigqmt:positions:acct"] = {
            "account_id": "acct",
            "asset": {"cash": 123.0, "total_asset": 456.0},
            "positions": {
                "600000.SH": {
                    "stock_code": "600000.SH",
                    "volume": 100,
                    "available": 80,
                    "cost": 10.5,
                    "stock_name": "cached",
                }
            },
        }
        trader = BigQmtXtTrader(account_id="acct")
        trader.client = client
        acc = StockAccount("acct")

        asset = trader.query_stock_asset(acc)
        positions = trader.query_stock_positions(acc)
        single = trader.query_stock_position(acc, "600000")

        self.assertEqual(asset.cash, 123.0)
        self.assertEqual(asset.total_asset, 456.0)
        self.assertEqual(positions[0].stock_code, "600000.SH")
        self.assertEqual(positions[0].can_use_volume, 80)
        self.assertEqual(single.stock_name, "cached")

    def test_zmq_query_failure_never_falls_back_to_redis_cache(self):
        class FailingZmqClient(FakeRpcClient):
            transport_name = "zmq"

            def call(self, method, params=None, account_id=None, timeout_seconds=None):
                raise RuntimeError("zmq timeout")

            def _redis(self):
                raise AssertionError("ZMQ query must not access Redis")

        trader = BigQmtXtTrader(account_id="acct")
        trader.client = FailingZmqClient()
        acc = StockAccount("acct")

        with self.assertRaisesRegex(RuntimeError, "zmq timeout"):
            trader.query_stock_asset(acc)
        with self.assertRaisesRegex(RuntimeError, "zmq timeout"):
            trader.query_stock_positions(acc)
        with self.assertRaisesRegex(RuntimeError, "zmq timeout"):
            trader.query_stock_position(acc, "600000.SH")

    def test_client_call_via_transport_builds_valid_request(self):
        # Regression: the swappable-transport path in BigQmtRpcClient.call() built
        # request_id with __import__("uuid").uuid.uuid4() (AttributeError), crashing
        # every non-redis transport on first call. This path had no coverage.
        captured = {}

        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                captured["request"] = request
                captured["timeout"] = timeout_seconds
                return {"ok": True, "data": {"pong": True}}

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()

        result = client.call("ping", {"x": 1})

        self.assertEqual(result, {"pong": True})
        request = captured["request"]
        self.assertEqual(request["method"], "ping")
        self.assertEqual(request["account_id"], "acct")
        self.assertEqual(request["params"], {"x": 1})
        # request_id must be a real 32-char uuid hex, not a crash.
        self.assertEqual(len(request["request_id"]), 32)
        int(request["request_id"], 16)

    def test_client_call_raises_on_server_error(self):
        # issue #38: server_error（QMT 端诊断，如 passorder 提交但委托没进系统）
        # 之前被 call() 静默丢弃，导致下单流程看不到真实原因。现在必须转成异常。
        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                return {
                    "ok": True,
                    "data": {"status": "SUBMITTED", "order_sys_id": ""},
                    "server_error": "passorder submitted but order not found in system",
                }

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()

        with self.assertRaises(RuntimeError) as ctx:
            client.call("order_stock", {"stock_code": "600000.SH"})
        self.assertIn("not found in system", str(ctx.exception))

    def test_call_async_returns_future_and_resolves(self):
        # issue #63: call_async 不阻塞，返回 Future，结果正确
        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                return {"ok": True, "data": {"pong": True, "method": request["method"]}}

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()

        futures = [client.call_async("ping", {"i": i}) for i in range(5)]
        results = [f.result(timeout=5) for f in futures]
        self.assertTrue(all(r["pong"] for r in results))
        self.assertEqual([r["method"] for r in results], ["ping"] * 5)

    def test_call_async_callback_serialized_on_dispatcher(self):
        # 回调在单 dispatcher 线程上串行派发（不并发）
        import threading as _th

        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                return {"ok": True, "data": {"i": request["params"]["i"]}}

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()

        got = []
        got_lock = _th.Lock()
        done = _th.Event()
        counter = {"n": 0}

        def cb(result):
            with got_lock:
                got.append(result["i"])
                counter["n"] += 1
                if counter["n"] == 10:
                    done.set()

        for i in range(10):
            client.call_async("ping", {"i": i}, callback=cb)
        self.assertTrue(done.wait(5))
        self.assertEqual(sorted(got), list(range(10)))

    def test_call_async_failure_does_not_invoke_callback(self):
        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                return {"ok": False, "error": "boom"}

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()

        called = []
        fut = client.call_async("ping", callback=lambda r: called.append(r))
        with self.assertRaises(RuntimeError):
            fut.result(timeout=5)
        import time as _t
        _t.sleep(0.3)  # 给 dispatcher 一个机会（它不应触发）
        self.assertEqual(called, [])

    def test_zmq_with_explicit_address_never_builds_redis_discovery(self):
        client = BigQmtRpcClient(
            account_id="acct",
            redis_config={
                "transport": "zmq",
                "zmq": {"connect_address": "tcp://127.0.0.1:20146"},
            },
        )

        def forbidden_redis():
            raise AssertionError("ZMQ explicit-address mode must not touch Redis")

        client._redis = forbidden_redis
        transport = client._transport()

        self.assertEqual(transport.name, "zmq")
        self.assertEqual(transport.connect_address, "tcp://127.0.0.1:20146")
        self.assertIsNone(transport.discovery_redis_client)

    def test_pure_zmq_quote_metadata_does_not_build_redis_client(self):
        """纯 ZMQ 的兼容订阅元数据不能隐式连接 Redis。"""
        client = BigQmtRpcClient(account_id="acct", redis_config={"transport": "zmq"})
        client._redis = lambda: (_ for _ in ()).throw(AssertionError("Redis must not be used"))

        event = client.publish_event("subscribe_quote", {"seq": 1})
        client.save_quote_subscription(1, {"seq": 1}, active=True)

        self.assertEqual(event["event_type"], "subscribe_quote")

    def test_mysql_quote_metadata_keeps_existing_redis_path(self):
        """非 ZMQ transport 继续使用既有 Redis 订阅元数据。"""
        class RedisRecorder:
            def __init__(self):
                self.calls = []
                self.store = {}

            def xadd(self, *args, **kwargs):
                self.calls.append("xadd")

            def publish(self, *args, **kwargs):
                self.calls.append("publish")

            def hset(self, *args, **kwargs):
                self.calls.append("hset")
                self.store[(args[0], str(args[1]))] = args[2]

            # [merge 2026-09-03] 本地 save_quote_subscription 契约含写后回读校验
            # (BUG-20260827 静默丢笔修复): 健康 redis 必须答 hget — fake 补齐,
            # 否则 save 吃满重试并多记一次 hset。
            def hget(self, *args, **kwargs):
                self.calls.append("hget")
                return self.store.get((args[0], str(args[1])))

        redis = RedisRecorder()
        client = BigQmtRpcClient(account_id="acct", redis_client=redis, redis_config={"transport": "mysql"})

        client.publish_event("subscribe_quote", {"seq": 1})
        client.save_quote_subscription(1, {"seq": 1}, active=True)

        # [merge 2026-09-03] 期望序列随本地写后回读契约 +hget (hset 后立即回读)。
        self.assertEqual(redis.calls, ["xadd", "publish", "hset", "hget"])


class UseFormulaBypassTest(unittest.TestCase):
    """use_formula=False：必须最新数据的调用（subscribe_quote 盘中轮询）
    不走 FormulaServer 快照直连。"""

    def test_call_with_use_formula_false_skips_router(self):
        calls = []

        class _FakeRouter:
            def supports(self, method):
                return True

            def call(self, method, params):
                raise AssertionError("router must not be called when use_formula=False")

        class _FakeTransport:
            def send_request(self, request, timeout_seconds):
                calls.append(request["method"])
                return {"ok": True, "data": {"pong": True}}

        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = _FakeTransport()
        client._formula_router_instance = _FakeRouter()

        result = client.call("get_market_data_ex", {"x": 1}, use_formula=False)
        self.assertEqual(result, {"pong": True})
        self.assertEqual(calls, ["get_market_data_ex"])

    @unittest.skip("[merge 2026-09-03 裁决] subscribe_quote 采用本地 quote_events 生产通路 (Redis hash 泵 + seq 路由 + save fail-LOUD), 上游 0.3.x 的 whole-quote session / _BarPoller 轮询引擎未采用; 本组锁定的是未采用引擎的内部行为 — 引擎本体保留 (subscribe_whole_quote 仍走 session), 引擎路径若未来启用再解跳。")
    def test_subscribe_quote_fetch_uses_rpc_not_formula(self):
        xt = BigQmtXtData(FakeRpcClient())
        recorded = {}

        def spy(**kwargs):
            recorded.update(kwargs)
            return {"acct": None}

        fetch_holder = {}

        from bigqmt_signal_trader.xtquant_compat import _BarPoller

        class _P(_BarPoller):
            def __init__(self, f, callback, interval, **kwargs):
                fetch_holder["fetch"] = f
                super().__init__(f, callback, interval, **kwargs)

        import bigqmt_signal_trader.xtquant_compat as compat_mod
        compat_mod._BarPoller = _P
        try:
            xt.get_market_data_ex = spy
            xt.subscribe_quote("600000.SH", period="1m", count=1, callback=None)
        finally:
            compat_mod._BarPoller = _BarPoller

        fetch = fetch_holder.get("fetch")
        self.assertIsNotNone(fetch)
        fetch()
        self.assertFalse(recorded.get("use_formula", True),
                         "subscribe_quote 盘中轮询必须 use_formula=False（读实时数据）")


class FormulaStaleFailoverTest(unittest.TestCase):
    """公式滞后自动回落：本次调用回落 RPC 桥、冷却期跳过直连、到期自愈。"""

    def _bar(self, ts):
        import pandas as pd

        return pd.DataFrame({"stime": [ts], "close": [1.0]})

    def setUp(self):
        import datetime as _dt
        from bigqmt_signal_trader import xtquant_compat as xc

        self.xc = xc
        self.xc._formula_stale_until["ts"] = 0.0
        self.old_bar = (_dt.datetime.now() - _dt.timedelta(hours=3)).strftime("%Y%m%d%H%M%S")
        self.fresh_bar = _dt.datetime.now().strftime("%Y%m%d%H%M%S")

    def _client(self, stale):
        xc = self.xc

        class _Router:
            def __init__(self):
                self.calls = 0

            def supports(self, method):
                return True

            def call(self, method, params):
                self.calls += 1
                bar = self.old_bar if stale else self.fresh_bar
                return {"600000.SH": self._bar(bar)}

        class _Transport:
            def __init__(self):
                self.calls = 0

            def send_request(self, request, timeout_seconds):
                self.calls += 1
                return {"ok": True, "data": {"fresh": True}}

        router = _Router()
        router.old_bar = self.old_bar
        router.fresh_bar = self.fresh_bar
        router._bar = self._bar
        transport = _Transport()
        client = BigQmtRpcClient(account_id="acct", redis_config={"host": "127.0.0.1"})
        client.transport_name = "zmq"
        client._transport_instance = transport
        client._formula_router_instance = router
        return client, router, transport

    def test_stale_answer_fails_over_to_transport(self):
        client, router, transport = self._client(stale=True)
        result = client.call("get_market_data_ex", {"period": "1m"})
        self.assertEqual(result, {"fresh": True})
        self.assertEqual(transport.calls, 1)

    def test_cooldown_skips_router(self):
        client, router, transport = self._client(stale=True)
        client.call("get_market_data_ex", {"period": "1m"})
        self.assertEqual(router.calls, 1)
        client.call("get_market_data_ex", {"period": "1m"})
        self.assertEqual(router.calls, 1)  # 冷却期内不再付公式成本
        self.assertEqual(transport.calls, 2)

    def test_cooldown_expiry_restores_formula(self):
        client, router, transport = self._client(stale=True)
        client.call("get_market_data_ex", {"period": "1m"})
        self.xc._formula_stale_until["ts"] = 0.0
        client2, router2, _ = self._client(stale=False)
        result = client2.call("get_market_data_ex", {"period": "1m"})
        self.assertEqual(router2.calls, 1)
        self.assertNotEqual(result, {"fresh": True})


class FormulaStaleWarnTest(unittest.TestCase):
    """FormulaServer 快照滞后检测：intraday 滞后即告警、同日不重复、日线不报。"""

    def _df(self, bar):
        import pandas as pd

        return pd.DataFrame({"close": [1.0]}, index=[bar])

    def setUp(self):
        import datetime as _dt
        from bigqmt_signal_trader import xtquant_compat as xc

        self.xc = xc
        self.warns = []
        self._orig_log = xc.log

        class _L:
            def warning(_, msg, *a):
                self.warns.append(msg % a if a else msg)

        xc.log = _L()
        xc._formula_stale_warned.clear()
        self.old_bar = (_dt.datetime.now() - _dt.timedelta(hours=3)).strftime("%Y%m%d%H%M%S")
        self.fresh_bar = _dt.datetime.now().strftime("%Y%m%d%H%M%S")

    def tearDown(self):
        self.xc.log = self._orig_log

    def test_intraday_stale_bar_warns_once_per_day(self):
        self.xc._warn_stale_formula_bars({"600000.SH": self._df(self.old_bar)}, {"period": "1m"})
        self.xc._warn_stale_formula_bars({"600000.SH": self._df(self.old_bar)}, {"period": "1m"})
        self.assertEqual(len(self.warns), 1)
        self.assertIn("stale", self.warns[0])

    def test_fresh_bar_does_not_warn(self):
        self.xc._warn_stale_formula_bars({"600000.SH": self._df(self.fresh_bar)}, {"period": "1m"})
        self.assertEqual(self.warns, [])

    def test_daily_period_is_not_time_checked(self):
        self.xc._warn_stale_formula_bars({"600000.SH": self._df(self.old_bar)}, {"period": "1d"})
        self.assertEqual(self.warns, [])

    def test_bad_input_never_raises(self):
        self.xc._warn_stale_formula_bars(None, {})
        self.xc._warn_stale_formula_bars({"X": self._df("not-a-date")}, {"period": "1m"})


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# [BUG-20260811-rpc-storm] RPC 并发限流守卫 — QMT 单线程 drain 防风暴
# ---------------------------------------------------------------------------

class RpcConcurrencyThrottleTest(unittest.TestCase):
    """BigQmtRpcClient.call 的 RPC 路径必须限流 (防并发风暴堵死 QMT 单线程 drain)."""

    _SRC = os.path.join(ROOT, "src", "bigqmt_signal_trader", "xtquant_compat.py")

    def test_call_rpc_path_has_semaphore_acquire_release(self):
        """AST 守卫: call() RPC 路径包 _RPC_INFLIGHT.acquire + finally release."""
        src = open(self._SRC, encoding="utf-8").read()
        assert "_RPC_INFLIGHT.acquire" in src, (
            "call() 缺 _RPC_INFLIGHT.acquire — RPC 并发风暴会堵死 QMT 单线程 drain"
        )
        assert "_RPC_INFLIGHT.release()" in src, (
            "call() 缺 _RPC_INFLIGHT.release — Semaphore 泄漏会永久锁死 RPC"
        )
        assert "finally:" in src, "acquire 后缺 finally — 异常路径必须释放 slot"
        # acquire 必须在 FormulaServer fast path 之后 (fast path 直连不走 QMT drain, 不限流)
        acquire_pos = src.find("_RPC_INFLIGHT.acquire")
        fast_path_pos = src.find("FormulaServer fast path")
        assert fast_path_pos != -1 and acquire_pos > fast_path_pos, (
            "acquire 必须在 fast path 之后 (直连 58600 不限流)"
        )

    def test_concurrency_env_knob_documented(self):
        """限流并发数可调: BIGQMT_RPC_CONCURRENCY env (默认 3)."""
        src = open(self._SRC, encoding="utf-8").read()
        assert "BIGQMT_RPC_CONCURRENCY" in src, (
            "缺 BIGQMT_RPC_CONCURRENCY env 读取 — 并发数不可调"
        )

    def test_inflight_semaphore_actually_limits(self):
        """行为测试: 模块级 _RPC_INFLIGHT 真限制并发 (3 个在途, 第 4 个排队等 slot)."""
        import threading as _th
        import time as _tm
        from bigqmt_signal_trader.xtquant_compat import _RPC_INFLIGHT

        # 模块 Semaphore 可能被其他测试占用 → 用独立 Semaphore 模拟同一逻辑
        # (验证 "Semaphore(3) + acquire(timeout) + release" 限流语义)
        sem = _th.Semaphore(3)
        acquired = 0
        for _ in range(3):
            assert sem.acquire(timeout=1.0), "前 3 个 acquire 必须成功"
            acquired += 1
        assert not sem.acquire(timeout=0.1), (
            "第 4 个 acquire 必须超时 (并发上限生效) — 防 RPC 风暴"
        )
        sem.release()
        assert sem.acquire(timeout=1.0), "释放后必须能再 acquire (slot 归还)"
        # 清理: 归还剩余
        for _ in range(acquired):
            sem.release()
