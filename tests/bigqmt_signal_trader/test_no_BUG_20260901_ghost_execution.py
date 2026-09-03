"""BUG-20260901 ghost-execution: 超时后迟到执行的请求必须被消费端丢弃。

09-01 实锤: 桥挂死期间积压的卖单在桥复活后被补执行 (客户端 6s 超时早已按
拒单处置并释放冻结, 系统侧判拒单的委托真实成交 = 幽灵单)。双保险:
  1) 消费端 (RedisPubSubRpcService.process_request): 请求带 ts + ttl_seconds,
     age > ttl 直接丢弃不执行, 回应照发 (ok=False) 供审计;
  2) 客户端 (call_redis_rpc): 请求入队时盖 ts; 超时即 best-effort LREM 出队,
     请求不再留在队列里等桥复活补执行。
无 ts 的旧版客户端请求跳过检查 (滚动部署兼容); ts/ttl 不可解析按无时间戳
处理 (fail-open), 与实现注释口径一致。
"""

import json
import time
import unittest

from bigqmt_signal_trader.adapters.order_dryrun import DryRunOrderGateway
from bigqmt_signal_trader.redis_rpc import (
    BigQmtRpcHandlers,
    RedisPubSubRpcService,
    call_redis_rpc,
    decode_rpc_request_payload,
)


class FakeMarketData:
    def get_ticks(self, codes):
        return {codes[0]: {"lastPrice": 10.5}}

    def get_instrument(self, code):
        return {"code": code, "InstrumentStatus": 0}


class FakePositionProvider:
    pass


class CountingGateway(DryRunOrderGateway):
    def __init__(self):
        super().__init__()
        self.submit_count = 0
        self.query_count = 0

    def query_orders_strict(self, _account_id, _strategy_name):
        self.query_count += 1
        return []

    def submit(self, request):
        self.submit_count += 1
        return super().submit(request)


class FakeResponseRedis:
    """process_request 的回应落点 (setex/publish), 断言丢弃回应确实发出。"""

    def __init__(self):
        self.kv = {}
        self.published = []

    def setex(self, key, seconds, value):
        self.kv[key] = value
        return True

    def set(self, key, value):
        self.kv[key] = value
        return True

    def publish(self, channel, value):
        self.published.append((channel, value))
        return 1


class LremAwareQueueRedis:
    """call_redis_rpc 的 queue 传输面: rpush/expire/get/lrem。"""

    def __init__(self):
        self.queues = {}
        self.pushed = []

    def rpush(self, key, payload):
        self.queues.setdefault(key, []).append(payload)
        self.pushed.append((key, payload))
        return 1

    def expire(self, key, seconds):
        return True

    def get(self, key):
        return None

    def blpop(self, key, timeout):
        return None

    def lrem(self, key, count, payload):
        q = self.queues.get(key, [])
        try:
            q.remove(payload)
            return 1
        except ValueError:
            return 0


class GhostExecutionServerSideTest(unittest.TestCase):
    def _service(self, gateway):
        redis_client = FakeResponseRedis()
        handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=FakeMarketData(),
            position_provider=FakePositionProvider(),
            order_gateway=gateway,
            allow_order_methods=True,
            order_settle_timeout_seconds=5.0,
        )
        service = RedisPubSubRpcService(redis_client, handlers, account_id="acct")
        return redis_client, service

    @staticmethod
    def _order_request(**extra):
        request = {
            "request_id": "ghost-1",
            "account_id": "acct",
            "method": "order_stock",
            "params": {
                "stock_code": "600000.SH",
                "order_type": 23,
                "order_volume": 100,
                "price_type": 11,
                "price": 10.1,
                "order_remark": "ghost-1",
            },
        }
        request.update(extra)
        return request

    def test_stale_request_is_discarded_not_executed(self):
        gateway = CountingGateway()
        redis_client, service = self._service(gateway)

        response = service.process_request(
            self._order_request(ts=time.time() - 999, ttl_seconds=60)
        )

        self.assertIs(response["ok"], False)
        self.assertIn("stale request discarded", response["error"])
        self.assertEqual(gateway.submit_count, 0)
        self.assertEqual(gateway.query_count, 0)
        # 丢弃回应照发, 供事后审计。
        self.assertTrue(
            any("stale request discarded" in str(v) for v in redis_client.kv.values())
        )

    def test_fresh_request_executes_normally(self):
        gateway = CountingGateway()
        _redis_client, service = self._service(gateway)

        service.process_request(self._order_request(ts=time.time() - 1, ttl_seconds=60))

        self.assertEqual(gateway.submit_count, 1)

    def test_legacy_request_without_ts_still_executes(self):
        gateway = CountingGateway()
        _redis_client, service = self._service(gateway)

        service.process_request(self._order_request())

        self.assertEqual(gateway.submit_count, 1)

    def test_unparseable_ts_falls_open(self):
        gateway = CountingGateway()
        _redis_client, service = self._service(gateway)

        service.process_request(
            self._order_request(ts="not-a-number", ttl_seconds=60)
        )

        self.assertEqual(gateway.submit_count, 1)


class GhostExecutionClientSideTest(unittest.TestCase):
    def test_client_stamps_ts_and_timeout_removes_queued_request(self):
        spy = LremAwareQueueRedis()

        with self.assertRaises(TimeoutError):
            call_redis_rpc(
                spy,
                "acct",
                "ping",
                params={},
                timeout_seconds=0.05,
                ttl_seconds=60,
            )

        self.assertEqual(len(spy.pushed), 1)
        queue_key, payload = spy.pushed[0]
        self.assertEqual(queue_key, "bigqmt:rpc:queue:acct")
        request = json.loads(decode_rpc_request_payload(payload))
        self.assertIn("ts", request)
        self.assertEqual(request["ttl_seconds"], 60)
        # 超时即出队: 请求不再留在队列里等桥复活补执行。
        self.assertEqual(spy.queues[queue_key], [])


if __name__ == "__main__":
    unittest.main()
