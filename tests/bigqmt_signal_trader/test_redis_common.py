"""[BUG-20260826-redis35-protocol-kwarg] build_redis_client 对旧版 redis-py 的兼容回归.

QMT 终端内嵌 Python 捆绑 redis-py 3.5.3 (无 `protocol` 形参), 无条件传 protocol
曾致桥接启动即崩 (TypeError: __init__() got an unexpected keyword argument 'protocol').
"""

import os
import sys
import types
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.redis_common import build_redis_client


class OldRedis35:
    """模拟 redis-py 3.5.3 的 Redis.__init__ 签名 (无 protocol 形参)."""

    def __init__(self, host="localhost", port=6379, db=0, username=None, password=None,
                 socket_timeout=None, socket_connect_timeout=None,
                 health_check_interval=0, retry_on_timeout=False):
        self.kwargs = {
            "host": host, "port": port, "db": db, "username": username,
            "password": password, "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "health_check_interval": health_check_interval,
        }


class NewRedis8:
    """模拟 redis-py 8.x 的 Redis.__init__ 签名 (有 protocol 形参, 默认 RESP3)."""

    def __init__(self, host="localhost", port=6379, db=0, username=None, password=None,
                 socket_timeout=None, socket_connect_timeout=None,
                 health_check_interval=0, retry_on_timeout=False, protocol=3):
        self.kwargs = dict(
            host=host, port=port, db=db, username=username, password=password,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
            health_check_interval=health_check_interval, protocol=protocol,
        )


class _FakeRedisModuleTestMixin(unittest.TestCase):
    REDIS_CLS = None

    def setUp(self):
        module = types.ModuleType("redis")
        module.Redis = self.REDIS_CLS
        self._saved_redis = sys.modules.get("redis")
        sys.modules["redis"] = module

    def tearDown(self):
        if self._saved_redis is None:
            sys.modules.pop("redis", None)
        else:
            sys.modules["redis"] = self._saved_redis


class BuildClientOnOldRedis35Test(_FakeRedisModuleTestMixin):
    """QMT 内嵌 redis-py 3.5.3: 不得传 protocol, 其余参数照传."""

    REDIS_CLS = OldRedis35

    def test_builds_client_without_protocol_kwarg(self):
        client = build_redis_client({"host": "127.0.0.1", "port": 6379, "db": 5})
        self.assertIsInstance(client, OldRedis35)
        self.assertNotIn("protocol", client.kwargs)
        self.assertEqual(client.kwargs["db"], 5)
        self.assertEqual(client.kwargs["host"], "127.0.0.1")

    def test_db_zero_is_not_swallowed(self):
        client = build_redis_client({"db": 0})
        self.assertEqual(client.kwargs["db"], 0)


class BuildClientOnNewRedis8Test(_FakeRedisModuleTestMixin):
    """新版 redis-py: 保持强制 protocol=2 (RESP2) 的原意图."""

    REDIS_CLS = NewRedis8

    def test_defaults_to_protocol_2(self):
        client = build_redis_client({})
        self.assertEqual(client.kwargs["protocol"], 2)

    def test_config_overrides_protocol(self):
        client = build_redis_client({"protocol": 3})
        self.assertEqual(client.kwargs["protocol"], 3)

    def test_env_overrides_protocol(self):
        os.environ["BIGQMT_REDIS_PROTOCOL"] = "3"
        try:
            client = build_redis_client({})
            self.assertEqual(client.kwargs["protocol"], 3)
        finally:
            del os.environ["BIGQMT_REDIS_PROTOCOL"]


if __name__ == "__main__":
    unittest.main()
