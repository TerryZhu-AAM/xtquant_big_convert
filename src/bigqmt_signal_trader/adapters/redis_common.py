"""Redis client helpers for Big QMT signal trader."""

import inspect
import os


def _float_or_none(value, default=None):
    if value is None:
        return default
    if value == "":
        return default
    text = str(value).strip()
    if text.lower() in ("none", "null"):
        return None
    return float(value)


def redis_supports_protocol_kw():
    """redis-py 的 Redis.__init__ 从 5.0 起才有 protocol 参数；QMT 自带的
    redis-py 3.5.3 不认，硬传直接 TypeError 崩掉（issue #71，PR #67 的回归）。
    [BUG-20260826-redis35-protocol-kwarg 本地同族定谳合并] 特性检测: 仅新版
    redis-py 传 protocol; 旧版只会 RESP2, 不传恰好等价强制 RESP2; C 实现取不
    到签名等异常一律按不支持处理。"""
    import inspect

    try:
        import redis

        return "protocol" in inspect.signature(redis.Redis.__init__).parameters
    except Exception:
        return False


def build_redis_client(config=None):
    config = config or {}
    try:
        import redis
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("redis package is required when Redis adapters are enabled") from exc

    url = config.get("url") or os.environ.get("BIGQMT_REDIS_URL")
    if url:
        return redis.Redis.from_url(
            url,
            socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
            socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
        )

    host = config.get("host") or os.environ.get("BIGQMT_REDIS_HOST") or "127.0.0.1"
    port = int(config.get("port") or os.environ.get("BIGQMT_REDIS_PORT") or 6379)
    # [BUG-P0-20260810-redis-db-mismatch] 禁 `config.get("db") or ...`: db=0 是合法选择
    # (falsy 会被 `or` 短路吞掉回退默认 5), QMT 端 transport 与 backend 客户端因此连到
    # 不同 DB, RPC 请求无人消费.
    _db_value = config.get("db")
    if _db_value is None or str(_db_value).strip() == "":
        _db_value = os.environ.get("BIGQMT_REDIS_DB")
    if _db_value is None or str(_db_value).strip() == "":
        _db_value = 5
    db = int(_db_value)
    # [对抗复审 DEF-6] 启动期显式留痕生效库号 — A/B 两侧 db 来源不对称
    # (QMT 端本地配置文件 vs backend .env env) 时, BUG-P0-20260810 式
    # 跨库静默分裂可由这行日志一眼确诊。url 形态无此分支 (本仓生产未用)。
    print("[redis_common] build_redis_client host=%s port=%s db=%d" % (host, port, db))
    username = config.get("username") or os.environ.get("BIGQMT_REDIS_USERNAME") or None
    password = config.get("password") or os.environ.get("BIGQMT_REDIS_PASSWORD") or None
    # redis-py 8.x 默认 RESP3，Redis 5.0 只支持 RESP2 -> 强制 protocol=2；
    # 但 QMT 自带的 redis-py 3.5.3 没有 protocol 参数（issue #71），按版本能力透传。
    protocol = int(config.get("protocol") or os.environ.get("BIGQMT_REDIS_PROTOCOL") or 2)
    kwargs = dict(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
        socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
        health_check_interval=int(config.get("health_check_interval", 30)),
    )
    if redis_supports_protocol_kw():
        kwargs["protocol"] = protocol
    return redis.Redis(**kwargs)


def decode_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def redis_mapping_to_text(mapping):
    return {decode_text(key): decode_text(value) for key, value in (mapping or {}).items()}
