"""Redis client helpers for Big QMT signal trader."""

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
    username = config.get("username") or os.environ.get("BIGQMT_REDIS_USERNAME") or None
    password = config.get("password") or os.environ.get("BIGQMT_REDIS_PASSWORD") or None
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        socket_connect_timeout=_float_or_none(config.get("socket_connect_timeout", 1.5), 1.5),
        socket_timeout=_float_or_none(config.get("socket_timeout", 1.5), 1.5),
        health_check_interval=int(config.get("health_check_interval", 30)),
    )


def decode_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def redis_mapping_to_text(mapping):
    return {decode_text(key): decode_text(value) for key, value in (mapping or {}).items()}
