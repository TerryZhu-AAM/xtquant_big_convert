"""Redis transport for the BigQMT RPC bridge.

This is the reference transport and the default. It preserves the exact wire
behavior of the original ``RedisPubSubRpcService``:

* Client ``send_request``: ``RPUSH`` the (base64-obfuscated) request onto the
  per-account request queue, then ``BLPOP`` the per-request response list with
  a ``GET response_key`` fallback. A ``pubsub`` transport variant is kept for
  callers that pass ``transport="pubsub"`` to ``call_redis_rpc``.
* Server receive: two background loops — a ``pubsub.subscribe`` loop and a
  ``brpop`` queue loop. Either delivers inbound payloads to the registered
  ``on_request`` callback.
* Server ``send_response``: fan-out writes to ``reply_key`` (``SETEX``),
  ``reply_list`` (``RPUSH`` + ``EXPIRE``) and ``reply_channel`` (``PUBLISH``).

The module-level :func:`call_redis_rpc` helper keeps its original signature and
delegates here so existing callers and ``bench_latency.py`` are unchanged.
"""

import threading
import time
import traceback
import uuid

from ..adapters.redis_common import decode_text
from ..redis_rpc import (
    decode_rpc_request_payload,
    encode_rpc_request_payload,
)
from .base import RpcTransport, TransportTimeout

import json  # noqa: E402  (kept here so transport owns all wire encoding)


REQUEST_CHANNEL_TEMPLATE = "bigqmt:rpc:req:{account_id}"
REQUEST_QUEUE_TEMPLATE = "bigqmt:rpc:queue:{account_id}"
RESPONSE_CHANNEL_TEMPLATE = "bigqmt:rpc:resp:{account_id}:{request_id}"
RESPONSE_LIST_TEMPLATE = "bigqmt:rpc:respq:{account_id}:{request_id}"
RESPONSE_KEY_TEMPLATE = "bigqmt:rpc:resp:{account_id}:{request_id}"


def _format(template, account_id, request_id):
    if not template:
        return ""
    return template.format(account_id=account_id, request_id=request_id)


def _loads(raw_payload):
    """Decode a wire payload (bytes/str/dict) into a request dict."""
    if isinstance(raw_payload, dict):
        return dict(raw_payload)
    text = decode_text(raw_payload)
    text = decode_rpc_request_payload(text)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("rpc payload must be a json object")
    return payload


class RedisTransport(RpcTransport):
    """Redis-backed transport. Owns rpush/blpop/brpop/publish/setex."""

    name = "redis"

    def __init__(
        self,
        redis_client,
        account_id="",
        response_redis_client=None,
        request_channel_template=REQUEST_CHANNEL_TEMPLATE,
        request_queue_template=REQUEST_QUEUE_TEMPLATE,
        response_channel_template=RESPONSE_CHANNEL_TEMPLATE,
        response_list_template=RESPONSE_LIST_TEMPLATE,
        response_key_template=RESPONSE_KEY_TEMPLATE,
        response_ttl_seconds=60,
        queue_poll_interval_seconds=0.02,
        debug_log_limit=0,
        print_prefix="[bigqmt_rpc]",
    ):
        super(RedisTransport, self).__init__(account_id=account_id, print_prefix=print_prefix)
        self.listen_redis = redis_client
        self.redis = response_redis_client or redis_client
        self.request_channel_template = request_channel_template
        self.request_queue_template = request_queue_template
        self.response_channel_template = response_channel_template
        self.response_list_template = response_list_template
        self.response_key_template = response_key_template
        self.response_ttl_seconds = int(response_ttl_seconds)
        self.queue_poll_interval_seconds = max(0.001, float(queue_poll_interval_seconds))
        self.debug_log_limit = int(debug_log_limit)
        self._received_count = 0
        self._published_count = 0
        self._pubsub = None
        self._thread = None
        self._queue_thread = None
        # Hooks so the service can observe/intercept received payloads (debug
        # logging, inline-vs-deferred dispatch). When None, the request is
        # delivered straight to the on_request callback.
        self.on_raw_payload = None

    # -- properties mirroring the original service -------------------------
    @property
    def request_channel(self):
        return self.request_channel_template.format(account_id=self.account_id)

    @property
    def request_queue(self):
        return self.request_queue_template.format(account_id=self.account_id)

    def _response_clients(self):
        clients = [self.redis]
        if self.listen_redis is not self.redis:
            clients.append(self.listen_redis)
        return clients

    # -- client side -------------------------------------------------------
    def send_request(self, request, timeout_seconds, transport="queue"):
        """Send ``request`` and block for the response dict.

        ``transport`` selects the Redis sub-transport: ``"queue"`` (default,
        RPUSH+BLPOP) or ``"pubsub"`` (PUBLISH+subscribe). Kept for parity with
        the original ``call_redis_rpc`` signature.
        """
        return _call_redis_rpc(
            self.listen_redis,
            self.account_id,
            request,
            timeout_seconds=float(timeout_seconds),
            transport=transport,
            request_channel_template=self.request_channel_template,
            request_queue_template=self.request_queue_template,
            response_channel_template=self.response_channel_template,
            response_list_template=self.response_list_template,
            response_key_template=self.response_key_template,
        )

    # -- server side -------------------------------------------------------
    def start_receiving(self, on_request, background_threads=True):
        """Spawn the pubsub + queue receive loops (unless ``background_threads``)."""
        super(RedisTransport, self).start_receiving(on_request)
        if not background_threads:
            print(
                "%s started queue=%s background_threads=False"
                % (self.print_prefix, self.request_queue)
            )
            return
        if (
            self._thread is not None
            and self._thread.is_alive()
            and self._queue_thread is not None
            and self._queue_thread.is_alive()
        ):
            return
        self._thread = threading.Thread(
            target=self._listen_loop, name="bigqmt-redis-rpc", daemon=True
        )
        self._queue_thread = threading.Thread(
            target=self._queue_loop, name="bigqmt-redis-rpc-queue", daemon=True
        )
        self._thread.start()
        self._queue_thread.start()
        print(
            "%s started channel=%s queue=%s"
            % (self.print_prefix, self.request_channel, self.request_queue)
        )

    def _listen_loop(self):
        while self._running:
            try:
                pubsub = self.listen_redis.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                pubsub.subscribe(self.request_channel)
                if self.debug_log_limit > 0:
                    print(
                        "%s subscribed channel=%s" % (self.print_prefix, self.request_channel)
                    )
                while self._running:
                    message = pubsub.get_message(timeout=1.0)
                    if not self._running:
                        break
                    if not message or message.get("type") != "message":
                        continue
                    self._handle_received_payload(message.get("data"), "pubsub")
            except Exception:
                print(
                    "%s listener failed:\n%s" % (self.print_prefix, traceback.format_exc())
                )
                time.sleep(1.0)
            finally:
                try:
                    if self._pubsub is not None:
                        self._pubsub.close()
                except Exception:
                    pass
                self._pubsub = None

    def _queue_loop(self):
        while self._running:
            try:
                if self.debug_log_limit > 0:
                    print(
                        "%s queue polling key=%s" % (self.print_prefix, self.request_queue)
                    )
                while self._running:
                    item = self.listen_redis.brpop(self.request_queue, timeout=1)
                    if not self._running:
                        break
                    if not item:
                        continue
                    raw = (
                        item[1]
                        if isinstance(item, (list, tuple)) and len(item) >= 2
                        else item
                    )
                    self._handle_received_payload(raw, "queue")
            except Exception:
                print(
                    "%s queue listener failed:\n%s"
                    % (self.print_prefix, traceback.format_exc())
                )
                time.sleep(1.0)

    def _handle_received_payload(self, raw_payload, source):
        self._received_count += 1
        if self.on_raw_payload is not None:
            # Service wants to observe/intercept (e.g. debug log + dispatch fork).
            self.on_raw_payload(raw_payload, source)
            return
        # Default: decode and deliver straight to the registered callback.
        request = _loads(raw_payload)
        self.deliver(request)

    def send_response(self, request, response):
        """Fan out the response to reply_key/reply_list/reply_channel."""
        request_id = response.get("request_id") or request.get("request_id") or ""
        account_id = response.get("account_id") or request.get("account_id") or self.account_id
        payload = json.dumps(response, ensure_ascii=False)
        ttl_seconds = int(request.get("ttl_seconds") or self.response_ttl_seconds)
        response_key = request.get("reply_key") or _format(
            self.response_key_template, account_id, request_id
        )
        response_channel = request.get("reply_channel") or _format(
            self.response_channel_template, account_id, request_id
        )
        response_list = request.get("reply_list")
        if response_key:
            self._write_response_key(response_key, ttl_seconds, payload)
        if response_list:
            self._push_response_list(response_list, ttl_seconds, payload)
        if response_channel:
            self._publish_response_channel(response_channel, payload)

    def _write_response_key(self, response_key, ttl_seconds, payload):
        first_error = None
        wrote = 0
        for client in self._response_clients():
            try:
                if ttl_seconds > 0:
                    client.setex(response_key, ttl_seconds, payload)
                else:
                    client.set(response_key, payload)
                wrote += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if wrote <= 0 and first_error is not None:
            raise first_error
        return wrote

    def _push_response_list(self, response_list, ttl_seconds, payload):
        first_error = None
        pushed = 0
        for client in self._response_clients():
            try:
                client.rpush(response_list, payload)
                if ttl_seconds > 0:
                    client.expire(response_list, ttl_seconds)
                pushed += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if pushed <= 0 and first_error is not None:
            raise first_error
        return pushed

    def _publish_response_channel(self, response_channel, payload):
        first_error = None
        receivers = 0
        published = 0
        for client in self._response_clients():
            try:
                receivers += int(client.publish(response_channel, payload) or 0)
                published += 1
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if published <= 0 and first_error is not None:
            raise first_error
        self._published_count += 1
        if self._published_count <= self.debug_log_limit:
            print("%s published response receivers=%s" % (self.print_prefix, receivers))
        return receivers

    # -- non-background drain helpers (used by the strategy adjust thread) -
    def drain_request_queue(self, max_items=20):
        # [BUG-20260811-rpc-storm] drain 加固: QMT 主循环 = drain + quote_push 单线程,
        # 单次 drain 拖太久 → cadence 停 (2026-08-11 实测先兆: drain 1959ms → 4174ms →
        # 主循环停摆 13min). 三层防御:
        #   1. 请求前打 method 日志 — 单请求挂起 (xtdata API 永不返回) 时 FormulaOutput
        #      最后一行 = 卡住的请求, 定位元凶 (13min 停摆根因待定位即此).
        #   2. drain 总时长预算 2s — 多请求累积慢时中断剩余批, 让 quote_push + cadence
        #      有机会跑 (单请求挂起时预算不生效, 靠 #1 定位 + 恢复时 drain Nms 日志).
        #   3. 单请求 >1s 打 warning — 慢但不挂的请求可见 (1959ms/4174ms 这类).
        processed = 0
        _drain_start = time.time()
        for _ in range(int(max_items)):
            if time.time() - _drain_start > 2.0:
                print(
                    "%s drain budget 2s exceeded (%.0fms), stopping early after %d"
                    % (self.print_prefix, (time.time() - _drain_start) * 1000, processed)
                )
                break
            item = self.listen_redis.lpop(self.request_queue)
            if not item:
                break
            _req_start = time.time()
            _req_method = "?"
            try:
                _req_meta = decode_rpc_request_payload(item)
                _req_method = str(_req_meta.get("method") or "?")
            except Exception:
                _req_method = "?"
            if self.on_raw_payload is not None:
                # [BUG-P1-20260810-transport-deadlock] 单 item 异常兜底: 旧实现异常中断
                # 整次 drain, 已 lpop 的 item 静默丢失 → 持续 timeout.
                try:
                    self.on_raw_payload(item, "queue-drain")
                except Exception as exc:
                    print(
                        "%s drain item failed err=%s: %s"
                        % (self.print_prefix, exc.__class__.__name__, exc)
                    )
            else:
                self.deliver(_loads(item))
            _req_elapsed = time.time() - _req_start
            if _req_elapsed > 1.0:
                print(
                    "%s drain item=%d method=%s slow=%.0fms"
                    % (self.print_prefix, processed + 1, _req_method, _req_elapsed * 1000)
                )
            processed += 1
        return processed

    def stop(self):
        super(RedisTransport, self).stop()
        pubsub = self._pubsub
        if pubsub is not None:
            try:
                pubsub.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        queue_thread = self._queue_thread
        if queue_thread is not None and queue_thread.is_alive():
            queue_thread.join(1.0)
        self._thread = None
        self._queue_thread = None
        self._pubsub = None


def _call_redis_rpc(
    redis_client,
    account_id,
    request,
    timeout_seconds=3.0,
    transport="queue",
    request_channel_template=REQUEST_CHANNEL_TEMPLATE,
    request_queue_template=REQUEST_QUEUE_TEMPLATE,
    response_channel_template=RESPONSE_CHANNEL_TEMPLATE,
    response_list_template=RESPONSE_LIST_TEMPLATE,
    response_key_template=RESPONSE_KEY_TEMPLATE,
    ttl_seconds=60,
):
    """Client-side round trip. Accepts a pre-built request envelope."""
    request_id = request.get("request_id") or uuid.uuid4().hex
    request_channel = request_channel_template.format(account_id=account_id)
    request_queue = request_queue_template.format(account_id=account_id)
    response_channel = response_channel_template.format(
        account_id=account_id, request_id=request_id
    )
    response_list = response_list_template.format(account_id=account_id, request_id=request_id)
    response_key = response_key_template.format(account_id=account_id, request_id=request_id)
    # Ensure reply routing is present (the original helper filled these in).
    request = dict(request)
    request.setdefault("request_id", request_id)
    request.setdefault("reply_channel", response_channel)
    request.setdefault("reply_list", response_list)
    request.setdefault("reply_key", response_key)
    request.setdefault("ttl_seconds", ttl_seconds)
    request["request_id"] = request_id
    payload = encode_rpc_request_payload(request)

    if str(transport or "queue").lower() in ("queue", "list", "blpop"):
        redis_client.rpush(request_queue, payload)
        redis_client.expire(request_queue, max(60, int(ttl_seconds)))
        wait_timeout = max(1, int(float(timeout_seconds) + 0.999))
        item = redis_client.blpop(response_list, timeout=wait_timeout)
        if item:
            raw_response = (
                item[1] if isinstance(item, (list, tuple)) and len(item) >= 2 else item
            )
            try:
                redis_client.delete(response_list)
            except Exception:
                pass
            return json.loads(decode_text(raw_response))
        raw_response = redis_client.get(response_key)
        if raw_response:
            return json.loads(decode_text(raw_response))
        raise TransportTimeout("redis rpc timeout: %s" % request.get("method"))

    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    try:
        pubsub.subscribe(response_channel)
        redis_client.publish(request_channel, payload)
        deadline = time.time() + float(timeout_seconds)
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            message = pubsub.get_message(timeout=remaining)
            if not message or message.get("type") != "message":
                continue
            response = json.loads(decode_text(message.get("data")))
            if response.get("request_id") == request_id:
                return response
        raw_response = redis_client.get(response_key)
        if raw_response:
            return json.loads(decode_text(raw_response))
        raise TransportTimeout("redis rpc timeout: %s" % request.get("method"))
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
