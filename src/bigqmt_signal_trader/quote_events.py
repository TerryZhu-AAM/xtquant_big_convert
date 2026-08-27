"""Real-time quote (market-data bar) push over Redis — mirrors exec_events.

exec_events.py (2026-08-11 landed, issue #27) proved the real-time push pattern
for order/trade callbacks: QMT-side native callback -> normalize -> Redis
xadd(capped stream) + publish -> backend _event_loop pubsub -> dispatch.

This module is the quote (market-data) twin. The QMT bridge strategy's
``subscribe_quote`` is documented (zread Pattern 4) as a degraded stub — it
publishes a ``subscribe_quote`` event and calls the callback ONCE with the
current snapshot, then nothing. Native miniQMT instead pushes on every new
trade (via ``xtdata.run()``). This module + the wiring in xtquant_compat.py
+ the QMT-side adjust-phase pump close that gap.

Channels (capped stream for short replay + pubsub for fan-out, same as exec):
- ``bigqmt:quote_events:{account_id}``

Why a pump in the QMT-side ``adjust`` callback (not a native callback)?
The bridge runs QMT embedded APIs (get_full_tick / get_market_data_ex) from
the strategy's ``adjust`` main thread — background threads return empty
(zread redis-transport-internals). ``adjust`` fires at ~100ms cadence and
already refreshes the full_tick cache each cycle, so the freshest bar is
in hand. Pushing the delta from there gives ~100ms latency vs the backend's
60s RPC poll — a 600x improvement, reusing existing infrastructure. Native
tick callbacks (HandleData/bindSubscribeQuote) are unavailable to the bridge
daemon (the example file 交易实时主推示例.py ships encrypted), so the
adjust-pump is the lowest-risk real-time path.
"""
import json
import time


QUOTE_CHANNEL_TEMPLATE = "bigqmt:quote_events:{account_id}"
EVENT_QUOTE = "quote"
# [BUG-20260827-quote-heartbeat-frame] INV-2 活性与变化解耦: 封板/一字/极端缩量
# 等"价格长期不变"形态下 pump 的按 close 去重会合法地静默 (600103.SH 2026-08-27
# 全天封死 3.82 实测: 重启后首批 4 帧后再无一帧, 而 get_full_tick 数据恒新鲜),
# 后端健康判定无法三分「断流/未订阅/无变化」。heartbeat 帧由 pump 对"在册但价格
# 未变"的码周期性发布 (带 last_price), 走同一条 stream+pubsub 通道, 消费端
# (xtquant_compat._dispatch_quote_event) 单独路由到 liveness 钩子, 绝不进入
# OMS tick 链路 (零伪造行情面)。老 backend 收到未知 event_type 直接忽略
# (既有 filter), 双端可非锁定序升级。
EVENT_HEARTBEAT = "heartbeat"


def heartbeat_interval_seconds_default():
    return 30.0


def should_send_heartbeat(last_sent_ts, now_ts, interval_seconds):
    """纯函数判定心跳是否到期 (None=从未发过 → 立即到期)."""
    interval = float(interval_seconds)
    if interval <= 0:
        return False
    if last_sent_ts is None:
        return True
    return (float(now_ts) - float(last_sent_ts)) >= interval

# Bar fields mirrored from get_market_data_ex / get_full_tick output. Names match
# what gateway_provider._handle_xt_tick already consumes (close/open/high/low/
# volume/amount + stime/time), so the backend dispatches the payload straight
# into the existing tick path without reshaping.
#
# [fix BUG-P0-20260811-bridge-etf-tick-001] tick snapshot fields extended for
# ETF scalping strategy (ETFScalpStrategy.on_tick reads lastPrice as the primary
# price, falls back to bidPrice/askPrice for G6 microstructure scoring, and reads
# tickvol/bidVol/askVol for volume imbalance). Without these the 6 OHLCV-only bar
# silently degrades ETF to price=0 early-return (永不交易). pullback_ma5 path B
# (_handle_xt_tick) only reads close so the extra fields are inert there.
_BAR_FIELDS = (
    "open", "high", "low", "close", "volume", "amount",
    "lastPrice", "lastClose",
    "bidPrice", "askPrice", "bidVol", "askVol",
    "tickvol",
)


def quote_channel(account_id):
    return QUOTE_CHANNEL_TEMPLATE.format(account_id=str(account_id or ""))


def normalize_quote_event(seq, stock_code, period, bar, account_id=""):
    """Build a JSON-able quote event dict from a single bar.

    ``bar`` may be a pandas DataFrame/Series row, a dict, or a numpy record —
    we read fields defensively (same _attr style as exec_events). The bar
    timestamp is carried as ``bar_time`` (the DataFrame index name when
    available) so the backend can dedup / order.
    """
    def _get(name):
        if bar is None:
            return None
        if isinstance(bar, dict):
            return bar.get(name)
        # DataFrame row (iloc[-1]) or Series
        try:
            return bar.get(name)
        except (AttributeError, KeyError):
            return None

    fields = {name: _get(name) for name in _BAR_FIELDS}
    # bar timestamp: prefer a 'time'/'stime' column, else the index (DataFrame)
    bar_time = _get("time") or _get("stime")
    if bar_time is None and bar is not None:
        try:
            idx = getattr(bar, "name", None)
            if idx is not None:
                bar_time = str(idx)
        except Exception:
            bar_time = None
    return {
        "event_type": EVENT_QUOTE,
        "seq": seq,
        "account_id": str(account_id or ""),
        "stock_code": str(stock_code or ""),
        "period": str(period or ""),
        "bar_time": str(bar_time) if bar_time is not None else "",
        "open": fields["open"],
        "high": fields["high"],
        "low": fields["low"],
        "close": fields["close"],
        "volume": fields["volume"],
        "amount": fields["amount"],
        # [fix BUG-P0-20260811-bridge-etf-tick-001] tick snapshot passthrough — ETF
        # 战法 on_tick 依赖 lastPrice (price), bidPrice/askPrice (G6 microstructure),
        # tickvol/bidVol/askVol (volume imbalance). 桥接 pump 走 get_full_tick (QMT 本地
        # 完整 tick 快照), cell 含这些字段; normalize 透传, 不裁剪. pullback_ma5 路径 B
        # 只取 close, 多余字段 inert.
        "lastPrice": fields["lastPrice"],
        "lastClose": fields["lastClose"],
        "bidPrice": fields["bidPrice"],
        "askPrice": fields["askPrice"],
        "bidVol": fields["bidVol"],
        "askVol": fields["askVol"],
        "tickvol": fields["tickvol"],
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "created_at_ts": time.time(),
    }


def normalize_heartbeat_event(seq, stock_code, account_id="", last_price=None):
    """[BUG-20260827] liveness-only 事件 — 不含 OHLCV bar 字段.

    消费端把它当"最近推送尝试证明", 与 quote 帧严格区分 (绝不可混入 tick 链)。
    """
    return {
        "event_type": EVENT_HEARTBEAT,
        "seq": seq,
        "account_id": str(account_id or ""),
        "stock_code": str(stock_code or ""),
        "period": "1m",
        "last_price": last_price,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "created_at_ts": time.time(),
    }


def _publish(redis_client, channel, event, maxlen=2000):
    raw = json.dumps(event, ensure_ascii=False, default=str)
    try:
        redis_client.xadd(channel, {"payload": raw}, maxlen=maxlen, approximate=True)
    except Exception:
        pass
    redis_client.publish(channel, raw)
    return event


def publish_quote_event(redis_client, account_id, event):
    return _publish(redis_client, quote_channel(account_id), event)
