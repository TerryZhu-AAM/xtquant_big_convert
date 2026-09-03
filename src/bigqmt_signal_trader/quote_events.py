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
import os
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


# ── [BUG-20260827-sub-registry-gc] INV-3 注册表卫生 ───────────────────────────
SEEN_KEY_TEMPLATE = "bigqmt:quote_subs_seen:{account_id}"
# [对抗复审 DEF-1 修复] 首见账本持久化在 Redis — 泵进程重启不再重置宽限起点。
# 原实现把首见账本记在策略进程内存里, 宽限锚随进程死亡 => 现实重启节律下
# reap 恒空转 (装置失效), 而 >72h 长活+单端升级时又一拍全池回收 (爆炸半径无界)。
FIRSTSEEN_KEY_TEMPLATE = "bigqmt:quote_subs_firstseen:{account_id}"
TOMBSTONE_KEY_TEMPLATE = "bigqmt:quote_subs_tombstone:{account_id}"
KEEPALIVE_MIN_INTERVAL_MS = 60_000
# 单周期回收上限: 全池级误收必须多周期分摊才可能发生, 且每笔都有 tombstone 可溯源自愈;
# 默认按注册表规模自适应 (10%, 至少 5 条), caller 可用 max_reap 显式覆盖。
DEFAULT_REAP_RATIO = 0.10
REAP_MIN_PER_CYCLE = 5
# tombstone zset 自我修剪上限 (审计面自身也受「有界资源」不变量约束)。
TOMBSTONE_MAX_ENTRIES = 2000


def seen_key(account_id):
    return SEEN_KEY_TEMPLATE.format(account_id=str(account_id or ""))


def firstseen_key(account_id):
    return FIRSTSEEN_KEY_TEMPLATE.format(account_id=str(account_id or ""))


def tombstone_key(account_id):
    return TOMBSTONE_KEY_TEMPLATE.format(account_id=str(account_id or ""))


def coerce_seconds_to_ms(value, fallback_sec):
    """[对抗复审 DEF-1 修复点3] 配置秒值 → 毫秒强转 (纯函数).

    数字/数字字符串均可; None/空串/非法/<=0 一律回退默认 — 修复原
    ``int(cfg * 1000)`` 对字符串配置做 str*1000 重复再 int() 的类型混淆
    (ValueError 被 pump 外层宽捕获吞掉 => GC 每 60s print 后静默摆烂)。
    """
    try:
        if value is None or str(value).strip() == "":
            return int(fallback_sec * 1000)
        sec = float(value)
    except (TypeError, ValueError):
        return int(fallback_sec * 1000)
    if sec <= 0:
        return int(fallback_sec * 1000)
    return int(sec * 1000)


def should_write_keepalive(last_written_ms, now_ms, min_interval_ms=KEEPALIVE_MIN_INTERVAL_MS):
    """客户端保活戳节流: 同 seq 每 min_interval_ms 至少间隔一次 redis 写."""
    if last_written_ms is None:
        return True
    return (now_ms - last_written_ms) >= min_interval_ms


def reap_stale_subscriptions(redis_client, account_id, *, now_ms, keep_ttl_ms,
                             grace_ms, enabled, max_reap=None):
    """回收订阅哈希中"消费端已死"的陈年条目 — INV-3 治类 (DEF-1 重设计版).

    判据 (须同时满足):
      1. 首见账本(bigqmt:quote_subs_firstseen:{acct}, **Redis 持久化**)中该 seq
         首次观测距今 > grace_ms — 宽限期内/从未见过的新条目绝不误收;
         泵重启不重置宽限起点 (修复原首见账本进程内存态缺陷);
      2. 保活戳缺失 或 now-stamp > keep_ttl_ms — 戳由消费端 dispatch 成功后节流写,
         消费端死亡 => 戳停更。

    动作 (每笔): hdel 注册条目 + hdel 首见账本条目 (账本自身保持有界) +
    zadd tombstone(score=now_ms, member="seq|stock_code" 可溯源可恢复) +
    zremrangebyrank 修剪 tombstone 到最近 TOMBSTONE_MAX_ENTRIES 笔。

    爆炸半径闸门: 单周期最多回收 max_reap 笔 (None → 注册表的 10%, 至少 5),
    超出候选顺延下一周期并计入 stats["deferred"] — 「全池一拍合法回收」在机制上
    不可能发生。

    返回 {"reaped": [seq,...], "deferred": n} 供调用方清理 pump 内存孤儿键;
    enabled=False 时严格零行为 (ops 双端部署后设 BIGQMT_QUOTE_SUB_GC=1 显式启用)。
    """
    stats = {"reaped": [], "deferred": 0}
    if not enabled or keep_ttl_ms <= 0 or grace_ms <= 0:
        return stats
    acct = str(account_id or "")
    subs_key = "bigqmt:quote_subscriptions:%s" % acct
    ledger_key = firstseen_key(acct)
    seen_hash_key = seen_key(acct)
    try:
        raw_subs = redis_client.hgetall(subs_key) or {}
    except Exception as exc:
        print("[bigqmt_quote_gc] hgetall failed: %s" % exc)
        return stats
    if not raw_subs:
        return stats

    def _text(v):
        return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)

    # 单趟读齐 首见账本 + 保活戳哈希 (取代旧实现 per-field hget 的 N+1)
    try:
        ledger_raw = redis_client.hgetall(ledger_key) or {}
    except Exception as exc:
        print("[bigqmt_quote_gc] ledger hgetall failed: %s" % exc)
        return stats
    try:
        seen_raw = redis_client.hgetall(seen_hash_key) or {}
    except Exception as exc:
        print("[bigqmt_quote_gc] seen hgetall failed: %s" % exc)
        return stats
    ledger = {_text(k): _text(v) for k, v in ledger_raw.items()}
    stamps = {_text(k): _text(v) for k, v in seen_raw.items()}
    del ledger_raw, seen_raw

    import json as _json
    fresh_ledger = {}
    candidates = []  # (first_seen_ms, seq_str, field, code_label)
    for field, payload_raw in raw_subs.items():
        seq_str = _text(field)
        code_label = "%s|<unreadable>" % seq_str
        try:
            payload = _json.loads(_text(payload_raw)) if payload_raw is not None else {}
            if isinstance(payload, dict) and payload.get("stock_code"):
                code_label = "%s|%s" % (seq_str, payload.get("stock_code"))
            else:
                code_label = "%s|<no_stock_code>" % seq_str
        except Exception:
            pass  # 残缺 JSON 也是回收对象 — member 里以 <unreadable> 标注
        first_seen_text = ledger.get(seq_str)
        if first_seen_text is None:
            # 本周期首次见到该 seq — 登记首见 (持久化), 本轮天然处于宽限保护内
            fresh_ledger[seq_str] = str(int(now_ms))
            continue
        try:
            first_seen = int(first_seen_text)
        except ValueError:
            fresh_ledger[seq_str] = str(int(now_ms))
            continue
        if (now_ms - first_seen) <= grace_ms:
            continue  # 观测不足一个宽限期 — 保护
        raw_stamp = stamps.get(seq_str)
        if raw_stamp is not None:
            try:
                stamp_ms = int(raw_stamp)
            except ValueError:
                stamp_ms = None
            if stamp_ms is not None and (now_ms - stamp_ms) <= keep_ttl_ms:
                continue
        candidates.append((first_seen, seq_str, field, code_label))

    # cap 闸门: 最老优先, 超出顺延 (全池级误收被机制性切碎)
    if max_reap is None:
        max_reap = max(REAP_MIN_PER_CYCLE, int(len(raw_subs) * DEFAULT_REAP_RATIO))
    candidates.sort(key=lambda item: item[0])
    deferred = len(candidates) - max_reap if len(candidates) > max_reap else 0
    for _, seq_str, field, code_label in candidates[:max_reap]:
        try:
            redis_client.hdel(subs_key, field)
            redis_client.hdel(ledger_key, seq_str)
            redis_client.zadd(tombstone_key(acct), {code_label: int(now_ms)})
            redis_client.zremrangebyrank(
                tombstone_key(acct), 0, -(TOMBSTONE_MAX_ENTRIES + 1))
            stats["reaped"].append(seq_str)
        except Exception as exc:
            print("[bigqmt_quote_gc] reap %s failed: %s" % (seq_str, exc))
    if fresh_ledger:
        try:
            redis_client.hset(ledger_key, mapping=fresh_ledger)
        except Exception as exc:
            print("[bigqmt_quote_gc] ledger hset failed: %s" % exc)
    stats["deferred"] = deferred
    return stats

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


# [BUG-20260827-quote-stream-maxlen-unify] 客户端 publish_event 原 maxlen=1000 与
# 服务端 _publish 2000 双写者不一致 → forensic 窗口由先触顶者决定。统一单一真相源
# (env 可调), 两端默认一致。
QUOTE_STREAM_MAXLEN = int(os.environ.get("BIGQMT_QUOTE_EVENT_MAXLEN", "2000"))


def _publish(redis_client, channel, event, maxlen=None):
    if maxlen is None:
        maxlen = QUOTE_STREAM_MAXLEN
    raw = json.dumps(event, ensure_ascii=False, default=str)
    try:
        redis_client.xadd(channel, {"payload": raw}, maxlen=maxlen, approximate=True)
    except Exception:
        pass
    redis_client.publish(channel, raw)
    return event


def publish_quote_event(redis_client, account_id, event):
    return _publish(redis_client, quote_channel(account_id), event)
