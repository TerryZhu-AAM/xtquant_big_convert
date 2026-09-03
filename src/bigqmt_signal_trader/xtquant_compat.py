"""MiniQMT-style client objects backed by Big QMT Redis RPC.

This module is the replacement edge for existing code that already calls
``xt_trader.query_stock_positions(...)`` or ``xtdata.get_full_tick(...)``.
The Big QMT process remains the only place that touches QMT runtime APIs.
"""

import os
import json
import time
import uuid
import queue as _queue
from collections import OrderedDict as _OrderedDict
import threading
import importlib
import datetime as _dt
from typing import Any, Dict, Iterable, List, Optional
# Only these three are used below, but every public constant is re-exported
# further down: docs/XTQUANT_COMPAT_REPLACEMENT.md tells callers to do
# ``from bigqmt_signal_trader import xtquant_compat as xtconstant`` and read
# e.g. ``xtconstant.ORDER_SUCCEEDED`` off this module.
#
# This is deliberately not ``from xtquant.xtconstant import *``. That form is a
# SyntaxError ("import * only allowed at module level") in the single-file QMT
# builds, which exec each module inside a function body (issue #76).
from xtquant import xtconstant as _xtconstant
from xtquant.xtconstant import ORDER_UNKNOWN, STOCK_BUY, STOCK_SELL
from xtquant.xttype import StockAccount

from .full_tick_cache import request_full_tick_cache, wait_full_tick_cache
from .local_cache import LocalMarketCache
from .quote_events import EVENT_HEARTBEAT as _QUOTE_EVENT_HEARTBEAT
from .quote_events import QUOTE_STREAM_MAXLEN as _QUOTE_STREAM_MAXLEN
from .quote_events import (
    seen_key as _quote_seen_key,
    should_write_keepalive as _should_write_keepalive,
)
from .order_id import OrderId, order_sys_id_of
from .redis_rpc import TYPED_PAYLOAD_FLAG, call_redis_rpc
from .logging_setup import get_logger

# [BUG-20260811-rpc-storm] RPC 并发限流 — QMT 端 transport 单线程 drain (QMT 沙箱禁
# 后台线程, rpc_background_threads=False 是 B 机钦定), backend 并发 RPC 风暴涌入 Redis
# 队列 → 单请求慢/挂起堵死 QMT 主循环 (2026-08-11 实测 14:30 卡 7.6min / 14:43 卡 13min,
# 根因均为 backend RPC 洪峰). 限流: 同时最多 BIGQMT_RPC_CONCURRENCY (默认 3) 个 RPC
# 在途, 多余排队等 slot — QMT 队列永远 ≤ N 个堆积, 单请求挂起时只有 N 个受影响.
# FormulaServer fast path (直连 58600, 不经 QMT 主循环 drain) 不限流.
_RPC_CONCURRENCY = int(os.environ.get("BIGQMT_RPC_CONCURRENCY", "3") or 3)
_RPC_INFLIGHT = threading.Semaphore(max(1, _RPC_CONCURRENCY))


log = get_logger("xtquant_compat")


# Re-export every public xtconstant name on this module, replacing what
# ``import *`` used to do implicitly. Before #73 these 110-odd constants were
# defined here outright, and the documented "approach 1" migration path binds
# this module as ``xtconstant``, so dropping them would break callers that read
# e.g. ``xtquant_compat.FIX_PRICE``.
#
# Written as an explicit loop rather than ``import *`` (a SyntaxError inside the
# single-file builds' function-scope exec, issue #76) and rather than a
# module-level ``__getattr__`` (PEP 562, Python 3.7+, while QMT ships 3.6).
for _const_name in dir(_xtconstant):
    if not _const_name.startswith("_"):
        globals().setdefault(_const_name, getattr(_xtconstant, _const_name))
del _const_name


# Default OHLCV fields pulled + cached by get_local_data fallback_rpc.
DEFAULT_DOWNLOAD_FIELDS = ["open", "high", "low", "close", "volume", "amount"]
# Codes per get_market_data_ex request. One request carries a single RPC timeout,
# so a wide stock_list either fits or loses everything (issue #47).
DEFAULT_MARKET_DATA_CHUNK = 100
_TIME_COL_NAMES = ("stime", "time", "index", "date", "datetime", "timetag")


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)



CLIENT_CONFIG_MODULE_ENV = "BIGQMT_CLIENT_CONFIG_MODULE"
DEFAULT_CLIENT_CONFIG_MODULES = (
    "bigqmt_signal_trader_client_config",
    "bigqmt_signal_trader_local_config",
)

# ---------------------------------------------------------------------------
# xtconstant 枚举常量（对齐原生 MiniQMT xtquant/xtconstant.py，91 个全量）
# ---------------------------------------------------------------------------

# 账号类型
FUTURE_ACCOUNT = 1            # 期货
SECURITY_ACCOUNT = 2          # 股票
CREDIT_ACCOUNT = 3            # 信用
FUTURE_OPTION_ACCOUNT = 5     # 期货期权
STOCK_OPTION_ACCOUNT = 6      # 股票期权
HUGANGTONG_ACCOUNT = 7        # 沪港通
SHENGANGTONG_ACCOUNT = 11     # 深港通

# 委托类型 - 期货六键风格
FUTURE_OPEN_LONG = 0                  # 开多
FUTURE_CLOSE_LONG_HISTORY = 1         # 平昨多
FUTURE_CLOSE_LONG_TODAY = 2           # 平今多
FUTURE_OPEN_SHORT = 3                 # 开空
FUTURE_CLOSE_SHORT_HISTORY = 4        # 平昨空
FUTURE_CLOSE_SHORT_TODAY = 5          # 平今空
# 委托类型 - 期货四键风格
FUTURE_CLOSE_LONG_TODAY_FIRST = 6     # 平多，优先平今
FUTURE_CLOSE_LONG_HISTORY_FIRST = 7   # 平多，优先平昨
FUTURE_CLOSE_SHORT_TODAY_FIRST = 8    # 平空，优先平今
FUTURE_CLOSE_SHORT_HISTORY_FIRST = 9  # 平空，优先平昨
# 委托类型 - 期货两键风格
FUTURE_CLOSE_LONG_TODAY_HISTORY_THEN_OPEN_SHORT = 10  # 卖出，优先平仓平今，余量开空
FUTURE_CLOSE_LONG_HISTORY_TODAY_THEN_OPEN_SHORT = 11  # 卖出，优先平仓平昨，余量开空
FUTURE_CLOSE_SHORT_TODAY_HISTORY_THEN_OPEN_LONG = 12  # 买入，优先平仓平今，余量开多
FUTURE_CLOSE_SHORT_HISTORY_TODAY_THEN_OPEN_LONG = 13  # 买入，优先平仓平昨，余量开多
FUTURE_OPEN = 14               # 买入，不优先平仓
FUTURE_CLOSE = 15              # 卖出，不优先平仓
# 委托类型 - 期货跨商品套利
FUTURE_ARBITRAGE_OPEN = 16               # 开仓
FUTURE_ARBITRAGE_CLOSE_HISTORY_FIRST = 17  # 平，优先平昨
FUTURE_ARBITRAGE_CLOSE_TODAY_FIRST = 18    # 平，优先平今
# 委托类型 - 期货展期
FUTURE_RENEW_LONG_CLOSE_HISTORY_FIRST = 19   # 看多，优先平昨
FUTURE_RENEW_LONG_CLOSE_TODAY_FIRST = 20     # 看多，优先平今
FUTURE_RENEW_SHORT_CLOSE_HISTORY_FIRST = 21  # 看空，优先平昨
FUTURE_RENEW_SHORT_CLOSE_TODAY_FIRST = 22    # 看空，优先平今

# 委托类型 - 股票
STOCK_BUY = 23
STOCK_SELL = 24
# 委托类型 - 信用交易
CREDIT_BUY = 23                       # 担保品买入
CREDIT_SELL = 24                      # 担保品卖出
CREDIT_FIN_BUY = 27                   # 融资买入
CREDIT_SLO_SELL = 28                  # 融券卖出
CREDIT_BUY_SECU_REPAY = 29            # 买券还券
CREDIT_DIRECT_SECU_REPAY = 30         # 直接还券
CREDIT_SELL_SECU_REPAY = 31           # 卖券还款
CREDIT_DIRECT_CASH_REPAY = 32         # 直接还款
CREDIT_FIN_BUY_SPECIAL = 40           # 专项融资买入
CREDIT_SLO_SELL_SPECIAL = 41          # 专项融券卖出
CREDIT_BUY_SECU_REPAY_SPECIAL = 42    # 专项买券还券
CREDIT_DIRECT_SECU_REPAY_SPECIAL = 43  # 专项直接还券
CREDIT_SELL_SECU_REPAY_SPECIAL = 44   # 专项卖券还款
CREDIT_DIRECT_CASH_REPAY_SPECIAL = 45  # 专项直接还款

# 委托类型 - 股票期权
STOCK_OPTION_BUY_OPEN = 48       # 买入开仓
STOCK_OPTION_SELL_CLOSE = 49     # 卖出平仓
STOCK_OPTION_SELL_OPEN = 50      # 卖出开仓
STOCK_OPTION_BUY_CLOSE = 51      # 买入平仓
STOCK_OPTION_COVERED_OPEN = 52   # 备兑开仓
STOCK_OPTION_COVERED_CLOSE = 53  # 备兑平仓
STOCK_OPTION_CALL_EXERCISE = 54  # 认购行权
STOCK_OPTION_PUT_EXERCISE = 55   # 认沽行权
STOCK_OPTION_SECU_LOCK = 56      # 证券锁定
STOCK_OPTION_SECU_UNLOCK = 57    # 证券解锁

# 委托类型 - 期货期权
OPTION_FUTURE_OPTION_EXERCISE = 100  # 期货期权行权

# 报价类型（市价）
LATEST_PRICE = 5                        # 最新价
FIX_PRICE = 11                          # 指定价/限价
MARKET_SH_CONVERT_5_CANCEL = 42         # 最优五档即时成交剩余撤销[上交所][股票]
MARKET_SH_CONVERT_5_LIMIT = 43          # 最优五档即时成交剩转限价[上交所][股票]
MARKET_PEER_PRICE_FIRST = 44            # 对手方最优价格委托
MARKET_MINE_PRICE_FIRST = 45            # 本方最优价格委托
MARKET_SZ_INSTBUSI_RESTCANCEL = 46      # 即时成交剩余撤销委托[深交所][股票][期权]
MARKET_SZ_CONVERT_5_CANCEL = 47         # 最优五档即时成交剩余撤销[深交所][股票][期权]
MARKET_SZ_FULL_OR_CANCEL = 48           # 全额成交或撤销委托[深交所][股票][期权]

# 市场代码
SH_MARKET = 0
SZ_MARKET = 1

# 委托状态
ORDER_UNREPORTED = 48
ORDER_WAIT_REPORTING = 49
ORDER_REPORTED = 50
ORDER_REPORTED_CANCEL = 51
ORDER_PARTSUCC_CANCEL = 52
ORDER_PART_CANCEL = 53
ORDER_CANCELED = 54
ORDER_PART_SUCC = 55
ORDER_SUCCEEDED = 56
ORDER_JUNK = 57
ORDER_UNKNOWN = 255

# 账号状态
ACCOUNT_STATUS_INVALID = -1       # 无效
ACCOUNT_STATUS_OK = 0             # 正常
ACCOUNT_STATUS_WAITING_LOGIN = 1  # 连接中
ACCOUNT_STATUSING = 2             # 登陆中
ACCOUNT_STATUS_FAIL = 3           # 失败
ACCOUNT_STATUS_INITING = 4        # 初始化中
ACCOUNT_STATUS_CORRECTING = 5     # 数据刷新校正中
ACCOUNT_STATUS_CLOSED = 6         # 收盘后
ACCOUNT_STATUS_ASSIS_FAIL = 7     # 穿透副链接断开
ACCOUNT_STATUS_DISABLEBYSYS = 8   # 系统停用
ACCOUNT_STATUS_DISABLEBYUSER = 9  # 用户停用


class CompatObject:
    """Small attribute object matching xtquant's object-style returns."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        items = ", ".join("%s=%r" % (key, value) for key, value in sorted(self.__dict__.items()))
        return "%s(%s)" % (self.__class__.__name__, items)





class XtQuantTraderCallback:
    def on_disconnected(self):
        pass

    def on_stock_order(self, order):
        pass

    def on_stock_trade(self, trade):
        pass

    def on_order_error(self, order_error):
        pass

    def on_cancel_error(self, cancel_error):
        pass

    def on_order_stock_async_response(self, response):
        pass

    def on_cancel_order_stock_async_response(self, response):
        pass

    def on_account_status(self, status):
        pass


def _env_int(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return int(value)


def _env_float(name, default):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return float(value)


def _env_bool(name, default=False):
    value = os.environ.get(name)
    if value in (None, ""):
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _bool_value(value, default=False):
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _import_optional_module(module_name):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            return None
        raise


def _quote_client_id():
    """Process-stable client id for whole-quote subscriptions. Config or env wins;
    otherwise read/create a persisted id so a restarted client is recognised as
    the same subscriber by the server."""
    client_config = load_client_config()
    configured = client_config.get("quote_client_id") or os.environ.get("BIGQMT_QUOTE_CLIENT_ID")
    if configured:
        return str(configured)
    cache_path = os.path.join(os.path.expanduser("~"), ".cache", "bigqmt", "quote_client_id")
    try:
        with open(cache_path, "r") as handle:
            existing = handle.read().strip()
            if existing:
                return existing
    except OSError:
        pass
    new_id = uuid.uuid4().hex
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as handle:
            handle.write(new_id)
    except OSError:
        pass
    return new_id


def _quote_push_zmq_address(client):
    """Derive the server whole-quote PUB address: same host as the RPC zmq
    endpoint, RPC port + 1 (the PUB socket binds a distinct port)."""
    from .transports.zmq_transport import DEFAULT_ZMQ_HOST, _default_zmq_port

    zmq_config = dict(getattr(client, "zmq_config", {}) or {})
    explicit = zmq_config.get("quote_push_connect_address")
    if explicit:
        return str(explicit)
    host = zmq_config.get("host") or DEFAULT_ZMQ_HOST
    port = zmq_config.get("port")
    base_port = int(port) if port is not None else _default_zmq_port(client.account_id)
    return "tcp://%s:%d" % (host, base_port + 1)


def _missing_account_id_message():
    """Say what was searched and what to do, not just that something is missing.

    "Big QMT account_id is required" told the reader nothing about where the
    config was looked for, so a config file placed one sys.path away from the
    running interpreter looked identical to no config at all (issue #90).
    """
    searched = list(DEFAULT_CLIENT_CONFIG_MODULES)
    selected = os.environ.get(CLIENT_CONFIG_MODULE_ENV)
    if selected and selected not in searched:
        searched.insert(0, selected)
    found = None
    try:
        config = load_client_config()
        found = (config or {}).get("module")
    except Exception:
        pass

    if found:
        detail = ("imported %s, but it defines no BIGQMT_ACCOUNT_ID"
                  % found)
    else:
        detail = ("none of these modules could be imported: %s"
                  % ", ".join(searched))
    lines = [
        "Big QMT account_id is required -- %s." % detail,
        "Fix it in any one of these ways:",
        "  1. put bigqmt_signal_trader_client_config.py somewhere on sys.path"
        " (the current working directory counts), with BIGQMT_ACCOUNT_ID set;",
        "  2. set the BIGQMT_ACCOUNT_ID environment variable;",
        "  3. call bigqmt_signal_trader.xtquant_compat.configure(account_id=...)"
        " before use;",
        "  or run `bigqmt-init`, which writes both config files for you.",
        "configure() also runs at import time, so a config put in place after"
        " importing this module needs configure() called again.",
    ]
    return "\n".join(lines)


def load_client_config(module_name=None):
    """Load local private client config without requiring environment variables."""
    candidates = []
    selected = module_name or os.environ.get(CLIENT_CONFIG_MODULE_ENV)
    if selected:
        candidates.append(str(selected))
    candidates.extend(name for name in DEFAULT_CLIENT_CONFIG_MODULES if name not in candidates)

    for candidate in candidates:
        module = _import_optional_module(candidate)
        if module is None:
            continue
        redis_config = dict(getattr(module, "BIGQMT_REDIS_CONFIG", {}) or {})
        account_id = getattr(module, "BIGQMT_ACCOUNT_ID", None) or redis_config.get("account_id")
        timeout_seconds = getattr(module, "BIGQMT_RPC_TIMEOUT_SECONDS", None)
        if timeout_seconds is None:
            timeout_seconds = redis_config.get("rpc_timeout_seconds")
        download_wait_seconds = getattr(module, "BIGQMT_DOWNLOAD_WAIT_SECONDS", None)
        if download_wait_seconds is None:
            download_wait_seconds = redis_config.get("download_wait_seconds")
        download_poll_interval_seconds = getattr(module, "BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS", None)
        if download_poll_interval_seconds is None:
            download_poll_interval_seconds = redis_config.get("download_poll_interval_seconds")
        full_tick_cache_config = dict(getattr(module, "BIGQMT_FULL_TICK_CACHE_CONFIG", {}) or {})
        for key in (
            "full_tick_cache_enabled",
            "full_tick_demand_ttl_seconds",
            "full_tick_cache_ttl_seconds",
            "full_tick_wait_seconds",
            "full_tick_poll_interval_seconds",
        ):
            if key in redis_config:
                full_tick_cache_config[key] = redis_config[key]
        local_cache_config = dict(getattr(module, "BIGQMT_LOCAL_CACHE_CONFIG", {}) or {})
        for key in ("local_cache_enabled", "local_cache_dir", "local_cache_fallback_rpc", "local_cache_format"):
            if key in redis_config:
                local_cache_config[key.replace("local_cache_", "")] = redis_config[key]
        formula_server_config = dict(getattr(module, "BIGQMT_FORMULA_SERVER_CONFIG", {}) or {})
        formula_server_config.update(dict(redis_config.get("formula_server") or {}))
        return {
            "module": candidate,
            "account_id": account_id,
            "redis_config": redis_config,
            "timeout_seconds": timeout_seconds,
            "download_wait_seconds": download_wait_seconds,
            "download_poll_interval_seconds": download_poll_interval_seconds,
            "full_tick_cache_config": full_tick_cache_config,
            "local_cache_config": local_cache_config,
            "formula_server_config": formula_server_config,
            "quote_client_id": getattr(module, "BIGQMT_QUOTE_CLIENT_ID", None),
        }
    return {}


def _account_id(account, fallback=""):
    if account is None:
        return str(fallback or "")
    if isinstance(account, str):
        return account
    for name in ("account_id", "m_strAccountID", "id"):
        value = getattr(account, name, None)
        if value:
            return str(value)
    if isinstance(account, dict):
        return str(account.get("account_id") or account.get("id") or fallback or "")
    return str(fallback or "")


def _action_to_order_type(action):
    text = str(action or "").upper()
    if text in ("BUY", str(STOCK_BUY)):
        return STOCK_BUY
    if text in ("SELL", str(STOCK_SELL)):
        return STOCK_SELL
    return 0


# xtquant offset_flag 值域: 48=OFFSET_FLAG_OPEN(股票=买) / 49=OFFSET_FLAG_CLOSE(卖).
# 与 app.domain.types.enums.XT_OFFSET_FLAG_BUY/SELL 同值 (23/24 是 order_stock 入参
# 的 STOCK_BUY/SELL namespace, 切勿混用 — 见 qmt_gateway.query_stock_trades_all 注释).
_OFFSET_FLAG_BUY = 48
_OFFSET_FLAG_SELL = 49


def _offset_flag_from_item(item, action):
    """[fix 2026-08-26 state.trades 丢笔] 推导 offset_flag (48/49) 供网关方向映射.

    背景: app 网关 query_stock_trades_all 读 XtTrade.offset_flag 判 BUY/SELL;
    旧实现 CompatObject 不设该属性 → getattr 默认 None → OMS 启动重建
    fail-CLOSED 跳过 (2026-08-26 09:36 两笔实成交被丢, state.trades/PG 台账双缺).
    三源依次取: 服务端查询行只带 action 字符串; 事件行额外带原始
    offset_flag/direction 整数. 全缺 → None (保持上游 fail-CLOSED, 不静默错记).
    """
    for key in ("offset_flag", "direction"):
        raw = item.get(key)
        if raw is None or raw == "":
            continue
        value = _safe_int(raw, -1)
        if value in (_OFFSET_FLAG_BUY, _OFFSET_FLAG_SELL):
            return value
    text = str(action or "").upper()
    if text == "BUY":
        return _OFFSET_FLAG_BUY
    if text == "SELL":
        return _OFFSET_FLAG_SELL
    return None


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_unix_seconds(value, default=0):
    """Normalize a trade/order time into Unix seconds (MiniQMT semantics).

    Accepts numeric epochs, ``YYYY-MM-DD HH:MM:SS[.ffffff]``,
    ``YYYYMMDDHHMMSS`` strings and — [fix 2026-08-26 state.trades 丢笔配套]
    bare ``HHMMSS`` digit strings (大 QMT m_strTradeTime 形态, 如 "93631";
    查询行只有时分秒无日期, 成交查询是当日口径 → 拼当天). Anything else
    falls back to ``default``.
    """
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%d%H%M%S"):
        try:
            return int(time.mktime(time.strptime(text, fmt)))
        except ValueError:
            continue
    if text.isdigit() and len(text) <= 6:
        try:
            return int(time.mktime(_dt.datetime.combine(
                _dt.date.today(),
                _dt.datetime.strptime(text.zfill(6), "%H%M%S").time(),
            ).timetuple()))
        except ValueError:
            pass
    return default


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, list):
        return value
    return [value]


def _account_type_name(value):
    """The NAME of an account type, whatever form it arrives in.

    StockAccount stores the numeric code, not the string it was constructed
    with: StockAccount(id, "CREDIT").account_type is 3. Comparing that against
    the server's "CREDIT" would report a mismatch on every credit account.
    """
    text = str("" if value is None else value).strip()
    if not text:
        return ""
    if text.isdigit():
        try:
            from xtquant.xtconstant import ACCOUNT_TYPE_DICT

            return str(ACCOUNT_TYPE_DICT.get(int(text), "")).strip().upper()
        except Exception:
            return ""
    return text.upper()


def _account_type_code(value):
    """The xtconstant NUMBER for an account type, whatever form it arrives in.

    The mirror of _account_type_name. XtOrder / XtTrade / XtPosition all carry
    account_type as an int (xttype sets it to SECURITY_ACCOUNT), so a name has
    to come back as a number before it reaches a caller. 0 means "nothing to
    go on" -- the caller decides what to fall back to.
    """
    text = str("" if value is None else value).strip()
    if not text:
        return 0
    try:
        return int(text)
    except (TypeError, ValueError):
        pass
    upper = text.upper()
    try:
        from xtquant import xtconstant
    except Exception:
        return 0
    # NOT ACCOUNT_TYPE_DICT alone: the xtquant that wins inside Big QMT is the
    # terminal's own bundled copy, which has 91 of this shim's 538 names and
    # does not include that dict. A client can land on it too -- appending the
    # QMT python directory to sys.path is a documented way to reach the config
    # modules. Fall back to the individual *_ACCOUNT constants, which both
    # copies have.
    table = getattr(xtconstant, "ACCOUNT_TYPE_DICT", None)
    if isinstance(table, dict):
        for code, name in table.items():
            if str(name).strip().upper() == upper:
                try:
                    return int(code)
                except (TypeError, ValueError):
                    break
    for attribute in ("%s_ACCOUNT" % upper,
                      "SECURITY_ACCOUNT" if upper == "STOCK" else ""):
        if not attribute:
            continue
        code = getattr(xtconstant, attribute, None)
        if isinstance(code, int) and not isinstance(code, bool):
            return int(code)
    return 0


def _restore_jsonable(value):
    if isinstance(value, dict):
        marker = value.get("__bigqmt_type__")
        if marker == "DataFrame":
            try:
                import pandas as pd

                return pd.DataFrame(value.get("records") or [], columns=value.get("columns") or None)
            except Exception:
                return value.get("records") or []
        if marker == "Panel":
            # pandas dropped Panel in 1.0, so a 3-D object cannot be rebuilt on
            # a modern client. It comes back as what a caller can actually use:
            # {item: DataFrame} (issue #115). The axis labels ride along for
            # anyone who needs to know how the cube was sliced.
            return {key: _restore_jsonable(item)
                    for key, item in (value.get("data") or {}).items()}
        if marker == "Series":
            try:
                import pandas as pd

                return pd.Series(value.get("data") or {})
            except Exception:
                return value.get("data") or {}
        return {key: _restore_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_restore_jsonable(item) for item in value]
    return value


def _digits_only(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _parse_qmt_stime(value):
    digits = _digits_only(value)
    if len(digits) >= 14:
        try:
            return _dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None
    if len(digits) >= 8:
        try:
            return _dt.datetime.strptime(digits[:8], "%Y%m%d")
        except ValueError:
            return None
    return None


# FormulaServer 快照滞后检测：实测它会把 1m 数据冻结数小时（11:30 后不再
# 更新，收盘后还在发午间数据）。对直连回答的 intraday 数据做时间差检测：
# 滞后即告警 + 本次自动回落 RPC 桥拿实时数据，并在冷却期内跳过直连
# （冷却到期自动重新探测，自愈）。
_FORMULA_STALE_INTRADAY_PERIODS = ("tick", "1m", "3m", "5m", "15m", "30m", "1h")
_FORMULA_STALE_WARN_LAG_SECONDS = 30 * 60
_FORMULA_STALE_COOLDOWN_SECONDS = 120.0
_formula_stale_warned = {}
_formula_stale_until = {"ts": 0.0}


def _formula_bars_stale(data, params):
    """返回 (code, newest_dt, lag_seconds) 或 None。只检测，不告警。"""
    try:
        period = str((params or {}).get("period") or "").lower()
        if period not in _FORMULA_STALE_INTRADAY_PERIODS:
            return None
        now = _dt.datetime.now()
        today = now.date()
        for code, df in (data or {}).items():
            # 公式直连返回的帧时间轴在 stime 列（RangeIndex），
            # 兼容层归一化后的帧在索引上——两种形态都认。
            newest_value = None
            for col in ("stime", "time"):
                if col in list(getattr(df, "columns", [])):
                    newest_value = df[col].iloc[-1]
                    break
            if newest_value is None:
                index = getattr(df, "index", None)
                if index is not None and len(index):
                    newest_value = index[-1]
            newest = _parse_qmt_stime(newest_value)
            if newest is None:
                continue
            lag = (now - newest).total_seconds()
            if (newest.date() < today) or (lag > _FORMULA_STALE_WARN_LAG_SECONDS):
                return (str(code), newest, lag)
    except Exception:
        pass
    return None


def _warn_stale_formula_bars(data, params, hit=None):
    try:
        period = str((params or {}).get("period") or "").lower()
        today = _dt.datetime.now().date()
        if hit is None:
            hit = _formula_bars_stale(data, params)
        if hit is None:
            return
        code, newest, lag = hit
        key = (code, period, str(today))
        if _formula_stale_warned.get(key):
            return
        _formula_stale_warned[key] = True
        log.warning(
            "FormulaServer data looks stale for %s %s: newest bar %s lags now by %.0fs. "
            "Falling back to the RPC bridge for live reads (cooldown %.0fs).",
            code, period, newest, lag, _FORMULA_STALE_COOLDOWN_SECONDS,
        )
    except Exception:
        pass


def _formula_stale_active():
    """冷却期内跳过公式直连（检测到滞后之后的一段时间）。"""
    return time.time() < _formula_stale_until.get("ts", 0.0)


def _qmt_stime_index(value):
    digits = _digits_only(value)
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) >= 8:
        return digits[:8]
    return str(value or "")


def _qmt_datetime_to_epoch_ms(dt_value):
    # QMT bar labels are China local time; MiniQMT's time column is epoch ms.
    china_tz = _dt.timezone(_dt.timedelta(hours=8))
    return int(dt_value.replace(tzinfo=china_tz).timestamp() * 1000)


def _normalize_market_data_frame(df, field_list=None):
    try:
        columns = list(df.columns)
    except Exception:
        return df
    if "stime" not in columns:
        return df

    requested = [str(field) for field in (field_list or [])]
    try:
        out = df.copy()
        stimes = list(out["stime"])
        out.index = [_qmt_stime_index(value) for value in stimes]
        if "time" in out.columns or "time" in requested:
            out["time"] = [
                _qmt_datetime_to_epoch_ms(parsed) if parsed is not None else None
                for parsed in (_parse_qmt_stime(value) for value in stimes)
            ]
        if requested:
            keep = [field for field in requested if field in out.columns]
            if keep:
                return out[keep]
        if "stime" in out.columns:
            return out.drop(columns=["stime"])
        return out
    except Exception:
        return df


def _normalize_market_data_result(data, field_list=None):
    if not isinstance(data, dict):
        return data
    return {
        code: _normalize_market_data_frame(frame, field_list=field_list)
        for code, frame in data.items()
    }


def _normalize_code_for_filter(code):
    text = str(code or "").strip().upper()
    if "." not in text:
        return text
    return text.split(".", 1)[0]


def _is_hs_a_share(code):
    text = str(code or "").strip().upper()
    pure = _normalize_code_for_filter(text)
    if not (len(pure) == 6 and pure.isdigit()):
        return False
    if text.endswith(".SH"):
        return pure.startswith(("600", "601", "603", "605", "688", "689"))
    if text.endswith(".SZ"):
        return pure.startswith(("000", "001", "002", "003", "300", "301"))
    return pure.startswith(
        ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
    )


def _full_a_share_code(code):
    """Ensure a callback stock_code carries its exchange suffix.

    Native MiniQMT XtOrder/XtTrade carry the full '600000.SH' form. Events from
    an older server (or when the callback object exposes no exchange info) may
    carry the bare 6-digit code; infer the suffix from the A-share code ranges
    so consumers can key on the full form. Non-6-digit or already-suffixed
    codes pass through unchanged.
    """
    text = str(code or "").strip().upper()
    if "." in text or not (len(text) == 6 and text.isdigit()):
        return text
    if text.startswith(("600", "601", "603", "605", "688", "689")):
        return text + ".SH"
    if text.startswith(("000", "001", "002", "003", "300", "301")):
        return text + ".SZ"
    return text


class BigQmtRpcClient:
    def __init__(
        self,
        account_id=None,
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
        transport=None,
    ):
        client_config = load_client_config()
        config_redis = dict(client_config.get("redis_config") or {})
        redis_config = dict(redis_config or {})
        merged_redis_config = dict(config_redis)
        merged_redis_config.update(redis_config)
        self.account_id = str(
            account_id
            or merged_redis_config.get("account_id")
            or client_config.get("account_id")
            or os.environ.get("BIGQMT_ACCOUNT_ID")
            or ""
        )
        self.redis_client = redis_client
        # [BUG-P0-20260810-redis-db-mismatch] db=0 是合法选择, 禁 `or` 短路 (falsy 0
        # 会被吞成默认 5 → 客户端与 QMT 端 transport 连不同 DB, RPC 无人消费)。
        # 与 redis_common.build_redis_client 同款显式 None/空串判断。
        _db_value = merged_redis_config.get("db")
        if _db_value is None or str(_db_value).strip() == "":
            _db_value = _env_int("BIGQMT_REDIS_DB", 5)
        self.redis_config = {
            "host": merged_redis_config.get("host") or os.environ.get("BIGQMT_REDIS_HOST", "127.0.0.1"),
            "port": int(merged_redis_config.get("port") or _env_int("BIGQMT_REDIS_PORT", 6379)),
            "db": int(_db_value),
            "username": merged_redis_config.get("username", os.environ.get("BIGQMT_REDIS_USERNAME") or ""),
            "password": merged_redis_config.get("password", os.environ.get("BIGQMT_REDIS_PASSWORD") or ""),
            # redis-py 8.x 默认 RESP3，Redis 5.0 只支持 RESP2 -> 透传 protocol
            "protocol": merged_redis_config.get("protocol") or _env_int("BIGQMT_REDIS_PROTOCOL", 2),
        }
        config_timeout = client_config.get("timeout_seconds")
        self.timeout_seconds = float(
            timeout_seconds
            if timeout_seconds is not None
            else config_timeout
            if config_timeout is not None
            else _env_float("BIGQMT_RPC_TIMEOUT_SECONDS", DEFAULT_RPC_TIMEOUT_SECONDS)
        )
        config_download_wait = client_config.get("download_wait_seconds")
        self.download_wait_seconds = float(
            config_download_wait
            if config_download_wait is not None
            else _env_float("BIGQMT_DOWNLOAD_WAIT_SECONDS", 1800.0)
        )
        config_download_poll = client_config.get("download_poll_interval_seconds")
        self.download_poll_interval_seconds = float(
            config_download_poll
            if config_download_poll is not None
            else _env_float("BIGQMT_DOWNLOAD_POLL_INTERVAL_SECONDS", 0.5)
        )
        full_tick_cache_config = dict(client_config.get("full_tick_cache_config") or {})
        self.full_tick_cache_config = {
            "enabled": _bool_value(
                full_tick_cache_config.get("enabled", full_tick_cache_config.get("full_tick_cache_enabled")),
                _env_bool("BIGQMT_FULL_TICK_CACHE_ENABLED", False),
            ),
            "demand_ttl_seconds": float(
                full_tick_cache_config.get("demand_ttl_seconds")
                or full_tick_cache_config.get("full_tick_demand_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_DEMAND_TTL_SECONDS", 10.0)
            ),
            "cache_ttl_seconds": float(
                full_tick_cache_config.get("cache_ttl_seconds")
                or full_tick_cache_config.get("full_tick_cache_ttl_seconds")
                or _env_float("BIGQMT_FULL_TICK_CACHE_TTL_SECONDS", 10.0)
            ),
            "wait_seconds": float(
                full_tick_cache_config.get("wait_seconds")
                or full_tick_cache_config.get("full_tick_wait_seconds")
                or _env_float("BIGQMT_FULL_TICK_WAIT_SECONDS", 3.5)
            ),
            "poll_interval_seconds": float(
                full_tick_cache_config.get("poll_interval_seconds")
                or full_tick_cache_config.get("full_tick_poll_interval_seconds")
                or _env_float("BIGQMT_FULL_TICK_POLL_INTERVAL_SECONDS", 0.2)
            ),
        }
        # Client-side local market-data cache. get_market_data_ex is cache-through;
        # fallback_rpc=True lets get_local_data fetch+cache a cache miss.
        local_cache_config = dict(client_config.get("local_cache_config") or {})
        self.local_cache_config = {
            "enabled": _bool_value(
                local_cache_config.get("enabled", merged_redis_config.get("local_cache_enabled")),
                _env_bool("BIGQMT_LOCAL_CACHE_ENABLED", True),
            ),
            "dir": (
                local_cache_config.get("dir")
                or merged_redis_config.get("local_cache_dir")
                or os.environ.get("BIGQMT_LOCAL_CACHE_DIR")
                or None
            ),
            "fallback_rpc": _bool_value(
                local_cache_config.get("fallback_rpc", merged_redis_config.get("local_cache_fallback_rpc")),
                _env_bool("BIGQMT_LOCAL_CACHE_FALLBACK_RPC", False),
            ),
            "format": str(
                local_cache_config.get("format")
                or merged_redis_config.get("local_cache_format")
                or os.environ.get("BIGQMT_LOCAL_CACHE_FORMAT")
                or "auto"  # parquet if pyarrow is available, else pickle
            ),
        }
        # Transport selection. Default "redis" keeps the legacy call_redis_rpc
        # path (so existing client configs are unchanged). Setting transport to
        # "zmq"/"mysql"/"shm" (via config or constructor) routes calls through
        # the swappable transport layer instead.
        self.transport_name = str(
            transport
            or merged_redis_config.get("transport")
            or os.environ.get("BIGQMT_RPC_TRANSPORT")
            or "redis"
        ).lower()
        self.zmq_config = dict(merged_redis_config.get("zmq") or {})
        self.mysql_config = dict(merged_redis_config.get("mysql") or {})
        self._transport_instance = None  # lazily built by _transport()
        # FormulaServer read fast-path. QMT's C++ quote service (port 58600)
        # answers reference/history reads in ~0.07ms without touching the QMT
        # python thread. Enabled by default; every miss falls back to RPC, so a
        # client that cannot reach it just runs as before.
        formula_config = dict(
            client_config.get("formula_server_config")
            or merged_redis_config.get("formula_server")
            or {}
        )
        if "enabled" not in formula_config:
            formula_config["enabled"] = _env_bool("BIGQMT_FORMULA_ENABLED", True)
        self.formula_server_config = formula_config
        self._formula_router_instance = None  # lazily built by _formula_router()

    def _redis(self):
        if self.redis_client is None:
            import redis

            from .adapters.redis_common import redis_supports_protocol_kw

            cfg = dict(self.redis_config)
            if not cfg.get("username"):
                cfg.pop("username", None)
            if not cfg.get("password"):
                cfg.pop("password", None)
            if not redis_supports_protocol_kw():
                # QMT 自带 redis-py 3.5.3 不认 protocol（issue #71）
                cfg.pop("protocol", None)
            self.redis_client = redis.Redis(**cfg)
        return self.redis_client

    def _transport(self):
        if self._transport_instance is None:
            if self.transport_name in ("redis", "", "default"):
                # Legacy path: call_redis_rpc builds its own request envelope.
                return None
            from .transports.factory import build_transport

            client_config = load_client_config()
            config_redis = dict(client_config.get("redis_config") or {})
            zmq_config = dict(config_redis.get("zmq") or {})
            zmq_config.update(self.zmq_config)
            # ZMQ must work without Redis. Discovery is opt-in and unnecessary
            # when connect_address is explicitly configured.
            if (
                not zmq_config.get("connect_address")
                and bool(zmq_config.get("redis_discovery_enabled", False))
            ):
                zmq_config.setdefault("discovery_redis_client", self._redis())
            factory_config = {
                "zmq": zmq_config,
                "mysql": dict(config_redis.get("mysql") or {}, **self.mysql_config),
            }
            self._transport_instance = build_transport(
                self.transport_name,
                factory_config,
                account_id=self.account_id,
                print_prefix="[bigqmt_client]",
            )
        return self._transport_instance

    def _formula_router(self):
        """Lazily build the FormulaServer router. Never raises — a router that
        cannot be built simply means every read goes over RPC."""
        if self._formula_router_instance is None:
            try:
                from .formula_server import build_router

                self._formula_router_instance = build_router(
                    self.formula_server_config, print_prefix="[bigqmt_formula]"
                )
            except Exception as exc:
                print("[bigqmt_formula] disabled (%s: %s)" % (exc.__class__.__name__, exc))

                class _Disabled(object):
                    def supports(self, method):
                        return False

                self._formula_router_instance = _Disabled()
        return self._formula_router_instance

    def call(self, method, params=None, account_id=None, timeout_seconds=None, use_formula=True):
        target_account = str(account_id or self.account_id or "")
        if not target_account:
            raise ValueError(_missing_account_id_message())
        wait_seconds = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        # Fast path: reference/history reads answered straight by QMT's
        # FormulaServer, bypassing the strategy process and its GIL. Anything it
        # declines (unmapped method, untranslatable params, server down) raises
        # Unroutable and drops through to the RPC bridge below.
        # use_formula=False 用于必须拿到最新数据的调用（如 subscribe_quote 的
        # 盘中形成 bar 轮询）——FormulaServer 的快照可能滞后数小时（实测盘中
        # 11:30 后冻结），形成 bar 只能走 RPC 桥读 QMT 实时数据。
        router = self._formula_router() if use_formula else None
        # 冷却期（公式数据刚被检出滞后）只对 get_market_data_ex 跳过直连——
        # 其他方法是静态参考数据，不受时间序列滞后影响，照常走快速路径。
        skip_formula = (
            router is not None
            and method == "get_market_data_ex"
            and _formula_stale_active()
        )
        if router is not None and router.supports(method) and not skip_formula:
            from .formula_server import Unroutable

            try:
                result = _restore_jsonable(router.call(method, params or {}))
                if method == "get_market_data_ex":
                    # 直连快照可能滞后（实测冻结数小时）——滞后即告警、
                    # 本次调用自动回落 RPC 桥拿实时数据，并进入冷却期
                    # 让后续调用直接跳过直连（到期重新探测，自愈）。
                    hit = _formula_bars_stale(result, params or {})
                    if hit is not None:
                        _warn_stale_formula_bars(result, params or {}, hit=hit)
                        _formula_stale_until["ts"] = time.time() + _FORMULA_STALE_COOLDOWN_SECONDS
                        return self.call(method, params, account_id=account_id,
                                         timeout_seconds=timeout_seconds, use_formula=False)
                return result
            except Unroutable:
                pass
        # [BUG-20260811-rpc-storm] RPC 路径限流 — 防并发风暴涌入 QMT 单线程 drain.
        # acquire 带超时 (wait_seconds*2): 拿不到 slot → TimeoutError (防无限排队 +
        # 防主 loop 阻塞超 watchdog 阈值). FormulaServer fast path 已提前 return.
        _acquire_timeout = max(1.0, float(wait_seconds or 30) * 2)
        if not _RPC_INFLIGHT.acquire(timeout=_acquire_timeout):
            # [trace 2026-08-21] 补 account_id + wall clock, 让 throttle 超时也能定位
            # 是哪个 account / 何时触发 (8-11 启动期 RPC 风暴复盘关键证据).
            raise TimeoutError(
                "bigqmt rpc concurrency throttle: %s (account=%s in-flight=%d t=%.3f)" %
                (method, target_account, _RPC_CONCURRENCY, time.time())
            )
        try:
            transport = self._transport()
            if transport is not None:
                # Swappable transport path (zmq/mysql/...). Build the request
                # envelope the same way call_redis_rpc does.
                request = {
                    "schema_version": 1,
                    "request_id": uuid.uuid4().hex,
                    "account_id": target_account,
                    "method": method,
                    "params": params or {},
                    "ttl_seconds": 60,
                    # [fix 2026-09-01 GHX1-03] 入队时刻, 与 call_redis_rpc envelope
                    # 对齐: 消费端 stale 检查要求 ts+ttl 双非 None, 缺 ts 则丢弃半
                    # 永不生效 (若未来切 queue 型 transport, 幽灵补执行形态会复活).
                    "ts": time.time(),
                }
                response = transport.send_request(request, wait_seconds)
            else:
                response = call_redis_rpc(
                    self._redis(),
                    account_id=target_account,
                    method=method,
                    params=params or {},
                    timeout_seconds=wait_seconds,
                )
        finally:
            _RPC_INFLIGHT.release()
        if not response.get("ok"):
            raise RuntimeError(response.get("error") or "Big QMT RPC failed: %s" % method)
        # server_error 携带 QMT 端诊断（如 passorder 提交但委托没进系统）。
        # 只在交易类方法上设置（读取类恒为空），转成异常让调用方看到真实原因，
        # 而不是把「无委托号」误判为 -1 失败（issue #38）。
        server_error = str(response.get("server_error") or "")
        if server_error:
            raise RuntimeError("Big QMT %s server_error: %s" % (method, server_error))
        # The transport already scanned the raw text for a typed envelope; when
        # it found none there is provably nothing to rebuild, and skipping the
        # walk turns 345.9ms into 3.7ms on a 51285-instrument snapshot. A None
        # flag means the text was never seen (in-process routing), so walk.
        if response.pop(TYPED_PAYLOAD_FLAG, None) is False:
            return response.get("data")
        return _restore_jsonable(response.get("data"))

    # ------------------------------------------------------------------
    # Async RPC (issue #63): call_async returns a Future immediately, so a
    # caller can have many independent requests in flight instead of one
    # blocking call at a time. The server still processes order RPCs on the
    # QMT main thread serially — client-side async overlaps the round-trip
    # latency, it does not parallelize the exchange leg.
    _ASYNC_RPC_MAX_IN_FLIGHT = 64

    def _async_rpc_pool(self):
        pool = getattr(self, "_rpc_async_pool", None)
        if pool is None:
            from concurrent.futures import ThreadPoolExecutor

            pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="bigqmt-rpc-async")
            self._rpc_async_pool = pool
            self._rpc_async_slots = threading.Semaphore(self._ASYNC_RPC_MAX_IN_FLIGHT)
            self._rpc_async_dispatcher = None
        return pool

    def call_async(self, method, params=None, account_id=None, timeout_seconds=None, callback=None):
        """Submit an RPC without blocking; returns concurrent.futures.Future.

        ``callback`` (optional) receives the result on a single dispatcher
        thread — callbacks fire serialized in completion order, never
        concurrently. In-flight requests are bounded; when the limit is hit
        the call raises instead of queueing unboundedly.
        """
        pool = self._async_rpc_pool()
        if not self._rpc_async_slots.acquire(timeout=30.0):
            raise RuntimeError(
                "too many RPCs in flight (max %d)" % self._ASYNC_RPC_MAX_IN_FLIGHT
            )

        def _run():
            try:
                return self.call(method, params, account_id=account_id,
                                 timeout_seconds=timeout_seconds)
            finally:
                self._rpc_async_slots.release()

        future = pool.submit(_run)
        if callback is not None:
            if self._rpc_async_dispatcher is None:
                from concurrent.futures import ThreadPoolExecutor

                self._rpc_async_dispatcher = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="bigqmt-rpc-dispatch"
                )
            dispatcher = self._rpc_async_dispatcher

            def _deliver(fut):
                try:
                    result = fut.result()
                except Exception as exc:
                    log.warning("call_async %s failed: %s", method, exc)
                    return
                try:
                    callback(result)
                except Exception:
                    log.exception("call_async callback failed: %s", method)

            future.add_done_callback(lambda fut: dispatcher.submit(_deliver, fut))
        return future

    def publish_event(self, event_type, payload, stream_template="bigqmt:quote_events:{account_id}"):
        account_id = str(self.account_id or "")
        event = {
            "event_type": str(event_type),
            "account_id": account_id,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "payload": payload or {},
        }
        # 显式地址且未启用 Redis discovery 的 ZMQ 是纯 ZMQ 模式，不应隐式连接 Redis。
        # [merge 2026-09-03] getattr 防御: 轻量 client/test fake 重写 __init__ 不调
        # super 时这两个属性不存在。
        if getattr(self, "transport_name", "") == "zmq" and not bool(
            (getattr(self, "zmq_config", None) or {}).get("redis_discovery_enabled", False)
        ):
            return event
        raw = json.dumps(event, ensure_ascii=False, default=str)
        stream_key = stream_template.format(account_id=account_id)
        redis_client = self._redis()
        try:
            redis_client.xadd(stream_key, {"payload": raw}, maxlen=_QUOTE_STREAM_MAXLEN, approximate=True)
        except Exception:
            pass
        try:
            redis_client.publish(stream_key, raw)
        except Exception:
            pass
        return event

    def save_quote_subscription(self, seq, payload, active=True):
        """Persist / remove a subscribe_quote entry in the Redis hash.

        Returns ``True`` on success, ``False`` on failure.  Retries 2× on
        ``hset`` failure so a transient Redis hiccup does not leave the hash
        out-of-sync (旧 seq 被清 + 新 seq 没写 → QMT 推旧 seq → backend 找不到
        callback → 持仓股卖出规则失明, BUG-20260813-quote-callback-seq-mismatch).
        2 retries = max 150ms block (50+100ms); 启动期批量 subscribe 500+ 票
        不会因 Redis 持续故障阻塞过久加剧 RPC 风暴.

        [BUG-20260827-quote-sub-registry-silent-loss] hset 表面成功不等于落库成立
        (ACK 后丢写 / 值残缺 / 主从切换丢键 = 000983.SZ 全天断流当日成因候选之一,
         失败仅 print 且 stdout 被滚动覆盖 → 事后不可裁决)。因此每次 hset 后立即
        hget 回读并按 JSON 语义比对; 不一致视同失败吃同一重试预算, 最终失败升级
        log.error(持久化大QMT侧 bigqmt.log) + print(stdout) 双通道 — INV-4。
        """
        # MySQL、SHM 和混合 ZMQ 继续保留既有 Redis subscription metadata 行为。
        # [merge 2026-09-03] getattr 防御: 轻量 client/test fake 不带这两个属性。
        if getattr(self, "transport_name", "") == "zmq" and not bool(
            (getattr(self, "zmq_config", None) or {}).get("redis_discovery_enabled", False)
        ):
            return
        account_id = str(self.account_id or "")
        key = "bigqmt:quote_subscriptions:%s" % account_id
        redis_client = self._redis()
        if active:
            value = json.dumps(payload or {}, ensure_ascii=False, default=str)
            stock_code = ""
            try:
                stock_code = str((payload or {}).get("stock_code", "") or "")
            except Exception:
                stock_code = "?"
            for _attempt in range(2):
                try:
                    redis_client.hset(key, str(seq), value)
                    raw_back = redis_client.hget(key, str(seq))
                    back_text = (
                        raw_back.decode("utf-8") if isinstance(raw_back, (bytes, bytearray)) else raw_back
                    )
                    if not back_text:
                        raise RuntimeError(
                            "readback miss: field %s absent right after hset" % seq
                        )
                    back_obj = json.loads(back_text)
                    expected_obj = json.loads(value)
                    if back_obj != expected_obj:
                        raise RuntimeError(
                            "readback mismatch: written payload differs from persisted value"
                        )
                    return True
                except Exception as exc:
                    if _attempt == 1:
                        log.error(
                            "save_quote_subscription failed 2x seq=%s code=%s last_error=%s",
                            seq, stock_code or "?", exc,
                        )
                        print("[bigqmt] save_quote_subscription failed 2x "
                              "seq=%s code=%s last_error=%s" % (seq, stock_code or "?", exc))
                    time.sleep(0.05 * (_attempt + 1))
            return False
        else:
            try:
                redis_client.hdel(key, str(seq))
            except Exception as exc:
                # [BUG-20260827] 注销失败也不许全吞: 打点即可 (返 True 维持既有语义,
                # 该路径无数据安全影响 — 条目多活一轮由 GC/清理兜底)。
                log.warning("save_quote_subscription hdel failed seq=%s: %s", seq, exc)
            return True


# IPO subscription codes are their own numbering, distinct from the listed
# share's code. Classification is FAIL-CLOSED: an unrecognised code returns None
# and the caller skips it. The version this replaced ended in `return True`
# ("默认放行"), i.e. it guessed in favour of placing an order -- on a path whose
# whole job was to keep BJ subscriptions, which freeze cash, out.
_IPO_SH_PREFIXES = ("730", "732", "780", "787", "789", "707")
_IPO_SZ_PREFIXES = ("00", "30")
_IPO_BJ_PREFIXES = ("920", "889", "8", "4")


def ipo_market_of(code):
    """Return "SH" / "SZ" / "BJ", or None when the code is not recognised."""
    text = str(code or "").strip().upper()
    if not text:
        return None
    for suffix, market in ((".SH", "SH"), (".SZ", "SZ"), (".BJ", "BJ")):
        if text.endswith(suffix):
            return market
    if not text.isdigit():
        return None
    # Order matters: BJ 920/889 would otherwise be caught by a looser rule.
    if text.startswith(_IPO_BJ_PREFIXES):
        return "BJ"
    if text.startswith(_IPO_SH_PREFIXES):
        return "SH"
    if text.startswith(_IPO_SZ_PREFIXES):
        return "SZ"
    return None


MARKET_TOKENS = frozenset({"SH", "SZ", "BJ", "HK"})
# Above this many explicit codes, one RPC's single timeout starts to matter more
# than the extra payload of reading the exchange and filtering (issue #104).
# Measured against a live bridge: query_orders 1.5s, get_asset 1.4s,
# get_financial_data 0.8s warm, a whole-market get_full_tick 7.7s. The old
# 6s default sat under the cost of ordinary QMT data calls, and timing out
# here is worse than waiting: the bridge keeps working on the abandoned
# request, so the next call queues behind it and one timeout breeds more.
# 30s is also what the whole-market snapshot path already used, so there is
# one number rather than two.
DEFAULT_RPC_TIMEOUT_SECONDS = 30.0

LARGE_CODE_LIST = 1000
# What the fallback reads first. Stocks are 8.7% of an exchange listing, so
# starting narrow is 1.08s against 7.4s; it widens to "all" only if that misses.
DEFAULT_FALLBACK_TYPES = ("stock",)

# How many int -> 合同编号 pairs a trader keeps so a cancel still resolves after
# the caller round-tripped the id through JSON and lost the string (issue #113).
_ORDER_ID_MEMORY = 4096


def _markets_of(codes):
    """Market tokens the given suffixed codes live on, or empty if any code
    carries no recognised suffix -- filtering an exchange read cannot recover a
    code we cannot place."""
    markets = set()
    for code in codes or []:
        _, _, suffix = str(code).rpartition(".")
        suffix = suffix.upper()
        if suffix not in MARKET_TOKENS:
            return set()
        markets.add(suffix)
    return markets


def _full_tick_params(codes, types=None):
    """RPC params for get_full_tick. `types` narrows a whole-market token to one
    instrument kind at REQUEST time -- filtering the reply would still pay QMT's
    per-instrument cost for everything the exchange lists (issue #104)."""
    params = {"codes": codes}
    if types:
        params["types"] = [types] if isinstance(types, str) else list(types)
    return params


_FIELD_LIST_NOTICE = {"shown": False}
DIRECT_PATH_FIELDS = ("open", "high", "low", "close", "volume", "amount")


def _notice_field_list_cost(field_list):
    """Say once that naming fields is what enables the fast path.

    Not a warning about a mistake: an empty field_list correctly returns all 11
    columns and only RPC can do that. But the speedup is invisible unless
    someone tells you it exists (issue #104). Asking for a field the direct
    path lacks is safe -- it falls back to RPC rather than returning NaN."""
    if field_list or _FIELD_LIST_NOTICE["shown"]:
        return
    _FIELD_LIST_NOTICE["shown"] = True
    try:
        log.info(
            "get_market_data_ex with an empty field_list returns all 11 columns "
            "and must go over RPC. If the six OHLCV columns %s are enough, pass "
            "them as field_list -- that path is served by FormulaServer, 0.015s "
            "against 5.8s measured. Naming preClose / suspendFlag / "
            "settelementPrice / openInterest falls back to RPC, so a wider "
            "field_list is safe, just not faster.", ", ".join(DIRECT_PATH_FIELDS))
    except Exception:
        pass


# Bar subscriptions poll, because there is no server-side push for K-lines: the
# bridge only exposes ContextInfo.subscribe_whole_quote, which carries ticks.
# Interval is a floor on how stale a bar can be, not a promise of freshness.
DEFAULT_BAR_POLL_INTERVAL_SECONDS = 3.0


class _BarPoller(object):
    """Emit a K-line callback when the newest bar changes.

    MiniQMT's subscribe_quote pushes each bar update. We approximate it by
    re-reading the last bars and firing only when the newest one differs, so a
    caller written against MiniQMT keeps working. Every callback is wrapped:
    a raising subscriber must not kill the polling thread and silently end the
    subscription.
    """

    def __init__(self, fetch, callback, interval_seconds, on_error=None,
                 on_no_data=None):
        self._fetch = fetch
        self._callback = callback
        self._interval = max(0.2, float(interval_seconds))
        self._on_error = on_error
        self._on_no_data = on_no_data
        self._stop = threading.Event()
        self._last_signature = None
        self._reported_no_data = False
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    @staticmethod
    def _signature(data):
        """Identify the newest bar cheaply, without assuming a container type."""
        try:
            for value in (data or {}).values():
                if value is None:
                    continue
                if hasattr(value, "empty"):          # pandas DataFrame
                    if getattr(value, "empty", True):
                        continue
                    return str(value.index[-1]), int(len(value))
                if isinstance(value, (list, tuple)) and value:
                    return repr(value[-1]), len(value)
                if isinstance(value, dict) and value:
                    key = sorted(value.keys())[-1]
                    return repr(key), len(value)
        except Exception:
            return None
        return None

    def _loop(self):
        while not self._stop.is_set():
            try:
                data = self._fetch()
                signature = self._signature(data)
                if signature is None:
                    # No bars for this period. The subscription is live and will
                    # start firing if data appears, but until then the caller
                    # gets nothing -- which is indistinguishable from a broken
                    # subscription unless we say so. Reported once, not per poll.
                    if not self._reported_no_data:
                        self._reported_no_data = True
                        if self._on_no_data is not None:
                            self._on_no_data()
                elif signature != self._last_signature:
                    self._reported_no_data = False
                    self._last_signature = signature
                    if self._callback is not None:
                        self._callback(data)
            except Exception as exc:
                if self._on_error is not None:
                    try:
                        self._on_error(exc)
                    except Exception:
                        pass
            self._stop.wait(self._interval)


_VERSION_WARNED = {"shown": False}


def warn_on_version_mismatch(ping_response):
    """Warn once when the QMT-side bridge is not the build this client is.

    Deploying into QMT is a file copy and QMT keeps modules across strategy
    re-runs, so a stale server behaves like a fix that "did not work". Say so on
    connect instead of leaving it to be discovered by debugging.

    Silent when the versions agree, when the server is too old to report one, or
    when it has already been said. Never raises: this is on the connect path.
    """
    if _VERSION_WARNED["shown"]:
        return None
    try:
        server = str((ping_response or {}).get("version") or "")
        if not server:
            return None      # server predates version reporting; nothing to compare
        from .version import __version__ as local

        if server == local:
            return None
        _VERSION_WARNED["shown"] = True
        log.warning(
            "version mismatch: this client is %s, the QMT-side bridge is %s. "
            "A copy alone does not take effect -- QMT keeps modules across "
            "strategy re-runs, so the strategy must be restarted too. Set "
            "BIGQMT_AUTO_SYNC=1 (or call xt_trader.sync_deployment()) to push "
            "this client's package into the QMT python directory.",
            local, server)
        return (local, server)
    except Exception:
        return None


def auto_sync_enabled():
    """Writing into a live trading terminal is opt-in, not a side effect of
    connecting."""
    return _bool_value(os.environ.get("BIGQMT_AUTO_SYNC"), False)


class BigQmtXtData:
    def __init__(self, client):
        self.client = client
        self._subscribe_seq = int(time.time() * 1000)
        self._cache_obj = None
        self._quote_session = None          # lazily built WholeQuoteClientSession
        self._quote_session_factory = None  # test hook: returns a session-like object
        # [quote_events] seq -> callback for single-stock subscribe_quote real-time
        # push dispatch (whole_quote 用 upstream 的 _quote_session; subscribe_quote 走
        # 这套 — upstream 只修了 whole_quote 没修 subscribe_quote, backend 用后者).
        self._quote_callbacks = {}
        # [BUG-P2-20260811-bridge-unsubscribe-quote-001] stock_code -> seq 反向索引.
        # subscribe_quote 返回 seq 但 gateway_provider 不接 (调 subscribe_quote(code, ...,
        # callback) 丢弃返回值), 后续 unsubscribe_quote(code, period=) 需要反查 seq.
        self._code_to_seq = {}
        # [BUG-20260903-sweep-keyspace] 本进程自写订阅 seq 账本 (str 口径) — save
        # 成功即记, callback 有无都记。清扫豁免「本进程故意无 callback 的设计订阅」
        # (常驻指数 subscribe_quote 两参形态, app/main.py subscribe_resident_index_codes),
        # 只清跨进程残渣。seq 单调不回收, 集合只增 (订阅量级 ~1e2/日, 有界)。
        self._self_seqs = set()
        # [BUG-20260827-quote-heartbeat-frame] liveness-only 事件独立钩子 — heartbeat
        # 帧绝不进 _quote_callbacks/tick 链 (零伪造行情面), 只供观测层记录
        # "最近推送尝试"。单 handler 槽位: 观测层 (gateway_provider) 订阅时注册。
        self._quote_heartbeat_handler = None
        # [BUG-20260827-sub-registry-gc] 保活戳节流账本 (seq_str → last_written_ms):
        # dispatch 成功即代表消费端活 — 服务端 GC 据此判条目死活。
        self._keepalive_last_ms = {}
        # [BUG-20260827-dispatch-drop-counters] INV-4: 路由层历史三处静默 return/pass
        # (json 残缺 / 未知 event_type / seq miss 兜底不中 + 回调异常) — 全部给计数,
        # get_dispatch_drop_stats() 快照给观测面, 阈值告警由消费方裁量。
        # [BUG-20260903-03] ① 新增 control_echo: 后端自身 publish_event 控制命令回声
        # (subscribe/unsubscribe_quote 同通道) 单列, 不再污染 unknown_type (真未知
        # 才喊); ② 计数带 code 维度 (_dispatch_drop_codes) — 09-03 实弹 22.5k/日
        # seq_miss 全是重启残渣, 无 code 维度则真断供 (09-02 型) 被噪音淹没不可分辨。
        self._dispatch_drop_lock = threading.Lock()
        self._dispatch_drops = {
            "malformed_event": 0,
            "unknown_type": 0,
            "control_echo": 0,
            "seq_miss_no_fallback": 0,
            "callback_exception": 0,
            "empty_stock_code": 0,
        }
        self._dispatch_drop_codes: dict = {}  # kind → {code: count} (有界, 快照取 top5)
        self._quote_event_thread = None
        self._quote_event_running = False
        # Unified bar quality contract — cumulative violation counts + throttled
        # print timestamps keyed by (violation_type, code).
        self._quality_violation_counts = {}   # {type_str: int}
        self._quality_violation_last_print = {}  # {(type_str, code): float}
        self._bar_pollers = {}              # seq -> _BarPoller, for K-line periods
        self._bar_poller_lock = threading.Lock()

    def _next_seq(self):
        self._subscribe_seq += 1
        return self._subscribe_seq

    def _local_cache(self):
        cfg = dict(getattr(self.client, "local_cache_config", {}) or {})
        if not _bool_value(cfg.get("enabled"), True):
            return None
        if self._cache_obj is None:
            self._cache_obj = LocalMarketCache(cache_dir=cfg.get("dir"), fmt=cfg.get("format", "auto"))
        return self._cache_obj

    def _call(self, method, **params):
        # [trace 2026-08-21 事故复盘] 统一咽喉打 ENTER/EXIT, 把 bridge 所有 read 方法
        # 调用与 redis rpc timeout 对齐. 8-11/8-12 复盘发现仅 method 名的告警无法定位
        # caller context (cron / call site / params). 默认关闭 (BIGQMT_TRACE=0) —
        # 5788 只股票每次 bridge read 2 行 trace 会显著放大日志, 出问题时人工开.
        # 与既有 [bigqmt_compat] / [bigqmt_formula] / [bigqmt_client] 前缀只增不替.
        if os.environ.get("BIGQMT_TRACE") == "1":
            _t0 = time.time()
            _params_keys = ",".join(sorted(params.keys())) or "-"
            print(
                f"[bigqmt_trace] ENTER method={method} params=[{_params_keys}] t={_t0:.3f}",
                flush=True,
            )
            try:
                _r = self.client.call(method, params)
                print(
                    f"[bigqmt_trace] EXIT  method={method} ok=True  dt_ms={(time.time()-_t0)*1000:.1f}",
                    flush=True,
                )
                return _r
            except Exception as _e:
                print(
                    f"[bigqmt_trace] EXIT  method={method} ok=False err={type(_e).__name__}:{_e}  dt_ms={(time.time()-_t0)*1000:.1f}",
                    flush=True,
                )
                raise
        return self.client.call(method, params)

    def get_full_tick(self, code_list, timeout_seconds=None, types=None):
        """Fetch full tick data for a list of codes.

        Args:
            code_list: stock codes to query.
            timeout_seconds: per-request RPC timeout. None = auto (30s for whole-market
                snapshots, else the client default, DEFAULT_RPC_TIMEOUT_SECONDS).
                Callers can pass a larger value when querying many codes (e.g. 1256
                ETF options may need 150-180s).
        """
        codes = list(code_list or [])
        if not codes:
            return {}
        cache_config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        if _bool_value(cache_config.get("enabled"), False):
            redis_client = self.client._redis()
            request_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                demand_ttl_seconds=cache_config.get("demand_ttl_seconds", 10),
                cache_ttl_seconds=cache_config.get("cache_ttl_seconds", 10),
            )
            data = wait_full_tick_cache(
                redis_client,
                self.client.account_id,
                codes,
                max_age_seconds=cache_config.get("cache_ttl_seconds", 10),
                wait_seconds=cache_config.get("wait_seconds", 3.5),
                poll_interval_seconds=cache_config.get("poll_interval_seconds", 0.2),
            )
            if data is not None:
                return data
            upper_codes = {str(code).strip().upper() for code in codes}
            if upper_codes & {"SH", "SZ", "BJ", "HK"}:
                # Whole-market snapshots must stay on the demand cache. A live RPC
                # here would ship ~50k rows on every miss, so surface the timeout.
                raise TimeoutError("full tick redis cache timeout: %s" % ",".join(str(code) for code in codes))
            # Symbol-list miss (cold start / expired snapshot): fall back to a live
            # RPC so the first call is ~ms instead of a hard wait_seconds stall.
            rpc_timeout = timeout_seconds if timeout_seconds is not None else None
            return self.client.call("get_full_tick", _full_tick_params(codes, types), timeout_seconds=rpc_timeout) or {}
        upper_codes = {str(code).strip().upper() for code in codes}
        # Caller-provided timeout takes priority; otherwise auto-detect whole-market.
        if timeout_seconds is not None:
            rpc_timeout = timeout_seconds
        else:
            rpc_timeout = 30 if upper_codes & {"SH", "SZ", "BJ", "HK"} else None
        failure = None
        try:
            data = self.client.call(
                "get_full_tick", _full_tick_params(codes, types),
                timeout_seconds=rpc_timeout) or {}
        except Exception as exc:
            data = None
            failure = exc
            if not self._can_fall_back_to_markets(codes, upper_codes):
                raise
        if self._should_fall_back(codes, upper_codes, data):
            fallback_errors = []
            recovered = self._full_tick_via_markets(
                codes, rpc_timeout, types, errors=fallback_errors)
            if recovered is not None:
                return recovered
            if failure is not None:
                # A bare `raise` here has no active exception -- the except
                # block above has already exited -- so it produced
                # "RuntimeError: No active exception to reraise" and buried
                # the real timeout (reported on issue #104). Re-raise the
                # actual failure, and say why the recovery did not help.
                if fallback_errors:
                    log.warning(
                        "get_full_tick: %d codes failed directly (%s) and the "
                        "market re-read failed too (%s: %s); raising the "
                        "original failure.",
                        len(codes), failure,
                        fallback_errors[-1].__class__.__name__,
                        fallback_errors[-1])
                else:
                    log.warning(
                        "get_full_tick: %d codes failed directly (%s) and "
                        "could not be recovered from a market read.",
                        len(codes), failure)
                raise failure
        return data or {}

    def _can_fall_back_to_markets(self, codes, upper_codes):
        """Only an explicit list of suffixed codes can be recovered this way."""
        if upper_codes & MARKET_TOKENS:
            return False          # already a whole-market request
        if len(codes) <= LARGE_CODE_LIST:
            return False          # small list: a failure here is a real failure
        return bool(_markets_of(codes))

    def _should_fall_back(self, codes, upper_codes, data):
        if data is None:
            return True           # the request raised
        if not self._can_fall_back_to_markets(codes, upper_codes):
            return False
        # Short answer: the server dropped codes, or truncated. Anything missing
        # is worth one whole-market read rather than silently returning less
        # than was asked for (issue #104).
        return len(data) < len(set(str(c) for c in codes))

    def _full_tick_via_markets(self, codes, rpc_timeout, types=None, errors=None):
        """Read the exchange(s) these codes live on, then filter to them.

        A long explicit list is one RPC carrying one timeout, so it either fits
        or loses everything; a market token is a single cheap argument that
        cannot truncate.

        Narrowed first, "all" only if that came up short. An exchange listing is
        mostly bonds -- "SH" is 26744 instruments of which 2315 are stocks -- so
        reading all of it costs 7.4s against 1.08s for the stocks. No reason to
        pay that when the codes being recovered are stocks (issue #104).
        """
        markets = _markets_of(codes)
        if not markets:
            return None
        wanted = set(str(code) for code in codes)

        attempts = [list(types) if types else list(DEFAULT_FALLBACK_TYPES)]
        if not any(str(k).lower() == "all" for k in attempts[0]):
            attempts.append(["all"])

        merged = {}
        for attempt in attempts:
            merged = {}
            try:
                for market in sorted(markets):
                    snapshot = self.client.call(
                        "get_full_tick", _full_tick_params([market], attempt),
                        timeout_seconds=max(rpc_timeout or 0, 60)) or {}
                    for key, value in snapshot.items():
                        if str(key) in wanted:
                            merged[key] = value
            except Exception as exc:
                # Swallowing the reason here left the caller with nothing to
                # report; hand it back so the raise can name it (issue #104).
                if errors is not None:
                    errors.append(exc)
                return None
            if len(merged) >= len(wanted):
                break          # everything asked for; no need to widen

        log.warning(
            "get_full_tick: %d codes did not come back directly; re-read %s as "
            "%s and filtered to %d. A market token with types= is cheaper than "
            "a list this long.",
            len(wanted), "/".join(sorted(markets)), "/".join(attempts[-1]
                                                             if len(merged) < len(wanted)
                                                             else attempts[0]),
            len(merged))
        return merged

    def get_deployment_info(self):
        """Where the QMT-side bridge is running from, and which build it is.

        Returns version / package_dir / qmt_python_dir / strategy_dir /
        python_version. Use it to check a deploy landed before hunting for a
        fix that was never actually there -- QMT keeps modules across strategy
        re-runs, so a forgotten copy and an un-reloaded one look the same.
        """
        return self.client.call("get_deployment_info", {}) or {}

    def get_instrument_detail(self, stock_code):
        return self.client.call("get_instrument_detail", {"code": stock_code}) or {}

    def get_instrumentdetail(self, stock_code):
        return self.get_instrument_detail(stock_code)

    def get_instrument_detail_list(self, stock_list):
        """批量 instrument_detail — gateway_provider.prefetch_instrument_details 期望 dict[code, detail].

        QMT 端 RPC server 无原生批量方法 (formula_server 路由表只有 get_instrument_detail /
        get_instrumentdetail 单票); 这里 backend 端并发调单票 get_instrument_detail 模拟批量.
        并发度受 _RPC_INFLIGHT Semaphore (BIGQMT_RPC_CONCURRENCY, 默认 3) 限流, 与串行 N 次相比
        加速 ~3x. 单票失败不阻塞其它 (try/except per call, 失败 code 不入 result).

        修复 BUG-P1-20260811-bridge-instrument-detail-list-001: 切桥接后桥接 BigQmtXtData
        无此方法 → gateway_provider.py:1249 hasattr 守卫返 False → 永远走 fallback 逐票 RPC
        (批量分支 line 1252-1261 是死代码, fixture spec xtdata_stub.py:75 有此方法名但生产
        从未激活). 本实现让 hasattr 返 True → 批量分支激活, 测试与生产对称.

        Returns:
            dict[code, detail], 缺失/失败 code 不含在 result 中.
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        if not codes:
            return {}

        def _fetch_one(code):
            try:
                return code, self.get_instrument_detail(code)
            except Exception:
                return code, None

        result = {}
        # 并发度富裕 (max_workers=8) → Semaphore (_RPC_INFLIGHT) 自动限流到 BIGQMT_RPC_CONCURRENCY.
        # 不读 env 是有意: Semaphore 是限流真理源, max_workers 富余不破坏限流不变量.
        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(8, len(codes))) as pool:
                for code, detail in pool.map(_fetch_one, codes):
                    if detail:
                        result[code] = detail
        except Exception:
            # 兜底: 并发框架异常 (OOM / 线程池满) → 串行降级, 永不崩.
            for code in codes:
                try:
                    detail = self.get_instrument_detail(code)
                    if detail:
                        result[code] = detail
                except Exception:
                    continue
        return result

    def get_instrument_type(self, stock_code, variety_list=None):
        return self._call("get_instrument_type", code=stock_code, variety_list=variety_list)

    def get_stock_type(self, stock_code, variety_list=None):
        """xtdata.get_stock_type 的同名封装 —— 大 QMT 上答不了，直接报错。

        服务端走的是 ContextInfo.get_stock_type(stock)。这个 stub 在大 QMT 上
        存在（缺失会抛 NotImplementedError），但**对任何代码都返回 0**：实测
        股票 600000.SH、ETF 589820.SH、沪市债券 186511.SH、期权
        10011096.SHO 全部是 0，换代码格式（600000 / SH600000 /
        600000.SSE）也一样。

        返回一个恒为 0 的"类型"比报 AttributeError 更糟：报错看得见，一个
        错的分类看不见。所以这里显式拒绝，并指向真正能用的那个：
        get_instrument_type()，实测能区分 stock / fund / etf / bond / index。
        """
        raise NotImplementedError(
            "get_stock_type is not usable on Big QMT: the server-side "
            "ContextInfo.get_stock_type stub returns 0 for every code "
            "(verified live against a stock, an ETF, a bond and an option, and "
            "against every code format). Use get_instrument_type(stock_code) "
            "instead -- it returns "
            "{'stock': ..., 'fund': ..., 'etf': ..., 'bond': ..., 'index': ...}."
        )

    def subscribe_l2thousand(self, stock_code, gear_num=None, callback=None):
        """千档盘口订阅。

        callback 在 RPC 模型下没有回调通道，服务端会忽略它 —— 想要推送请用
        subscribe_whole_quote。这里保留形参只为和 xtdata 签名一致。
        """
        return self._call(
            "subscribe_l2thousand",
            stock_code=stock_code,
            gear_num=0 if gear_num is None else gear_num,
        )

    def get_stock_list_in_sector(self, sector_name, real_timetag=-1):
        name = str(sector_name or "")
        try:
            return self._call("get_stock_list_in_sector", sector_name=sector_name, real_timetag=real_timetag) or []
        except Exception as exc:
            # RPC failed — try fallback for well-known sectors, or re-raise.
            print(
                "[bigqmt_compat] get_stock_list_in_sector(%s) RPC failed: %s: %s"
                % (sector_name, exc.__class__.__name__, exc)
            )
        if name in ("沪深A股", "沪深A股".encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")):
            ticks = self.get_full_tick(["SH", "SZ"])
            return sorted(code for code in ticks.keys() if _is_hs_a_share(code))
        raise NotImplementedError("sector is not supported by BigQMT compat: %s" % sector_name)

    def get_market_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
    ):
        params = dict(
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
        data = self._call("get_market_data", **params)
        # Self-heal adjusted reads (all-zero bars -> server raw download + retry).
        data = self._heal_adjusted("get_market_data", params, data)
        # Unified quality contract — record violations for monitoring.
        self._check_bar_quality(data, context="get_market_data")
        return data

    def _get_market_data_ex_batch(self, params, timeout_seconds=None, use_formula=True):
        """One RPC's worth of bars, healed and normalized. No caching."""
        if use_formula:
            data = self.client.call("get_market_data_ex", params, timeout_seconds=timeout_seconds)
        else:
            # 只在绕路时显式传参——保持既有调用方/桩签名不变。
            data = self.client.call("get_market_data_ex", params, timeout_seconds=timeout_seconds,
                                    use_formula=False)
        # Self-heal adjusted reads (all-zero bars -> server raw download + retry).
        data = self._heal_adjusted("get_market_data_ex", params, data, timeout_seconds=timeout_seconds)
        # Normalize Big QMT's stime-indexed frame to MiniQMT shape (time-indexed).
        if isinstance(data, dict):
            data = _normalize_market_data_result(data, field_list=params.get("field_list"))
        return data

    def get_market_data_ex(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        chunk_size=None,
        timeout_seconds=None,
        use_formula=True,
    ):
        """Pull bars over RPC, in batches of ``chunk_size`` codes.

        Cache-through: whatever is fetched is written to the local cache (keyed
        by dividend_type), so it stays the latest -- important for 前复权 data,
        whose history re-scales on each dividend.

        Batching exists because one request carrying every code shares a single
        RPC timeout (6s by default), so a wide stock_list times out and loses
        the whole pull rather than degrading (issue #47). Splitting keeps each
        request small enough to answer, and a batch that still fails only costs
        its own codes -- the rest are returned.

        ``chunk_size=0`` restores the old single-request behaviour.

        An empty ``field_list`` means "every field", which only the RPC path can
        answer: FormulaServer has the six bar columns plus time, and not the
        four daily ones (settelementPrice, openInterest, preClose,
        suspendFlag). Naming the fields you actually want is what unlocks the
        direct path -- 0.015s against 5.8s, measured (issue #104).

        Asking it for a field it lacks is now refused at the router and served
        by RPC instead. It used to answer with a column of NaN, so naming all
        eleven columns looked like a free speedup and quietly cost four of them
        (see formula_server.SERVED_FIELDS).
        """
        _notice_field_list_cost(field_list)
        codes = list(stock_list or [])
        base = dict(
            field_list=list(field_list or []),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            fill_data=fill_data,
        )
        step = DEFAULT_MARKET_DATA_CHUNK if chunk_size is None else int(chunk_size)

        if step <= 0 or len(codes) <= step:
            data = self._get_market_data_ex_batch(
                dict(base, stock_list=codes), timeout_seconds=timeout_seconds,
                use_formula=use_formula,
            )
        else:
            data = {}
            failures = []
            for index in range(0, len(codes), step):
                batch = codes[index:index + step]
                try:
                    part = self._get_market_data_ex_batch(
                        dict(base, stock_list=batch), timeout_seconds=timeout_seconds,
                        use_formula=use_formula,
                    )
                except Exception as exc:
                    # Losing one batch must not lose the others: a partial
                    # result beats an exception when 500 codes were asked for.
                    failures.append((batch, exc))
                    continue
                if isinstance(part, dict):
                    data.update(part)
            if failures and not data:
                # Nothing came back at all -- surface the first cause rather
                # than returning a silent empty dict.
                raise failures[0][1]
            for batch, exc in failures:
                print("[bigqmt_client] get_market_data_ex batch failed (%d codes, first=%s): %s"
                      % (len(batch), batch[0] if batch else "", exc))

        # Unified quality contract — record violations for monitoring.
        # Runs AFTER self-heal so we see the final quality, not intermediate.
        self._check_bar_quality(data, context="get_market_data_ex")

        cache = self._local_cache()
        if cache is not None and isinstance(data, dict):
            for code, df in data.items():
                try:
                    cache.write(code, period, df, dividend_type=dividend_type)
                except Exception as exc:
                    print(
                        "[bigqmt_compat] cache write failed code=%s period=%s: %s"
                        % (code, period, exc)
                    )
        return data

    def get_local_data(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=True,
        data_dir=None,
    ):
        """Read bars from the CLIENT-side local cache — no RPC to Big QMT.

        Returns a dict {code: DataFrame}. A cache-missed code is omitted, unless
        local_cache_fallback_rpc is enabled (then it is fetched + cached over RPC).
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        cache = self._local_cache()
        if cache is None:
            # Cache disabled -> behave like a plain RPC local-data read.
            return self._call(
                "get_local_data",
                field_list=_as_list(field_list),
                stock_list=codes,
                period=period,
                start_time=start_time,
                end_time=end_time,
                count=count,
                dividend_type=dividend_type,
                fill_data=fill_data,
                data_dir=data_dir,
            )
        fields = list(field_list or [])
        result = {}
        missing = []
        for code in codes:
            df = cache.read(code, period, start_time, end_time, count, dividend_type=dividend_type)
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                result[code] = self._select_fields(
                    _normalize_market_data_frame(df, field_list=fields),
                    fields,
                )
            else:
                missing.append(code)
        if missing and _bool_value(self.client.local_cache_config.get("fallback_rpc"), False):
            fetched = self._pull_and_cache(missing, period, start_time, end_time, count, dividend_type)
            for code in missing:
                df = fetched.get(code)
                if df is not None and getattr(df, "shape", (0,))[0] > 0:
                    result[code] = self._select_fields(
                        _normalize_market_data_frame(df, field_list=fields),
                        fields,
                    )
        return result

    @staticmethod
    def _select_fields(df, fields):
        if not fields:
            return df
        try:
            keep = [c for c in df.columns if c in fields or (c in _TIME_COL_NAMES and c != "stime")]
            return df[keep] if keep else df
        except Exception:
            return df

    @staticmethod
    def _is_all_zero_any(data):
        """Detect the all-zero adjusted-bars symptom (server lacks raw data).

        Big QMT computes front/back-adjusted bars from raw bars + dividend
        factors; when those are missing server-side the price columns come
        back all 0.0 (only the last bar may hold the live price). Recursively
        handles DataFrame, {code: DataFrame} and {field: {code: [..]}} shapes.
        """
        try:
            if data is None:
                return False
            cols = getattr(data, "columns", None)
            if cols is not None:  # pandas DataFrame
                if "close" not in list(cols):
                    return False
                closes = data["close"]
                if len(closes) == 0:
                    return False
                head = closes.iloc[:-1] if len(closes) > 1 else closes
                return bool((head == 0).all())
            if isinstance(data, dict):
                return any(BigQmtXtData._is_all_zero_any(v) for v in data.values())
            if isinstance(data, (list, tuple)) and data and all(
                isinstance(x, (int, float)) for x in data
            ):
                head = data[:-1] if len(data) > 1 else data
                return bool(head) and all(x == 0 for x in head)
            return False
        except Exception:
            return False

    # ── Unified bar quality contract ──────────────────────────────────────────
    # Single choke-point for "is this data trustworthy?" — every bar read goes
    # through _check_bar_quality before returning to the caller. Violations are
    # counted (quality_stats()) and printed (throttled) so corrupt data is
    # never silently consumed.

    # Quality violation types:
    #   "all_zero_close"  — close column all 0.0 except possibly the last bar
    #   "nan_heavy"       — >50% of close values are NaN
    #   "negative_price"  — any close < 0
    #   "empty"           — DataFrame is empty or code missing from dict
    #   "malformed_shape" — dict payload whose values are not bar frames/lists
    #                       (e.g. a field-major dict or a leaked object dump);
    #                       the per-code contract does not apply, so it is
    #                       flagged instead of silently skipped

    @staticmethod
    def _looks_like_bar_frame(value):
        """True when value can plausibly hold bars (DataFrame / list of rows).

        Dict values are ambiguous: {code: DataFrame} (code-major) is the
        documented bar shape, but {field: {code: [...]}} (field-major) and
        garbage dumps also arrive as dicts. Callers treat a dict-of-non-bars
        as malformed rather than iterating it as if keyed by codes.
        """
        return value is None or hasattr(value, "columns") or isinstance(value, (list, tuple))

    @staticmethod
    def _check_bar_quality_frame(df):
        """Check one DataFrame for quality violations.

        Returns a list of violation type strings (empty = healthy).
        """
        violations = []
        try:
            cols = getattr(df, "columns", None)
            if cols is None:
                return violations
            if "close" not in list(cols):
                return violations
            closes = df["close"]
            n = len(closes)
            if n == 0:
                violations.append("empty")
                return violations
            # NaN ratio
            nan_count = int(closes.isna().sum()) if hasattr(closes, "isna") else 0
            if nan_count > n * 0.5:
                violations.append("nan_heavy")
            # Negative prices
            try:
                if bool((closes < 0).any()):
                    violations.append("negative_price")
            except Exception:
                pass
            # All-zero (head only — last bar may hold live price)
            head = closes.iloc[:-1] if n > 1 else closes
            if bool((head == 0).all()):
                violations.append("all_zero_close")
        except Exception:
            pass
        return violations

    def _check_bar_quality(self, data, context=""):
        """Run quality checks on bar data and record violations.

        Works on DataFrame, {code: DataFrame}, or None. Returns True if any
        violation was found (caller may use this to decide whether to warn).
        """
        if data is None:
            return False
        found = False
        if isinstance(data, dict):
            for key, value in data.items():
                if not self._looks_like_bar_frame(value):
                    # Not a bar frame — flag the payload shape itself.
                    self._record_quality_violation("malformed_shape", str(key)[:32], context)
                    found = True
                    continue
                vs = self._check_bar_quality_frame(value)
                if vs:
                    found = True
                    for v in vs:
                        self._record_quality_violation(v, key, context)
        else:
            vs = self._check_bar_quality_frame(data)
            if vs:
                found = True
                for v in vs:
                    self._record_quality_violation(v, "*", context)
        return found

    def _record_quality_violation(self, violation_type, code, context=""):
        """Increment counter + throttled print for a quality violation."""
        self._quality_violation_counts[violation_type] = (
            self._quality_violation_counts.get(violation_type, 0) + 1
        )
        key = (violation_type, code)
        now = time.time()
        last = self._quality_violation_last_print.get(key, 0.0)
        if now - last >= 60.0:
            self._quality_violation_last_print[key] = now
            total = self._quality_violation_counts[violation_type]
            print(
                "[bigqmt_quality] %s code=%s total=%d%s"
                % (violation_type, code, total, (" ctx=%s" % context) if context else "")
            )

    def quality_stats(self):
        """Cumulative quality violation counts — queryable for monitoring."""
        return dict(self._quality_violation_counts)

    def _ensure_server_raw(self, codes, period, start_time, end_time):
        """Trigger a server-side raw download so adjusted bars can be computed.

        Best-effort: failures are non-fatal (the retry may still work if raw
        data already exists server-side), but silently swallowing exceptions
        makes debugging impossible — log a one-line diagnostic on failure.
        """
        try:
            self.client.call(
                "download_history_data2",
                {
                    "stock_list": list(codes),
                    "period": period,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                timeout_seconds=60.0,
            )
        except Exception as exc:
            # [P7 root-cause fix] silent swallow → visible diagnostic. The bridge
            # uses print() for all diagnostics (no logging framework). One line,
            # no traceback — enough to grep, not enough to flood.
            print(
                "[bigqmt_compat] _ensure_server_raw failed (%d codes, period=%s): "
                "%s: %s"
                % (len(codes), period, exc.__class__.__name__, exc)
            )

    @staticmethod
    def _zero_codes_from_data(data):
        """Extract the subset of codes whose adjusted bars are all-zero.

        Returns a list of codes that need re-downloading. For non-dict shapes
        (single DataFrame), returns a sentinel ['*'] meaning 'all codes'.
        """
        if isinstance(data, dict):
            result = []
            for code, frame in data.items():
                # Skip non-bar values (field-major dicts / garbage dumps):
                # a field name must never be sent to download_history_data2
                # as if it were a stock code.
                if not BigQmtXtData._looks_like_bar_frame(frame):
                    return ["*"]
                if BigQmtXtData._is_all_zero_any(frame):
                    result.append(code)
            return result
        if BigQmtXtData._is_all_zero_any(data):
            return ["*"]
        return []

    @staticmethod
    def _served_codes(data):
        """Codes the server actually served, across both return shapes:
        get_market_data_ex is code-keyed ({code: DataFrame}), get_market_data
        is field-keyed ({field: {code: [..]}}) -- reading keys off the wrong
        level would make every code look missing."""
        if not isinstance(data, dict):
            return set()
        nested = {code for value in data.values() if isinstance(value, dict) for code in value}
        return nested if nested else set(data.keys())

    def _heal_adjusted(
        self, method, params, data,
        max_wait_seconds=10.0, poll_interval=0.5, timeout_seconds=None,
    ):
        """Self-heal adjusted reads: if the adjusted pull came back all-zero,
        trigger a server-side raw download, then poll until the data lands or
        the timeout expires.

        Root-cause fix for P5: the old ``time.sleep(2.0)`` + single retry failed
        for large downloads where the server-side async landing took >2s. Now
        we poll with short intervals up to ``max_wait_seconds``, returning the
        first non-zero result. Only the all-zero codes are re-downloaded (P6),
        not the entire batch.

        [merge 2026-09-03] 上游 0.3.x 同名函数为 retry-once 语义并带
        timeout_seconds 透传。裁决: 轮询骨架取本地 (P5/P6, 生产实证), 上游的
        timeout_seconds 透传并入 (其 get_market_data_ex 调用方传该 kwarg);
        上游的 none-adjusted majority-missing 自愈**已并入** (仅直读路径生效,
        少数缺失不自愈 = 上游 #104 教训), 下载批循环内以 _download_batch_depth
        护栏抑制 — 本地下载层 (download_history_data2 data_wait_seconds) 自带
        分批轮询, get 内再触发服务端下载会双重轮询且把读路径放大成 10s 级
        (本地 test_download_gives_up_after_wait_timeout 锁住该语义)。
        """
        dividend_type = str(params.get("dividend_type") or "none").lower()
        if dividend_type in ("", "none"):
            # [merge 2026-09-03] 上游 none-read majority-missing 自愈并入, 但仅
            # 直读路径生效: download_history_data2 的分批轮询自持节奏
            # (data_wait_seconds), get 内再触发服务端下载会双重轮询并把读路径
            # 放大成 10s 级 — 下载批循环内以 _download_batch_depth 抑制。
            # 少数缺失(退市/停牌/无权限)不自愈 = 上游 #104 教训 (每次读都付费)。
            if getattr(self, "_download_batch_depth", 0) > 0:
                return data
            all_codes = list(params.get("stock_list") or params.get("stock_code") or [])
            if not all_codes:
                return data
            served = self._served_codes(data)
            missing = sum(1 for code in all_codes if code not in served)
            if missing < max(1, len(all_codes) // 2):
                return data
            target_codes = [code for code in all_codes if code not in served]
            self._ensure_server_raw(
                target_codes,
                params.get("period", "1d"),
                params.get("start_time", ""),
                params.get("end_time", ""),
            )

            def _healed(candidate):
                served = self._served_codes(candidate)
                missing = sum(1 for code in all_codes if code not in served)
                return missing < max(1, len(all_codes) // 2)

        else:
            if not self._is_all_zero_any(data):
                return data
            all_codes = list(params.get("stock_list") or params.get("stock_code") or [])
            if not all_codes:
                return data
            # [P6 root-cause fix] Only re-download the codes that are actually
            # all-zero, not the entire batch. For non-dict shapes, re-download all.
            zero_codes = self._zero_codes_from_data(data)
            target_codes = all_codes if "*" in zero_codes else zero_codes
            self._ensure_server_raw(
                target_codes,
                params.get("period", "1d"),
                params.get("start_time", ""),
                params.get("end_time", ""),
            )

            def _healed(candidate):
                return not self._is_all_zero_any(candidate)

        # Poll until the data lands or timeout. Each poll re-reads from the
        # server; the first non-zero result wins. The old single-shot retry
        # with a hard-coded sleep was a race against the server's async
        # landing — this poll loop removes the race entirely.
        deadline = time.time() + max_wait_seconds
        retry_params = dict(params)
        while time.time() < deadline:
            time.sleep(poll_interval)
            if timeout_seconds is not None:
                retry_data = self.client.call(method, retry_params, timeout_seconds=timeout_seconds)
            else:
                retry_data = self._call(method, **retry_params)
            if _healed(retry_data):
                return retry_data
            data = retry_data  # keep the latest for the final return
        # Timeout: return the last result (still all-zero). The consumer
        # (main repo scheduler_phases) detects zero and logs critical.
        return data

    def _pull_and_cache(self, codes, period, start_time, end_time, count, dividend_type="none"):
        """Fetch codes over RPC (get_market_data_ex already caches them)."""
        data = self.get_market_data_ex(
            field_list=DEFAULT_DOWNLOAD_FIELDS,
            stock_list=list(codes),
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
        )
        out = {}
        for code in codes:
            df = data.get(code) if isinstance(data, dict) else None
            if df is not None and getattr(df, "shape", (0,))[0] > 0:
                out[code] = df
        return out

    def subscribe_quote(self, stock_code, period="1d", start_time="", end_time="", count=0, callback=None):
        """Subscribe to one instrument, MiniQMT-style: the callback keeps firing.

        [merge 2026-09-03 裁决] 采用本地 quote_events 生产通路: 订阅写 Redis hash
        (save_quote_subscription fail-LOUD), QMT 端 adjust pump 读 hash → get_full_tick
        → publish, 本端 _quote_listener 按 seq 路由回 callback。上游 0.3.x 的 K-line
        _BarPoller / whole-quote session 订阅路径未采用 — B 机端到端依赖 hash 泵 +
        seq 路由 + 丢弃计数/心跳观测面 (upstream issue #95 的「callback 只发一次」
        缺陷本通路 08-11 起已由实时推送修复)。
        """
        seq = self._next_seq()
        payload = {
            "seq": seq,
            "stock_code": stock_code,
            "period": period,
            "start_time": start_time,
            "end_time": end_time,
            "count": count,
        }
        _saved = self.client.save_quote_subscription(seq, payload, active=True)
        # [BUG-20260827-quote-sub-registry-silent-loss] save 失败必须 fail-LOUD:
        # 000983.SZ 实证 — save 静默 False + 后续无任何重试/对账 → 该码从注册表消失
        # → 泵按哈希遍历永不推 → 全天断流且无人可裁决成因。现有唯一生产调用方
        # (gateway_provider._subscribe_impl / admin_subscribe / etf_tick_router)
        # 全部 per-code try/except 包裹: raise 后走 failed 列表 → 黑名单 TTL →
        # TTL 过期差集补订, 由既有收敛回路自愈 — 而非"假成功后永久饿死"。
        # 硬约束: raise 必须先于 _code_to_seq/_quote_callbacks/publish_event 快照 —
        # 无哈希条目的 seq 不允许在客户端半状态中留孤儿。
        if _saved is not True:
            raise RuntimeError(
                "save_quote_subscription failed seq=%s code=%s "
                "(registry entry lost, caller must blacklist-and-retry)" % (seq, stock_code)
            )
        # [BUG-P2-20260811-bridge-unsubscribe-quote-001] 反向索引 stock_code -> seq,
        # 无条件维护 (admin_subscribe 不传 callback 也写 Redis hash, admin_unsubscribe
        # 需反查 seq 清 hash). 放 callback 块外覆盖所有 subscribe 调用路径.
        self._code_to_seq[stock_code] = seq
        # [BUG-20260903-sweep-keyspace] save 已确认成功 (fail-LOUD 先于此), hash 条目
        # 必在场 — 记入自写账本供清扫豁免 (callback 形态不拘)。
        self._self_seqs.add(str(seq))
        # [quote_events] 清同 code 旧 seq — 防订阅 hash 无限堆积 (每次重订阅新建 seq
        # 不清旧, 实测 498 条/15 code). pump 全量取会放大 Redis 写. hlen>50 才清摊销成本.
        # [2026-08-12 审查] 阈值 50 → 10: 88 条堆积实况 (600309×23/601899×22/603993×22
        # 等 08-10/11 残留) 导致 QMT 端 quote_push 全量推 88 票 → backend cb_found=False
        # 路由丢弃 + 刷屏. 每次 subscribe 清同 code 旧 seq 摊销成本 < 10 条 hgetall, 阈值
        # 降到 10 让清理更早触发.
        # [BUG-20260813-quote-callback-seq-mismatch] 仅在 _saved is True 时清旧 seq:
        # hset 失败时新 seq 没写进 hash, 清旧 seq 会导致该 code 在 hash 中完全消失
        # → QMT 不推 → callback 永远不触发 (持仓股卖出规则失明).
        # [BUG-20260903-multiproc-sweep/BMG4-04] 同码清理是跨进程破坏性 hdel (会删
        # 他人进程活跃 seq, 成功路径无日志) — 挂属主门: 属主进程行为不变, 非属主
        # 进程 (测试/preflight) 不再清理, 其残渣由属主清扫按跨进程残渣清除。
        if _saved is True and self._is_subscription_owner():
            try:
                _redis = self.client._redis()
                _sub_key = "bigqmt:quote_subscriptions:%s" % (self.client.account_id or "")
                if _redis.hlen(_sub_key) > 10:
                    _stale = []
                    for _k, _v in _redis.hgetall(_sub_key).items():
                        try:
                            _p = json.loads(_v.decode() if isinstance(_v, bytes) else _v)
                            if _p.get("stock_code") == stock_code and str(_p.get("seq")) != str(seq):
                                _stale.append(_k)
                        except Exception:
                            pass
                    if _stale:
                        _redis.hdel(_sub_key, *_stale)
            except Exception as exc:
                # Stale subscription cleanup failed — non-fatal, but log for visibility.
                print(
                    "[bigqmt_compat] subscribe_quote stale cleanup failed: %s: %s"
                    % (exc.__class__.__name__, exc)
                )
        self.client.publish_event("subscribe_quote", payload)
        if callback is not None:
            # [quote_events] register callback for real-time push dispatch. upstream
            # 的 _quote_session 只管 whole_quote; subscribe_quote 走这套. QMT 端 adjust
            # pump 读 hash → get_full_tick → publish → 这里按 seq 路由回 callback.
            self._quote_callbacks[seq] = callback
            self._start_quote_listener()
            try:
                if str(period).lower() in ("tick", "full_tick"):
                    callback(self.get_full_tick([stock_code]))
                else:
                    callback(
                        self.get_market_data_ex(
                            stock_list=[stock_code],
                            period=period,
                            start_time=start_time,
                            end_time=end_time,
                            count=count,
                        )
                    )
            except Exception as exc:
                # Initial snapshot callback failed — subscription is registered
                # but the caller never received the first data point. This is
                # observable: downstream code expecting an immediate snapshot
                # will see nothing until the next push.
                print(
                    "[bigqmt_compat] subscribe_quote(%s) initial snapshot callback "
                    "failed (subscription still active, push will deliver next): %s: %s"
                    % (stock_code, exc.__class__.__name__, exc)
                )
        return seq

    def _bar_poll_interval_seconds(self):
        config = dict(getattr(self.client, "full_tick_cache_config", {}) or {})
        value = (config.get("bar_poll_interval_seconds")
                 or os.environ.get("BIGQMT_BAR_POLL_INTERVAL_SECONDS"))
        try:
            return float(value)
        except (TypeError, ValueError):
            return DEFAULT_BAR_POLL_INTERVAL_SECONDS

    def _record_subscription(self, seq, payload, active=True):
        """Bookkeeping only -- nothing on the server consumes it, and it needs a
        Redis client, which a zmq deployment does not have. Never let it break
        an otherwise working subscription."""
        try:
            self.client.save_quote_subscription(seq, dict(payload, seq=seq), active=active)
            self.client.publish_event(
                "subscribe_quote" if active else "unsubscribe_quote",
                dict(payload, seq=seq))
        except Exception:
            pass

    def subscribe_quote2(self, stock_code, period="1d", start_time="", end_time="", count=0, dividend_type=None, callback=None):
        return self.subscribe_quote(
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            callback=callback,
        )

    def _whole_quote_session(self):
        if self._quote_session is None:
            if self._quote_session_factory is not None:
                self._quote_session = self._quote_session_factory()
            else:
                self._quote_session = self._build_quote_session()
        return self._quote_session

    def _build_quote_session(self):
        from .whole_quote_session import WholeQuoteClientSession

        client = self.client

        def rpc_call(method, params):
            return client.call(method, params)

        return WholeQuoteClientSession(
            rpc_call=rpc_call,
            push_channel=self._build_quote_push_channel(),
            client_id=_quote_client_id(),
            heartbeat_interval_seconds=_env_float("BIGQMT_QUOTE_HEARTBEAT_SECONDS", 3.0),
            sub_id_func=self._next_seq,
        )

    def _build_quote_push_channel(self):
        """Build the push-channel subscriber matching the RPC transport: redis
        deployments derive the channel locally; zmq deployments connect to the
        server PUB socket (host from zmq config, RPC port + 1)."""
        client = self.client
        from .quote_push_channel import RedisQuotePushChannel, ZmqQuotePushChannel

        transport_name = str(getattr(client, "transport_name", "redis") or "redis").lower()
        if transport_name in ("zmq",):
            address = _quote_push_zmq_address(client)
            return ZmqQuotePushChannel(connect_address=address)
        return RedisQuotePushChannel(client._redis(), account_id=client.account_id)

    def subscribe_whole_quote(self, code_list, callback=None):
        session = self._whole_quote_session()
        session.start()
        sub_id = session.subscribe_whole_quote(code_list, callback=callback)
        # The big-QMT whole-quote callback is incremental (changed symbols only),
        # so prime the callback once with a full get_full_tick snapshot.
        #
        # types=["all"] deliberately: the push side is ContextInfo's own
        # subscribe_whole_quote, which is not narrowed, so a narrowed snapshot
        # would hand the subscriber 2315 stocks and then start pushing all
        # 26744 instruments. The primer has to cover what the push covers.
        if callback is not None:
            try:
                callback(self.get_full_tick(code_list, types=["all"]))
            except Exception:
                pass
        return sub_id

    def unsubscribe_quote(self, seq_or_code, period=None):
        """取消订阅. 兼容两种调用签名:

        1. ``unsubscribe_quote(seq)`` — 原生 xtdata 协议 (seq = subscribe_quote 返回值, int).
        2. ``unsubscribe_quote(code, period='1m')`` — gateway_provider 调用范式 (code-based,
           反查 ``_code_to_seq`` 找 seq). ``period`` kwarg 接受但忽略 (订阅时已存 period).

        修复 BUG-P2-20260811-bridge-unsubscribe-quote-001: 桥接原签名 ``unsubscribe_quote(seq)``
        不接 ``period`` kwarg → gateway_provider 调 ``unsubscribe_quote(code, period='1m')`` 抛
        ``TypeError: ... got an unexpected keyword argument 'period'`` 被 try/except 吞 →
        silent no-op (订阅 hash 持续累积, 靠 subscribe_quote line 833-846 hash>50 自动清兜底).
        miniQMT 原生 ``unsubscribe_quote(subscribe_id)`` 同样不接 period, 此 bug 在 miniQMT 时代
        也存在 (非切桥接回归). 本修复让桥接兼容 gateway_provider 调用范式, 同时保留 seq 调用.
        """
        # code → seq 反查 (gateway_provider 调用范式)
        if isinstance(seq_or_code, str):
            seq = self._code_to_seq.pop(seq_or_code, None)
            if seq is None:
                # code 未订阅 / 已退订 → 静默 no-op (与 miniQMT 原生一致, 不抛错)
                return 0
        else:
            seq = seq_or_code
            # seq-based 退订时, 同步清反向索引 (避免 _code_to_seq 留 stale 映射)
            for _code, _s in list(self._code_to_seq.items()):
                if _s == seq:
                    del self._code_to_seq[_code]
                    break
        # subscribe_whole_quote handles are owned by the push session; single-stock
        # subscribe_quote seqs still retire through the legacy redis-event path.
        session = self._quote_session
        if session is not None and session.has_subscription(seq):
            session.unsubscribe_quote(seq)
        else:
            payload = {"seq": seq}
            self.client.save_quote_subscription(seq, payload, active=False)
            self.client.publish_event("unsubscribe_quote", payload)
        # [quote_events] drop single-stock subscribe_quote dispatch mapping.
        self._quote_callbacks.pop(seq, None)
        return 0

    # [quote_events] single-stock subscribe_quote real-time push listener
    # (upstream 的 _quote_session 只管 whole_quote; backend 用 subscribe_quote 走这套).
    def _start_quote_listener(self):
        if self._quote_event_thread is not None and self._quote_event_thread.is_alive():
            return
        self._quote_event_running = True
        self._quote_event_thread = threading.Thread(
            target=self._quote_event_loop, name="bigqmt-quote-events", daemon=True
        )
        self._quote_event_thread.start()
        # [BUG-20260903-03] 首次起监听 (进程首个订阅) 后延时清扫跨进程残留订阅 —
        # hash 持久于 Redis, 重启后口径收缩时旧 seq 条目被 QMT pump 照推, 本进程
        # 无回调 → seq_miss 纯残渣噪音 (09-03 实弹 22.5k+/日)。
        # [BUG-20260903-multiproc-sweep] 武装改显式门控 (默认关): 测试/preflight 进程
        # 同样经真 .env 构造桥客户端, 无门控时其空 callbacks 会把后端活跃订阅当孤儿
        # hdel (09-03 实弹: 19:03 后端 4 条订阅被静默清空 → tick 断供)。唯一合法
        # 属主 = 后端主进程 (app/main.py setdefault 开)。
        self._arm_orphan_sweep()

    def _is_subscription_owner(self) -> bool:
        """[BUG-20260903-multiproc-sweep] 订阅属主判定 — 清扫武装与同码清理共用一门.

        只有「订阅唯一属主」进程 (后端主进程, app/main.py setdefault 开) 才允许
        做跨进程破坏性 hash 操作 (孤儿清扫 / hlen>10 同码清理): 非属主进程
        (测试/preflight/工具) 的 callbacks 为空或不含他人 seq, 一律不武装。
        """
        return _bool_value(os.environ.get("BIGQMT_ORPHAN_SWEEP"), False)

    def _arm_orphan_sweep(self) -> None:
        """[BUG-20260903-multiproc-sweep] 武装一次性孤儿清扫 — 显式 env 门控, 默认关."""
        if not self._is_subscription_owner():
            return
        try:
            _sweep_t = threading.Timer(
                120.0, self._sweep_orphan_subscriptions,
            )
            _sweep_t.daemon = True
            _sweep_t.start()
        except Exception:
            pass  # 清扫是降噪面, 失败无害 (残留仅致 seq_miss 计数噪音)

    def _sweep_orphan_subscriptions(self) -> None:
        """[BUG-20260903-03] 一次性清理 hash 中无本进程回调的残留订阅条目.

        判据 (键空间归一后): field(seq) 不属于 {str(本进程 _quote_callbacks 键)}
        与 {本进程 _self_seqs} 的并集 = 跨进程残渣 — QMT pump 对这些 seq 的推送
        只会落 seq_miss_no_fallback 丢弃; 本进程在订 (callback 或设计性无
        callback 的常驻指数) 一律豁免。清扫时点重验 (120s 延时窗内迟到的新订阅
        已注册回调并入自写账本, 天然豁免); 订阅侧 seq 单调不回收, hgetall→hdel
        窗口内新订阅只新增 field 不复用旧 seq, 无 TOCTOU 误删。属主门
        (BIGQMT_ORPHAN_SWEEP) 保证只有唯一属主进程武装本清扫。

        [BUG-20260903-multiproc-sweep] 自动武装已收进 _arm_orphan_sweep 的
        BIGQMT_ORPHAN_SWEEP 显式门控 (默认关) — 本方法仍可显式直调 (测试/运维),
        但只有后端主进程会自动触发, 防并存进程把他人活跃订阅当孤儿清除。
        """
        try:
            _redis = self.client._redis()
            _sub_key = "bigqmt:quote_subscriptions:%s" % (self.client.account_id or "")
            _fields = _redis.hgetall(_sub_key) or {}
            # [BUG-20260903-sweep-keyspace] 判据键空间必须归一: _quote_callbacks 键
            # 是内存 int seq (_next_seq), redis 回读 field 是 str — 直接 membership
            # 永真, 属主进程会把自己的活跃订阅判成孤儿 (2026-09-03 19:04:58 实弹:
            # 后端清扫删 64 条含自身在订 4 条 → tick 断供, WinSW out.log 铁证)。
            # 统一以 str 比较; 另豁免本进程自写 seq (_self_seqs) — 常驻指数订阅
            # 设计性无 callback, 不豁免则每次重启后 T+120s 被自删 → 大盘闸指数
            # 1m 断流 (BMG4-01)。
            _cb_seq_strs = {str(_s) for _s in self._quote_callbacks.keys()}
            _self_written = {str(_s) for _s in getattr(self, "_self_seqs", ()) or ()}
            _orphans = []
            for _k in _fields.keys():
                _seq = _k.decode() if isinstance(_k, (bytes, bytearray)) else str(_k)
                if _seq not in _cb_seq_strs and _seq not in _self_written:
                    _orphans.append(_seq)
            if _orphans:
                _redis.hdel(_sub_key, *_orphans)
                log.warning(
                    "[BUG-20260903-orphan-sweep] 清理上一进程残留订阅 %d 条 "
                    "(seq 无本进程回调, QMT pump 不再空推 → seq_miss 残渣噪音归零): "
                    "%s",
                    len(_orphans), _orphans[:20],
                )
        except Exception as exc:
            log.warning(
                "[BUG-20260903-orphan-sweep] 清理失败 (无害, 残留仅致计数噪音): %s", exc,
            )

    def _quote_event_loop(self):
        from .quote_events import quote_channel

        while self._quote_event_running:
            if not self._quote_callbacks:
                time.sleep(0.5)
                continue
            account_id = str(self.client.account_id or "")
            pubsub = None
            try:
                pubsub = self.client._redis().pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(quote_channel(account_id))
                while self._quote_event_running and self._quote_callbacks:
                    if str(self.client.account_id or "") != account_id:
                        break
                    message = pubsub.get_message(timeout=1.0)
                    if not message or message.get("type") != "message":
                        continue
                    self._dispatch_quote_event(message.get("data"))
            except Exception:
                time.sleep(1.0)
            finally:
                try:
                    if pubsub is not None:
                        pubsub.close()
                except Exception:
                    pass

    def get_dispatch_drop_stats(self) -> dict:
        """[BUG-20260827-dispatch-drop-counters] 丢弃计数快照 (线程安全拷贝)."""
        with self._dispatch_drop_lock:
            return dict(self._dispatch_drops)

    def _bump_dispatch_drop(self, kind: str, code=None) -> None:
        notify_at = 0
        with self._dispatch_drop_lock:
            if kind in self._dispatch_drops:
                self._dispatch_drops[kind] += 1
                notify_at = self._dispatch_drops[kind]
                # [BUG-20260903-03] code 维度: 区分「重启残渣噪音」与「在订码真断供」
                if code:
                    _per_code = self._dispatch_drop_codes.setdefault(kind, {})
                    _per_code[code] = _per_code.get(code, 0) + 1
        # [对抗复审 DEF-2 收口] INV-4 半程补齐: 计数不再只有"有人来读才有出口" —
        # 每 20 次/类 经 log.warning 落一行累计快照 (bigqmt.log 持久化通道), 热路径
        # 仅一次取模判断, 无额外 IO 直到触发。锁外取快照避免重入。
        if notify_at and notify_at % 20 == 0:
            try:
                with self._dispatch_drop_lock:
                    _per_code = self._dispatch_drop_codes.get(kind) or {}
                    _top = dict(sorted(_per_code.items(), key=lambda kv: -kv[1])[:5])
                log.warning(
                    "[BUG-20260827-dispatch-drops] kind=%s count=%d snapshot=%s"
                    " top_codes=%s",
                    kind, notify_at, self.get_dispatch_drop_stats(), _top,
                )
            except Exception:
                pass  # 观测告警自身不许反噬派发线程

    def _write_keepalive(self, seq_str: str) -> None:
        """[BUG-20260827-sub-registry-gc] 消费端保活戳 (节流 60s/seq)。

        写失败静默 — redis 故障时整个事件通道同样故障, 该路径不引入新故障面。
        """
        try:
            now_ms = int(time.time() * 1000)
            if not _should_write_keepalive(self._keepalive_last_ms.get(seq_str), now_ms):
                return
            account_id = str(self.client.account_id or "")
            self.client._redis().hset(
                _quote_seen_key(account_id), seq_str, str(now_ms)
            )
            self._keepalive_last_ms[seq_str] = now_ms
        except Exception:
            pass

    def set_quote_heartbeat_handler(self, handler):
        """[BUG-20260827-quote-heartbeat-frame] 注册 liveness 钩子 (传 None 注销).

        handler 签名 ``handler(event: dict)`` — event 含 seq/stock_code/last_price/
        created_at_ts。跑在 _quote_event_thread 上, handler 自己负责并发防护与
        快速返回 (禁止 RPC / OMS 写)。
        """
        self._quote_heartbeat_handler = handler

    def _dispatch_heartbeat_event(self, event):
        """heartbeat → liveness 钩子; 无钩子/钩子异常一律静默 (观测通道不许反噬交易)."""
        handler = self._quote_heartbeat_handler
        if handler is None:
            return
        try:
            handler(event)
            self._write_keepalive(str(event.get("seq") or ""))
        except Exception as exc:
            log.warning("quote heartbeat handler failed: %s", exc)

    def _dispatch_quote_event(self, raw):
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            event = json.loads(text)
        except Exception:
            self._bump_dispatch_drop("malformed_event")
            return
        if not isinstance(event, dict):
            self._bump_dispatch_drop("malformed_event")
            return
        # [BUG-20260827-quote-heartbeat-frame] 心跳帧独立路由 — 与 quote 帧严格分流,
        # 保证 liveness 观测永不污染 tick 数据面。
        if event.get("event_type") == _QUOTE_EVENT_HEARTBEAT:
            self._dispatch_heartbeat_event(event)
            return
        _etype = event.get("event_type")
        if _etype != "quote":
            # [BUG-20260903-03] 后端自身 publish_event 控制命令回声 (subscribe/
            # unsubscribe_quote 与行情同通道, 监听线程自己消费) 不是未知事件 —
            # 单列 control_echo, 不污染 unknown_type (真未知才值得喊)。
            if _etype in ("subscribe_quote", "unsubscribe_quote"):
                self._bump_dispatch_drop("control_echo")
            else:
                self._bump_dispatch_drop("unknown_type")
            return
        seq = event.get("seq")
        callback = self._quote_callbacks.get(seq)
        if callback is None:
            # [BUG-20260813-quote-callback-seq-mismatch] Fallback: QMT 端 quote_push
            # 读 Redis hash 决定用哪个 seq 推. 当 save_quote_subscription hset 曾失败
            # 或 re-subscribe 后旧 callback 被清, QMT 可能仍推旧 seq → _quote_callbacks
            # 查不到 → tick 静默丢弃 → 持仓股卖出规则失明 (601869.SH 实例).
            # 通过 stock_code → _code_to_seq 反查当前活 seq → 找到 callback 兜底路由.
            _code = str(event.get("stock_code") or "")
            if _code:
                _current_seq = self._code_to_seq.get(_code)
                if _current_seq is not None and _current_seq != seq:
                    callback = self._quote_callbacks.get(_current_seq)
            if callback is None:
                # [BUG-20260827-dispatch-drop-counters] 不再纯静默 — 计数留痕
                # [BUG-20260903-03] 带 code 维度: 残渣噪音 vs 在订码真断供可分辨
                self._bump_dispatch_drop("seq_miss_no_fallback", code=_code or None)
                return
        stock_code = str(event.get("stock_code") or "")
        if not stock_code:
            self._bump_dispatch_drop("empty_stock_code")
            return
        bar = {
            "open": event.get("open"),
            "high": event.get("high"),
            "low": event.get("low"),
            "close": event.get("close"),
            "volume": event.get("volume"),
            "amount": event.get("amount"),
            # [BUG-20260811-etf-tick-time] 空 time 不再产生: bar_time 优先 (QMT 端
            # row.time 真 tick 时间), 缺失兜底 created_at (QMT 端北京墙上时间,
            # "%Y-%m-%d %H:%M:%S", pump ~1s cadence 精度, parse_tick_time ISO 格式兼容).
            # 禁后端 datetime.now() fallback (timezone-001 防跨日/跨时区漂移).
            "time": event.get("bar_time") or event.get("created_at") or "",
            # [BUG-P0-20260811-bridge-etf-tick-001] tick snapshot 字段透传 — QMT 端
            # _push_quote_updates row 已补这些字段 (quote_events._BAR_FIELDS 同步扩展),
            # backend dispatch 时一并透传给 callback. ETF 战法 on_tick 依赖 lastPrice
            # (price), bidPrice/askPrice (G6), tickvol/bidVol/askVol (量能). pullback_ma5
            # 路径 B (_handle_xt_tick) 只取 close, 多余字段 inert.
            "lastPrice": event.get("lastPrice") or event.get("close"),
            "lastClose": event.get("lastClose"),
            "bidPrice": event.get("bidPrice"),
            "askPrice": event.get("askPrice"),
            "bidVol": event.get("bidVol"),
            "askVol": event.get("askVol"),
            "tickvol": event.get("tickvol"),
        }
        try:
            callback({stock_code: [bar]})
            # [BUG-20260827-sub-registry-gc] 成功派发 = 消费端活, 节流写保活戳
            self._write_keepalive(str(seq or ""))
        except Exception:
            self._bump_dispatch_drop("callback_exception", code=stock_code or None)

    def run(self):
        while True:
            time.sleep(3600)

    def get_divid_factors(self, stock_code, start_time="", end_time=""):
        return self._call("get_divid_factors", stock_code=stock_code, start_time=start_time, end_time=end_time)

    def download_server_raw(
        self,
        stock_list,
        period,
        start_time="",
        end_time="",
        batch_size=100,
        timeout_seconds=60.0,
        max_consecutive_failures=2,
        max_total_seconds=600.0,
    ):
        """[fix 2026-08-20 subscribe-cap-storm] 服务端真批量下载 (native SDK, 数据服务通道).

        背景: QMT 引擎 serve get_market_data_ex 时, 对本地 store 缺失的票退化为
        逐票 quote subscribe — 2026-08-20 XtClient_datasource 实测 ErrorID 210000
        (订阅超过上限) 10470 次/日 (15h 日线 4690 源于 daily sync + 18-19h 分钟线
        5780 源于 minute ETL, 全部来自全市场 get 的 cache-miss); cache 命中的票
        零订阅 (同日探针 tmp/probe_pre_close.py 6 次 get 零 onSubscribe).

        本方法把数据先落到 QMT 本地 store (服务端 native download_history_data2,
        走数据服务通道, 无订阅), 之后 caller 的 get 走本地 cache 命中, 消灭订阅风暴.

        与 2026-08-11 移除的"预下载"区别: 旧版假下载 = client 侧 get 循环
        (大 payload 连发 → QMT drain 堵死, BUG-20260811-rpc-storm); 本方法每次
        RPC 只传 code 列表 + 收 ack, 下载在 QMT 进程内 native 完成.

        Best-effort + 双重熔断: 连续 max_consecutive_failures 批失败即 abort
        (维护窗口实测 2026-08-20 21:15: 单批 180s 全超时, 数据服务不可达时快速
        退化为 legacy get 行为); 总耗时超 max_total_seconds 也 abort.

        Env knob: BIGQMT_SERVER_DOWNLOAD=0 关闭 (ops 逃生舱, 行为回到纯 get).

        Returns: {"ok": n, "fail": n, "aborted": bool, "total_batches": n}
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        result = {"ok": 0, "fail": 0, "aborted": False, "total_batches": 0}
        if not codes:
            return result
        if os.environ.get("BIGQMT_SERVER_DOWNLOAD", "1").strip() in ("0", "false", "False"):
            result["aborted"] = True
            return result
        step = max(1, int(batch_size or 100))
        total_batches = (len(codes) + step - 1) // step
        result["total_batches"] = total_batches
        t0 = time.time()
        consecutive_failures = 0
        for i in range(0, len(codes), step):
            if (time.time() - t0) > float(max_total_seconds or 600.0):
                print(
                    "[bigqmt_compat] download_server_raw total budget %.0fs exceeded at batch %d/%d "
                    "(period=%s) — abort rest, caller get 兜底"
                    % (max_total_seconds, result["ok"] + result["fail"], total_batches, period)
                )
                result["aborted"] = True
                break
            batch = codes[i : i + step]
            try:
                self.client.call(
                    "download_history_data2",
                    {
                        "stock_list": batch,
                        "period": period,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                    timeout_seconds=float(timeout_seconds or 60.0),
                )
                result["ok"] += 1
                consecutive_failures = 0
            except Exception as exc:
                result["fail"] += 1
                consecutive_failures += 1
                # [merge 2026-09-03] 记录末次原始异常: cache 禁用路径 (#47 语义)
                # 需要把真实失败原因 raise 给调用方, 而非吞成 {fail: n} 计数。
                result["last_error"] = exc
                print(
                    "[bigqmt_compat] download_server_raw batch %d/%d failed "
                    "(%d codes, period=%s): %s: %s"
                    % (
                        result["ok"] + result["fail"],
                        total_batches,
                        len(batch),
                        period,
                        exc.__class__.__name__,
                        exc,
                    )
                )
                if consecutive_failures >= max(1, int(max_consecutive_failures or 2)):
                    print(
                        "[bigqmt_compat] download_server_raw %d consecutive failures — "
                        "abort rest (data service unreachable?), caller get 兜底"
                        % consecutive_failures
                    )
                    result["aborted"] = True
                    break
        return result

    def download_history_data2(self, stock_list, period, start_time="", end_time="", callback=None, incrementally=None, dividend_type="none", chunk_size=None, download_timeout_seconds=180.0, data_wait_seconds=60.0):
        """Pull bars from Big QMT over RPC and cache them locally, in batches.

        Mirrors xtdata.download_history_data2: after this, get_local_data(..., the
        same dividend_type) reads the data locally with no further RPC. Each batch
        re-pulls live, so re-running keeps the cache latest — needed for 前复权
        (front-adjusted) data. ``callback`` (optional) is invoked once per stock with
        {finished, total, stockcode} — xtdata-style. Returns {finished, total}.

        Server-side real download runs FIRST for ALL dividend types
        ([fix 2026-08-20 subscribe-cap-storm]): without it, the get loop below makes
        the QMT engine per-code SUBSCRIBE for local-store misses (ErrorID 210000
        订阅超过上限 — see download_server_raw). This also matches xtdata semantics
        ("populate the local QMT store"): an unadjusted download used to skip the
        server RPC and read only what Big QMT already had — a no-op that still
        reported progress (issue #47).
        Adjusted types additionally require raw bars + dividend factors server-side
        (front-adjusted closes come back all-zero otherwise, verified live), which
        the same download provides. The pre-download is best-effort: on failure the
        get loop still runs.
        """
        codes = [str(c) for c in (stock_list or []) if str(c or "").strip()]
        if not codes:
            return {"finished": 0, "total": 0}

        # Server-side download first, for EVERY dividend_type (分块 + 双重熔断,
        # 见 download_server_raw — 2026-08-20 subscribe-cap-storm 根治).
        # [merge 2026-09-03] 上游 #47 语义并入: local cache 禁用时, 服务端下载即
        # 全部工作 — 失败必须 raise (假进度 {finished: total} 正是 #47 的 bug),
        # 成功则直接回报进度、不做 client pull (数据已落服务端 DAT)。cache 启用
        # (B 机生产形态) 保持本地 best-effort + 分批轮询, 行为零变化。
        _server_dl = self.download_server_raw(codes, period, start_time, end_time)
        if self._local_cache() is None:
            if _server_dl.get("fail") or _server_dl.get("aborted"):
                last_error = _server_dl.get("last_error")
                if isinstance(last_error, BaseException):
                    raise last_error
                raise RuntimeError(
                    "server-side download failed (fail=%s aborted=%s) with local "
                    "cache disabled -- download cannot make progress"
                    % (_server_dl.get("fail"), _server_dl.get("aborted"))
                )
            total = len(codes)
            finished = 0
            for code in codes:
                finished += 1
                if callback is not None:
                    try:
                        callback({"finished": finished, "total": total, "stockcode": code})
                    except Exception:
                        pass
            return {"finished": finished, "total": total}

        total = len(codes)
        step = int(chunk_size or 300)
        if step <= 0:
            step = 300
        finished = 0
        # [merge 2026-09-03] 批轮询内抑制 none-read 自愈 (重入门护栏, 见
        # _heal_adjusted): 下载层自持节奏 (data_wait_seconds), get 内不得再
        # 触发服务端下载造成双重轮询。
        self._download_batch_depth = getattr(self, "_download_batch_depth", 0) + 1
        # [BMG1-01 2026-09-03] try/finally 兜住重入计数: 批循环内 get_market_data_ex
        # 是 RPC 调用可抛异常 (超时/断连), 旧实现无 finally → 计数器泄漏为永久正数 →
        # _heal_adjusted none-read majority-missing 自愈 (2290 判 >0 即 return) 被持续抑制。
        try:
            for i in range(0, total, step):
                batch = codes[i:i + step]
                # QMT 的下载全局是「提交任务即返回」，数据在服务端异步落地
                # （秒~分钟级）。下载后立刻读只能看到旧数据——issue #66 里
                #「tick 只能获得最近 1 天」的真正原因就是这个竞态：数据还没落地
                # 就已经被读走并缓存了空结果。这里分批轮询，直到批内每个代码都
                # 出现真实数据行或超时（超时容忍停牌/退市等确实无数据的代码）。
                deadline = time.time() + float(data_wait_seconds)
                while True:
                    # get_market_data_ex 是 cache-through：每次轮询都会写入缓存，
                    # 最后一次（数据齐或超时）的结果即最终缓存内容。
                    data = self.get_market_data_ex(
                        field_list=DEFAULT_DOWNLOAD_FIELDS,
                        stock_list=batch,
                        period=period,
                        start_time=start_time,
                        end_time=end_time,
                        count=-1,
                        dividend_type=dividend_type,
                        fill_data=False,  # fill 会用全 0 占位行冒充数据，轮询判定必须关掉
                        timeout_seconds=float(data_wait_seconds),
                    )
                    ready = 0
                    for code in batch:
                        df = (data or {}).get(code)
                        if df is not None and getattr(df, "shape", (0,))[0] > 0:
                            ready += 1
                    if ready >= len(batch) or time.time() >= deadline:
                        break
                    time.sleep(1.5)
                for code in batch:
                    finished += 1
                    if callback is not None:
                        try:
                            callback({"finished": finished, "total": total, "stockcode": code})
                        except Exception:
                            pass
        finally:
            self._download_batch_depth -= 1
        return {"finished": finished, "total": total}

    def download_history_data(self, stock_code, period, start_time="", end_time="", incrementally=None, dividend_type="none"):
        return self.download_history_data2([stock_code], period, start_time, end_time, dividend_type=dividend_type)

    def local_cache_stats(self):
        """Return (cached files, periods) for the client-side local cache."""
        cache = self._local_cache()
        return cache.stats() if cache is not None else (0, [])

    def get_trading_dates(self, market, start_time="", end_time="", count=-1):
        return self._call("get_trading_dates", market=market, start_time=start_time, end_time=end_time, count=count)

    def get_holidays(self):
        return self._call("get_holidays")

    def download_holiday_data(self, incrementally=True):
        return self._call("download_holiday_data", incrementally=incrementally)

    def get_ipo_info(self, start_time="", end_time=""):
        return self._call("get_ipo_info", start_time=start_time, end_time=end_time)

    def get_etf_info(self):
        return self._call("get_etf_info")

    def download_etf_info(self):
        return self._call("download_etf_info")

    # 下面四个的服务端实现和 RPC 白名单一直都在（market_bigqmt 的
    # download_* 方法 + redis_rpc 的 MARKET_DATA_METHODS），只是客户端漏了这层
    # 包装，于是外部调用直接撞 AttributeError（issue #130）。
    def download_sector_data(self):
        return self._call("download_sector_data")

    def download_cb_data(self):
        return self._call("download_cb_data")

    def download_index_weight(self):
        return self._call("download_index_weight")

    def download_history_contracts(self, incrementally=True):
        # 形参保留是为了和 xtdata.download_history_contracts(incrementally=True)
        # 签名一致；大 QMT 那边这个调用没有增量参数，服务端按全量下载处理。
        return self._call("download_history_contracts")

    def get_option_list(self, undl_code, dedate, opttype="", isavailavle=False):
        return self._call("get_option_list", undl_code=undl_code, dedate=dedate, opttype=opttype, isavailavle=isavailavle)

    def get_his_option_list(self, undl_code, dedate):
        return self._call("get_his_option_list", undl_code=undl_code, dedate=dedate)

    def get_his_option_list_batch(self, undl_code, start_time="", end_time=""):
        return self._call("get_his_option_list_batch", undl_code=undl_code, start_time=start_time, end_time=end_time)

    def get_financial_data(self, stock_list, table_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def download_financial_data(self, stock_list, table_list=None, start_time="", end_time="", incrementally=None):
        return self._call(
            "download_financial_data",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
            incrementally=incrementally,
        )

    def download_financial_data2(self, stock_list, table_list=None, start_time="", end_time="", callback=None):
        result = self._call(
            "download_financial_data2",
            stock_list=list(stock_list or []),
            table_list=list(table_list or []),
            start_time=start_time,
            end_time=end_time,
        )
        if callback is not None:
            callback(result)
        return result

    def get_sector_list(self, allow_fallback=False):
        """Sector names, or an error saying the terminal cannot list them.

        ``allow_fallback=True`` opts into the 13 curated well-known names,
        which still drive ``get_stock_list_in_sector``. Big QMT cannot
        enumerate real sectors at all, and handing back the curated list
        unasked made a fake answer indistinguishable from a real one (#143).
        """
        return self._call("get_sector_list", allow_fallback=bool(allow_fallback))

    def get_sector_info(self, sector_name=""):
        return self._call("get_sector_info", sector_name=sector_name)

    def get_markets(self):
        return self._call("get_markets")

    def get_market_last_trade_date(self, market):
        return self._call("get_market_last_trade_date", market=market)

    def call_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None):
        return self._call(
            "call_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )

    def subscribe_formula(self, formula_name, stock_code, period, start_time="", end_time="", count=-1, dividend_type=None, extend_param=None, callback=None):
        result = self._call(
            "subscribe_formula",
            formula_name=formula_name,
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            dividend_type=dividend_type,
            extend_param=extend_param or {},
        )
        if callback is not None:
            callback(result)
        return result

    def unsubscribe_formula(self, request_id):
        return self._call("unsubscribe_formula", request_id=request_id)

    def get_formula_result(self, request_id, start_time="", end_time="", count=-1, timeout_second=-1):
        return self._call(
            "get_formula_result",
            request_id=request_id,
            start_time=start_time,
            end_time=end_time,
            count=count,
            timeout_second=timeout_second,
        )

    def gen_factor_index(self, data_name, formula_name, vars, sector_list, start_time="", end_time="", period="1d", dividend_type="none"):
        return self._call(
            "gen_factor_index",
            data_name=data_name,
            formula_name=formula_name,
            vars=vars,
            sector_list=list(sector_list or []),
            start_time=start_time,
            end_time=end_time,
            period=period,
            dividend_type=dividend_type,
        )

    # ------------------------------------------------------------------
    # 扩展行情/基本面方法（对应 ContextInfo 方法，走 RPC 白名单）。
    # 仅对最常用的显式声明签名；其余通过 __getattr__ 自动转发。
    # ------------------------------------------------------------------

    def get_longhubang(self, stock_list=None, start_time="", end_time="", count=-1):
        return self._call(
            "get_longhubang",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            count=count,
        )

    def get_top10_share_holder(self, stock_list, data_name, start_time, end_time, report_type="report_time"):
        return self._call(
            "get_top10_share_holder",
            stock_list=list(stock_list or []),
            data_name=data_name,
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_holder_num(self, stock_list=None, start_time="", end_time="", report_type="report_time"):
        return self._call(
            "get_holder_num",
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
        )

    def get_turnover_rate(self, stock_code=None, start_time="19720101", end_time="22010101"):
        return self._call(
            "get_turnover_rate",
            stock_code=list(stock_code or []),
            start_time=start_time,
            end_time=end_time,
        )

    def get_industry(self, industry_name):
        return self._call("get_industry", industry_name=industry_name)

    def bsm_price(self, opt_type, target_price, strike_price, risk_free, sigma, days, dividend=0):
        return self._call(
            "bsm_price",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            risk_free=risk_free,
            sigma=sigma,
            days=days,
            dividend=dividend,
        )

    def bsm_iv(self, opt_type, target_price, strike_price, option_price, risk_free, days, dividend=0):
        return self._call(
            "bsm_iv",
            opt_type=opt_type,
            target_price=target_price,
            strike_price=strike_price,
            option_price=option_price,
            risk_free=risk_free,
            days=days,
            dividend=dividend,
        )

    def get_option_iv(self, opt_code):
        return self._call("get_option_iv", opt_code=opt_code)

    def get_option_analytics(
        self,
        opt_code,
        option_price=None,
        underlying_price=None,
        as_of=None,
        risk_free_rate=None,
        dividend_yield=None,
        price_period="1m",
        include_native_iv=False,
    ):
        """Return client-side IV and Greeks for one option contract."""
        from .option_analytics_client import get_option_analytics

        return get_option_analytics(
            self,
            opt_code,
            option_price=option_price,
            underlying_price=underlying_price,
            as_of=as_of,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            price_period=price_period,
            include_native_iv=include_native_iv,
        )

    def get_option_chain_analytics(
        self,
        undl_code,
        dedate,
        opttype="",
        isavailavle=False,
        underlying_price=None,
        as_of=None,
        risk_free_rate=None,
        dividend_yield=None,
        price_period="1m",
    ):
        """Return batched client-side IV and Greeks for one option expiry."""
        from .option_analytics_client import get_option_chain_analytics

        return get_option_chain_analytics(
            self,
            undl_code,
            dedate,
            opttype=opttype,
            isavailavle=isavailavle,
            underlying_price=underlying_price,
            as_of=as_of,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            price_period=price_period,
        )

    def get_option_detail_data(self, stockcode):
        return self._call("get_option_detail_data", stockcode=stockcode)

    def get_option_undl_data(self, undl_code_ref=""):
        return self._call("get_option_undl_data", undl_code_ref=undl_code_ref)

    def get_option_undl(self, opt_code):
        return self._call("get_option_undl", opt_code=opt_code)

    def get_raw_financial_data(self, field_list, stock_list, start_time, end_time, report_type="report_time", data_type="dict"):
        return self._call(
            "get_raw_financial_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_time=start_time,
            end_time=end_time,
            report_type=report_type,
            data_type=data_type,
        )

    def get_factor_data(self, field_list, stock_list, start_date, end_date):
        return self._call(
            "get_factor_data",
            field_list=list(field_list or []),
            stock_list=list(stock_list or []),
            start_date=start_date,
            end_date=end_date,
        )

    def get_north_finance_change(self, period):
        return self._call("get_north_finance_change", period=period)

    def get_hkt_statistics(self, stock_code):
        return self._call("get_hkt_statistics", stock_code=stock_code)

    def get_hkt_details(self, stock_code):
        return self._call("get_hkt_details", stock_code=stock_code)

    # 自定义板块写入（issue #143）。每一个都在服务端写入后回读校验，所以
    # 「没报错」现在真的代表写进去了 —— 以前 create_sector 是静默空操作。
    def create_sector(self, sector_name, stock_list):
        return self._call("create_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def create_sector_folder(self, parent_node, folder_name, overwrite=False):
        return self._call("create_sector_folder", parent_node=parent_node,
                          folder_name=folder_name, overwrite=overwrite)

    def reset_sector_stock_list(self, sector, stock_list):
        return self._call("reset_sector_stock_list", sector=sector,
                          stock_list=list(stock_list or []))

    def add_stock_to_sector(self, sector, stock_code):
        return self._call("add_stock_to_sector", sector=sector, stock_code=stock_code)

    def remove_stock_from_sector(self, sector, stock_code):
        return self._call("remove_stock_from_sector", sector=sector, stock_code=stock_code)

    def get_stock_name(self, stock):
        return self._call("get_stock_name", stock=stock)

    def get_close_price(self, market, stock_code, real_timetag, period=86400000, divid_type=0):
        return self._call(
            "get_close_price",
            market=market,
            stock_code=stock_code,
            real_timetag=real_timetag,
            period=period,
            divid_type=divid_type,
        )

    def get_main_contract(self, code_market):
        return self._call("get_main_contract", code_market=code_market)

    def get_his_contract_list(self, market):
        return self._call("get_his_contract_list", market=market)

    def get_date_location(self, date):
        return self._call("get_date_location", date=date)

    def get_his_st_data(self, stock_code):
        return self._call("get_his_st_data", stock_code=stock_code)

    def get_his_index_data(self, stock_code):
        return self._call("get_his_index_data", stock_code=stock_code)

    def call_method(self, method, **params):
        """Generic escape hatch: call any RPC market-data method by name.

        Use this for ContextInfo methods that don't have an explicit wrapper
        above (e.g. ``xtdata.call_method("get_last_close", stock="000001.SZ")``,
        ``xtdata.call_method("get_float_caps", stockcode="000001.SZ")``). The
        full list of callable methods is in ``MARKET_DATA_METHODS``.
        """
        return self._call(method, **params)

    # ------------------------------------------------------------------
    # L2 行情（需 L2 权限 + 原生 xtdata SDK 行情服务）
    # ------------------------------------------------------------------

    def get_l2_quote(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_quote", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_order(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_order", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    def get_l2_transaction(self, field_list=None, stock_code="", start_time="", end_time="", count=-1):
        return self._call("get_l2_transaction", field_list=list(field_list or []),
                          stock_code=stock_code, start_time=start_time, end_time=end_time, count=count)

    # ------------------------------------------------------------------
    # 指数权重 / 交易日历 / 交易时段 / 可转债 / 品种判断
    # ------------------------------------------------------------------

    def get_index_weight(self, index_code):
        return self._call("get_index_weight", index_code=index_code)

    def get_trading_calendar(self, market, start_time="", end_time="", tradetimes=False):
        return self._call("get_trading_calendar", market=market, start_time=start_time,
                          end_time=end_time, tradetimes=tradetimes)

    def get_trade_times(self, stockcode):
        return self._call("get_trade_times", stockcode=stockcode)

    def get_cb_info(self, stockcode):
        return self._call("get_cb_info", stockcode=stockcode)

    def is_stock_type(self, stock, tag):
        return self._call("is_stock_type", stock=stock, tag=tag)

    # ------------------------------------------------------------------
    # 板块增删
    # ------------------------------------------------------------------

    def add_sector(self, sector_name, stock_list):
        return self._call("add_sector", sector_name=sector_name, stock_list=list(stock_list or []))

    def remove_sector(self, sector_name):
        return self._call("remove_sector", sector_name=sector_name)

    # ------------------------------------------------------------------
    # 时间戳转换（纯计算）
    # ------------------------------------------------------------------

    @staticmethod
    def datetime_to_timetag(datetime_str, format="%Y%m%d%H%M%S"):
        import datetime as _dt
        try:
            return int(_dt.datetime.strptime(str(datetime_str), format).timestamp() * 1000)
        except Exception:
            return 0

    @staticmethod
    def timetag_to_datetime(timetag, format):
        import datetime as _dt
        try:
            return _dt.datetime.fromtimestamp(int(timetag) / 1000.0).strftime(format)
        except Exception:
            return ""

    @staticmethod
    def timetagToDateTime(timetag, format):
        return BigQmtXtData.timetag_to_datetime(timetag, format)


class BigQmtXtTrader:
    def __init__(
        self,
        path=None,
        session_id=None,
        account_id=None,
        redis_client=None,
        redis_config=None,
        timeout_seconds=None,
    ):
        self.path = path
        self.session_id = session_id
        self.client = BigQmtRpcClient(
            account_id=account_id,
            redis_client=redis_client,
            redis_config=redis_config,
            timeout_seconds=timeout_seconds,
        )
        self.callback = None
        self._event_thread = None
        self._event_running = False
        # Async order submission (issue #50). One worker, started on first use,
        # so a client that never calls order_stock_async pays nothing.
        self._async_order_queue = _queue.Queue()
        self._async_order_thread = None
        self._async_order_lock = threading.Lock()
        # int -> 合同编号 for ids handed out as OrderId (issue #113).
        self._order_sys_ids = _OrderedDict()
        # on_account_status used to report a hardcoded "STOCK" even for a
        # credit deployment (issue #103). The server is authoritative -- the
        # client's StockAccount(..., "CREDIT") never travels -- so prefer what
        # ping reports, fall back to what the caller declared.
        self._server_account_type = ""
        self._declared_account_type = ""

    def _cached_position_snapshot(self, account_id):
        key = "bigqmt:positions:%s" % str(account_id or self.client.account_id or "")
        try:
            raw = self.client._redis().get(key)
        except Exception:
            return {}
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw))
        except Exception:
            return {}

    def _cached_positions(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        if isinstance(positions, dict):
            return positions
        if isinstance(positions, list):
            return {str(item.get("stock_code") or idx): item for idx, item in enumerate(positions)}
        return {}

    def _cached_asset(self, account_id):
        snapshot = self._cached_position_snapshot(account_id)
        asset = snapshot.get("asset") if isinstance(snapshot, dict) else None
        return asset if isinstance(asset, dict) else {}

    def _redis_cache_enabled(self):
        return str(getattr(self.client, "transport_name", "redis") or "redis").lower() in (
            "redis",
            "",
            "default",
        )

    def register_callback(self, callback):
        self.callback = callback
        return 0

    def start(self):
        # Launch the real-time execution-event listener so a registered callback's
        # on_stock_order / on_stock_trade fire as soon as Big QMT pushes them.
        self._start_event_listener()
        return 0

    def connect(self):
        if self.client.account_id:
            pong = self.client.call("ping")
            self._note_server_account_type(pong)
            mismatch = warn_on_version_mismatch(pong)
            if mismatch and auto_sync_enabled():
                self.sync_deployment()
        self._fire_account_status()
        return 0

    def sync_deployment(self, dry_run=False):
        """Push this client's package into the QMT python directory.

        The copy runs here, not inside QMT: a trading process rewriting its own
        code mid-session would put whatever is in the source tree, half-finished
        edits included, straight onto the live terminal.

        Config files are never written. The strategy still has to be restarted
        afterwards -- QMT keeps modules in sys.modules across re-runs, so a copy
        on its own changes nothing.
        """
        from .sync import sync_deployment as _sync

        # get_deployment_info lives on BigQmtXtData; this class never had it,
        # so xt_trader.sync_deployment() died with AttributeError. Call the
        # RPC directly -- the trader's client is the same one.
        info = self.client.call("get_deployment_info", {}) or {}
        target = info.get("qmt_python_dir") or ""
        if not target:
            log.warning("sync_deployment: the bridge did not report a "
                        "qmt_python_dir (server too old?); nothing copied")
            return {"updated": [], "error": "no qmt_python_dir reported"}

        result = _sync(target, dry_run=dry_run)
        if result.get("error"):
            log.warning("sync_deployment: %s", result["error"])
        elif result["updated"]:
            log.warning(
                "sync_deployment: %d file(s) %s in %s. RESTART THE STRATEGY -- "
                "QMT keeps modules across re-runs, so this has no effect until "
                "you do. Config files untouched: %s",
                len(result["updated"]),
                "would be updated" if dry_run else "updated",
                target, ", ".join(result["skipped_config"]) or "none present")
        else:
            log.info("sync_deployment: already up to date (%d files identical)",
                     result["identical"])
        return result

    def subscribe(self, account):
        declared = _account_type_name(getattr(account, "account_type", None))
        if declared:
            self._declared_account_type = declared
            self._warn_on_account_type_mismatch()
        if not self.client.account_id:
            self.client.account_id = _account_id(account)
        # (Re)start the listener now that the account is known; the loop resubscribes
        # to the account's channels within ~1s if the account changed.
        self._start_event_listener()
        self._fire_account_status()
        return 0

    def stop(self):
        # Drain first: orders already queued must go out before teardown.
        # Costs nothing when the queue is empty (issue #156).
        self._drain_async_orders_on_exit()
        self._event_running = False
        thread = self._event_thread
        if thread is not None and thread.is_alive():
            thread.join(1.0)
        self._event_thread = None
        return 0

    def _start_event_listener(self):
        if self._event_thread is not None and self._event_thread.is_alive():
            return
        self._event_running = True
        self._event_thread = threading.Thread(
            target=self._event_loop, name="bigqmt-exec-events", daemon=True
        )
        self._event_thread.start()

    def _note_server_account_type(self, pong):
        """Remember what the deployment says it trades as."""
        try:
            reported = str((pong or {}).get("account_type") or "").strip().upper()
        except Exception:
            return
        if reported:
            self._server_account_type = reported
            self._warn_on_account_type_mismatch()

    def _warn_on_account_type_mismatch(self):
        """Say so when the caller and the deployment disagree.

        A client asking for CREDIT against a STOCK deployment gets an all-zero
        asset row and no error at all -- that was issue #92, and it cost the
        reporter a long time because nothing anywhere said the two disagreed.
        """
        server = self._server_account_type
        declared = self._declared_account_type
        if not server or not declared or server == declared:
            return
        log.warning(
            "account_type mismatch: this client asked for %s but the QMT "
            "deployment is configured as %s. The client's StockAccount type "
            "does NOT travel to the server -- set BIGQMT_ACCOUNT_TYPE = %r in "
            "the QMT-side local config and restart the strategy. Until then "
            "queries answer as %s (a credit account read as STOCK returns an "
            "all-zero asset row).", declared, server, declared, server)

    def _fire_account_status(self):
        """Fire on_account_status after connect/subscribe (MiniQMT parity).

        Big QMT has no per-strategy account-status push; we synthesize a
        CONNECTED status once the RPC link is up so client code that waits
        for on_account_status before trading keeps working.
        """
        callback = self.callback
        if callback is None:
            return
        try:
            callback.on_account_status(
                CompatObject(
                    account_id=str(self.client.account_id or ""),
                    account_type=(self._server_account_type
                                  or self._declared_account_type or "STOCK"),
                    status=1,  # ACCOUNT_STATUS_ONLINE (MiniQMT XtAccountStatus)
                )
            )
        except Exception:
            log.exception("user callback failed: on_account_status")

    def _event_loop_push_channel(self):
        """One push-channel round: zmq exec events arrive on the same PUB
        socket as whole-quote data.

        Reuses _build_quote_push_channel so the address derivation stays in one
        place. Single round: returns when the account changes or the channel
        dies, so the caller's per-round channel selection runs again (Redis may
        have come back, or gone away).
        """
        from .exec_events import EXEC_TOPICS

        topics = sorted(set(EXEC_TOPICS.values()))
        channel = None
        account_id = str(self.client.account_id or "")
        try:
            channel = self._build_quote_push_channel()
            channel.start_subscriber(topics, self._on_push_exec_event)
            while self._event_running:
                if str(self.client.account_id or "") != account_id:
                    return       # account changed -> rebuild against the new address
                time.sleep(0.5)
        except Exception:
            time.sleep(1.0)
        finally:
            if channel is not None:
                try:
                    channel.stop()
                except Exception:
                    pass

    def _on_push_exec_event(self, topic, data):
        """Push-channel callback. The payload is already a decoded dict, unlike
        the Redis path which hands over raw bytes."""
        try:
            self._dispatch_event(data)
        except Exception:
            pass

    def _event_loop(self):
        """Receive exec events, mirroring the server's sink choice.

        The server publishes to Redis FIRST whenever it can build a Redis
        client (its channels carry streams for short replay), even when the
        RPC transport is zmq, and only falls to the quote push channel after
        repeated Redis publish failures (strategy _exec_event_sink, issue
        #145).  This loop used to choose by transport instead -- zmq -> push
        channel only -- so a zmq deployment with a working Redis published
        every order/trade event to Redis while the client listened on the
        push channel: callbacks never fired (issue #144; reproduced
        2026-09-02, the day's events sat in the Redis stream while a
        zmq-transport listener saw nothing).

        Re-select per reconnect round, so the client follows a server that
        demotes Redis mid-session (its Redis publish failing usually means
        our Redis reads fail too).
        """
        while self._event_running:
            redis_client = self._exec_events_redis_or_none()
            if redis_client is not None:
                self._event_loop_redis(redis_client)
            elif self._exec_transport_is_zmq():
                self._event_loop_push_channel()
            else:
                # redis transport with redis down: nothing else carries
                # events; keep retrying as before.
                time.sleep(1.0)

    def _exec_transport_is_zmq(self):
        return str(getattr(self.client, "transport_name", "redis") or "redis").lower() == "zmq"

    def _exec_events_redis_or_none(self):
        """A REACHABLE Redis client for the exec-event channels, or None.

        _redis() only builds the client object; the connection is lazy, so
        an unreachable server would still return one. Ping it -- the channel
        choice must reflect reachability, not configuration.
        """
        try:
            client = self.client._redis()
            if client is None:
                return None
            client.ping()
            return client
        except Exception:
            return None

    def _event_loop_redis(self, redis_client):
        """One Redis round: subscribe the per-account channels until the
        account changes or the connection dies, then return for re-selection."""
        from .exec_events import (
            order_channel,
            trade_channel,
            order_error_channel,
            cancel_error_channel,
        )

        account_id = str(self.client.account_id or "")
        pubsub = None
        try:
            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(
                order_channel(account_id),
                trade_channel(account_id),
                order_error_channel(account_id),
                cancel_error_channel(account_id),
            )
            while self._event_running:
                if str(self.client.account_id or "") != account_id:
                    return  # account changed -> reconnect and resubscribe
                message = pubsub.get_message(timeout=1.0)
                if not message or message.get("type") != "message":
                    continue
                self._dispatch_event(message.get("data"))
        except Exception:
            time.sleep(1.0)
        finally:
            try:
                if pubsub is not None:
                    pubsub.close()
            except Exception:
                pass

    def _dispatch_event(self, raw):
        """Accepts raw bytes/str (Redis pub/sub) or an already-decoded dict.

        The push channel decodes msgpack/json itself, so it hands over a dict --
        str(dict) is not valid JSON and would be silently dropped here.
        """
        callback = self.callback
        if callback is None:
            return
        if isinstance(raw, dict):
            event = raw
        else:
            try:
                text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
                event = json.loads(text)
            except Exception:
                return
        if not isinstance(event, dict):
            return
        # 放行超时的屏障, 再决定这条事件是直通还是暂存 (issue #51)。
        try:
            self._sweep_order_barriers()
            if event.get("event_type") in ("order", "trade", "order_error", "cancel_error") and self._hold_if_pending(event):
                return
        except Exception:
            pass  # 屏障故障绝不能吞掉事件
        self._deliver_event(event)

    def _deliver_event(self, event):
        callback = self.callback
        if callback is None:
            return
        account_id = str(event.get("account_id") or self.client.account_id or "")
        try:
            event_type = event.get("event_type")
            if event_type == "trade":
                callback.on_stock_trade(self._trade_from_dict(account_id, event))
            elif event_type == "order":
                callback.on_stock_order(self._order_from_dict(account_id, event))
            elif event_type == "order_error":
                _sysid = str(event.get("order_sys_id") or "")
                callback.on_order_error(
                    CompatObject(
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sysid=_sysid,       # MiniQMT 规范名 (issue #65)
                        order_sys_id=_sysid,      # 兼容别名
                        order_id=_sysid,
                        stock_code=event.get("stock_code") or "",
                        order_remark=str(
                            event.get("order_remark") or event.get("remark")
                            or event.get("user_order_id") or ""
                        ),
                        strategy_name=str(event.get("strategy_name") or ""),
                        status=_safe_int(event.get("status", event.get("order_status")), 0),
                    )
                )
            elif event_type == "cancel_error":
                _sysid = str(event.get("order_sys_id") or "")
                callback.on_cancel_error(
                    CompatObject(
                        error_id=event.get("error_id"),
                        error_msg=event.get("error_msg") or "",
                        order_sysid=_sysid,       # MiniQMT 规范名 (issue #65)
                        order_sys_id=_sysid,
                        order_id=_sysid,
                        stock_code=event.get("stock_code") or "",
                        order_remark=str(
                            event.get("order_remark") or event.get("remark")
                            or event.get("user_order_id") or ""
                        ),
                    )
                )
        except Exception:
            # 业务回调异常不能打崩事件线程，但必须留痕（issue: 静默吞错）。
            log.exception(
                "user callback failed: event_type=%s account=%s",
                event.get("event_type"),
                account_id,
            )

    def run_forever(self):
        while True:
            time.sleep(3600)

    def query_stock_asset(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_asset", {"account_id": account_id}, account_id=account_id) or {}
        except Exception:
            if not self._redis_cache_enabled():
                raise
            data = self._cached_asset(account_id)
            if not data:
                raise
        if (
            self._redis_cache_enabled()
            and data.get("cash") is None
            and data.get("total_asset") is None
        ):
            data = self._cached_asset(account_id) or data
        cash = data.get("cash")
        total_asset = data.get("total_asset")
        frozen_cash = data.get("frozen_cash")
        market_value = data.get("market_value")
        if market_value is None and cash is not None and total_asset is not None:
            # total_asset = cash(available) + frozen_cash + market_value. Older
            # servers send neither frozen_cash nor market_value; deriving without
            # frozen_cash overstates market value by the frozen amount, so
            # subtract it whenever the server did report it.
            market_value = _safe_float(total_asset) - _safe_float(cash)
            if frozen_cash is not None:
                market_value -= _safe_float(frozen_cash)
        return CompatObject(
            account_id=account_id,
            cash=_safe_float(cash, 0.0) if cash is not None else None,
            available_cash=_safe_float(cash, 0.0) if cash is not None else None,
            # MiniQMT's XtAsset always exposes frozen_cash, so default to 0.0
            # rather than None: callers do arithmetic on it.
            frozen_cash=_safe_float(frozen_cash, 0.0) if frozen_cash is not None else 0.0,
            total_asset=_safe_float(total_asset, 0.0) if total_asset is not None else None,
            market_value=_safe_float(market_value, 0.0) if market_value is not None else 0.0,
            # ===== 原生 xtquant 字段名别名（兼容 m_ 前缀访问）=====
            m_strAccountID=account_id,
            m_dCash=_safe_float(cash, 0.0) if cash is not None else None,
            m_dAvailableCash=_safe_float(cash, 0.0) if cash is not None else None,
            m_dFrozenCash=_safe_float(frozen_cash, 0.0) if frozen_cash is not None else 0.0,
            m_dTotalAsset=_safe_float(total_asset, 0.0) if total_asset is not None else None,
            m_dMarketValue=_safe_float(market_value, 0.0) if market_value is not None else 0.0,
        )

    def _position_object(self, account_id, item):
        volume = _safe_int(item.get("volume"))
        available = _safe_int(item.get("available", item.get("can_use_volume")))
        cost = _safe_float(item.get("cost", item.get("avg_price")))
        price = _safe_float(item.get("price", item.get("last_price")), cost)
        stock_code = str(item.get("stock_code") or "")
        stock_name = str(item.get("stock_name") or "")
        market_value = item.get("market_value")
        if market_value is None:
            market_value = price * volume
        return CompatObject(
            # 以前硬编码 2（SECURITY_ACCOUNT），信用账户上就是错的 —— 和 #103
            # 报的 on_account_status 同一类。现在跟服务端说的走。
            account_type=self._account_type_value(item),
            account_id=account_id,
            stock_code=stock_code,
            stock_name=stock_name,
            volume=volume,
            can_use_volume=available,
            enable_amount=available,
            available_amount=available,
            avg_price=cost,
            price=price,
            open_price=_safe_float(item.get("open_price"), cost),
            cost_price=cost,
            market_value=_safe_float(market_value, 0.0),
            frozen_volume=_safe_int(item.get("frozen_volume")),
            on_road_volume=_safe_int(item.get("on_road_volume")),
            yesterday_volume=_safe_int(item.get("yesterday_volume"), volume),
            direction=_safe_int(item.get("direction"), 48),
            # ===== 原生 xtquant 字段名别名（兼容 m_ 前缀访问）=====
            m_strAccountID=account_id,
            m_strStockCode=stock_code,
            m_strStockName=stock_name,
            m_nVolume=volume,
            m_nCanUseVolume=available,
            m_nCanUseVol=available,
            m_nEnableAmount=available,
            m_dOpenPrice=_safe_float(item.get("open_price"), cost),
            m_dAvgPrice=cost,
            m_dLastPrice=price,
            m_dMarketValue=_safe_float(market_value, 0.0),
            m_nFrozenVolume=_safe_int(item.get("frozen_volume")),
            m_nOnRoadVolume=_safe_int(item.get("on_road_volume")),
            m_nYesterdayVolume=_safe_int(item.get("yesterday_volume"), volume),
            m_nDirection=_safe_int(item.get("direction"), 48),
        )

    @staticmethod
    def _position_items(data):
        if isinstance(data, dict):
            return list(data.values())
        return _as_list(data)

    def query_stock_positions(self, account):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call("query_stock_positions", {"account_id": account_id}, account_id=account_id) or {}
        except Exception:
            if not self._redis_cache_enabled():
                raise
            data = self._cached_positions(account_id)
            if not data:
                raise
        return [self._position_object(account_id, item) for item in self._position_items(data)]

    def query_position_statistics(self, account):
        """Intraday position statistics (futures), mirroring MiniQMT ``query_position_statistics``.

        Returns a list of :class:`XtPositionStatistics`-style :class:`CompatObject`.
        The server queries via ``get_trade_detail_data(..., "POSITION_STATISTICS")``.
        """
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_position_statistics",
            {"account_id": account_id},
            account_id=account_id,
        ) or {}
        return [self._position_statistics_object(account_id, item) for item in _as_list(data)]

    def _position_statistics_object(self, account_id, item):
        return CompatObject(
            account_id=account_id,
            exchange_id=str(item.get("exchange_id") or ""),
            exchange_name=str(item.get("exchange_name") or ""),
            product_id=str(item.get("product_id") or ""),
            instrument_id=str(item.get("instrument_id") or ""),
            instrument_name=str(item.get("instrument_name") or ""),
            stock_code=str(item.get("stock_code") or ""),
            direction=_safe_int(item.get("direction"), 0),
            hedge_flag=_safe_int(item.get("hedge_flag"), 0),
            position=_safe_int(item.get("position"), 0),
            yesterday_position=_safe_int(item.get("yesterday_position"), 0),
            today_position=_safe_int(item.get("today_position"), 0),
            can_close_vol=_safe_int(item.get("can_close_vol"), 0),
            position_cost=_safe_float(item.get("position_cost"), None),
            avg_price=_safe_float(item.get("avg_price"), None),
            position_profit=_safe_float(item.get("position_profit"), None),
            float_profit=_safe_float(item.get("float_profit"), None),
            open_price=_safe_float(item.get("open_price"), None),
            used_margin=_safe_float(item.get("used_margin"), None),
            used_commission=_safe_float(item.get("used_commission"), None),
            frozen_margin=_safe_float(item.get("frozen_margin"), None),
            frozen_commission=_safe_float(item.get("frozen_commission"), None),
            instrument_value=_safe_float(item.get("instrument_value"), None),
            open_times=_safe_int(item.get("open_times"), 0),
            open_volume=_safe_int(item.get("open_volume"), 0),
            cancel_times=_safe_int(item.get("cancel_times"), 0),
            last_price=_safe_float(item.get("last_price"), None),
            rise_ratio=_safe_float(item.get("rise_ratio"), None),
            product_name=str(item.get("product_name") or ""),
            royalty=_safe_float(item.get("royalty"), None),
            expire_date=str(item.get("expire_date") or ""),
            assest_weight=_safe_float(item.get("assest_weight"), None),
            increase_by_settlement=_safe_float(item.get("increase_by_settlement"), None),
            margin_ratio=_safe_float(item.get("margin_ratio"), None),
            float_profit_divide_by_used_margin=_safe_float(
                item.get("float_profit_divide_by_used_margin"), None
            ),
            float_profit_divide_by_balance=_safe_float(
                item.get("float_profit_divide_by_balance"), None
            ),
            today_profit_loss=_safe_float(item.get("today_profit_loss"), None),
            yesterday_init_position=_safe_int(item.get("yesterday_init_position"), 0),
            frozen_royalty=_safe_float(item.get("frozen_royalty"), None),
            today_close_profit_loss=_safe_float(item.get("today_close_profit_loss"), None),
            close_profit=_safe_float(item.get("close_profit"), None),
            ft_product_name=str(item.get("ft_product_name") or ""),
            open_cost=_safe_float(item.get("open_cost"), None),
            # ===== native xtquant field-name aliases (m_-prefixed access) =====
            m_strAccountID=account_id,
            m_strStockCode=str(item.get("stock_code") or ""),
            m_strExchangeID=str(item.get("exchange_id") or ""),
            m_strExchangeName=str(item.get("exchange_name") or ""),
            m_strProductID=str(item.get("product_id") or ""),
            m_strInstrumentID=str(item.get("instrument_id") or ""),
            m_strInstrumentName=str(item.get("instrument_name") or ""),
            m_nDirection=_safe_int(item.get("direction"), 0),
            m_nHedgeFlag=_safe_int(item.get("hedge_flag"), 0),
            m_nPosition=_safe_int(item.get("position"), 0),
            m_nYestodayPosition=_safe_int(item.get("yesterday_position"), 0),
            m_nTodayPosition=_safe_int(item.get("today_position"), 0),
            m_nCanCloseVol=_safe_int(item.get("can_close_vol"), 0),
            m_dPositionCost=_safe_float(item.get("position_cost"), None),
            m_dAvgPrice=_safe_float(item.get("avg_price"), None),
            m_dPositionProfit=_safe_float(item.get("position_profit"), None),
            m_dFloatProfit=_safe_float(item.get("float_profit"), None),
            m_dOpenPrice=_safe_float(item.get("open_price"), None),
            m_dUsedMargin=_safe_float(item.get("used_margin"), None),
            m_dUsedCommission=_safe_float(item.get("used_commission"), None),
            m_dFrozenMargin=_safe_float(item.get("frozen_margin"), None),
            m_dFrozenCommission=_safe_float(item.get("frozen_commission"), None),
            m_dInstrumentValue=_safe_float(item.get("instrument_value"), None),
            m_nOpenTimes=_safe_int(item.get("open_times"), 0),
            m_nOpenVolume=_safe_int(item.get("open_volume"), 0),
            m_nCancelTimes=_safe_int(item.get("cancel_times"), 0),
            m_dLastPrice=_safe_float(item.get("last_price"), None),
            m_dRiseRatio=_safe_float(item.get("rise_ratio"), None),
            m_strProductName=str(item.get("product_name") or ""),
            m_dRoyalty=_safe_float(item.get("royalty"), None),
            m_strExpireDate=str(item.get("expire_date") or ""),
            m_dAssestWeight=_safe_float(item.get("assest_weight"), None),
            m_dIncreaseBySettlement=_safe_float(item.get("increase_by_settlement"), None),
            m_dMarginRatio=_safe_float(item.get("margin_ratio"), None),
            m_dFloatProfitDivideByUsedMargin=_safe_float(
                item.get("float_profit_divide_by_used_margin"), None
            ),
            m_dFloatProfitDivideByBalance=_safe_float(
                item.get("float_profit_divide_by_balance"), None
            ),
            m_dTodayProfitLoss=_safe_float(item.get("today_profit_loss"), None),
            m_nYestodayInitPosition=_safe_int(item.get("yesterday_init_position"), 0),
            m_dFrozenRoyalty=_safe_float(item.get("frozen_royalty"), None),
            m_dTodayCloseProfitLoss=_safe_float(item.get("today_close_profit_loss"), None),
            m_dCloseProfit=_safe_float(item.get("close_profit"), None),
            m_strFtProductName=str(item.get("ft_product_name") or ""),
            m_dOpenCost=_safe_float(item.get("open_cost"), None),
        )

    def query_stock_position(self, account, stock_code):
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "query_stock_position",
                {"account_id": account_id, "stock_code": stock_code},
                account_id=account_id,
            )
        except Exception:
            if not self._redis_cache_enabled():
                raise
            normalized = str(stock_code or "").strip().upper()
            data = None
            for code, item in self._cached_positions(account_id).items():
                if str(code).upper() == normalized or str(code).split(".", 1)[0].upper() == normalized:
                    data = item
                    break
            if data is None:
                raise
        if not data:
            return None
        return [
            self._position_object(account_id, item)
            for item in [data]
        ][0]

    def query_stock_orders(self, account, cancelable_only=False, strategy_name=""):
        # strategy_name 默认 ""（返回全部）：与服务端一致，避免下单用的策略名
        # 与查询默认值不匹配导致委托查不到（strategy_name 陷阱）。
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_orders",
            {
                "account_id": account_id,
                "cancelable_only": bool(cancelable_only),
                "strategy_name": strategy_name,
            },
            account_id=account_id,
        ) or []
        return [self._order_from_dict(account_id, item) for item in _as_list(data)]

    def query_stock_order(self, account, order_id):
        order_id = str(order_id or "")
        for order in self.query_stock_orders(account, cancelable_only=False):
            if str(order.order_id) == order_id or str(order.order_sysid) == order_id:
                return order
        return None

    def query_stock_trades(self, account, strategy_name=""):
        # 默认 "" = 查询账户全部成交 (与服务端 _handle_query_trades 一致)。
        # 旧默认 "bigqmt_signal_trader" 会过滤掉其他策略名的成交;
        # 按策略过滤时由调用方显式传入。
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_stock_trades",
            {"account_id": account_id, "strategy_name": strategy_name},
            account_id=account_id,
        ) or []
        return [self._trade_from_dict(account_id, item) for item in _as_list(data)]

    def describe_trade_detail_fields(self, account, detail_types=None):
        """Which attributes QMT's own ORDER / DEAL rows carry. Names only.

        A debugging aid, not part of MiniQMT: when a field comes back empty,
        this says whether the terminal is not providing it or the bridge is
        not forwarding it. Those two look identical from the client and have
        cost a deploy-and-restart each time (#113, #130, #133).

            xt_trader.describe_trade_detail_fields(account)
            -> {'ORDER': {'rows': 15, 'attributes': [...], 'error': ''}, ...}
        """
        account_id = _account_id(account, self.client.account_id)
        params = {"account_id": account_id}
        if detail_types:
            params["detail_types"] = list(detail_types)
        return self.client.call("describe_trade_detail_fields", params,
                                account_id=account_id) or {}

    def reload_deployment(self, reason="", account=None):
        """Re-import the deployed package and re-run init, without a restart.

        Returns as soon as the reload is SCHEDULED -- it runs on the next
        adjust tick, because performing it stops the RPC service answering the
        request. Poll reload_status() (or get_deployment_info()) for the
        outcome.

        Refreshes everything under bigqmt_signal_trader/. It cannot refresh
        bigqmt_signal_trader_strategy.py or the BIGQMT_REDIS_DRYRUN entry --
        QMT execs those, and a module cannot reload the one it is running in.
        Changes there still need a strategy restart.
        """
        account_id = _account_id(account, self.client.account_id)
        return self.client.call("reload_deployment", {"reason": str(reason or "")},
                                account_id=account_id) or {}

    def reload_status(self, account=None):
        """Outcome of the last reload_deployment, or what is still pending."""
        account_id = _account_id(account, self.client.account_id)
        return self.client.call("reload_status", {},
                                account_id=account_id) or {}

    def query_execution_snapshot(
        self,
        account,
        order_strategy_name="bigqmt_signal_trader",
        trade_strategy_name="",
    ):
        """Query orders and account-wide trades in one RPC round trip."""
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "query_execution_snapshot",
            {
                "account_id": account_id,
                "order_strategy_name": order_strategy_name,
                "trade_strategy_name": trade_strategy_name,
            },
            account_id=account_id,
        ) or {}
        result = dict(data) if isinstance(data, dict) else {}
        result["orders"] = [
            self._order_from_dict(account_id, item)
            for item in _as_list(result.get("orders"))
        ]
        result["trades"] = [
            self._trade_from_dict(account_id, item)
            for item in _as_list(result.get("trades"))
        ]
        return result

    def order_stock(
        self,
        account,
        stock_code,
        order_type,
        order_volume,
        price_type,
        price,
        strategy_name="",
        order_remark="",
    ):
        # [BUG-P0-20260811-bridge-order-stock-signature] strategy_name / order_remark
        # 必须有 default '' (与 miniQMT 原生 XtQuantTrader.order_stock 对齐). 此前桥接
        # 8 位置参无 default, 生产 caller (qmt_gateway.py / etf_live_broker.py) 只传
        # 6 位置 + strategy_name= keyword → 切桥接后 silent TypeError → 被 oms_signals
        # broad except 吞 → 全部 buy/sell rejected 但不崩进程不触发风控 (最阴 silent fail).
        # 加 default 让 miniQMT 时代 caller 0 改动继续工作.
        data = self.order_stock_result(
            account, stock_code, order_type, order_volume, price_type,
            price, strategy_name, order_remark,
        )
        return self._order_id(data.get("order_sys_id"))

    def _order_id(self, order_sys_id):
        """MiniQMT's return contract: a positive int, or -1 on failure.

        Big QMT only has the broker's 合同编号 string, so an OrderId carries
        both (issue #113). "-1" arrives as a *string* from the server when the
        submit itself failed, and used to be returned as one -- truthy, and
        never equal to -1, so a rejected order read as success.
        """
        text = str(order_sys_id or "").strip()
        if not text or text == "-1":
            return -1
        order_id = OrderId(text)
        self._remember_order_id(order_id)
        return order_id

    def _order_object_id(self, order_sys_id):
        """``order_id`` for an XtOrder / XtTrade: int, empty stays empty.

        Unlike the order_stock return there is no -1 here -- a query result
        either has an id or does not.
        """
        text = str(order_sys_id or "").strip()
        if not text:
            return OrderId("")
        order_id = OrderId(text)
        self._remember_order_id(order_id)
        return order_id

    def _remember_order_id(self, order_id):
        """Keep int -> 合同编号 so a cancel still works after a round trip.

        A caller who stores the id in JSON or a database gets a plain int back,
        losing the string half. Bounded: this is a convenience, not a ledger.
        """
        sys_id = getattr(order_id, "order_sys_id", "")
        if not sys_id or str(int(order_id)) == sys_id:
            return                       # nothing to remember: they agree
        table = self._order_sys_ids
        table[int(order_id)] = sys_id
        while len(table) > _ORDER_ID_MEMORY:
            table.popitem(last=False)

    def _resolve_order_sys_id(self, value):
        """The broker string for whatever a caller passed to a cancel."""
        carried = getattr(value, "order_sys_id", None)
        if carried:
            return str(carried)
        if isinstance(value, int) and not isinstance(value, bool):
            remembered = self._order_sys_ids.get(int(value))
            if remembered:
                return remembered
        return order_sys_id_of(value)

    def order_stock_result(
        self, account, stock_code, order_type, order_volume, price_type,
        price, strategy_name="", order_remark="", wait_settlement=True,
    ):
        """Submit one order over RPC.

        ``wait_settlement=False`` tells the server to reply as soon as passorder
        returns instead of holding the reply until QMT assigns the order id.
        The async path uses it; the id then arrives through order_callback
        (issue #50).
        """
        account_id = _account_id(account, self.client.account_id)
        user_order_id = str(order_remark or "").strip()
        if not user_order_id:
            user_order_id = "bqrpc:%s:%s" % (int(time.time() * 1000), uuid.uuid4().hex[:10])
        payload = {
            "account_id": account_id,
            "stock_code": stock_code,
            "order_type": order_type,
            "order_volume": order_volume,
            "price_type": price_type,
            "price": price,
            "strategy_name": strategy_name,
            "order_remark": user_order_id,
        }
        if not wait_settlement:
            payload["wait_settlement"] = False
        try:
            return self.client.call("order_stock", payload, account_id=account_id) or {}
        except TimeoutError as exc:
            raise TimeoutError(
                "order_stock rpc timeout; user_order_id=%s. Query orders/trades before retrying to avoid duplicate orders. %s"
                % (user_order_id, exc)
            )

    def _async_order_worker(self):
        """Drain queued async orders, one at a time.

        A single worker rather than a pool: the server handles order RPCs on
        the QMT adjust thread serially anyway, so concurrency here buys little,
        while serializing keeps on_order_stock_async_response arriving in
        submission order. For real batch throughput use order_stock_batch.
        """
        while True:
            job = self._async_order_queue.get()
            if job is None:          # shutdown sentinel
                self._async_order_queue.task_done()
                return
            seq, args, kwargs = job
            try:
                self._run_async_order(seq, args, kwargs)
            except Exception:
                # A worker that dies takes every later async order with it.
                pass
            finally:
                self._async_order_queue.task_done()

    def _ensure_async_order_worker(self):
        with self._async_order_lock:
            if self._async_order_thread is not None and self._async_order_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._async_order_worker, name="bigqmt-async-order", daemon=True
            )
            self._async_order_thread = thread
            thread.start()

    def _register_exit_drain(self):
        """atexit hook, registered once: a fire-and-forget script that exits
        right after queueing must not drop the queued orders silently
        (issue #156)."""
        with self._async_order_lock:
            if getattr(self, "_exit_drain_registered", False):
                return
            self._exit_drain_registered = True
        import atexit
        atexit.register(self._drain_async_orders_on_exit)

    def _drain_async_orders_on_exit(self):
        """Best-effort flush of queued async orders at stop()/process exit.

        The async worker is a daemon thread: a script that exits right after
        queueing kills it mid-queue, and every order past the first is lost
        without a word (issue #156: "循环下单只有第一条成功，加 sleep 才正常"
        -- the sleep was keeping the process alive; reproduced live as 1/3 vs
        3/3 orders reaching QMT). Drain the queue, then give armed barriers a
        bounded moment so in-flight responses can fire their callbacks.
        Never raises: this runs at interpreter exit and inside stop().
        """
        try:
            self.wait_async_orders(timeout=self.ASYNC_EXIT_DRAIN_SECONDS)
        except Exception:
            pass
        barrier_lock = getattr(self, "_async_barrier_lock", None)
        if barrier_lock is None:
            return
        deadline = time.time() + self.ASYNC_EXIT_CALLBACK_GRACE_SECONDS
        try:
            while time.time() < deadline:
                with barrier_lock:
                    if not getattr(self, "_async_barrier", None):
                        return
                time.sleep(0.05)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # issue #51 A: 同一笔委托的 async_response 必须先于它的 order/trade 到达。
    #
    # 两条回调走的是不同线程和不同通道: async_response 在异步下单的工作线程上
    # 触发, order/trade 来自 Redis pub/sub 监听线程。服务端在 order_callback
    # 里先推事件再回 RPC, 所以顺序颠倒是常态而非偶发。
    #
    # 做法是给「已提交但尚未收到 async_response」的委托设一道屏障: 它的
    # order/trade 事件先暂存, 等 response 触发后按到达顺序放行。延迟只加在
    # 异步下单这一条路径上——手工下单、同步下单、以及任何未登记的委托一律直通。
    # ------------------------------------------------------------------
    ASYNC_BARRIER_TIMEOUT_SECONDS = 10.0
    # response 触发前等屏障从暂存的委托事件里学到委托号的上限（issue #72）。
    # 委托号异步分配：推送通常比 RPC 应答快，几百毫秒内就能学到；超时则按
    # 原样发 response（order_id 回落 remark），不拖住回调。
    ASYNC_SYSID_WAIT_SECONDS = 2.0
    # stop()/进程退出时：等队列里已排队的 async 委托发完的上限，以及给
    # 在途 response 触发回调的宽限（issue #156）。
    ASYNC_EXIT_DRAIN_SECONDS = 5.0
    ASYNC_EXIT_CALLBACK_GRACE_SECONDS = 3.0

    def _order_barrier(self):
        barrier = getattr(self, "_async_barrier", None)
        if barrier is None:
            barrier = {}
            self._async_barrier = barrier
            self._async_barrier_lock = threading.Lock()
        return barrier

    @staticmethod
    def _async_remark(args, kwargs):
        """下单调用里的 order_remark —— 拿到 order_sys_id 之前唯一的关联键。"""
        return str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "") or "")

    def _arm_order_barrier(self, remark, seq):
        """登记一笔待响应的委托。remark 为空则不设屏障(无从关联)。"""
        if not remark:
            return
        self._order_barrier()
        with self._async_barrier_lock:
            # remark 不强制唯一(网格类策略常复用同一 remark)。同 remark 的上一笔
            # 可能还扣着暂存事件, 直接覆盖会把它们永久丢掉——丢事件比顺序错乱
            # 更糟, 所以接管旧 entry 并在锁外放行它的事件。
            superseded = self._async_barrier.pop(remark, None)
            self._async_barrier[remark] = {
                "seq": seq,
                "sys_ids": set(),
                "events": [],
                "deadline": time.time() + self.ASYNC_BARRIER_TIMEOUT_SECONDS,
            }
        for event in (superseded or {}).get("events", []):
            self._deliver_event(event)

    def _release_order_barrier(self, remark, seq=None):
        """response 已触发, 按到达顺序放行暂存的事件。"""
        if not remark:
            return
        self._order_barrier()
        with self._async_barrier_lock:
            entry = self._async_barrier.get(remark)
            if entry is None:
                return
            if seq is not None and entry["seq"] != seq:
                # 同 remark 的后一笔委托已接管屏障; 前一笔的 response 不该放它。
                return
            entry = self._async_barrier.pop(remark, None)
        for event in (entry or {}).get("events", []):
            self._deliver_event(event)

    def _sweep_order_barriers(self):
        """放行超时未收到 response 的委托。

        没有这一步, 一次失败的提交会把它的事件永久扣住——丢事件比顺序错乱更糟。
        """
        now = time.time()
        expired = []
        with self._async_barrier_lock:
            for remark, entry in list(self._async_barrier.items()):
                if now >= entry["deadline"]:
                    expired.append(self._async_barrier.pop(remark))
        for entry in expired:
            for event in entry.get("events", []):
                self._deliver_event(event)

    def _hold_if_pending(self, event):
        """属于待响应委托则暂存并返回 True, 否则返回 False 直通。"""
        barrier = self._order_barrier()
        if not barrier:
            return False
        remark = str(event.get("remark") or event.get("user_order_id") or "")
        sys_id = str(event.get("order_sys_id") or "")
        with self._async_barrier_lock:
            entry = barrier.get(remark) if remark else None
            if entry is None and sys_id:
                # 成交事件可能没有 remark; 用委托事件里学到的 order_sys_id 关联。
                for candidate in barrier.values():
                    if sys_id in candidate["sys_ids"]:
                        entry = candidate
                        break
            if entry is None:
                return False
            if sys_id:
                entry["sys_ids"].add(sys_id)
            entry["events"].append(event)
            return True

    def _run_async_order(self, seq, args, kwargs):
        """Do the actual submit and fire the matching callback. Worker thread."""
        stock_code = str(kwargs.get("stock_code") or (args[1] if len(args) > 1 else ""))
        remark = self._async_remark(args, kwargs)
        callback = self.callback
        try:
            # wait_settlement=False：passorder 一返回就应答，不在 worker 里等
            # 服务端结算（那是 #69 要的吞吐）。委托号从推送事件学——屏障暂存的
            # 委托事件里会带上（触发 response 前至多等 2s，学不到就回落 remark）。
            result = self.order_stock_result(*args, wait_settlement=False, **kwargs)
        except Exception as exc:
            if callback is not None:
                try:
                    callback.on_order_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sysid="",          # MiniQMT 规范名 (issue #65)
                            order_sys_id="",
                            order_id=self._order_object_id(""),   # int (#113)
                            stock_code=stock_code,
                            seq=seq,
                            order_remark=str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "") or ""),
                        )
                    )
                except Exception:
                    log.exception("user callback failed: on_order_error seq=%s", seq)
            self._release_order_barrier(remark, seq)
            return

        order_sys_id = ""
        user_order_id = ""
        if isinstance(result, dict):
            order_sys_id = str(result.get("order_sys_id") or result.get("order_sysid") or "")
            user_order_id = str(result.get("user_order_id") or "")
        elif result is not None:
            order_sys_id = str(result)

        # order_stock returns -1 when the submit itself failed. The server also
        # pushes an order_error for a 废单; the two carry different information
        # (RPC submit failure vs QMT rejection detail), so both stay available.
        if order_sys_id == "-1" or result == -1:
            if callback is not None:
                try:
                    callback.on_order_error(
                        CompatObject(
                            error_id=-1,
                            error_msg="order submit failed (order_stock returned -1)",
                            order_sysid="",          # MiniQMT 规范名 (issue #65)
                            order_sys_id="",
                            order_id=self._order_object_id(""),   # int (#113)
                            stock_code=stock_code,
                            seq=seq,
                            order_remark=str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "") or ""),
                        )
                    )
                except Exception:
                    log.exception("user callback failed: on_order_error seq=%s", seq)
            self._release_order_barrier(remark, seq)
            return

        if callback is not None:
            try:
                # Native XtOrderResponse shape: one argument carrying
                # account_id/order_id/seq/error_msg.
                #
                # 委托号异步分配（#50）：服务端应答时通常还没有。触发 response 前
                # 先等屏障从暂存的委托事件里学到委托号（事件推送一般比 RPC 应答
                # 快，bounded 2s），否则 order_id 只能回落成 remark，调用方按
                # order_id 管理委托时会拿不到真实委托号（issue #72）。
                if not order_sys_id and remark:
                    wait_deadline = time.time() + self.ASYNC_SYSID_WAIT_SECONDS
                    while time.time() < wait_deadline:
                        learned = ""
                        with self._async_barrier_lock:
                            entry = self._async_barrier.get(remark)
                            if entry and entry["sys_ids"]:
                                learned = sorted(entry["sys_ids"])[0]
                        if learned:
                            order_sys_id = learned
                            break
                        time.sleep(0.05)
                callback.on_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        order_id=self._order_object_id(order_sys_id or user_order_id),
                        order_sysid=order_sys_id,    # MiniQMT 规范名 (issue #65)
                        order_sys_id=order_sys_id,
                        stock_code=stock_code,
                        strategy_name=str(kwargs.get("strategy_name") or (args[6] if len(args) > 6 else "")),
                        order_remark=str(kwargs.get("order_remark") or (args[7] if len(args) > 7 else "")),
                        error_msg="",
                    ),
                )
            except Exception:
                log.exception(
                    "user callback failed: on_order_stock_async_response seq=%s", seq
                )
        # response 已触发 -> 放行这笔委托暂存的 order/trade (issue #51)。
        self._release_order_barrier(remark, seq)

    def order_stock_async(self, *args, **kwargs):
        """Queue an order and return its seq immediately (MiniQMT semantics).

        This used to call order_stock inline, so it blocked for the full RPC
        round trip plus -- after the issue #44 change -- however long the server
        waited for QMT to assign an order id. That is 0.5-1s per order, which
        defeats the point of an async API (issue #50).

        Now the submit runs on a worker thread and the outcome arrives through
        on_order_stock_async_response / on_order_error, both carrying the seq so
        callers can correlate. Returns the seq without touching the network.

        The worker is a daemon thread: a script whose main thread exits right
        after queueing kills it mid-queue, losing every order not yet
        submitted (issue #156). stop() and an atexit hook both drain the queue
        (bounded); callbacks still require the process to be alive -- they
        cannot arrive after it is gone. Long-running strategies are unaffected.
        """
        seq = self._next_async_seq()
        # 屏障要在入队之前设好: 委托可能在本函数返回之前就被推送出来。
        self._arm_order_barrier(self._async_remark(args, kwargs), seq)
        self._ensure_async_order_worker()
        self._register_exit_drain()
        self._async_order_queue.put((seq, args, kwargs))
        return seq

    def wait_async_orders(self, timeout=10.0):
        """Block until every queued async order has been submitted.

        For tests and for shutdown; the API itself is fire-and-forget. Returns
        False on timeout rather than hanging. Uses task_done bookkeeping, so it
        waits for the in-flight job too, not merely for the queue to drain.
        """
        queue_obj = getattr(self, "_async_order_queue", None)
        if queue_obj is None:
            return True
        deadline = time.time() + float(timeout)
        while queue_obj.unfinished_tasks:
            if time.time() > deadline:
                return False
            time.sleep(0.005)
        return True

    def order_stock_batch(self, account, orders, batch_id=""):
        account_id = _account_id(account, self.client.account_id)
        payload = []
        for item in orders or []:
            entry = dict(item or {})
            entry.setdefault("account_id", account_id)
            payload.append(entry)
        params = {"account_id": account_id, "orders": payload}
        if batch_id:
            params["batch_id"] = str(batch_id)
        return self.client.call(
            "order_stock_batch",
            params,
            account_id=account_id,
        ) or []

    def cancel_order_stock_sysid(self, account, market, order_sysid):
        """MiniQMT contract: 0 on success, -1 on failure (issue #113).

        This returned a bool, which is worse than a type mismatch -- it inverts
        the meaning. ``if trader.cancel_order_stock(...) == 0`` is how MiniQMT
        code checks success, and ``False == 0`` is True, so a *failed* cancel
        read as a successful one while a successful one read as failed.
        """
        account_id = _account_id(account, self.client.account_id)
        data = self.client.call(
            "cancel_order_stock_sysid",
            {
                "account_id": account_id,
                "market": market,
                # Send the broker's own 合同编号, not the int we derived from it.
                "order_sysid": self._resolve_order_sys_id(order_sysid),
            },
            account_id=account_id,
        ) or {}
        return 0 if bool(data.get("success", data)) else -1

    def cancel_order_stock(self, account, order_id):
        return self.cancel_order_stock_sysid(account, "", order_id)

    def unsubscribe(self, account):
        # MiniQMT xttrader.unsubscribe(account) — 取消账户订阅。
        # Big QMT RPC 模式下账户是被动响应，unsubscribe 为 no-op。
        return 0

    # ------------------------------------------------------------------
    # 账户 / 融资融券扩展查询
    # 这些在 MiniQMT 走 XtQuantServer RPC；Big QMT 经
    # get_trade_detail_data 查询，需相应账户权限（两融账户等）。
    # 无权限/上下文未绑定时服务端降级为 []。
    # ------------------------------------------------------------------

    def _query_account_list(self, account, method):
        account_id = _account_id(account, self.client.account_id)
        try:
            return self.client.call(method, {"account_id": account_id}, account_id=account_id) or []
        except Exception:
            return []

    def query_account_infos(self, account=None):
        return self._query_account_list(account, "query_account_infos")

    def query_account_status(self, account=None):
        return self._query_account_list(account, "query_account_status")

    def query_credit_detail(self, account):
        return self._query_account_list(account, "query_credit_detail")

    def query_stk_compacts(self, account):
        return self._query_account_list(account, "query_stk_compacts")

    def query_credit_subjects(self, account):
        return self._query_account_list(account, "query_credit_subjects")

    def query_credit_slo_code(self, account):
        return self._query_account_list(account, "query_credit_slo_code")

    def query_credit_assure(self, account):
        return self._query_account_list(account, "query_credit_assure")

    def query_appointment_info(self, account):
        return self._query_account_list(account, "query_appointment_info")

    def query_smt_secu_info(self, account):
        return self._query_account_list(account, "query_smt_secu_info")

    def query_smt_secu_rate(self, account, stock_code, max_term, fare_way, credit_type, trade_type):
        account_id = _account_id(account, self.client.account_id)
        try:
            return self.client.call(
                "query_smt_secu_rate",
                {"account_id": account_id, "stock_code": stock_code, "max_term": max_term,
                 "fare_way": fare_way, "credit_type": credit_type, "trade_type": trade_type},
                account_id=account_id,
            ) or []
        except Exception:
            return []

    def query_ipo_data(self, account=None, stock_type=""):
        """新股申购信息 (大 QMT get_ipo_data).
        stock_type: "" 全部, "STOCK" 新股, "BOND" 新债.
        8-28 修复: 直接调 get_ipo_data 带 type 参数 (原走 query_appointment_info
        传 account_id 导致返回空)."""
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "get_ipo_data",
                {"type": stock_type},
                account_id=account_id,
            )
            # get_ipo_data answers with a dict keyed by subscription code.
            if isinstance(data, dict):
                return data
            if data:
                # Non-empty and not a dict: an older server is still routing
                # this through the detail-row normaliser, which iterates the
                # dict by key and discards every value -- real IPOs arrive as
                # [{}, {}]. Coercing that to {} silently reports "no IPOs
                # today", so say so instead of swallowing it.
                log.warning(
                    "query_ipo_data: server returned %s, not a mapping -- the "
                    "QMT-side bridge is too old to preserve get_ipo_data's "
                    "shape and the rows are empty. Update the server side.",
                    type(data).__name__)
            return {}
        except Exception:
            return {}

    def query_new_purchase_limit(self, account):
        """新股申购额度 (大 QMT get_new_purchase_limit).
        返回 {板块: 额度} 或 {} (失败/无权限)."""
        account_id = _account_id(account, self.client.account_id)
        try:
            data = self.client.call(
                "get_new_purchase_limit",
                {"account_id": account_id},
                account_id=account_id,
            ) or {}
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}

    def ipo_subscribe_all(self, account=None, stock_type="STOCK",
                          markets=("SH", "SZ"), dry_run=False,
                          strategy_name="ipo"):
        """Subscribe to today's IPOs. Nothing here runs on a timer.

        This is deliberately a call you make, not a behaviour the bridge takes
        on: it places real orders, so it must be something the operator asked
        for on that day. It also goes through order_stock, which means it obeys
        rpc_allow_order_methods and inherits the gateway's passorder settings
        (orderType 1101, prType 11 指定价, quickTrade 2 -- 2 being the value the
        API reference requires for a non-bar context).

        markets: exchanges to subscribe on. SH/SZ subscriptions are backed by
            market value and freeze no cash; BJ freezes cash, so it is excluded
            by default. A code whose market cannot be identified is SKIPPED,
            never subscribed on a guess.
        dry_run: return the plan without placing anything.

        Returns one dict per candidate: stock_code, name, price, volume,
        action ("subscribed" / "skipped" / "failed") and reason.
        """
        allowed = set(str(m).upper() for m in (markets or ()))
        results = []
        for code, info in (self.query_ipo_data(account, stock_type=stock_type) or {}).items():
            info = info or {}
            entry = {
                "stock_code": code,
                "name": str(info.get("name") or ""),
                "price": _safe_float(info.get("issuePrice"), 0.0),
                "volume": _safe_int(info.get("maxPurchaseNum"), 0),
                "action": "skipped",
                "reason": "",
            }
            market = ipo_market_of(code)
            if market is None:
                entry["reason"] = "market not identified"
            elif market not in allowed:
                entry["reason"] = "%s not in %s" % (market, sorted(allowed))
            elif entry["price"] <= 0 or entry["volume"] <= 0:
                entry["reason"] = "issuePrice/maxPurchaseNum missing or non-positive"
            elif dry_run:
                entry["action"] = "planned"
            else:
                try:
                    entry["result"] = self.ipo_subscribe(
                        account, code, entry["volume"], entry["price"],
                        strategy_name=strategy_name,
                        order_remark="ipo:%s" % code)
                    entry["action"] = "subscribed"
                except Exception as exc:
                    entry["action"] = "failed"
                    entry["reason"] = "%s: %s" % (exc.__class__.__name__, exc)
            results.append(entry)
        return results

    def ipo_subscribe(self, account, stock_code, volume, price, strategy_name="ipo",
                      order_remark="ipo_sub"):
        """新股申购 (打新). 复用现有 order_stock RPC (passorder opType=23 指定价).
        stock_code 应为 get_ipo_data 返回的申购代码 (带后缀), price=发行价.
        返回 {order_sys_id} 或 {} (失败)."""
        return self.order_stock_result(
            account, stock_code, STOCK_BUY, int(volume),
            FIX_PRICE, float(price), strategy_name, order_remark,
        )

    # ------------------------------------------------------------------
    # async 变体：MiniQMT 的 *_async 方法返回 seq 后异步回调。
    # 在 RPC 模型里请求-响应本就是同步的，这里直接转发到同步实现并
    # 返回一个递增 seq，让旧代码 ``xt_trader.query_stock_positions_async(acc)``
    # 不报错（回调仍由 register_callback 注册的回调在事件来时触发）。
    # ------------------------------------------------------------------

    _async_seq = 0

    def _next_async_seq(self):
        BigQmtXtTrader._async_seq += 1
        return BigQmtXtTrader._async_seq

    def _async_query(self, sync_call, account, callback, *args, **kwargs):
        """Shared async query helper.

        MiniQMT's *_async query methods take a callback and hand the result to
        it (they return None). We accept an OPTIONAL callback for compat: when
        given, we call callback(result) synchronously (our RPC is already
        synchronous) and return None like MiniQMT; when omitted, we keep our
        seq-returning extension so existing callers don't break.
        """
        result = sync_call(account, *args, **kwargs)
        if callback is not None:
            try:
                callback(result)
            except Exception:
                log.exception(
                    "user callback failed: %s",
                    getattr(sync_call, "__name__", "async_query_callback"),
                )
            return None
        return self._next_async_seq()

    def query_stock_asset_async(self, account, callback=None):
        return self._async_query(self.query_stock_asset, account, callback)

    def query_stock_positions_async(self, account, callback=None):
        return self._async_query(self.query_stock_positions, account, callback)

    def query_stock_orders_async(self, account, cancelable_only=False, callback=None):
        if callback is not None:
            result = self.query_stock_orders(account, cancelable_only)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_stock_trades_async(self, account, callback=None):
        return self._async_query(self.query_stock_trades, account, callback)

    def query_account_infos_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_infos(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_account_status_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_account_status(account)
            try:
                callback(result)
            except Exception:
                pass
            return None
        return self._next_async_seq()

    def query_credit_detail_async(self, account, callback=None):
        return self._async_query(self.query_credit_detail, account, callback)

    def query_stk_compacts_async(self, account, callback=None):
        return self._async_query(self.query_stk_compacts, account, callback)

    def query_credit_subjects_async(self, account, callback=None):
        return self._async_query(self.query_credit_subjects, account, callback)

    def query_credit_slo_code_async(self, account, callback=None):
        return self._async_query(self.query_credit_slo_code, account, callback)

    def query_credit_assure_async(self, account, callback=None):
        return self._async_query(self.query_credit_assure, account, callback)

    def query_ipo_data_async(self, account=None, callback=None):
        if callback is not None:
            result = self.query_ipo_data(account)
            try:
                callback(result)
            except Exception:
                log.exception("user callback failed: query_ipo_data_async")
            return None
        return self._next_async_seq()

    def query_new_purchase_limit_async(self, account, callback=None):
        return self._async_query(self.query_new_purchase_limit, account, callback)

    def query_appointment_info_async(self, account, callback=None):
        return self._async_query(self.query_appointment_info, account, callback)

    def cancel_order_stock_async(self, account, order_id):
        # MiniQMT: returns seq, result comes back via on_cancel_order_stock_async_response.
        seq = self._next_async_seq()
        try:
            # 0 == success now (MiniQMT contract, issue #113), so a bare
            # truthiness test on the return would read backwards.
            ok = self.cancel_order_stock(account, order_id) == 0
        except Exception as exc:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_cancel_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sysid=str(order_id or ""),
                            order_sys_id=str(order_id or ""),
                            order_id=self._order_object_id(order_id),
                            stock_code="",
                        )
                    )
                except Exception:
                    log.exception("user callback failed: on_cancel_error")
            return seq
        callback = self.callback
        if callback is not None:
            try:
                callback.on_cancel_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        success=bool(ok),
                        # MiniQMT XtCancelOrderResponse 契约: cancel_result=0 成功,
                        # 失败时给出非零错误码和可读 error_msg。
                        cancel_result=0 if ok else -1,
                        error_msg="" if ok else "cancel_order_stock rejected by server",
                        order_sysid=str(order_id or ""),
                        order_sys_id=str(order_id or ""),
                        order_id=self._order_object_id(order_id),
                    ),
                )
            except Exception:
                log.exception(
                    "user callback failed: on_cancel_order_stock_async_response seq=%s", seq
                )
        return seq

    def cancel_order_stock_sysid_async(self, account, market, order_sysid):
        seq = self._next_async_seq()
        try:
            ok = self.cancel_order_stock_sysid(account, market, order_sysid) == 0
        except Exception as exc:
            callback = self.callback
            if callback is not None:
                try:
                    callback.on_cancel_error(
                        CompatObject(
                            error_id=getattr(exc, "errno", 0),
                            error_msg=str(exc),
                            order_sysid=str(order_sysid or ""),
                            order_sys_id=str(order_sysid or ""),
                            order_id=self._order_object_id(order_sysid),
                            stock_code="",
                        )
                    )
                except Exception:
                    log.exception("user callback failed: on_cancel_error")
            return seq
        callback = self.callback
        if callback is not None:
            try:
                callback.on_cancel_order_stock_async_response(
                    CompatObject(
                        account_id=self.client.account_id,
                        seq=seq,
                        success=bool(ok),
                        # MiniQMT XtCancelOrderResponse 契约: cancel_result=0 成功,
                        # 失败时给出非零错误码和可读 error_msg。
                        cancel_result=0 if ok else -1,
                        error_msg="" if ok else "cancel_order_stock rejected by server",
                        order_sysid=str(order_sysid or ""),
                        order_sys_id=str(order_sysid or ""),
                        order_id=self._order_object_id(order_sysid),
                    ),
                )
            except Exception:
                log.exception(
                    "user callback failed: on_cancel_order_stock_async_response seq=%s", seq
                )
        return seq

    def set_relaxed_response_order_enabled(self, enabled=True):
        # 内部行为开关，RPC 模式下无意义，no-op。
        return 0

    def smt_appointment_async(self, account, stock_code, apt_days, apt_volume,
                              fare_ratio, sub_rare_ratio, fine_ratio, begin_date):
        # SMB/预约打新走独立通道，RPC 桥不支持；返回 -1 表示失败（对齐 MiniQMT
        # 语义：seq 为 -1 表示委托失败）。
        return -1

    def _account_type_value(self, item=None):
        """account_type for an XtOrder / XtTrade / XtPosition, as an int.

        Server first (it is the one that knows what this deployment trades
        as -- #103), then what the caller declared, then SECURITY_ACCOUNT so
        the field is never absent. Positions used to hardcode 2 and orders and
        trades did not carry it at all (#133).
        """
        code = _account_type_code((item or {}).get("account_type"))
        if code:
            return code
        # [merge 2026-09-03] getattr 防御: __new__ 绕 init 的构造形态 (主仓端到端
        # 测试/轻量宿主) 不带这两个属性 — 两者本就是可选线索, 缺失即落 SECURITY 兜底。
        code = _account_type_code(getattr(self, "_server_account_type", None)
                                 or getattr(self, "_declared_account_type", None))
        if code:
            return code
        try:
            from xtquant.xtconstant import SECURITY_ACCOUNT

            return int(SECURITY_ACCOUNT)
        except Exception:
            return 2

    def _order_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = _action_to_order_type(action)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or item.get("order_id") or "")
        return CompatObject(
            account_id=account_id,
            stock_code=_full_a_share_code(item.get("stock_code")),
            order_type=order_type,
            order_status=_safe_int(item.get("status", item.get("order_status")), ORDER_UNKNOWN),
            order_volume=_safe_int(item.get("volume", item.get("order_volume"))),
            traded_volume=_safe_int(item.get("traded_volume")),
            price=_safe_float(item.get("price")),
            traded_price=_safe_float(
                item.get("traded_price", item.get("avg_traded_price", item.get("m_dTradedPrice")))
            ),
            order_sysid=order_sysid,
            # MiniQMT: order_id is the int 委托编号, order_sysid the string
            # 柜台编号. Both, from one 合同编号 (issue #113).
            order_id=self._order_object_id(
                order_sysid or str(item.get("user_order_id") or "")),
            strategy_name=str(item.get("strategy_name") or ""),
            order_remark=str(item.get("remark") or item.get("user_order_id") or ""),
            # MiniQMT XtOrder.order_time 是 Unix 秒。服务端订单事件只发
            # created_at_ts 不发 order_time，所以没有显式 order_time 时
            # 用 created_at_ts 兜底；两者都没有才落到 0（不要当成 1970 年）。
            order_time=_safe_int(item.get("order_time") or item.get("created_at_ts"), 0),
            # MiniQMT XtOrder.status_msg —— 废单时柜台给的原因 (issue #60)。
            status_msg=str(item.get("status_msg") or ""),
            price_type=item.get("price_type"),
            # xttype.XtOrder 契约里有、以前没发的字段 (issue #133)。旧部署不发
            # 这些键，所以每个都要能在缺失时给出 MiniQMT 语义的默认值，而不是
            # 让调用方撞 AttributeError —— 那正是这个 issue 报的现象。
            account_type=self._account_type_value(item),
            instrument_name=str(item.get("instrument_name") or ""),
            secu_account=str(item.get("secu_account") or ""),
            offset_flag=item.get("offset_flag"),
            direction=item.get("direction"),
        )

    def _trade_from_dict(self, account_id, item):
        action = item.get("action")
        order_type = _action_to_order_type(action)
        order_sysid = str(item.get("order_sys_id") or item.get("order_sysid") or "")
        trade_id = str(item.get("trade_id") or "")
        traded_volume = _safe_int(item.get("volume", item.get("traded_volume")))
        traded_price = _safe_float(item.get("price", item.get("traded_price")))
        amount = item.get("amount")
        if not amount:
            # 服务端未取到金额（缺失或 0）时按 价格 * 数量 估算，保证盈亏统计不为 0。
            amount = traded_price * traded_volume
        return CompatObject(
            account_id=account_id,
            stock_code=_full_a_share_code(item.get("stock_code")),
            order_type=order_type,
            # [fix 2026-08-26 state.trades 丢笔] 网关读 offset_flag(48/49)判 BUY/SELL,
            # 缺失时 OMS 重建 fail-CLOSED 丢笔 — 三源推导见 _offset_flag_from_item.
            offset_flag=_offset_flag_from_item(item, action),
            order_sysid=order_sysid,
            order_id=self._order_object_id(order_sysid),
            trade_id=trade_id,
            # MiniQMT 字段契约: traded_id/traded_time 是业务代码读取的名字。
            traded_id=trade_id,
            traded_volume=traded_volume,
            traded_price=traded_price,
            # 优先级: 服务端真实成交时间(traded_time) -> 事件到达时间(created_at_ts)
            # -> traded_at 字符串解析。
            traded_time=_to_unix_seconds(
                item.get("traded_time") or item.get("created_at_ts") or item.get("traded_at")
            ),
            traded_amount=_safe_float(amount, 0.0),
            traded_at=str(item.get("traded_at") or ""),
            strategy_name=str(item.get("strategy_name") or ""),
            order_remark=str(item.get("user_order_id") or item.get("remark") or ""),
            # 同 _order_from_dict：xttype.XtTrade 契约里有而以前没发的 (issue #133)。
            account_type=self._account_type_value(item),
            instrument_name=str(item.get("instrument_name") or ""),
            secu_account=str(item.get("secu_account") or ""),
            commission=_safe_float(item.get("commission"), 0.0),
            # offset_flag 已由上方 _offset_flag_from_item 三源推导 (本地 2026-08-26
            # 丢笔修复, 首选源即裸 offset_flag, 严格超集), 不重复透传 [merge 裁决]。
            direction=item.get("direction"),
        )


XtQuantTrader = BigQmtXtTrader


_default_client = None
xt_trader = None
xtdata = None


def configure(account_id=None, redis_client=None, redis_config=None, timeout_seconds=None):
    global _default_client, xt_trader, xtdata
    _default_client = BigQmtRpcClient(
        account_id=account_id,
        redis_client=redis_client,
        redis_config=redis_config,
        timeout_seconds=timeout_seconds,
    )
    if xt_trader is None:
        xt_trader = BigQmtXtTrader(account_id=_default_client.account_id, redis_client=_default_client.redis_client)
    xt_trader.client = _default_client
    if xtdata is None:
        xtdata = BigQmtXtData(_default_client)
    else:
        xtdata.client = _default_client
    return xt_trader, xtdata


def get_default_client():
    global _default_client
    if _default_client is None:
        configure()
    return _default_client


configure()


__all__ = [
    "BigQmtRpcClient",
    "BigQmtXtData",
    "BigQmtXtTrader",
    "CompatObject",
    "StockAccount",
    "XtQuantTrader",
    "XtQuantTraderCallback",
    "configure",
    "get_default_client",
    "load_client_config",
    "xt_trader",
    "xtdata",
]
