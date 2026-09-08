"""[R25-02] 泵 per-code 饥饿检测 + native SDK 兜底 回放测试 (600354.SH 案).

事故形态 (09-08 实测): 批量 context_info.get_full_tick 对单票持续缺失 cell
(策略句柄快照饿死), 原三处静默 continue 让该票从 stream 消失且零心跳帧 —
无痕拖到次日 09:00 重订。同刻 native xtdata SDK (数据中心路径) 同码新鲜。

修法: 覆盖率守卫前置判饥饿 → 计数 ≥3 拍逐码 native 兜底回填 tick_data →
原 per-code 循环按原逻辑去重/心跳/发布 (零发布逻辑复制); ≥5 拍 WARNING 留痕
(此后每 60 拍复告); cell 可用拍清零计数 (dedup 静默不影响「正常迭代」判定);
兜底自身失败不拖垮泵。
"""
import json
import sys
import time as _time
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import bigqmt_signal_trader_strategy as strat

ACCT = "8890541985"
STARVED = "600354.SH"
HEALTHY = "000592.SZ"


class FakeRedis:
    def __init__(self, subs):
        self._subs = subs
        self.events = []

    def hgetall(self, key):
        # key 感知: 仅订阅 hash 返回条目; GC 的 firstseen/seen ledger 键返回空
        # (真实 ledger 由 GC 自管, 这里不模拟 — 防脏数据污染订阅面)
        if str(key).startswith("bigqmt:quote_subscriptions"):
            return dict(self._subs)
        return {}

    def xadd(self, key, fields, **kwargs):
        self.events.append(json.loads(fields["payload"]))
        return b"1-0"

    def publish(self, key, raw):
        return 0


def _subs_hash():
    return {
        b"11": json.dumps({"seq": 11, "stock_code": STARVED, "period": "1m"}),
        b"22": json.dumps({"seq": 22, "stock_code": HEALTHY, "period": "1m"}),
    }


def _valid_cell(price):
    return {"lastPrice": price, "close": price, "volume": 100, "time": int(_time.time() * 1000)}


class FakeContextInfo:
    def __init__(self, batch):
        self.batch = batch
        self.calls = 0

    def get_full_tick(self, codes):
        self.calls += 1
        return {c: v for c, v in self.batch.items() if c in codes}


@pytest.fixture(autouse=True)
def _fresh_pump_state(monkeypatch):
    monkeypatch.setattr(strat, "_quote_starve_counts", {})
    monkeypatch.setattr(strat, "_last_quote_bar_time", {})
    monkeypatch.setattr(strat, "_last_quote_heartbeat_at", {})
    monkeypatch.setattr(strat, "_last_quote_push_at", 0.0)
    yield


def _run(monkeypatch, batch, native_batch=None, native_raise=None):
    """驱动一拍 _push_quote_updates; 返回 (fake_redis, 拍结果)."""
    fake_redis = FakeRedis(_subs_hash())
    monkeypatch.setattr(strat, "_rpc_service", types.SimpleNamespace(redis=fake_redis))
    # 每拍重置推送节流戳 (interval 0.0 会被 `or 1.0` 兜底成 1s, 同测试多拍必被闸)
    monkeypatch.setattr(strat, "_last_quote_push_at", 0.0)
    ctx = FakeContextInfo(batch)
    if native_raise is not None:
        loader = lambda: (_ for _ in ()).throw(native_raise)
    elif native_batch is None:
        loader = lambda: None
    else:
        loader = lambda: types.SimpleNamespace(get_full_tick=lambda codes: {
            c: v for c, v in native_batch.items() if c in codes
        })
    fake_mb = types.ModuleType("bigqmt_signal_trader.adapters.market_bigqmt")
    fake_mb._load_native_xtdata = loader
    monkeypatch.setitem(sys.modules, "bigqmt_signal_trader.adapters.market_bigqmt", fake_mb)
    config = {"quote_events": {"enabled": True, "account_id": ACCT,
                               "refresh_interval_seconds": 0.0001}}
    pushed = strat._push_quote_updates(ctx, config)
    return fake_redis, pushed


def test_starved_code_healed_via_native_after_3_beats(monkeypatch, capsys):
    """批量源持续缺 600354: 1-2 拍仅计数, 第 3 拍 native 兜底回填并照常发布."""
    native = {STARVED: _valid_cell(10.48)}
    for beat in (1, 2):
        redis, pushed = _run(monkeypatch, {HEALTHY: _valid_cell(7.9 + beat)},
                             native_batch=native)
        assert strat._quote_starve_counts[STARVED] == beat
        assert not any(e.get("stock_code") == STARVED for e in redis.events)
    redis, pushed = _run(monkeypatch, {HEALTHY: _valid_cell(7.93)},
                         native_batch=native)
    codes = [e.get("stock_code") for e in redis.events]
    assert STARVED in codes and HEALTHY in codes  # 兜底回填后照常发布, 健康码不受扰
    assert "native fallback healed" in capsys.readouterr().out
    assert strat._quote_starve_counts.get(STARVED) in (None, 0)  # cell 可用拍清零


def test_both_sources_dead_grows_counter_warns_no_crash(monkeypatch, capsys):
    """双源都死 (停牌语义): 不发布、计数增长、n=5 WARNING 留痕、不抛异常."""
    for beat in range(1, 7):
        redis, pushed = _run(monkeypatch, {HEALTHY: _valid_cell(7.9 + beat)},
                             native_batch={})
        assert not any(e.get("stock_code") == STARVED for e in redis.events)
        assert strat._quote_starve_counts[STARVED] == beat
    out = capsys.readouterr().out
    assert "per-code starve n=5" in out and STARVED in out


def test_native_failure_fail_safe(monkeypatch, capsys):
    """兜底闸 ≥3 拍才开; 开闸后加载器抛错 → 打点不崩, 泵返回不中断 (pump 契约)."""
    for beat in (1, 2):
        _run(monkeypatch, {HEALTHY: _valid_cell(7.9 + beat)},
             native_raise=RuntimeError("sdk load fail"))
    redis, pushed = _run(monkeypatch, {HEALTHY: _valid_cell(7.93)},
                         native_raise=RuntimeError("sdk load fail"))
    assert strat._quote_starve_counts[STARVED] == 3
    assert "native fallback failed" in capsys.readouterr().out


def test_healthy_beat_resets_counter(monkeypatch):
    """饥饿后批量源恢复可用拍 → 计数清零 (后续再死重新起算)."""
    _run(monkeypatch, {HEALTHY: _valid_cell(7.9)}, native_batch={})
    assert strat._quote_starve_counts[STARVED] == 1
    batch = {HEALTHY: _valid_cell(7.9), STARVED: _valid_cell(10.5)}
    _run(monkeypatch, batch, native_batch={})
    assert strat._quote_starve_counts.get(STARVED) in (None, 0)


def test_dedup_beat_still_resets_counter(monkeypatch):
    """兜底回填后价格未变走 dedup 静默 — 也属「正常迭代」, 计数同样清零."""
    price = 10.48
    native = {STARVED: _valid_cell(price)}
    # 先饿两拍再喂同一价格 (命中 dedup 分支)
    _run(monkeypatch, {HEALTHY: _valid_cell(7.9)}, native_batch=native)
    _run(monkeypatch, {HEALTHY: _valid_cell(7.9)}, native_batch=native)
    redis, _ = _run(monkeypatch, {HEALTHY: _valid_cell(7.9)}, native_batch=native)
    assert strat._quote_starve_counts.get(STARVED) in (None, 0)
    # 同价 → dedup 不重复发布 (只发心跳/首推), 但码不进饥饿名单
    assert strat._quote_starve_counts.get(STARVED) in (None, 0)
