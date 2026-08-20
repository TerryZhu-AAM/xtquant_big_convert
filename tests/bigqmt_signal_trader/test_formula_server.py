import datetime
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import formula_server as fs


class BsonCodecTest(unittest.TestCase):
    """The built-in codec is the no-dependency path; it must round-trip every
    type this wire carries and stay byte-compatible with pymongo's bson."""

    def _round_trip(self, document):
        return fs._decode_document(fs._encode_document(document.items()), 0)[0]

    def test_round_trips_scalars(self):
        doc = {"s": "平安银行", "i": 42, "big": 2 ** 40, "f": 10.29, "t": True, "f2": False, "n": None}

        self.assertEqual(self._round_trip(doc), doc)

    def test_round_trips_nested_containers(self):
        doc = {"func": "getMarketData", "params": {"fields": ["close", "volume"], "count": -1}}

        self.assertEqual(self._round_trip(doc), doc)

    def test_round_trips_the_actual_request_envelope(self):
        doc = {
            "func": "getMarketData",
            "params": {
                "fields": ["close"],
                "stockCodes": ["000001.SZ"],
                "startTime": "",
                "endTime": "",
                "period": "1d",
                "dividendType": "none",
                "count": 3,
            },
        }

        self.assertEqual(self._round_trip(doc), doc)

    def test_array_order_is_preserved(self):
        doc = {"codes": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]}

        self.assertEqual(self._round_trip(doc)["codes"], doc["codes"])

    def test_matches_pymongo_bson_when_available(self):
        try:
            import bson
        except ImportError:
            self.skipTest("pymongo bson not installed")
        doc = {"func": "getLastVolume", "params": {"stockCode": "000001.SZ", "n": 1.5}}

        self.assertEqual(fs._encode_document(doc.items()), bson.BSON.encode(doc))
        self.assertEqual(fs._decode_document(bson.BSON.encode(doc), 0)[0], doc)

    def test_unsupported_type_is_rejected(self):
        with self.assertRaises(TypeError):
            fs._encode_document({"bad": object()}.items())


class AddressResolutionTest(unittest.TestCase):
    def test_reads_port_from_formulaserver_ini(self):
        import tempfile

        root = tempfile.mkdtemp()
        ini_dir = os.path.join(root, "config", "formulaserver")
        os.makedirs(ini_dir)
        with open(os.path.join(ini_dir, "formulaserver.ini"), "w") as handle:
            handle.write("[server_formula]\naddress = 0.0.0.0:58600\n")

        self.assertEqual(fs.read_formulaserver_port(root), 58600)
        self.assertEqual(fs.resolve_address({"qmt_root": root}), ("127.0.0.1", 58600))

    def test_missing_ini_falls_back_to_default_port(self):
        self.assertIsNone(fs.read_formulaserver_port(os.path.join(ROOT, "no-such-dir")))
        host, port = fs.resolve_address({"qmt_root": os.path.join(ROOT, "no-such-dir")})

        self.assertEqual((host, port), ("127.0.0.1", fs.DEFAULT_PORT))

    def test_explicit_config_wins(self):
        self.assertEqual(
            fs.resolve_address({"host": "10.0.0.5", "port": 59999}), ("10.0.0.5", 59999)
        )


class FakeClient(object):
    host = "127.0.0.1"
    port = 58600

    def __init__(self, responses=None, error=None):
        self.responses = responses or {}
        self.error = error
        self.calls = []

    def request(self, func, params=None):
        self.calls.append((func, dict(params or {})))
        if self.error is not None:
            raise self.error
        return self.responses.get(func, {"result": None})

    def close(self):
        pass


class ParamTranslationTest(unittest.TestCase):
    def _router(self, responses=None, error=None):
        client = FakeClient(responses=responses, error=error)
        return fs.FormulaServerRouter(client=client), client

    def test_instrument_aliases_the_misspelled_volume_fields(self):
        """FormulaServer ships FloatVolumn/TotalVolumn; the xtdata SDK spells
        them FloatVolume/TotalVolume. Downstream reads the SDK spelling."""
        router, _ = self._router(
            {"getInstrumentDetail": {"result": {"FloatVolumn": 1.0, "TotalVolumn": 2.0}}}
        )

        out = router.call("get_instrument", {"code": "000001.SZ"})

        self.assertEqual(out["FloatVolume"], 1.0)
        self.assertEqual(out["TotalVolume"], 2.0)
        self.assertEqual(out["FloatVolumn"], 1.0)  # raw key still present

    def test_sector_normalizes_the_minus_one_sentinel(self):
        router, client = self._router({"getStockListInSector": {"result": ["600000.SH"]}})

        router.call("get_stock_list_in_sector", {"sector_name": "沪深300", "real_timetag": -1})

        self.assertEqual(client.calls[0][1], {"sectorName": "沪深300", "realtime": 0})

    def test_market_data_refuses_adjusted_bars(self):
        """dividendType is not honoured by the server; serving an adjusted
        request from here would hand back unadjusted prices silently."""
        router, client = self._router({"getMarketData": {"result": []}})

        for dividend_type in ("front", "back", "front_ratio"):
            with self.assertRaises(fs.Unroutable):
                router.call(
                    "get_market_data_ex",
                    {
                        "field_list": ["close"],
                        "stock_list": ["000001.SZ"],
                        "dividend_type": dividend_type,
                    },
                )
        self.assertEqual(client.calls, [])

    def test_market_data_allows_unadjusted(self):
        router, client = self._router({"getMarketData": {"result": []}})

        router.call(
            "get_market_data_ex",
            {
                "field_list": ["close"],
                "stock_list": ["000001.SZ"],
                "dividend_type": "none",
                "start_time": "20260701000000",
                "end_time": "20260703150000",
            },
        )

        self.assertEqual(client.calls[0][1]["dividendType"], "none")

    def test_market_data_translates_flat_wire_shape(self):
        router, _ = self._router(
            {
                "getMarketData": {
                    "result": [
                        "000001.SZ",
                        ["20260703", ["close", 10.29, "volume", 863327.0]],
                        "600000.SH",
                        ["20260703", ["close", 8.69, "volume", 695133.0]],
                    ]
                }
            }
        )

        out = router.call(
            "get_market_data_ex",
            {
                "field_list": ["close", "volume"],
                "stock_list": ["000001.SZ", "600000.SH"],
                "start_time": "20260701000000",
                "end_time": "20260703150000",
            },
        )

        self.assertEqual(out["000001.SZ"]["columns"], ["stime", "close", "volume"])
        self.assertEqual(
            out["000001.SZ"]["records"],
            [{"stime": "20260703", "close": 10.29, "volume": 863327.0}],
        )
        self.assertEqual(out["600000.SH"]["records"][0]["close"], 8.69)

    def test_market_data_keeps_requested_codes_with_no_bars(self):
        router, _ = self._router({"getMarketData": {"result": []}})

        out = router.call(
            "get_market_data_ex",
            {
                "field_list": ["close"],
                "stock_list": ["000001.SZ", "600000.SH"],
                "start_time": "20260701000000",
                "end_time": "20260703150000",
            },
        )

        self.assertEqual(sorted(out), ["000001.SZ", "600000.SH"])
        self.assertEqual(out["000001.SZ"]["records"], [])

    def test_missing_required_params_is_unroutable_not_a_crash(self):
        router, client = self._router()

        with self.assertRaises(fs.Unroutable):
            router.call("get_instrument", {})
        self.assertEqual(client.calls, [])


class FallbackBehaviourTest(unittest.TestCase):
    def test_unmapped_method_is_not_supported(self):
        router = fs.FormulaServerRouter(client=FakeClient())

        self.assertFalse(router.supports("get_asset"))
        self.assertFalse(router.supports("submit_order"))
        self.assertFalse(router.supports("get_full_tick"))

    def test_trading_dates_and_dividends_stay_on_rpc(self):
        """Their FormulaServer params mean something different from ours."""
        router = fs.FormulaServerRouter(client=FakeClient())

        self.assertFalse(router.supports("get_trading_dates"))
        self.assertFalse(router.supports("get_divid_factors"))
        self.assertFalse(router.supports("get_risk_free_rate"))

    def test_transport_failure_trips_the_cooldown(self):
        router = fs.FormulaServerRouter(
            client=FakeClient(error=fs.FormulaServerUnavailable("down")),
            failure_cooldown_seconds=60,
        )

        with self.assertRaises(fs.Unroutable):
            router.call("get_last_volume", {"stock": "000001.SZ"})
        # Breaker is open: no further attempts until the cooldown expires.
        self.assertFalse(router.supports("get_last_volume"))

    def test_method_not_found_disables_only_that_method(self):
        router = fs.FormulaServerRouter(
            client=FakeClient(
                error=fs.FormulaServerError("nope", error_id=fs.ERROR_METHOD_NOT_FOUND)
            )
        )

        with self.assertRaises(fs.Unroutable):
            router.call("get_main_contract", {"code_market": "IF00.IF"})

        self.assertFalse(router.supports("get_main_contract"))
        self.assertTrue(router.supports("get_last_volume"))  # breaker not tripped

    def test_disabled_router_supports_nothing(self):
        router = fs.build_router({"enabled": False})

        for method in fs.SUPPORTED_METHODS:
            self.assertFalse(router.supports(method))

    def test_enabled_accepts_string_flags(self):
        self.assertFalse(fs.build_router({"enabled": "false"}).enabled)
        self.assertFalse(fs.build_router({"enabled": "0"}).enabled)
        self.assertTrue(fs.build_router({"enabled": "true", "port": 1}).enabled)


class ClientCallIntegrationTest(unittest.TestCase):
    """BigQmtRpcClient.call must prefer the router and fall back cleanly."""

    def _client(self, router):
        from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient

        client = BigQmtRpcClient(account_id="acct")
        client._formula_router_instance = router
        return client

    def test_routed_method_never_touches_rpc(self):
        router = fs.FormulaServerRouter(
            client=FakeClient({"getLastVolume": {"result": 123.0}})
        )
        client = self._client(router)

        def explode(*args, **kwargs):
            raise AssertionError("RPC must not be used for a routed method")

        client._transport = explode

        self.assertEqual(client.call("get_last_volume", {"stock": "000001.SZ"}), 123.0)

    def test_unroutable_falls_back_to_rpc(self):
        router = fs.FormulaServerRouter(
            client=FakeClient(error=fs.FormulaServerUnavailable("down"))
        )
        client = self._client(router)
        calls = []

        class FakeTransport:
            def send_request(self, request, timeout):
                calls.append(request["method"])
                return {"ok": True, "data": "from-rpc"}

        client._transport = lambda: FakeTransport()

        self.assertEqual(client.call("get_last_volume", {"stock": "000001.SZ"}), "from-rpc")
        self.assertEqual(calls, ["get_last_volume"])

    def test_unmapped_method_goes_straight_to_rpc(self):
        router = fs.FormulaServerRouter(client=FakeClient())
        client = self._client(router)
        calls = []

        class FakeTransport:
            def send_request(self, request, timeout):
                calls.append(request["method"])
                return {"ok": True, "data": {"cash": 1.0}}

        client._transport = lambda: FakeTransport()

        self.assertEqual(client.call("get_asset", {}), {"cash": 1.0})
        self.assertEqual(calls, ["get_asset"])

    def test_routed_dataframe_payload_is_restored_like_rpc(self):
        router = fs.FormulaServerRouter(
            client=FakeClient(
                {
                    "getMarketData": {
                        "result": ["000001.SZ", ["20260703", ["close", 10.29]]]
                    }
                }
            )
        )
        client = self._client(router)

        out = client.call(
            "get_market_data_ex",
            {
                "field_list": ["close"],
                "stock_list": ["000001.SZ"],
                "start_time": "20260701000000",
                "end_time": "20260703150000",
            },
        )

        frame = out["000001.SZ"]
        # _restore_jsonable rebuilds a DataFrame when pandas is present, and
        # degrades to the record list otherwise — same as the RPC path.
        if hasattr(frame, "columns"):
            self.assertEqual(list(frame.columns), ["stime", "close"])
            self.assertEqual(frame.iloc[0]["close"], 10.29)
        else:
            self.assertEqual(frame, [{"stime": "20260703", "close": 10.29}])


class _StaleFakeFormulaClient(object):
    """Serves getMarketData with a caller-chosen list of bar stamps."""

    host = "127.0.0.1"
    port = 58600

    def __init__(self, stamps):
        self._stamps = list(stamps)

    def request(self, func, wire_params):
        timeline = []
        for stamp in self._stamps:
            timeline.extend([stamp, ["close", 1.0, "volume", 100]])
        return {"result": ["600048.SH", timeline]}

    def close(self):
        pass


class StaleViewGuardTest(unittest.TestCase):
    """[BUG-20260814-formula-58600-stale-view] frozen-view guard.

    On 2026-08-14 the C++ FormulaServer answered same-day 1m windows with
    zero bars (view frozen at the previous close) while the RPC path had the
    live data. The router must re-read such answers over RPC and leave purely
    historical windows on the fast path.
    """

    FRIDAY_1430 = datetime.datetime(2026, 8, 14, 14, 30, 0)
    SATURDAY_1000 = datetime.datetime(2026, 8, 15, 10, 0, 0)

    TODAY_WINDOW = {
        "field_list": ["close", "volume"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "20260814093000",
        "end_time": "20260814150000",
    }
    HISTORICAL_WINDOW = {
        "field_list": ["close", "volume"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "20260801000000",
        "end_time": "20260813150000",
    }
    OPEN_ENDED_WINDOW = {
        "field_list": ["close", "volume"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "",
        "end_time": "",
    }
    YESTERDAY_ONLY = ["20260813 09:30:00", "20260813 15:00:00"]
    WITH_TODAY = ["20260813 15:00:00", "20260814 14:29:00"]

    def setUp(self):
        self._orig_now = fs._local_now
        fs._local_now = lambda: self.FRIDAY_1430

    def tearDown(self):
        fs._local_now = self._orig_now

    def _router(self, stamps):
        return fs.FormulaServerRouter(
            enabled=True,
            client=_StaleFakeFormulaClient(stamps),
            print_prefix="[bigqmt_formula:test]",
        )

    def test_stale_same_day_window_falls_back_to_rpc(self):
        router = self._router(self.YESTERDAY_ONLY)
        with self.assertRaises(fs.Unroutable):
            router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.stale_hits, 1)
        self.assertEqual(router.misses, 1)

    def test_fresh_today_bars_served_direct(self):
        router = self._router(self.WITH_TODAY)
        out = router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        records = out["600048.SH"]["records"]
        self.assertEqual(len(records), 2)
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_purely_historical_window_not_guarded(self):
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.HISTORICAL_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_pre_market_not_guarded(self):
        fs._local_now = lambda: datetime.datetime(2026, 8, 14, 9, 0, 0)
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.stale_hits, 0)

    def test_weekend_not_guarded(self):
        fs._local_now = lambda: self.SATURDAY_1000
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.stale_hits, 0)

    def test_open_ended_end_expects_today(self):
        router = self._router(self.YESTERDAY_ONLY)
        with self.assertRaises(fs.Unroutable):
            router.call("get_market_data_ex", dict(self.OPEN_ENDED_WINDOW))
        self.assertEqual(router.stale_hits, 1)

    def test_stale_does_not_trip_the_global_cooldown(self):
        # A frozen view is only stale for same-day reads; history must keep
        # using the direct path immediately afterwards (no 30s breaker).
        router = self._router(self.YESTERDAY_ONLY)
        with self.assertRaises(fs.Unroutable):
            router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        router.call("get_market_data_ex", dict(self.HISTORICAL_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 1)

    def test_yyyymmdd_tolerates_wire_shapes(self):
        self.assertEqual(fs._yyyymmdd("20260813 09:30:00"), "20260813")
        self.assertEqual(fs._yyyymmdd("20260813093000"), "20260813")
        self.assertEqual(fs._yyyymmdd("2026-08-13 09:30:00"), "20260813")
        self.assertEqual(fs._yyyymmdd(""), "")
        self.assertEqual(fs._yyyymmdd(None), "")


class StaleViewFreshnessTest(unittest.TestCase):
    """v2 freshness dimension — a mid-session freeze must not pass the guard.

    A view frozen at 11:00 still answers "some bar today", so the zero-bar
    check alone would feed half-day data to virtual-K synthesis. The newest
    current-day bar must trail the window's expected edge by <= 5 minutes;
    lunch and post-close windows expect the session edge, not wall-clock now.

    [fix BUG-P2-20260818-mock-tick-pump-bark-storm 2026-08-18] 午休/盘后冻结视图
    是预期 (frozen view 在非交易时段是正常状态, 不是 stale), 守卫一律跳过 — 避免
    Unroutable → 慢 RPC → mock-tick-pump Bark 风暴. 新语义: 午休/盘后无论视图
    多旧都走 fast path (router.hits=1, no Unroutable).
    """

    FRIDAY_1430 = datetime.datetime(2026, 8, 14, 14, 30, 0)
    FRIDAY_NOON = datetime.datetime(2026, 8, 14, 12, 0, 0)
    FRIDAY_1530 = datetime.datetime(2026, 8, 14, 15, 30, 0)

    TODAY_WINDOW = {
        "field_list": ["close", "volume"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "20260814093000",
        "end_time": "20260814150000",
    }

    def setUp(self):
        self._orig_now = fs._local_now
        fs._local_now = lambda: self.FRIDAY_1430

    def tearDown(self):
        fs._local_now = self._orig_now

    def _router(self, stamps):
        return fs.FormulaServerRouter(
            enabled=True,
            client=_StaleFakeFormulaClient(stamps),
            print_prefix="[bigqmt_formula:test]",
        )

    def test_mid_session_freeze_falls_back_to_rpc(self):
        # Today bars exist but stop at 10:30 while the window expects ~14:30 —
        # the zero-bar check would pass this; freshness must not.
        router = self._router(["20260814 10:30:00"])
        with self.assertRaises(fs.Unroutable):
            router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.stale_hits, 1)

    def test_trailing_bar_within_lag_served_direct(self):
        router = self._router(["20260814 14:28:00"])  # 2-minute lag
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_lunch_skips_stale_check_altogether(self):
        """[fix BUG-P2-20260818] 午休 12:00 frozen view 不算 stale, 直接走 fast path.
        旧 v2 期望 11:30 边缘 → 10:00 算 90min lag → Unroutable,
        新语义: 午休任何冻结视图都是预期, 守卫跳过 (无论多少 lag).
        """
        fs._local_now = lambda: self.FRIDAY_NOON
        # 新鲜 11:30 边缘 → fast path (旧 v2 行为也走 fast path).
        fresh = self._router(["20260814 11:30:00"])
        fresh.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(fresh.hits, 1)
        self.assertEqual(fresh.stale_hits, 0)
        # 旧的 10:00 视图 → 旧 v2 期望 Unroutable, 新语义直走 fast path.
        # 修复: 避免午休期间 stale→Unroutable→慢 RPC→mock-tick-pump Bark 风暴.
        frozen = self._router(["20260814 10:00:00"])
        frozen.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(frozen.hits, 1,
            "午休期 10:00 frozen view 应走 fast path (新语义, 避免 Bark 风暴)")
        self.assertEqual(frozen.stale_hits, 0,
            "午休期不应触发 stale 检查")

    def test_after_close_skips_stale_check_altogether(self):
        """[fix BUG-P2-20260818] 盘后 15:30 frozen view 不算 stale.
        旧 v2 期望 15:00 边缘 → 14:00 算 60min lag → Unroutable,
        新语义: 盘后任何冻结视图都是预期, 守卫跳过 (无论多少 lag).
        """
        fs._local_now = lambda: self.FRIDAY_1530
        fresh = self._router(["20260814 15:00:00"])
        fresh.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(fresh.hits, 1)
        self.assertEqual(fresh.stale_hits, 0)
        # 旧的 14:00 视图 → 新语义走 fast path.
        frozen = self._router(["20260814 14:00:00"])
        frozen.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(frozen.hits, 1,
            "盘后期 14:00 frozen view 应走 fast path (新语义)")
        self.assertEqual(frozen.stale_hits, 0,
            "盘后期不应触发 stale 检查")

    def test_explicit_end_clamps_expectation(self):
        # Window ends 10:30 — a healthy view must serve up to 10:30 even at 14:30.
        window = dict(self.TODAY_WINDOW, end_time="20260814103000")
        fresh = self._router(["20260814 10:29:00"])
        fresh.call("get_market_data_ex", window)
        self.assertEqual(fresh.hits, 1)
        frozen = self._router(["20260814 09:40:00"])
        with self.assertRaises(fs.Unroutable):
            frozen.call("get_market_data_ex", window)
        self.assertEqual(frozen.stale_hits, 1)

    def test_date_granularity_stamp_skips_freshness(self):
        # Daily bars stamped 00:00:00 carry no intraday time — the zero-bar
        # check governs them; freshness must not flag every healthy 1d read.
        router = self._router(["20260814 00:00:00"])
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_expected_latest_hhmm_shapes(self):
        self.assertEqual(
            fs._expected_latest_hhmm(self.FRIDAY_1430, ""), "143000"
        )  # afternoon session: expect up to now
        self.assertEqual(
            fs._expected_latest_hhmm(self.FRIDAY_NOON, ""), "113000"
        )  # lunch: expect the morning close edge
        self.assertEqual(
            fs._expected_latest_hhmm(self.FRIDAY_1530, ""), "150000"
        )  # after close: expect the 15:00 edge
        self.assertEqual(
            fs._expected_latest_hhmm(self.FRIDAY_1430, "20260814103000"), "103000"
        )  # explicit intraday end clamps below session close
        self.assertEqual(
            fs._expected_latest_hhmm(self.FRIDAY_1430, "20260814"), "143000"
        )  # date-only end = up to now

    def test_guard_error_fails_through_and_is_visible(self):
        import contextlib
        import io

        router = self._router(["20260814 14:29:00"])
        orig = fs._yyyymmdd

        def _boom(_stamp):
            raise ValueError("guard internals broken")

        fs._yyyymmdd = _boom
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                result = router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        finally:
            fs._yyyymmdd = orig
        # fail-through: the direct answer survives a broken guard...
        self.assertEqual(len(result["600048.SH"]["records"]), 1)
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)
        # [P3 root-cause fix] the guard error counter must increment...
        self.assertEqual(router.stale_guard_errors, 1)
        # ...and appear in stats()...
        self.assertEqual(router.stats()["stale_guard_errors"], 1)
        # ...but not silently — the throttled diagnostic must fire with count.
        output = out.getvalue()
        self.assertIn("stale-view guard check failed", output)
        self.assertIn("1 total", output)


class InSessionTradingHoursTest(unittest.TestCase):
    """[fix BUG-P2-20260818-mock-tick-pump-bark-storm 2026-08-18]
    formula_server._in_session(now) helper — 纯函数, 边界与 backend
    trading_calendar.is_trading_hours byte-equal (保 third_party 无反向依赖).

    半开区间:
    09:30:00 ≤ t < 11:30:00 → True (上午)
    13:00:00 ≤ t < 15:00:00 → True (下午)
    11:30:00 ≤ t < 13:00:00 → False (午休)
    15:00:00 ≤ t          → False (盘后)
    集合竞价 09:00-09:30    → False (上午前)
    周末 weekday>=5        → False
    """

    def test_morning_open_to_lunch(self):
        # 09:30:00 整秒开启; 11:29:59 仍 True; 11:30:00 整秒 = 午休起点.
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 9, 30, 0)))
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 10, 30, 0)))
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 11, 29, 59)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 11, 30, 0)))

    def test_lunch_break(self):
        # 11:30:00 ≤ t < 13:00:00 全段 False (午休).
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 11, 30, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 12, 0, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 12, 59, 59)))

    def test_afternoon_open_to_close(self):
        # 13:00:00 整秒开启 (上午最后交易秒 11:29:59, 下午第一秒 13:00:00).
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 13, 0, 0)))
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 14, 30, 0)))
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 18, 14, 59, 59)))
        # 15:00:00 整秒 = 收盘.
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 15, 0, 0)))

    def test_after_close(self):
        # 15:00+ 全段 False (盘后).
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 15, 30, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 18, 0, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 23, 59, 59)))

    def test_pre_open_auction(self):
        # 09:00-09:30 集合竞价 → False.
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 8, 30, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 9, 0, 0)))
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 18, 9, 29, 59)))

    def test_weekend(self):
        # 周六/日 → False (全天, 不论时间).
        # weekday: Mon=0, Sat=5, Sun=6.
        sat = datetime.datetime(2026, 8, 22, 10, 30, 0)  # Sat
        sun = datetime.datetime(2026, 8, 23, 14, 0, 0)   # Sun
        self.assertEqual(sat.weekday(), 5)
        self.assertEqual(sun.weekday(), 6)
        self.assertFalse(fs._in_session(sat))
        self.assertFalse(fs._in_session(sun))


class StaleGuardNonTradingHoursTest(unittest.TestCase):
    """[fix BUG-P2-20260818-mock-tick-pump-bark-storm 2026-08-18]
    _stale_market_data 应在 非交易时段 直接返 False (不 stale),
    避免午休/盘后 frozen view 误 stale → Unroutable → 慢 RPC → Bark 风暴.
    """

    TODAY_WINDOW = {
        "field_list": ["close", "volume"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "20260818100000",
        "end_time": "20260818150000",
    }
    YESTERDAY_ONLY = ["20260813 09:30:00", "20260813 15:00:00"]

    def setUp(self):
        self._orig_now = fs._local_now

    def tearDown(self):
        fs._local_now = self._orig_now

    def _router(self, stamps):
        return fs.FormulaServerRouter(
            enabled=True,
            client=_StaleFakeFormulaClient(stamps),
            print_prefix="[bigqmt_formula:test-trading-hours]",
        )

    def test_lunch_break_no_stale_check(self):
        """午休 12:00 (周五) → frozen view 不算 stale, 直接 fast path."""
        fs._local_now = lambda: datetime.datetime(2026, 8, 14, 12, 0, 0)
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.hits, 1,
            "午休应走 fast path (router.hits) 而非 stale→Unroutable")
        self.assertEqual(router.stale_hits, 0)

    def test_after_close_no_stale_check(self):
        """盘后 18:00 → frozen view 不算 stale."""
        fs._local_now = lambda: datetime.datetime(2026, 8, 14, 18, 0, 0)
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_lunch_edge_113000_no_stale_check(self):
        """11:30:00 整秒 (午休起点) → 不 stale."""
        fs._local_now = lambda: datetime.datetime(2026, 8, 14, 11, 30, 0)
        router = self._router(self.YESTERDAY_ONLY)
        router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        self.assertEqual(router.hits, 1)
        self.assertEqual(router.stale_hits, 0)

    def test_afternoon_open_130000_stale_check_resumes(self):
        """13:00:00 整秒 → stale 检查恢复 (冷启动场景下老 view 仍判 stale).
        验证 _in_session 边界正确: 12:59:59 False, 13:00:00 True."""
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 14, 12, 59, 59)))
        self.assertTrue(fs._in_session(datetime.datetime(2026, 8, 14, 13, 0, 0)))

    def test_pre_morning_edge_92959_no_stale_check(self):
        """09:29:59 (集合竞价末) → 仍 False (09:30 前都不判 stale).

        验证与原 `weekday>=5 or hour<9 or (hour==9 and minute<30)` 行为 byte-equal."""
        self.assertFalse(fs._in_session(datetime.datetime(2026, 8, 14, 9, 29, 59)))


if __name__ == "__main__":
    unittest.main()
