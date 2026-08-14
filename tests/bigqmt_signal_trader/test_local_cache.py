import os
import shutil
import sys
import tempfile
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.local_cache import LocalMarketCache


def _has_pyarrow():
    try:
        import pyarrow  # noqa: F401

        return True
    except Exception:
        return False


class LocalMarketCacheTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_write_read_merge_dedupe(self):
        import pandas as pd

        c = LocalMarketCache(self.dir)
        c.write("600000.SH", "1d", pd.DataFrame({"stime": ["20260101", "20260102"], "close": [1.0, 2.0]}))
        # overlapping second write: 20260102 should be replaced (keep last), 20260103 appended
        c.write("600000.SH", "1d", pd.DataFrame({"stime": ["20260102", "20260103"], "close": [2.5, 3.0]}))

        df = c.read("600000.SH", "1d")
        self.assertEqual(list(df["stime"]), ["20260101", "20260102", "20260103"])
        self.assertEqual(df[df["stime"] == "20260102"]["close"].iloc[0], 2.5)

    def test_range_and_count_filters(self):
        import pandas as pd

        c = LocalMarketCache(self.dir)
        c.write("X", "1d", pd.DataFrame({"stime": ["20260101", "20260102", "20260103"], "close": [1, 2, 3]}))

        self.assertEqual(list(c.read("X", "1d", start_time="20260102")["stime"]), ["20260102", "20260103"])
        self.assertEqual(list(c.read("X", "1d", end_time="20260102")["stime"]), ["20260101", "20260102"])
        self.assertEqual(list(c.read("X", "1d", count=1)["stime"]), ["20260103"])
        self.assertIsNone(c.read("MISSING", "1d"))
        self.assertEqual(c.covered("X", "1d"), ("20260101", "20260103", 3))

    def test_drops_zero_fill_placeholder_rows(self):
        import pandas as pd

        c = LocalMarketCache(self.dir)
        df = pd.DataFrame(
            {"stime": ["20200101", "20200102", "20260701"], "close": [0.0, 0.0, 8.65], "open": [0.0, 0.0, 8.58]}
        )
        c.write("X", "1d", df)
        self.assertEqual(list(c.read("X", "1d")["stime"]), ["20260701"])  # 0-fill dropped

        # an all-placeholder write must not create/overwrite a cache file
        self.assertEqual(c.write("Y", "1d", pd.DataFrame({"stime": ["20200101"], "close": [0.0]})), 0)
        self.assertIsNone(c.read("Y", "1d"))

    def test_dividend_type_keeps_separate_caches(self):
        import pandas as pd

        c = LocalMarketCache(self.dir)
        c.write("X", "1d", pd.DataFrame({"stime": ["20260101"], "close": [10.0]}), dividend_type="none")
        c.write("X", "1d", pd.DataFrame({"stime": ["20260101"], "close": [9.0]}), dividend_type="front")

        self.assertEqual(c.read("X", "1d", dividend_type="none")["close"].iloc[0], 10.0)
        self.assertEqual(c.read("X", "1d", dividend_type="front")["close"].iloc[0], 9.0)
        self.assertIsNone(c.read("X", "1d", dividend_type="back"))

    def test_pickle_format_roundtrip(self):
        import pandas as pd

        c = LocalMarketCache(self.dir, fmt="pkl")
        c.write("X", "1d", pd.DataFrame({"stime": ["20260101", "20260102"], "close": [1.0, 2.0]}))
        self.assertTrue(c.path("X", "1d").endswith(".pkl"))
        self.assertEqual(list(c.read("X", "1d")["close"]), [1.0, 2.0])

    @unittest.skipUnless(_has_pyarrow(), "pyarrow not installed")
    def test_parquet_format_roundtrip(self):
        import pandas as pd

        c = LocalMarketCache(self.dir, fmt="parquet")
        c.write("X", "1d", pd.DataFrame({"stime": ["20260101", "20260102"], "close": [1.0, 2.0]}))
        self.assertTrue(c.path("X", "1d").endswith(".parquet"))
        self.assertEqual(list(c.read("X", "1d")["close"]), [1.0, 2.0])

    @unittest.skipUnless(_has_pyarrow(), "pyarrow not installed")
    def test_migrates_pickle_to_parquet(self):
        import pandas as pd

        LocalMarketCache(self.dir, fmt="pkl").write("X", "1d", pd.DataFrame({"stime": ["20260101"], "close": [1.0]}))
        pq = LocalMarketCache(self.dir, fmt="parquet")
        self.assertEqual(list(pq.read("X", "1d")["close"]), [1.0])  # reads the old pkl
        pq.write("X", "1d", pd.DataFrame({"stime": ["20260102"], "close": [2.0]}))
        self.assertTrue(os.path.isfile(pq.path("X", "1d")))  # parquet now exists
        self.assertFalse(os.path.isfile(pq.path("X", "1d")[:-8] + ".pkl"))  # old pkl removed
        self.assertEqual(list(pq.read("X", "1d")["close"]), [1.0, 2.0])  # merged across formats


class FakeClient:
    def __init__(self, cache_dir, fallback_rpc=False):
        self.account_id = "acct"
        self.calls = []
        self.call_params = []
        self.local_cache_config = {"enabled": True, "dir": cache_dir, "fallback_rpc": fallback_rpc}

    def _redis(self):
        return None

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append(method)
        self.call_params.append((method, params))
        if method == "get_market_data_ex":
            import pandas as pd

            codes = (params or {}).get("stock_list") or []
            return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [8.76, 8.73]}) for c in codes}
        if method == "download_history_data2":
            # Server-side raw download (raw bars + dividend factors).
            return True
        raise AssertionError("unexpected rpc: %s" % method)


class LocalCacheClientTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _xt(self, fallback_rpc=False):
        from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

        return BigQmtXtData(FakeClient(self.dir, fallback_rpc=fallback_rpc))

    def test_download_caches_then_get_local_reads_without_rpc(self):
        xt = self._xt()
        progress = []
        res = xt.download_history_data2(["600000.SH", "000001.SZ"], "1d", callback=lambda d: progress.append(d))

        self.assertEqual(res, {"finished": 2, "total": 2})
        self.assertEqual(len(progress), 2)
        self.assertEqual(progress[-1]["stockcode"], "000001.SZ")
        self.assertEqual(progress[-1]["finished"], 2)
        calls_after_download = list(xt.client.calls)

        data = xt.get_local_data(stock_list=["600000.SH", "000001.SZ"], period="1d")

        self.assertIn("600000.SH", data)
        self.assertIn("000001.SZ", data)
        self.assertEqual(list(data["600000.SH"]["close"]), [8.76, 8.73])
        # get_local_data must NOT issue any further RPC — pure local read.
        self.assertEqual(xt.client.calls, calls_after_download)

    def test_get_market_data_ex_caches_through(self):
        xt = self._xt()
        # a plain live read must also populate the cache (cache-through)
        xt.get_market_data_ex(field_list=["close"], stock_list=["600000.SH"], period="1d")
        n = len(xt.client.calls)

        data = xt.get_local_data(stock_list=["600000.SH"], period="1d")
        self.assertIn("600000.SH", data)
        self.assertEqual(len(xt.client.calls), n)  # served from cache, no extra RPC

    def test_get_local_miss_returns_empty_and_no_rpc(self):
        xt = self._xt()
        data = xt.get_local_data(stock_list=["600000.SH"], period="1d")
        self.assertEqual(data, {})
        self.assertEqual(xt.client.calls, [])

    def test_get_local_fallback_rpc_fetches_and_caches(self):
        xt = self._xt(fallback_rpc=True)
        data = xt.get_local_data(stock_list=["600000.SH"], period="1d")
        self.assertIn("600000.SH", data)
        self.assertIn("get_market_data_ex", xt.client.calls)  # fetched on miss
        # second read is served from cache — no new RPC
        n = len(xt.client.calls)
        xt.get_local_data(stock_list=["600000.SH"], period="1d")
        self.assertEqual(len(xt.client.calls), n)


class AdjustedDownloadTest(unittest.TestCase):
    """Adjusted (front/back) downloads must trigger the server-side raw
    download FIRST: Big QMT computes adjusted bars from raw bars + dividend
    factors, and without the server-side download the adjusted result is
    all zeros (verified live with 600654.SH)."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _xt(self):
        from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

        return BigQmtXtData(FakeClient(self.dir))

    def test_front_download_triggers_server_side_raw_download_first(self):
        xt = self._xt()
        xt.download_history_data2(["600000.SH"], "1d", start_time="20200101", dividend_type="front")

        # The server-side raw download must run BEFORE the adjusted pull.
        method_calls = [m for m, _ in xt.client.call_params]
        self.assertIn("download_history_data2", method_calls)
        self.assertIn("get_market_data_ex", method_calls)
        self.assertLess(
            method_calls.index("download_history_data2"),
            method_calls.index("get_market_data_ex"),
            "server-side raw download must precede the adjusted pull",
        )
        # The raw download carries the same codes/period/window.
        raw_call = next(p for m, p in xt.client.call_params if m == "download_history_data2")
        self.assertEqual(raw_call["stock_list"], ["600000.SH"])
        self.assertEqual(raw_call["period"], "1d")
        self.assertEqual(raw_call["start_time"], "20200101")

    def test_none_download_skips_server_side_raw_download(self):
        xt = self._xt()
        xt.download_history_data2(["600000.SH"], "1d", dividend_type="none")

        # Unadjusted pulls need no server-side download.
        method_calls = [m for m, _ in xt.client.call_params]
        self.assertNotIn("download_history_data2", method_calls)
        self.assertIn("get_market_data_ex", method_calls)

    def test_front_download_survives_server_download_failure(self):
        # Deployments without the QMT global must still get the adjusted pull
        # (best-effort raw download, never fatal).
        xt = self._xt()
        original_call = xt.client.call

        def failing_download(method, params=None, account_id=None, timeout_seconds=None):
            if method == "download_history_data2":
                raise RuntimeError("global not available")
            return original_call(method, params, account_id=account_id, timeout_seconds=timeout_seconds)

        xt.client.call = failing_download
        result = xt.download_history_data2(["600000.SH"], "1d", dividend_type="front")
        self.assertEqual(result, {"finished": 1, "total": 1})  # adjusted pull still ran


class _AllZeroThenRealClient(FakeClient):
    """First adjusted get_market_data_ex returns all-zero bars (server lacks
    raw data); after a server-side raw download, subsequent pulls are real."""

    def __init__(self, cache_dir):
        super(_AllZeroThenRealClient, self).__init__(cache_dir)
        self._downloaded = False

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append(method)
        self.call_params.append((method, params))
        import pandas as pd

        if method == "download_history_data2":
            self._downloaded = True
            return True
        if method == "get_market_data_ex":
            codes = (params or {}).get("stock_list") or []
            if self._downloaded:
                return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [8.76, 8.73]}) for c in codes}
            # all-zero symptom: head zeros, last bar live
            return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [0.0, 8.73]}) for c in codes}
        raise AssertionError("unexpected rpc: %s" % method)


class AdjustedReadSelfHealTest(unittest.TestCase):
    """Reading adjusted bars that come back all-zero must self-heal:
    trigger a server-side raw download, wait, and retry once."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _xt(self):
        from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

        return BigQmtXtData(_AllZeroThenRealClient(self.dir))

    def test_front_read_self_heals_all_zero_to_real(self):
        xt = self._xt()
        data = xt.get_market_data_ex(
            field_list=["close"], stock_list=["600000.SH"], period="1d",
            dividend_type="front",
        )
        # After self-heal the retry returns real (non-zero) bars.
        self.assertEqual(list(data["600000.SH"]["close"]), [8.76, 8.73])
        # The heal path must have triggered a server-side raw download.
        method_calls = [m for m, _ in xt.client.call_params]
        self.assertIn("download_history_data2", method_calls)
        # get_market_data_ex called twice: initial all-zero pull + retry.
        self.assertEqual(method_calls.count("get_market_data_ex"), 2)

    def test_none_read_does_not_self_heal(self):
        xt = self._xt()
        data = xt.get_market_data_ex(
            field_list=["close"], stock_list=["600000.SH"], period="1d",
            dividend_type="none",
        )
        # none returns whatever the server sent (no heal, single pull).
        self.assertEqual(list(data["600000.SH"]["close"]), [0.0, 8.73])
        method_calls = [m for m, _ in xt.client.call_params]
        self.assertNotIn("download_history_data2", method_calls)
        self.assertEqual(method_calls.count("get_market_data_ex"), 1)


class _PollClient(FakeClient):
    """Simulates a server that needs N polls before adjusted data lands.

    The first adjusted pull returns all-zero. After a server-side raw download,
    each subsequent poll returns all-zero until ``polls_until_real`` polls have
    elapsed — then returns real data. This tests the poll-until-ready loop (P5).
    """

    def __init__(self, cache_dir, polls_until_real=3):
        super(_PollClient, self).__init__(cache_dir)
        self._downloaded = False
        self._polls = 0
        self._polls_until_real = polls_until_real

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append(method)
        self.call_params.append((method, params))
        import pandas as pd

        if method == "download_history_data2":
            self._downloaded = True
            return True
        if method == "get_market_data_ex":
            codes = (params or {}).get("stock_list") or []
            if self._downloaded:
                self._polls += 1
            if self._downloaded and self._polls >= self._polls_until_real:
                return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [8.76, 8.73]}) for c in codes}
            # all-zero symptom
            return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [0.0, 8.73]}) for c in codes}
        raise AssertionError("unexpected rpc: %s" % method)


class _PartialZeroClient(FakeClient):
    """Multi-code batch where only some codes are all-zero (P6 targeted heal).

    600000.SH is healthy, 600654.SH is all-zero. After server-side download,
    only 600654.SH should be re-downloaded (not 600000.SH).
    """

    def __init__(self, cache_dir):
        super(_PartialZeroClient, self).__init__(cache_dir)
        self._downloaded = False
        self._download_codes = []

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append(method)
        self.call_params.append((method, params))
        import pandas as pd

        if method == "download_history_data2":
            self._downloaded = True
            self._download_codes = list((params or {}).get("stock_list") or [])
            return True
        if method == "get_market_data_ex":
            codes = (params or {}).get("stock_list") or []
            result = {}
            for c in codes:
                if c == "600000.SH":
                    result[c] = pd.DataFrame({"stime": ["20260626", "20260629"], "close": [8.76, 8.73]})
                elif self._downloaded:
                    result[c] = pd.DataFrame({"stime": ["20260626", "20260629"], "close": [9.12, 9.15]})
                else:
                    result[c] = pd.DataFrame({"stime": ["20260626", "20260629"], "close": [0.0, 9.15]})
            return result
        raise AssertionError("unexpected rpc: %s" % method)


class _FailingDownloadClient(FakeClient):
    """Server-side download fails (P7 diagnostic test).

    The adjusted pull returns all-zero, the server-side download raises, and
    the diagnostic print must fire (not silent swallow).
    """

    def __init__(self, cache_dir):
        super(_FailingDownloadClient, self).__init__(cache_dir)

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append(method)
        self.call_params.append((method, params))
        import pandas as pd

        if method == "download_history_data2":
            raise RuntimeError("QMT global not available on this server")
        if method == "get_market_data_ex":
            codes = (params or {}).get("stock_list") or []
            return {c: pd.DataFrame({"stime": ["20260626", "20260629"], "close": [0.0, 8.73]}) for c in codes}
        raise AssertionError("unexpected rpc: %s" % method)


class PollUntilReadyTest(unittest.TestCase):
    """[P5 root-cause fix] Self-heal must poll until data lands, not sleep(2)+retry."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        # Patch time.sleep to avoid actual waiting in tests.
        import bigqmt_signal_trader.xtquant_compat as xc
        self._orig_sleep = xc.time.sleep
        xc.time.sleep = lambda s: None
        self._xc = xc

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self._xc.time.sleep = self._orig_sleep

    def test_poll_succeeds_after_multiple_retries(self):
        """Server needs 3 polls before data lands — poll loop must persist."""
        xt = self._xc.BigQmtXtData(_PollClient(self.dir, polls_until_real=3))
        data = xt.get_market_data_ex(
            field_list=["close"], stock_list=["600000.SH"], period="1d",
            dividend_type="front",
        )
        self.assertEqual(list(data["600000.SH"]["close"]), [8.76, 8.73])
        method_calls = [m for m, _ in xt.client.call_params]
        self.assertIn("download_history_data2", method_calls)
        # 1 initial + 3 polls = 4 total get_market_data_ex calls
        self.assertEqual(method_calls.count("get_market_data_ex"), 4)

    def test_poll_timeout_returns_last_result(self):
        """Server never lands — poll loop must timeout and return last all-zero."""
        import pandas as pd

        client = _PollClient(self.dir, polls_until_real=999)
        xt = self._xc.BigQmtXtData(client)
        # Call _heal_adjusted directly with a very short timeout so the test
        # doesn't wait 10s of wall-clock time.
        initial_data = {"600000.SH": pd.DataFrame({"stime": ["20260626", "20260629"], "close": [0.0, 8.73]})}
        params = {
            "field_list": ["close"],
            "stock_list": ["600000.SH"],
            "period": "1d",
            "start_time": "",
            "end_time": "",
            "count": -1,
            "dividend_type": "front",
            "fill_data": True,
        }
        data = xt._heal_adjusted("get_market_data_ex", params, initial_data, max_wait_seconds=0.1)
        # Timeout: returns the last all-zero result (graceful degradation).
        self.assertIn("600000.SH", data)
        closes = list(data["600000.SH"]["close"])
        self.assertEqual(closes, [0.0, 8.73])

    def test_zero_codes_from_data_dict(self):
        """_zero_codes_from_data identifies only the all-zero codes in a batch."""
        import pandas as pd
        from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

        data = {
            "600000.SH": pd.DataFrame({"close": [8.76, 8.73]}),
            "600654.SH": pd.DataFrame({"close": [0.0, 9.15]}),
        }
        zeros = BigQmtXtData._zero_codes_from_data(data)
        self.assertEqual(zeros, ["600654.SH"])

    def test_zero_codes_from_data_all_healthy(self):
        import pandas as pd
        from bigqmt_signal_trader.xtquant_compat import BigQmtXtData

        data = {
            "600000.SH": pd.DataFrame({"close": [8.76, 8.73]}),
        }
        zeros = BigQmtXtData._zero_codes_from_data(data)
        self.assertEqual(zeros, [])


class TargetedRedownloadTest(unittest.TestCase):
    """[P6 root-cause fix] Self-heal only re-downloads all-zero codes."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        import bigqmt_signal_trader.xtquant_compat as xc
        self._orig_sleep = xc.time.sleep
        xc.time.sleep = lambda s: None
        self._xc = xc

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self._xc.time.sleep = self._orig_sleep

    def test_partial_zero_only_redownloads_zero_codes(self):
        """600000.SH is healthy, 600654.SH is all-zero.
        The server-side download must only request 600654.SH."""
        client = _PartialZeroClient(self.dir)
        xt = self._xc.BigQmtXtData(client)
        data = xt.get_market_data_ex(
            field_list=["close"],
            stock_list=["600000.SH", "600654.SH"],
            period="1d",
            dividend_type="front",
        )
        # Both codes should have real data after heal.
        self.assertEqual(list(data["600000.SH"]["close"]), [8.76, 8.73])
        self.assertEqual(list(data["600654.SH"]["close"]), [9.12, 9.15])
        # The server-side download must only contain the all-zero code.
        download_calls = [p for m, p in client.call_params if m == "download_history_data2"]
        self.assertEqual(len(download_calls), 1)
        self.assertEqual(download_calls[0]["stock_list"], ["600654.SH"])


class EnsureServerRawDiagnosticTest(unittest.TestCase):
    """[P7 root-cause fix] _ensure_server_raw must log on failure, not swallow."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        import bigqmt_signal_trader.xtquant_compat as xc
        self._orig_sleep = xc.time.sleep
        xc.time.sleep = lambda s: None
        self._xc = xc

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self._xc.time.sleep = self._orig_sleep

    def test_failing_download_prints_diagnostic(self):
        import contextlib
        import io

        xt = self._xc.BigQmtXtData(_FailingDownloadClient(self.dir))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            data = xt.get_market_data_ex(
                field_list=["close"], stock_list=["600000.SH"], period="1d",
                dividend_type="front",
            )
        output = out.getvalue()
        # The diagnostic must appear (not silent swallow).
        self.assertIn("_ensure_server_raw failed", output)
        self.assertIn("RuntimeError", output)
        # The method still returns data (graceful degradation).
        self.assertIn("600000.SH", data)


if __name__ == "__main__":
    unittest.main()
