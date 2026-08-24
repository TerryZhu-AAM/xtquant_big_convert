"""Regression guards for bridge review fixes (2026-08-14, adversarial re-review).

Three guards, each closing a blind spot found by reviewing the SHIM layer
rather than the compat layer:

1. Shim signature parity — every explicit wrapper in ``xtquant/xtdata.py``
   must accept the same parameters as the ``BigQmtXtData`` method it
   forwards to. The ``unsubscribe_quote(code, period=)`` production call
   path (gateway_provider) previously raised TypeError INSIDE the shim
   while the compat layer was fixed — mocks of the compat layer never see
   this (the "mock _xt_data blind spot").
2. Client-side Redis db=0 — ``BigQmtRpcClient`` must honor an explicit
   ``db=0`` in redis_config instead of letting the falsy value fall
   through ``or`` to the default 5 (same class of bug as the fixed
   server-side redis_common).
3. Quality layer shape awareness — a dict whose values are not bar frames
   (field-major dict / garbage dump) must be flagged ``malformed_shape``,
   never silently skipped; ``_zero_codes_from_data`` must never emit a
   field name as a stock code.
"""

import inspect
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import xtquant.xtdata as xtdata_shim  # noqa: E402
from bigqmt_signal_trader.xtquant_compat import BigQmtRpcClient, BigQmtXtData  # noqa: E402


class ShimSignatureParityTest(unittest.TestCase):
    """[review fix] shim wrappers must not be narrower than compat methods."""

    # [compat v0.2.6 merge] 已知合理 drift 白名单:
    # shim 与 compat 设计目标不同 — shim 伪装原生 xtquant 协议,
    # compat 是 RPC bridge 真实实现. 以下白名单登记"shim 双参 / compat 单参"
    # 的故意差异 (反向兼容原生 xtquant 调用范式, 而 compat RPC 不区分):
    # - get_instrument_detail: shim 收 is_detail=False 兼容原生双参调用,
    #   compat 只接 stock_code (RPC 协议本就不区分).
    KNOWN_DRIFT = {
        "get_instrument_detail": "shim 兼容原生 xtquant 双参 (code, is_detail), compat RPC 单参",
    }

    def test_all_explicit_shim_wrappers_match_compat_signatures(self):
        compat_cls = BigQmtXtData
        drifted = []
        for name, fn in sorted(vars(xtdata_shim).items()):
            if name.startswith("_") or not isinstance(fn, types.FunctionType):
                continue
            method = getattr(compat_cls, name, None)
            if method is None:
                continue
            if name in self.KNOWN_DRIFT:
                # 白名单项: 验证 shim 是 compat 的超集 (shim 多收的参数都有默认值)
                shim_params = list(inspect.signature(fn).parameters)
                compat_params = list(inspect.signature(method).parameters)
                if compat_params and compat_params[0] == "self":
                    compat_params = compat_params[1:]
                extra = [p for p in shim_params if p not in compat_params]
                for p in extra:
                    default = inspect.signature(fn).parameters[p].default
                    if default is inspect.Parameter.empty:
                        drifted.append(
                            "%s: KNOWN_DRIFT 漂移参数 %s 无默认值, 必须默认兼容 (当前 default=%r)"
                            % (name, p, default)
                        )
                continue
            shim_params = list(inspect.signature(fn).parameters)
            compat_params = list(inspect.signature(method).parameters)
            if compat_params and compat_params[0] == "self":
                compat_params = compat_params[1:]
            if shim_params != compat_params:
                drifted.append(
                    "%s: shim(%s) != compat(%s)"
                    % (name, ", ".join(shim_params), ", ".join(compat_params))
                )
        self.assertEqual(drifted, [])

    def test_unsubscribe_quote_accepts_code_and_period_kwargs(self):
        """The exact production call (gateway_provider.py) must not TypeError."""
        recorded = {}

        class _FakeXtData:
            def unsubscribe_quote(self, seq_or_code, period=None):
                recorded["args"] = (seq_or_code, period)
                return 0

        orig = xtdata_shim._compat.xtdata
        xtdata_shim._compat.xtdata = _FakeXtData()
        try:
            result = xtdata_shim.unsubscribe_quote("600000.SH", period="1m")
        finally:
            xtdata_shim._compat.xtdata = orig
        self.assertEqual(result, 0)
        self.assertEqual(recorded["args"], ("600000.SH", "1m"))

    def test_unsubscribe_quote_still_accepts_seq(self):
        """Native-style seq call keeps working through the shim."""
        recorded = {}

        class _FakeXtData:
            def unsubscribe_quote(self, seq_or_code, period=None):
                recorded["args"] = (seq_or_code, period)
                return 0

        orig = xtdata_shim._compat.xtdata
        xtdata_shim._compat.xtdata = _FakeXtData()
        try:
            xtdata_shim.unsubscribe_quote(12345)
        finally:
            xtdata_shim._compat.xtdata = orig
        self.assertEqual(recorded["args"], (12345, None))

    def test_download_history_data2_accepts_dividend_type_and_chunk(self):
        """Adjusted downloads pass dividend_type through the shim."""
        recorded = {}

        class _FakeXtData:
            def download_history_data2(self, stock_list, period, start_time="",
                                       end_time="", callback=None, incrementally=None,
                                       dividend_type="none", chunk_size=None,
                                       download_timeout_seconds=180.0, data_wait_seconds=60.0):
                recorded["kwargs"] = dict(
                    dividend_type=dividend_type, chunk_size=chunk_size,
                    download_timeout_seconds=download_timeout_seconds,
                    data_wait_seconds=data_wait_seconds,
                )
                return {"finished": 0, "total": 0}

        orig = xtdata_shim._compat.xtdata
        xtdata_shim._compat.xtdata = _FakeXtData()
        try:
            xtdata_shim.download_history_data2(
                ["600000.SH"], "1d", dividend_type="front", chunk_size=10
            )
        finally:
            xtdata_shim._compat.xtdata = orig
        self.assertEqual(
            recorded["kwargs"],
            {
                "dividend_type": "front",
                "chunk_size": 10,
                "download_timeout_seconds": 180.0,
                "data_wait_seconds": 60.0,
            },
        )


class ClientRedisDbZeroTest(unittest.TestCase):
    """[review fix] db=0 in client redis_config must reach redis.Redis as 0."""

    def _make(self, redis_config):
        # Real constructor path with a stubbed config loader: exercises the
        # db normalization under test without environment-dependent modules.
        import bigqmt_signal_trader.xtquant_compat as xc

        orig_loader = xc.load_client_config
        saved_env = os.environ.pop("BIGQMT_REDIS_DB", None)
        xc.load_client_config = lambda module_name=None: {}
        try:
            return BigQmtRpcClient(account_id="acc1", redis_config=dict(redis_config))
        finally:
            xc.load_client_config = orig_loader
            if saved_env is not None:
                os.environ["BIGQMT_REDIS_DB"] = saved_env

    def test_explicit_db_zero_is_honored(self):
        client = self._make({"db": 0})
        self.assertEqual(client.redis_config["db"], 0)

    def test_missing_db_falls_back_to_env_default(self):
        client = self._make({})
        self.assertEqual(client.redis_config["db"], 5)

    def test_empty_string_db_falls_back(self):
        client = self._make({"db": ""})
        self.assertEqual(client.redis_config["db"], 5)


class QualityShapeAwarenessTest(unittest.TestCase):
    """[review fix] quality layer + self-heal on non-bar dict shapes."""

    def setUp(self):
        self.bridge = BigQmtXtData(client=MagicMock())

    def test_field_major_dict_is_flagged_malformed(self):
        """{field: {code: list}} must be flagged, not iterated as codes."""
        data = {"close": {"600000.SH": [1.0, 2.0, 0.0]}}
        found = self.bridge._check_bar_quality(data, context="get_market_data")
        self.assertTrue(found)
        self.assertGreaterEqual(
            self.bridge._quality_violation_counts.get("malformed_shape", 0), 1
        )

    def test_garbage_dict_is_flagged_malformed(self):
        """A leaked object dump (values are None/scalars) is flagged too."""
        data = {"is_copy": None, "ref": "0x7f"}
        found = self.bridge._check_bar_quality(data)
        self.assertTrue(found)
        self.assertGreaterEqual(
            self.bridge._quality_violation_counts.get("malformed_shape", 0), 1
        )

    def test_code_major_dict_unchanged_behavior(self):
        """{code: DataFrame} keeps the documented per-code contract."""
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        data = {"600000.SH": pd.DataFrame({"close": [0.0, 0.0, 8.73]})}
        found = self.bridge._check_bar_quality(data)
        self.assertTrue(found)
        self.assertEqual(
            self.bridge._quality_violation_counts.get("all_zero_close"), 1
        )
        self.assertNotIn("malformed_shape", self.bridge._quality_violation_counts)

    def test_zero_codes_never_emits_field_names(self):
        """Field-major dict -> sentinel ['*'] (re-download all), not field names."""
        data = {"close": {"600000.SH": [0.0, 0.0]}}
        zeros = BigQmtXtData._zero_codes_from_data(data)
        self.assertEqual(zeros, ["*"])


if __name__ == "__main__":
    unittest.main()
