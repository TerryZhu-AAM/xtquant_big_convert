"""
Tests for bridge quality layer fixes (2026-08-14)

Covers three root-cause fixes:
1. Unified data quality layer with violation tracking
2. FormulaServer stale view circuit breaker (cooldown)
3. Critical bare except fixes for data corruption paths
"""

import sys
import time
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData
from bigqmt_signal_trader.formula_server import FormulaServerRouter


class TestUnifiedQualityLayer:
    """Test unified data quality check layer and violation tracking."""

    def setup_method(self):
        """Create a fresh BigQmtXtData instance for each test."""
        mock_client = MagicMock()
        self.bridge = BigQmtXtData(client=mock_client)

    def test_quality_layer_detects_all_zero_close(self):
        """Quality layer detects all-zero close prices (server lacks raw data)."""
        import pandas as pd
        # Simulate response with all-zero close except last bar
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [0.0, 0.0, 0.0, 1800.0],  # All zero except last
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        result = self.bridge._check_bar_quality(bad_data)

        assert result is True  # Returns True if violations found
        assert self.bridge._quality_violation_counts["all_zero_close"] == 1

    def test_quality_layer_detects_nan_ratio(self):
        """Quality layer detects high NaN ratio (>50%)."""
        import pandas as pd
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [float("nan"), float("nan"), float("nan"), 1800.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        result = self.bridge._check_bar_quality(bad_data)

        assert result is True
        assert self.bridge._quality_violation_counts["nan_heavy"] == 1

    def test_quality_layer_detects_negative_price(self):
        """Quality layer detects negative prices."""
        import pandas as pd
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [-100.0, 1760.0, 1770.0, 1790.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        result = self.bridge._check_bar_quality(bad_data)

        assert result is True
        assert self.bridge._quality_violation_counts["negative_price"] == 1

    def test_quality_layer_detects_empty_dataframe(self):
        """Quality layer detects empty DataFrame."""
        import pandas as pd
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [],
                "open": [],
                "high": [],
                "low": [],
                "volume": [],
                "amount": [],
            })
        }

        result = self.bridge._check_bar_quality(bad_data)

        assert result is True
        assert self.bridge._quality_violation_counts["empty"] == 1

    def test_quality_layer_passes_good_data(self):
        """Quality layer returns False for good data (no violations)."""
        import pandas as pd
        good_data = {
            "600519.SH": pd.DataFrame({
                "close": [1750.0, 1760.0, 1770.0, 1790.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        result = self.bridge._check_bar_quality(good_data)

        assert result is False
        assert sum(self.bridge._quality_violation_counts.values()) == 0

    def test_quality_layer_detects_multiple_violations(self):
        """Quality layer can detect multiple violations in same data."""
        import pandas as pd
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [0.0, 0.0, float("nan"), -100.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        result = self.bridge._check_bar_quality(bad_data)

        # Should detect violations
        assert result is True
        total_violations = sum(self.bridge._quality_violation_counts.values())
        assert total_violations >= 1

    def test_quality_layer_handles_dataframe_object(self):
        """Quality layer works with pandas DataFrame objects."""
        try:
            import pandas as pd

            df = pd.DataFrame({
                "close": [0.0, 0.0, 0.0, 1800.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })

            result = self.bridge._check_bar_quality({"600519.SH": df})

            assert result is True
            assert self.bridge._quality_violation_counts["all_zero_close"] == 1
        except ImportError:
            pytest.skip("pandas not installed")

    def test_quality_violation_throttled_printing(self):
        """Quality layer throttles prints to once per 60 seconds."""
        import pandas as pd
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [0.0, 0.0, 0.0, 1800.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }

        with patch("builtins.print") as mock_print:
            # First call should print
            self.bridge._check_bar_quality(bad_data)
            first_call_count = mock_print.call_count

            # Immediate second call should be throttled
            self.bridge._check_bar_quality(bad_data)
            second_call_count = mock_print.call_count

            # Should not print again (throttled)
            assert second_call_count == first_call_count

            # Violation counter should still increment
            assert self.bridge._quality_violation_counts["all_zero_close"] == 2


class TestStaleCooldownCircuitBreaker:
    """Circuit breaker behavior, verified END-TO-END through router.call().

    A fake formula client serves a frozen (yesterday-only) view; the guard
    must classify it stale, and 3 consecutive stale hits must degrade
    get_market_data_ex in supports() until the cooldown expires.
    """

    FROZEN_STAMPS = ["20260813 15:00:00"]  # yesterday only -> stale for today window
    TODAY_WINDOW = {
        "field_list": ["close"],
        "stock_list": ["600048.SH"],
        "period": "1m",
        "start_time": "20260814093000",
        "end_time": "20260814150000",
    }

    def setup_method(self):
        import datetime as _dt

        import bigqmt_signal_trader.formula_server as fs_mod

        self._fs = fs_mod
        self._orig_now = fs_mod._local_now
        fs_mod._local_now = lambda: _dt.datetime(2026, 8, 14, 14, 30, 0)

        class _FrozenClient:
            host, port = "127.0.0.1", 58600

            def request(self, func, wire_params):
                timeline = []
                for stamp in TestStaleCooldownCircuitBreaker.FROZEN_STAMPS:
                    timeline.extend([stamp, ["close", 1.0, "volume", 100]])
                return {"result": ["600048.SH", timeline]}

            def close(self):
                pass

        self.router = fs_mod.FormulaServerRouter(client=_FrozenClient())

    def teardown_method(self):
        self._fs._local_now = self._orig_now

    def _stale_call(self):
        with pytest.raises(self._fs.Unroutable):
            self.router.call("get_market_data_ex", dict(self.TODAY_WINDOW))

    def test_two_stale_hits_no_degradation(self):
        """Below threshold the router stays available for get_market_data_ex."""
        self._stale_call()
        self._stale_call()
        assert self.router._stale_consecutive == 2
        assert self.router.supports("get_market_data_ex") is True

    def test_circuit_breaker_triggers_after_three_stale_hits(self):
        """3rd consecutive stale hit enters cooldown - supports() flips False."""
        self._stale_call()
        self._stale_call()
        self._stale_call()
        assert self.router._stale_consecutive == 3
        assert self.router.stale_hits == 3
        assert self.router._stale_degraded_until > 0
        assert self.router.supports("get_market_data_ex") is False
        stats = self.router.stats()
        assert stats["stale_degraded"] is True
        assert stats["stale_cooldown_remaining_seconds"] > 0

    def test_circuit_breaker_cooldown_first_value_is_60s(self):
        """First degradation arms the initial 60s cooldown (next armed at 120s)."""
        for _ in range(3):
            self._stale_call()
        assert self.router._stale_cooldown_seconds == 120.0  # armed for NEXT time
        remaining = self.router.stats()["stale_cooldown_remaining_seconds"]
        assert 55.0 < remaining <= 60.0

    def test_circuit_breaker_during_cooldown_other_methods_still_routed(self):
        """Cooldown degrades only get_market_data_ex, not other methods."""
        for _ in range(3):
            self._stale_call()
        assert self.router.supports("get_market_data_ex") is False
        assert self.router.supports("get_instrument_detail") is True

    def test_cooldown_expires_and_supports_returns_true(self):
        """After the cooldown elapses, supports() recovers."""
        for _ in range(3):
            self._stale_call()
        self.router._stale_degraded_until = time.time() - 1.0
        assert self.router.supports("get_market_data_ex") is True

    def test_circuit_breaker_resets_on_success(self):
        """A fresh get_market_data_ex answer resets consecutive + cooldown."""
        for _ in range(2):
            self._stale_call()
        # Fresh view: latest bar stamped today 14:29 (within the 5-min lag).
        fresh = ["20260813 15:00:00", "20260814 14:29:00"]

        class _FreshClient:
            host, port = "127.0.0.1", 58600

            def request(self, func, wire_params):
                timeline = []
                for stamp in fresh:
                    timeline.extend([stamp, ["close", 1.0, "volume", 100]])
                return {"result": ["600048.SH", timeline]}

            def close(self):
                pass

        self.router.client = _FreshClient()
        out = self.router.call("get_market_data_ex", dict(self.TODAY_WINDOW))
        assert "600048.SH" in out
        assert self.router._stale_consecutive == 0
        assert self.router._stale_cooldown_seconds == self.router._STALE_COOLDOWN_INITIAL
        assert self.router.supports("get_market_data_ex") is True

    def test_non_stale_method_does_not_reset_stale_counter(self):
        """A successful non-market-data call must not clear stale state."""
        for _ in range(2):
            self._stale_call()

        class _DetailClient:
            host, port = "127.0.0.1", 58600

            def request(self, func, wire_params):
                return {"result": {"InstrumentName": "x"}}

            def close(self):
                pass

        self.router.client = _DetailClient()
        self.router.call("get_instrument_detail", {"code": "600048.SH"})
        assert self.router._stale_consecutive == 2

    def test_circuit_breaker_exponential_backoff(self):
        """Backoff doubles 60-120-240-300 and stays capped at 300."""
        cooldown = self.router._STALE_COOLDOWN_INITIAL
        seen = []
        for _ in range(4):
            seen.append(cooldown)
            cooldown = min(cooldown * 2, self.router._STALE_COOLDOWN_MAX)
        assert seen == [60.0, 120.0, 240.0, 300.0]
        assert cooldown == 300.0


class TestCriticalBareExceptFixes:
    """Test that critical bare except patterns now have diagnostics."""

    def setup_method(self):
        """Create a fresh BigQmtXtData instance for each test."""
        mock_client = MagicMock()
        self.bridge = BigQmtXtData(client=mock_client)

    def test_get_stock_list_in_sector_logs_rpc_failure(self):
        """get_stock_list_in_sector logs RPC failure instead of silent pass."""
        # Mock RPC to fail
        self.bridge.client.call = MagicMock(side_effect=Exception("RPC timeout"))

        with patch("builtins.print") as mock_print:
            try:
                self.bridge.get_stock_list_in_sector("沪深A股")
            except Exception:
                pass  # Expected to fail

            # Should have printed diagnostic
            assert mock_print.called
            call_args = str(mock_print.call_args)
            assert "get_stock_list_in_sector" in call_args
            assert "failed" in call_args.lower()

    def test_subscribe_quote_callback_logs_initial_snapshot_failure(self):
        """subscribe_quote logs initial snapshot callback failure."""
        # Mock successful subscription but callback fails
        self.bridge.client.call = MagicMock(return_value={"seq": 123})

        def failing_callback(data):
            raise Exception("Callback error")

        with patch("builtins.print") as mock_print:
            self.bridge.subscribe_quote("600519.SH", callback=failing_callback)

            # Should have printed diagnostic
            assert mock_print.called
            call_args = str(mock_print.call_args)
            assert "subscribe_quote" in call_args
            assert "initial snapshot" in call_args.lower()

    def test_subscribe_quote_stale_cleanup_logs_failure(self):
        """subscribe_quote logs stale cleanup failure instead of silent pass."""
        # Mock successful subscription save (triggers cleanup path)
        self.bridge.client.save_quote_subscription = MagicMock(return_value=True)
        self.bridge.client.publish_event = MagicMock()

        # Mock Redis to fail during cleanup
        mock_redis = MagicMock()
        mock_redis.hlen.return_value = 15  # Trigger cleanup (> 10)
        mock_redis.hgetall.side_effect = Exception("Redis error")
        self.bridge.client._redis = MagicMock(return_value=mock_redis)
        self.bridge.client.account_id = "test_account"

        with patch("builtins.print") as mock_print:
            self.bridge.subscribe_quote("600519.SH")

            # Should have printed diagnostic
            assert mock_print.called
            call_args = str(mock_print.call_args)
            assert "stale cleanup" in call_args.lower()


class TestIntegration:
    """Integration tests for the quality layer with real bridge operations."""

    def setup_method(self):
        """Create a fresh BigQmtXtData instance for each test."""
        mock_client = MagicMock()
        self.bridge = BigQmtXtData(client=mock_client)

    def test_get_market_data_ex_runs_quality_check(self):
        """get_market_data_ex runs quality check on returned data."""
        import pandas as pd
        # Mock RPC to return bad data
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [0.0, 0.0, 0.0, 1800.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }
        self.bridge.client.call = MagicMock(return_value=bad_data)

        with patch("builtins.print"):
            # Call should trigger quality check
            self.bridge.get_market_data_ex(["600519.SH"])

            # Violation should be recorded
            assert self.bridge._quality_violation_counts.get("all_zero_close", 0) >= 1

    def test_get_market_data_runs_quality_check(self):
        """get_market_data runs quality check on returned data."""
        import pandas as pd
        # Mock RPC to return bad data
        bad_data = {
            "600519.SH": pd.DataFrame({
                "close": [0.0, 0.0, 0.0, 1800.0],
                "open": [1750.0, 1760.0, 1770.0, 1790.0],
                "high": [1780.0, 1790.0, 1800.0, 1810.0],
                "low": [1740.0, 1750.0, 1760.0, 1780.0],
                "volume": [1000, 1200, 1100, 1300],
                "amount": [1750000, 1760000, 1770000, 1790000],
            })
        }
        self.bridge.client.call = MagicMock(return_value=bad_data)

        with patch("builtins.print"):
            # Call should trigger quality check
            self.bridge.get_market_data(["600519.SH"])

            # Violation should be recorded
            assert self.bridge._quality_violation_counts.get("all_zero_close", 0) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
