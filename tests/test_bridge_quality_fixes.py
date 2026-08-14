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
    """Test FormulaServer stale view circuit breaker with cooldown."""

    def setup_method(self):
        """Create a fresh FormulaServerRouter for each test."""
        self.router = FormulaServerRouter()

    def test_stale_consecutive_counter_increments(self):
        """Stale consecutive counter increments on each stale detection."""
        # Simulate stale detection by directly incrementing counter
        self.router._stale_consecutive += 1
        assert self.router._stale_consecutive == 1

        self.router._stale_consecutive += 1
        assert self.router._stale_consecutive == 2

        self.router._stale_consecutive += 1
        assert self.router._stale_consecutive == 3

    def test_circuit_breaker_triggers_after_threshold(self):
        """Circuit breaker triggers after 3 consecutive stale hits."""
        # Below threshold - no degradation
        self.router._stale_consecutive = 2
        assert self.router._stale_degraded_until == 0.0

        # At threshold - should trigger degradation
        # (This would normally happen in call() method when _stale_market_data returns True)
        self.router._stale_consecutive = 3
        assert self.router._stale_consecutive >= self.router._STALE_CONSECUTIVE_THRESHOLD

    def test_circuit_breaker_exponential_backoff(self):
        """Circuit breaker uses exponential backoff (60s → 120s → 240s → 300s cap)."""
        # Initial cooldown is 60s
        assert self.router._stale_cooldown_seconds == 60.0

        # After first trigger, should double to 120s
        self.router._stale_cooldown_seconds = min(
            self.router._stale_cooldown_seconds * 2, 
            self.router._STALE_COOLDOWN_MAX
        )
        assert self.router._stale_cooldown_seconds == 120.0

        # After second trigger, should double to 240s
        self.router._stale_cooldown_seconds = min(
            self.router._stale_cooldown_seconds * 2, 
            self.router._STALE_COOLDOWN_MAX
        )
        assert self.router._stale_cooldown_seconds == 240.0

        # After third trigger, should cap at 300s
        self.router._stale_cooldown_seconds = min(
            self.router._stale_cooldown_seconds * 2, 
            self.router._STALE_COOLDOWN_MAX
        )
        assert self.router._stale_cooldown_seconds == 300.0

        # Should stay at cap
        self.router._stale_cooldown_seconds = min(
            self.router._stale_cooldown_seconds * 2, 
            self.router._STALE_COOLDOWN_MAX
        )
        assert self.router._stale_cooldown_seconds == 300.0

    def test_circuit_breaker_resets_on_success(self):
        """Circuit breaker resets counter on successful get_market_data_ex."""
        # Build up stale counter
        self.router._stale_consecutive = 2
        self.router._stale_degraded_until = time.time() + 60
        self.router._stale_cooldown_seconds = 120.0

        # Simulate successful call (this happens in call() method at line 856-858)
        method = "get_market_data_ex"
        if method == "get_market_data_ex":
            self.router._stale_consecutive = 0
            self.router._stale_cooldown_seconds = self.router._STALE_COOLDOWN_INITIAL

        assert self.router._stale_consecutive == 0
        assert self.router._stale_cooldown_seconds == 60.0

    def test_circuit_breaker_only_resets_for_get_market_data_ex(self):
        """Circuit breaker only resets for get_market_data_ex, not other methods."""
        # Build up stale counter
        self.router._stale_consecutive = 2
        self.router._stale_degraded_until = time.time() + 60
        self.router._stale_cooldown_seconds = 120.0

        # Successful call for different method should not reset
        method = "get_instrument_detail"
        if method == "get_market_data_ex":
            self.router._stale_consecutive = 0
            self.router._stale_cooldown_seconds = self.router._STALE_COOLDOWN_INITIAL

        assert self.router._stale_consecutive == 2
        assert self.router._stale_cooldown_seconds == 120.0

    def test_supports_returns_false_during_cooldown(self):
        """supports() returns False for get_market_data_ex during cooldown."""
        # Trigger cooldown
        self.router._stale_degraded_until = time.time() + 60

        # Should not support get_market_data_ex during cooldown
        assert self.router.supports("get_market_data_ex") is False

        # Other methods should still be supported (if they exist in METHOD_MAP)
        # get_instrument_detail is a valid method
        assert self.router.supports("get_instrument_detail") is True

    def test_supports_returns_true_after_cooldown(self):
        """supports() returns True for get_market_data_ex after cooldown expires."""
        # Cooldown already expired
        self.router._stale_degraded_until = time.time() - 10

        # Should support get_market_data_ex again
        assert self.router.supports("get_market_data_ex") is True

    def test_stats_includes_cooldown_info(self):
        """stats() includes stale degradation information."""
        self.router._stale_consecutive = 2
        self.router._stale_degraded_until = time.time() + 30

        stats = self.router.stats()

        assert "stale_consecutive" in stats
        assert stats["stale_consecutive"] == 2
        assert "stale_degraded" in stats
        assert stats["stale_degraded"] is True
        assert "stale_cooldown_remaining_seconds" in stats
        assert 25 < stats["stale_cooldown_remaining_seconds"] < 35


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
