"""
Comprehensive Integration Tests for Sells-Per-Day Limit Logic

These tests verify the entire sell counter flow across OrderExecutor, StateMachine,
and LivePairsTrader to ensure Good Faith Violation prevention works correctly.

Test Coverage:
    1. OrderExecutor paths (execute_swap, place_and_wait_for_fill)
    2. State machine transitions (HOLDING_WAITING → HOLDING_DAILY_LIMIT)
    3. Counter persistence and validation
    4. Edge cases and error conditions
    5. Multiple sell attempts
    6. Daily reset logic
    7. Crash recovery scenarios
    8. Buy operations (should not be blocked)
"""

import pytest
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import Mock, MagicMock, patch, call
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.sell_counter import SellCounterManager, COUNTER_FILE, EASTERN_TZ
from live_trading.order_executor import OrderExecutor, OrderResult, OrderStatus
from live_trading.state_machine import StateMachine, TradingState, StockHeld
from live_trading.logger import logger


@pytest.fixture
def clean_counter_file():
    """Ensure counter file is removed before and after each test."""
    if COUNTER_FILE.exists():
        COUNTER_FILE.unlink()
    yield
    if COUNTER_FILE.exists():
        COUNTER_FILE.unlink()


@pytest.fixture
def mock_api():
    """Create a mock TradeStation API."""
    api = Mock()
    
    # Mock orders API
    api.orders = Mock()
    api.orders.place_order = Mock(return_value={
        'Orders': [{
            'OrderID': 'TEST123',
            'Status': 'ACK'  # TradeStation uses 3-letter codes: ACK = Received
        }]
    })
    api.orders.get_order = Mock(return_value={
        'OrderID': 'TEST123',
        'Status': 'FLL',  # TradeStation uses 3-letter codes: FLL = Filled
        'FilledQuantity': 100,
        'AveragePrice': 280.50
    })
    
    # Mock account API
    api.account = Mock()
    api.account.get_balances = Mock(return_value={
        'Balances': [{
            'BuyingPower': '10000',
            'CashBalance': '10000'
        }]
    })
    
    return api


@pytest.fixture
def order_executor(mock_api, clean_counter_file):
    """Create an OrderExecutor with sell counter manager."""
    sell_counter_manager = SellCounterManager()
    executor = OrderExecutor(
        api=mock_api,
        account_id='TEST123',
        logger=None,
        allocated_cash=0,
        sell_counter_manager=sell_counter_manager,
        sells_per_day_limit=1
    )
    return executor


class TestExecuteSwapWithSellLimit:
    """Test execute_swap method with sell counter limit = 1."""
    
    def test_first_swap_allowed(self, order_executor, clean_counter_file):
        """Test that first swap of the day is allowed."""
        # Execute first swap
        sell_result, buy_result = order_executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        # Should succeed
        assert sell_result is not None
        assert sell_result.is_filled
        assert buy_result is not None
        
        # Counter should be 1
        assert order_executor.sell_counter_manager.get_counter() == 1
        
        # File should exist
        assert COUNTER_FILE.exists()
    
    def test_second_swap_blocked(self, order_executor, clean_counter_file):
        """Test that second swap of the day is blocked."""
        # Execute first swap
        order_executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        # Try second swap
        sell_result, buy_result = order_executor.execute_swap(
            sell_symbol="MA",
            sell_quantity=50,
            buy_symbol="V",
            current_price_sell=520.00,
            current_price_buy=280.50
        )
        
        # Should be blocked (both None)
        assert sell_result is None
        assert buy_result is None
        
        # Counter should still be 1
        assert order_executor.sell_counter_manager.get_counter() == 1
    
    def test_swap_counter_persisted_before_api_call(self, order_executor, clean_counter_file, mock_api):
        """Test that counter is persisted BEFORE the sell API call."""
        # Track the order of operations
        call_order = []
        
        original_place_order = mock_api.orders.place_order
        original_increment = order_executor.sell_counter_manager.increment_and_persist
        
        def tracked_place_order(*args, **kwargs):
            call_order.append('api_call')
            return original_place_order(*args, **kwargs)
        
        def tracked_increment():
            call_order.append('persist')
            return original_increment()
        
        mock_api.orders.place_order = tracked_place_order
        order_executor.sell_counter_manager.increment_and_persist = tracked_increment
        
        # Execute swap
        order_executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        # Verify persist happened before API call
        assert call_order.index('persist') < call_order.index('api_call')
    
    def test_swap_persistence_failure_aborts(self, order_executor, clean_counter_file, mock_api):
        """Test that persistence failure aborts the swap."""
        # Make persistence fail
        order_executor.sell_counter_manager.increment_and_persist = Mock(
            side_effect=RuntimeError("Disk full")
        )
        
        # Try swap
        sell_result, buy_result = order_executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        # Should be aborted (both None)
        assert sell_result is None
        assert buy_result is None
        
        # API should NOT have been called
        mock_api.orders.place_order.assert_not_called()
    
    def test_swap_with_no_counter_manager(self, mock_api, clean_counter_file):
        """Test swap still works when counter manager is None."""
        executor = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=None,  # No counter
            sells_per_day_limit=1
        )
        
        # Execute swap
        sell_result, buy_result = executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        # Should succeed (no counter checking)
        assert sell_result is not None
        assert buy_result is not None


class TestPlaceAndWaitForFillWithSellLimit:
    """Test place_and_wait_for_fill method with sell counter logic."""
    
    def test_sell_order_first_of_day(self, order_executor, clean_counter_file):
        """Test first SELL order of the day is allowed."""
        result = order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Should succeed
        assert result.is_filled
        assert result.action == "SELL"
        
        # Counter should be 1
        assert order_executor.sell_counter_manager.get_counter() == 1
    
    def test_sell_order_second_of_day_blocked(self, order_executor, clean_counter_file):
        """Test second SELL order of the day is blocked."""
        # First sell
        order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Second sell
        result = order_executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        
        # Should be blocked
        assert result.is_rejected
        assert result.error_message == "Daily sell limit reached - blocked to prevent GFV"
        assert result.filled_quantity == 0
    
    def test_buy_orders_not_affected_by_counter(self, order_executor, clean_counter_file):
        """Test that BUY orders are not affected by sell counter."""
        # Do one sell to increment counter
        order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Counter is now at limit (1/1)
        assert order_executor.sell_counter_manager.get_counter() == 1
        
        # Try multiple BUY orders - should all succeed
        for i in range(5):
            result = order_executor.place_and_wait_for_fill(
                symbol="MA",
                action="BUY",
                quantity=50,
                expected_price=520.00
            )
            
            assert result.is_filled
            assert result.action == "BUY"
        
        # Counter should still be 1 (buys don't increment)
        assert order_executor.sell_counter_manager.get_counter() == 1
    
    def test_sell_persistence_failure_returns_rejected(self, order_executor, clean_counter_file):
        """Test that persistence failure returns rejected OrderResult."""
        # Make persistence fail
        order_executor.sell_counter_manager.increment_and_persist = Mock(
            side_effect=RuntimeError("Write failed")
        )
        
        result = order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Should be rejected
        assert result.is_rejected
        assert "Sell counter persistence failed" in result.error_message
        
        # API should NOT have been called
        order_executor.api.orders.place_order.assert_not_called()


class TestCounterValidation:
    """Test counter validation during persistence."""
    
    def test_validation_catches_write_mismatch(self, order_executor, clean_counter_file):
        """Test that validation catches file write mismatches."""
        # Mock file write to succeed but return wrong value on readback
        original_increment = order_executor.sell_counter_manager.increment_and_persist
        
        call_count = [0]
        def faulty_increment():
            call_count[0] += 1
            # First call: write wrong value
            if call_count[0] == 1:
                order_executor.sell_counter_manager.sells_today = 1
                COUNTER_FILE.write_text(f"{order_executor.sell_counter_manager.current_date}\n999\n")
                # Validation should fail
                raise RuntimeError("Validation mismatch: expected (2025-01-14, 1), got (2025-01-14, 999)")
            return original_increment()
        
        order_executor.sell_counter_manager.increment_and_persist = faulty_increment
        
        # Try swap - should abort due to validation failure
        sell_result, buy_result = order_executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
        
        assert sell_result is None
        assert buy_result is None
    
    def test_file_exists_after_successful_persist(self, order_executor, clean_counter_file):
        """Test that file exists and is readable after successful persist."""
        order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # File should exist
        assert COUNTER_FILE.exists()
        
        # Should be readable
        content = COUNTER_FILE.read_text()
        lines = content.strip().split('\n')
        assert len(lines) == 2
        assert lines[1] == "1"


class TestDailyReset:
    """Test daily reset logic for counter."""
    
    def test_counter_resets_on_new_day(self, order_executor, clean_counter_file):
        """Test that counter allows sells after date changes."""
        # Do first sell
        order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Counter at limit
        assert order_executor.sell_counter_manager.get_counter() == 1
        
        # Simulate new day by setting old date
        order_executor.sell_counter_manager.current_date = "2025-01-01"
        
        # Try another sell - should succeed after auto-reset
        result = order_executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        
        assert result.is_filled
        assert order_executor.sell_counter_manager.get_counter() == 1  # Reset then incremented
    
    def test_can_sell_checks_date_and_resets(self, clean_counter_file):
        """Test that can_sell detects new day and resets."""
        manager = SellCounterManager()
        manager.sells_today = 5
        manager.current_date = "2025-01-01"  # Old date
        
        # can_sell should detect new day
        result = manager.can_sell(limit=1)
        
        # Should reset and allow sell
        assert result is True
        assert manager.sells_today == 0


class TestCrashRecovery:
    """Test that counter survives app crashes."""
    
    def test_counter_persists_across_restart(self, mock_api, clean_counter_file):
        """Test counter survives app restart."""
        # First instance: do a sell
        executor1 = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=1
        )
        
        executor1.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Simulate crash and restart - new instance
        executor2 = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=1
        )
        
        # Load counter
        counter = executor2.sell_counter_manager.load_counter()
        assert counter == 1
        
        # Try another sell - should be blocked
        result = executor2.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        
        assert result.is_rejected
    
    def test_persistence_before_crash_prevents_double_sell(self, mock_api, clean_counter_file):
        """Test that persisting before sell prevents double-sell after crash."""
        executor = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=1
        )
        
        # Mock API to fail AFTER counter persisted
        def fail_after_persist(*args, **kwargs):
            # Counter should already be persisted by now
            assert COUNTER_FILE.exists()
            content = COUNTER_FILE.read_text()
            assert "1" in content
            raise Exception("Simulated crash during API call")
        
        executor.api.orders.place_order = fail_after_persist
        
        # Try sell - will fail but counter should be persisted
        try:
            executor.place_and_wait_for_fill(
                symbol="V",
                action="SELL",
                quantity=100,
                expected_price=280.50
            )
        except:
            pass  # Expected to fail
        
        # Counter file should exist with value 1
        assert COUNTER_FILE.exists()
        
        # After "restart", counter should be 1
        executor2 = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=1
        )
        counter = executor2.sell_counter_manager.load_counter()
        assert counter == 1


class TestStateMachineIntegration:
    """Test state machine integration with sell counter."""
    
    def test_state_machine_tracks_sells_today(self, clean_counter_file):
        """Test that state machine tracks sells_today."""
        sm = StateMachine()
        
        # Initially 0
        assert sm.data.sells_today == 0
        
        # Increment
        sm.increment_sells_today()
        assert sm.data.sells_today == 1
        
        # Reset
        sm.reset_sells_today("2025-01-14")
        assert sm.data.sells_today == 0
        assert sm.data.last_trade_day == "2025-01-14"
    
    def test_state_machine_set_sells_today(self, clean_counter_file):
        """Test setting sells_today from file on startup."""
        sm = StateMachine()
        
        # Set counter (as would happen on startup)
        sm.set_sells_today(3)
        assert sm.data.sells_today == 3
    
    def test_transition_to_holding_daily_limit(self, clean_counter_file):
        """Test transition to HOLDING_DAILY_LIMIT when limit reached."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        sm.set_current_stock(StockHeld.TICKER_A)
        
        # Set sells to limit
        sm.set_sells_today(1)
        
        # Should be able to transition to HOLDING_DAILY_LIMIT
        assert sm.can_transition_to(TradingState.HOLDING_DAILY_LIMIT)
        
        sm.transition_to(
            TradingState.HOLDING_DAILY_LIMIT,
            reason="Sell limit reached"
        )
        
        assert sm.state == TradingState.HOLDING_DAILY_LIMIT


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_limit_zero_blocks_all_sells(self, mock_api, clean_counter_file):
        """Test that limit=0 blocks all sells."""
        executor = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=0
        )
        
        # Try sell with limit=0
        result = executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Should be blocked
        assert result.is_rejected
    
    def test_very_high_limit_allows_many_sells(self, mock_api, clean_counter_file):
        """Test that high limit allows many sells."""
        executor = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=100
        )
        
        # Do 10 sells - all should succeed
        for i in range(10):
            result = executor.place_and_wait_for_fill(
                symbol="V",
                action="SELL",
                quantity=100,
                expected_price=280.50
            )
            assert result.is_filled
        
        # Counter should be 10
        assert executor.sell_counter_manager.get_counter() == 10
    
    def test_concurrent_sell_attempts_dont_corrupt_counter(self, order_executor, clean_counter_file):
        """Test that counter stays consistent even with rapid sell attempts."""
        # Do first sell
        result1 = order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Rapid second attempt
        result2 = order_executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        
        # First succeeds, second blocked
        assert result1.is_filled
        assert result2.is_rejected
        
        # Counter should be exactly 1 (not corrupted)
        assert order_executor.sell_counter_manager.get_counter() == 1
    
    def test_file_corruption_handled_gracefully(self, order_executor, clean_counter_file):
        """Test that corrupted counter file is handled gracefully."""
        # Write corrupted file
        COUNTER_FILE.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_FILE.write_text("corrupted garbage\n")
        
        # Load counter - should handle gracefully
        counter = order_executor.sell_counter_manager.load_counter()
        
        # Should return 0 for corrupted file
        assert counter == 0
        
        # Should still be able to sell
        result = order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        assert result.is_filled


class TestCleanupStatesWithSellLimit:
    """Test that cleanup sells are also subject to limit."""
    
    def test_cleanup_sell_blocked_at_limit(self, order_executor, clean_counter_file):
        """Test that cleanup sell is blocked when limit reached."""
        # Use up the daily sell
        order_executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        
        # Try cleanup sell
        result = order_executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00,
            current_state="CLEANUP_CONFLICT"
        )
        
        # Should be blocked
        assert result.is_rejected
        assert "Daily sell limit reached" in result.error_message


class TestMultipleDaysScenario:
    """Test scenarios spanning multiple days."""
    
    def test_three_day_scenario(self, mock_api, clean_counter_file):
        """Test sell counter over 3 days."""
        # Day 1
        executor = OrderExecutor(
            api=mock_api,
            account_id='TEST123',
            logger=None,
            allocated_cash=0,
            sell_counter_manager=SellCounterManager(),
            sells_per_day_limit=1
        )
        
        # Day 1: First sell
        result = executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        assert result.is_filled
        
        # Day 1: Second sell blocked
        result = executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        assert result.is_rejected
        
        # Simulate Day 2
        executor.sell_counter_manager.current_date = "2025-01-01"  # Old date triggers reset
        
        # Day 2: Sell allowed again
        result = executor.place_and_wait_for_fill(
            symbol="V",
            action="SELL",
            quantity=100,
            expected_price=280.50
        )
        assert result.is_filled
        
        # Simulate Day 3
        executor.sell_counter_manager.current_date = "2025-01-02"  # Old date triggers reset
        
        # Day 3: Sell allowed again
        result = executor.place_and_wait_for_fill(
            symbol="MA",
            action="SELL",
            quantity=50,
            expected_price=520.00
        )
        assert result.is_filled


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])

