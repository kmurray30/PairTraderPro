"""
Unit Tests for StateMachine StateData Management

Tests all StateData manipulation methods and ensures data persists
correctly across state transitions.
"""

import unittest
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.state_machine import (
    TradingState, StockHeld, StateMachine, StateData
)


class TestStateDataManagement(unittest.TestCase):
    """Test StateData manipulation methods."""
    
    def test_set_current_stock_updates_correctly(self):
        """Test set_current_stock() updates correctly."""
        sm = StateMachine()
        
        # Initially NONE
        self.assertEqual(sm.data.current_stock, StockHeld.NONE)
        
        # Set to TICKER_A
        sm.set_current_stock(StockHeld.TICKER_A)
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        
        # Set to TICKER_B
        sm.set_current_stock(StockHeld.TICKER_B)
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_B)
        
        # Set back to NONE
        sm.set_current_stock(StockHeld.NONE)
        self.assertEqual(sm.data.current_stock, StockHeld.NONE)
    
    def test_set_pending_order(self):
        """Test set_pending_order() sets order ID."""
        sm = StateMachine()
        
        self.assertIsNone(sm.data.pending_order_id)
        
        sm.set_pending_order("ORD123456")
        self.assertEqual(sm.data.pending_order_id, "ORD123456")
    
    def test_clear_pending_order(self):
        """Test set_pending_order(None) clears order ID."""
        sm = StateMachine()
        
        sm.set_pending_order("ORD123456")
        self.assertEqual(sm.data.pending_order_id, "ORD123456")
        
        sm.set_pending_order(None)
        self.assertIsNone(sm.data.pending_order_id)
    
    def test_increment_sells_today(self):
        """Test increment_sells_today() increments counter."""
        sm = StateMachine()
        
        self.assertEqual(sm.data.sells_today, 0)
        
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 1)
        
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 2)
        
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 3)
    
    def test_reset_sells_today(self):
        """Test reset_sells_today() resets counter and updates date."""
        sm = StateMachine()
        
        # Increment counter
        sm.increment_sells_today()
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 2)
        
        # Reset
        sm.reset_sells_today("2025-01-15")
        
        self.assertEqual(sm.data.sells_today, 0)
        self.assertEqual(sm.data.last_trade_day, "2025-01-15")
    
    def test_set_sells_today_loads_from_file(self):
        """Test set_sells_today() sets counter (used on startup)."""
        sm = StateMachine()
        
        self.assertEqual(sm.data.sells_today, 0)
        
        # Simulate loading from file
        sm.set_sells_today(5)
        
        self.assertEqual(sm.data.sells_today, 5)
    
    def test_portfolio_value_at_trade_start_tracking(self):
        """Test portfolio_value_at_trade_start tracking."""
        sm = StateMachine()
        
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 0.0)
        
        # Set value when trade starts
        sm.data.portfolio_value_at_trade_start = 10000.50
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 10000.50)
        
        # Update value
        sm.data.portfolio_value_at_trade_start = 10500.75
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 10500.75)
    
    def test_last_trade_day_updates_on_trades(self):
        """Test last_trade_day updates correctly."""
        sm = StateMachine()
        
        self.assertIsNone(sm.data.last_trade_day)
        
        # Update on first trade
        sm.reset_sells_today("2025-01-14")
        self.assertEqual(sm.data.last_trade_day, "2025-01-14")
        
        # Update on next day
        sm.reset_sells_today("2025-01-15")
        self.assertEqual(sm.data.last_trade_day, "2025-01-15")


class TestStateDataPersistence(unittest.TestCase):
    """Test StateData persists correctly across transitions."""
    
    def test_state_data_survives_state_transitions(self):
        """Test StateData survives state transitions."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Set some data
        sm.set_current_stock(StockHeld.TICKER_A)
        sm.set_sells_today(2)
        sm.data.portfolio_value_at_trade_start = 10000.0
        
        # Transition through states
        sm.transition_to(TradingState.HOLDING_WAITING, reason="test")
        
        # Data should persist
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        self.assertEqual(sm.data.sells_today, 2)
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 10000.0)
        
        # Transition again
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="test")
        
        # Data still persists
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        self.assertEqual(sm.data.sells_today, 2)
    
    def test_current_stock_persists_through_pending_states(self):
        """Test current_stock persists through PENDING states."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Start with cash, buy TICKER_A
        sm.transition_to(TradingState.CASH, reason="test")
        sm.transition_to(TradingState.PENDING_BUY, reason="buying A")
        sm.set_current_stock(StockHeld.TICKER_A)
        
        # Transition to HOLDING_WAITING
        sm.transition_to(TradingState.HOLDING_WAITING, reason="buy filled")
        
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        
        # Initiate swap
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="trigger")
        sm.transition_to(TradingState.PENDING_SELL, reason="selling A")
        
        # Still shows TICKER_A until sell completes
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        
        # After sell, update to NONE
        sm.set_current_stock(StockHeld.NONE)
        sm.transition_to(TradingState.CASH, reason="sell filled")
        
        self.assertEqual(sm.data.current_stock, StockHeld.NONE)
    
    def test_sells_today_counter_persists_through_day(self):
        """Test sells_today counter persists throughout trading day."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        
        # Execute first sell
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 1)
        
        # Go through swap
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="trigger")
        sm.transition_to(TradingState.PENDING_SELL, reason="selling")
        sm.transition_to(TradingState.CASH, reason="sell filled")
        sm.transition_to(TradingState.PENDING_BUY, reason="buying")
        sm.transition_to(TradingState.HOLDING_WAITING, reason="buy filled")
        
        # Counter still at 1
        self.assertEqual(sm.data.sells_today, 1)
        
        # Try to sell again (would be blocked in real code)
        sm.increment_sells_today()
        self.assertEqual(sm.data.sells_today, 2)
    
    def test_data_not_cleared_on_error(self):
        """Test StateData NOT automatically cleared on ERROR state."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        
        # Set some data
        sm.set_current_stock(StockHeld.TICKER_A)
        sm.set_sells_today(1)
        sm.data.portfolio_value_at_trade_start = 10000.0
        
        # Force error
        sm.force_error_state(reason="API failure")
        
        # Data should still be accessible for debugging
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        self.assertEqual(sm.data.sells_today, 1)
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 10000.0)


class TestStateHistory(unittest.TestCase):
    """Test state history tracking."""
    
    def test_get_state_history_returns_all_transitions(self):
        """Test get_state_history() returns all transitions."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Perform several transitions
        sm.transition_to(TradingState.CASH, reason="MA ready")
        sm.transition_to(TradingState.PENDING_BUY, reason="buying")
        sm.transition_to(TradingState.HOLDING_WAITING, reason="buy filled")
        
        history = sm.get_state_history()
        
        # Should have 4 entries: init + 3 transitions
        self.assertEqual(len(history), 4)
        
        # Check entries
        self.assertEqual(history[0]['to_state'], 'WARMING_UP')
        self.assertIsNone(history[0]['from_state'])
        
        self.assertEqual(history[1]['from_state'], 'WARMING_UP')
        self.assertEqual(history[1]['to_state'], 'CASH')
        
        self.assertEqual(history[2]['from_state'], 'CASH')
        self.assertEqual(history[2]['to_state'], 'PENDING_BUY')
        
        self.assertEqual(history[3]['from_state'], 'PENDING_BUY')
        self.assertEqual(history[3]['to_state'], 'HOLDING_WAITING')
    
    def test_state_history_records_timestamps(self):
        """Test state history records timestamps."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        sm.transition_to(TradingState.CASH, reason="test")
        
        history = sm.get_state_history()
        
        # Both entries should have timestamps
        self.assertIn('timestamp', history[0])
        self.assertIn('timestamp', history[1])
        
        # Timestamps should be ISO format strings
        self.assertIsInstance(history[0]['timestamp'], str)
        self.assertIsInstance(history[1]['timestamp'], str)
    
    def test_state_history_records_reasons(self):
        """Test state history records reasons."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        sm.transition_to(TradingState.CASH, reason="MA bootstrap complete")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying undervalued stock")
        
        history = sm.get_state_history()
        
        self.assertEqual(history[0]['reason'], 'initialization')
        self.assertEqual(history[1]['reason'], 'MA bootstrap complete')
        self.assertEqual(history[2]['reason'], 'Buying undervalued stock')
    
    def test_state_history_survives_multiple_transitions(self):
        """Test state history accumulates over many transitions."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Perform complete trading cycle
        transitions = [
            (TradingState.CASH, "MA ready"),
            (TradingState.PENDING_BUY, "buying V"),
            (TradingState.HOLDING_WAITING, "buy filled"),
            (TradingState.HOLDING_TRIGGERED, "trigger met"),
            (TradingState.PENDING_SELL, "selling V"),
            (TradingState.CASH, "sell filled"),
            (TradingState.PENDING_BUY, "buying MA"),
            (TradingState.HOLDING_WAITING, "buy filled"),
        ]
        
        for state, reason in transitions:
            sm.transition_to(state, reason=reason)
        
        history = sm.get_state_history()
        
        # Should have init + 8 transitions = 9 entries
        self.assertEqual(len(history), 9)
        
        # Last entry should be final transition
        self.assertEqual(history[-1]['to_state'], 'HOLDING_WAITING')
        self.assertEqual(history[-1]['reason'], 'buy filled')


class TestStateDataInitialization(unittest.TestCase):
    """Test StateData initialization and defaults."""
    
    def test_state_data_default_values(self):
        """Test StateData has correct default values."""
        data = StateData()
        
        self.assertEqual(data.current_stock, StockHeld.NONE)
        self.assertIsNone(data.pending_order_id)
        self.assertEqual(data.sells_today, 0)
        self.assertIsNone(data.last_trade_day)
        self.assertEqual(data.portfolio_value_at_trade_start, 0.0)
    
    def test_state_machine_initializes_with_default_data(self):
        """Test StateMachine initializes with default StateData."""
        sm = StateMachine()
        
        self.assertIsInstance(sm.data, StateData)
        self.assertEqual(sm.data.current_stock, StockHeld.NONE)
        self.assertEqual(sm.data.sells_today, 0)


class TestStateDataEdgeCases(unittest.TestCase):
    """Test StateData edge cases and boundary conditions."""
    
    def test_multiple_pending_order_updates(self):
        """Test setting pending_order multiple times."""
        sm = StateMachine()
        
        sm.set_pending_order("ORD001")
        self.assertEqual(sm.data.pending_order_id, "ORD001")
        
        # Overwrite with new order
        sm.set_pending_order("ORD002")
        self.assertEqual(sm.data.pending_order_id, "ORD002")
        
        # Clear and set again
        sm.set_pending_order(None)
        sm.set_pending_order("ORD003")
        self.assertEqual(sm.data.pending_order_id, "ORD003")
    
    def test_sells_today_large_values(self):
        """Test sells_today with large values."""
        sm = StateMachine()
        
        # Increment many times
        for _ in range(100):
            sm.increment_sells_today()
        
        self.assertEqual(sm.data.sells_today, 100)
        
        # Reset should work
        sm.reset_sells_today("2025-01-15")
        self.assertEqual(sm.data.sells_today, 0)
    
    def test_portfolio_value_negative(self):
        """Test portfolio_value_at_trade_start can be negative (loss scenario)."""
        sm = StateMachine()
        
        # Set negative value (account in loss)
        sm.data.portfolio_value_at_trade_start = -500.0
        
        self.assertEqual(sm.data.portfolio_value_at_trade_start, -500.0)
    
    def test_portfolio_value_very_large(self):
        """Test portfolio_value_at_trade_start with very large value."""
        sm = StateMachine()
        
        # Set large value
        sm.data.portfolio_value_at_trade_start = 1000000.50
        
        self.assertEqual(sm.data.portfolio_value_at_trade_start, 1000000.50)


def run_tests():
    """Run the test suite."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()

