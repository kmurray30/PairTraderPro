"""
Unit Tests for StateMachine Helper Methods

Comprehensive tests for all StateMachine helper/query methods to ensure
they correctly identify state categories and capabilities.
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.state_machine import (
    TradingState, StockHeld, StateMachine
)


class TestStateQueryHelpers(unittest.TestCase):
    """Test all state query helper methods."""
    
    def test_is_in_error_for_all_states(self):
        """Test is_in_error() returns correct value for all states."""
        # Test ERROR state
        sm_error = StateMachine(initial_state=TradingState.WARMING_UP)
        sm_error.force_error_state(reason="test")
        self.assertTrue(sm_error.is_in_error())
        
        # Test all other states return False
        non_error_states = [
            TradingState.WARMING_UP,
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT,
            TradingState.CASH,
            TradingState.PENDING_BUY,
            TradingState.HOLDING_WAITING,
            TradingState.HOLDING_TRIGGERED,
            TradingState.PENDING_SELL,
            TradingState.HOLDING_DAILY_LIMIT,
        ]
        
        for state in non_error_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_in_error(), f"is_in_error() should be False for {state.name}")
    
    def test_is_ready_to_trade_for_all_states(self):
        """Test is_ready_to_trade() returns correct value for all states."""
        # States that ARE ready to trade
        ready_states = [
            TradingState.CASH,
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT,
            TradingState.HOLDING_WAITING,
        ]
        
        for state in ready_states:
            sm = StateMachine(initial_state=state)
            self.assertTrue(sm.is_ready_to_trade(), f"is_ready_to_trade() should be True for {state.name}")
        
        # States that are NOT ready to trade
        not_ready_states = [
            TradingState.WARMING_UP,
            TradingState.PENDING_BUY,
            TradingState.PENDING_SELL,
            TradingState.HOLDING_TRIGGERED,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.ERROR,
        ]
        
        for state in not_ready_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_ready_to_trade(), f"is_ready_to_trade() should be False for {state.name}")
    
    def test_is_warming_up_for_all_states(self):
        """Test is_warming_up() returns correct value for all states."""
        # Only WARMING_UP returns True
        sm_warming = StateMachine(initial_state=TradingState.WARMING_UP)
        self.assertTrue(sm_warming.is_warming_up())
        
        # All other states return False
        other_states = [
            TradingState.CLEANUP_CASH,
            TradingState.CASH,
            TradingState.PENDING_BUY,
            TradingState.HOLDING_WAITING,
            TradingState.ERROR,
        ]
        
        for state in other_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_warming_up(), f"is_warming_up() should be False for {state.name}")
    
    def test_has_reached_daily_limit_for_all_states(self):
        """Test has_reached_daily_limit() returns correct value for all states."""
        # Only HOLDING_DAILY_LIMIT returns True
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="test")
        sm.transition_to(TradingState.HOLDING_DAILY_LIMIT, reason="limit reached")
        self.assertTrue(sm.has_reached_daily_limit())
        
        # All other states return False
        other_states = [
            TradingState.WARMING_UP,
            TradingState.CASH,
            TradingState.HOLDING_WAITING,
            TradingState.PENDING_SELL,
            TradingState.ERROR,
        ]
        
        for state in other_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.has_reached_daily_limit(), f"has_reached_daily_limit() should be False for {state.name}")
    
    def test_can_initiate_swap_for_all_states(self):
        """Test can_initiate_swap() returns correct value for all states."""
        # Only HOLDING_WAITING returns True
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        self.assertTrue(sm.can_initiate_swap())
        
        # All other states return False
        other_states = [
            TradingState.WARMING_UP,
            TradingState.CASH,
            TradingState.PENDING_BUY,
            TradingState.HOLDING_TRIGGERED,
            TradingState.PENDING_SELL,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.ERROR,
        ]
        
        for state in other_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.can_initiate_swap(), f"can_initiate_swap() should be False for {state.name}")
    
    def test_is_holding_stock_with_each_stock_value(self):
        """Test is_holding_stock() with each StockHeld value."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        
        # Test TICKER_A
        sm.set_current_stock(StockHeld.TICKER_A)
        self.assertTrue(sm.is_holding_stock(StockHeld.TICKER_A))
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_B))
        self.assertFalse(sm.is_holding_stock(StockHeld.NONE))
        
        # Test TICKER_B
        sm.set_current_stock(StockHeld.TICKER_B)
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_A))
        self.assertTrue(sm.is_holding_stock(StockHeld.TICKER_B))
        self.assertFalse(sm.is_holding_stock(StockHeld.NONE))
        
        # Test NONE
        sm.set_current_stock(StockHeld.NONE)
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_A))
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_B))
        # is_holding_stock(NONE) would be False because is_holding_position() is False
    
    def test_is_holding_position_for_all_states(self):
        """Test is_holding_position() for all states."""
        # States that hold positions
        holding_states = [
            TradingState.HOLDING_WAITING,
            TradingState.HOLDING_TRIGGERED,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.CLEANUP_MIXED,  # Partial position
        ]
        
        for state in holding_states:
            sm = StateMachine(initial_state=state)
            sm.set_current_stock(StockHeld.TICKER_A)  # Must have stock set
            self.assertTrue(sm.is_holding_position(), f"is_holding_position() should be True for {state.name}")
        
        # States that don't hold positions
        non_holding_states = [
            TradingState.WARMING_UP,
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_CONFLICT,  # Ambiguous - both stocks
            TradingState.CASH,
            TradingState.PENDING_BUY,
            TradingState.PENDING_SELL,
            TradingState.ERROR,
        ]
        
        for state in non_holding_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_holding_position(), f"is_holding_position() should be False for {state.name}")
    
    def test_is_in_cleanup_for_all_states(self):
        """Test is_in_cleanup() for all states."""
        # Cleanup states
        cleanup_states = [
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT,
        ]
        
        for state in cleanup_states:
            sm = StateMachine(initial_state=TradingState.WARMING_UP)
            sm.transition_to(state, reason="test")
            self.assertTrue(sm.is_in_cleanup(), f"is_in_cleanup() should be True for {state.name}")
        
        # Non-cleanup states
        non_cleanup_states = [
            TradingState.WARMING_UP,
            TradingState.CASH,
            TradingState.PENDING_BUY,
            TradingState.HOLDING_WAITING,
            TradingState.HOLDING_TRIGGERED,
            TradingState.PENDING_SELL,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.ERROR,
        ]
        
        for state in non_cleanup_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_in_cleanup(), f"is_in_cleanup() should be False for {state.name}")
    
    def test_is_order_pending_for_all_states(self):
        """Test is_order_pending() for all states."""
        # Pending states
        pending_states = [
            TradingState.PENDING_BUY,
            TradingState.PENDING_SELL,
        ]
        
        for state in pending_states:
            sm = StateMachine(initial_state=TradingState.WARMING_UP)
            if state == TradingState.PENDING_BUY:
                sm.transition_to(TradingState.CASH, reason="setup")
            else:  # PENDING_SELL
                sm.transition_to(TradingState.HOLDING_WAITING, reason="setup")
                sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="setup")
            sm.transition_to(state, reason="test")
            self.assertTrue(sm.is_order_pending(), f"is_order_pending() should be True for {state.name}")
        
        # Non-pending states
        non_pending_states = [
            TradingState.WARMING_UP,
            TradingState.CLEANUP_CASH,
            TradingState.CASH,
            TradingState.HOLDING_WAITING,
            TradingState.HOLDING_TRIGGERED,
            TradingState.HOLDING_DAILY_LIMIT,
            TradingState.ERROR,
        ]
        
        for state in non_pending_states:
            sm = StateMachine(initial_state=state)
            self.assertFalse(sm.is_order_pending(), f"is_order_pending() should be False for {state.name}")


class TestStateProperties(unittest.TestCase):
    """Test state property accessors."""
    
    def test_state_name_property_returns_correct_string(self):
        """Test state_name property returns correct string for all states."""
        test_cases = [
            (TradingState.WARMING_UP, "WARMING_UP"),
            (TradingState.CLEANUP_CASH, "CLEANUP_CASH"),
            (TradingState.CLEANUP_MIXED, "CLEANUP_MIXED"),
            (TradingState.CLEANUP_CONFLICT, "CLEANUP_CONFLICT"),
            (TradingState.CASH, "CASH"),
            (TradingState.PENDING_BUY, "PENDING_BUY"),
            (TradingState.HOLDING_WAITING, "HOLDING_WAITING"),
            (TradingState.HOLDING_TRIGGERED, "HOLDING_TRIGGERED"),
            (TradingState.PENDING_SELL, "PENDING_SELL"),
            (TradingState.HOLDING_DAILY_LIMIT, "HOLDING_DAILY_LIMIT"),
            (TradingState.ERROR, "ERROR"),
        ]
        
        for state, expected_name in test_cases:
            sm = StateMachine(initial_state=state)
            self.assertEqual(sm.state_name, expected_name)
    
    def test_state_value_property_returns_correct_integer(self):
        """Test state_value property returns correct integer for metrics."""
        test_cases = [
            (TradingState.WARMING_UP, 0),
            (TradingState.CLEANUP_CASH, 1),
            (TradingState.CLEANUP_MIXED, 2),
            (TradingState.CLEANUP_CONFLICT, 3),
            (TradingState.CASH, 4),
            (TradingState.PENDING_BUY, 5),
            (TradingState.HOLDING_WAITING, 6),
            (TradingState.HOLDING_TRIGGERED, 7),
            (TradingState.PENDING_SELL, 8),
            (TradingState.HOLDING_DAILY_LIMIT, 9),
            (TradingState.ERROR, 10),
        ]
        
        for state, expected_value in test_cases:
            sm = StateMachine(initial_state=state)
            self.assertEqual(sm.state_value, expected_value)
    
    def test_state_property_returns_correct_enum(self):
        """Test state property returns correct TradingState enum."""
        for state in TradingState:
            sm = StateMachine(initial_state=state)
            self.assertEqual(sm.state, state)
            self.assertIsInstance(sm.state, TradingState)


class TestHelperMethodCombinations(unittest.TestCase):
    """Test combinations of helper methods."""
    
    def test_ready_to_trade_and_in_cleanup(self):
        """Test states that are both ready_to_trade AND in_cleanup."""
        cleanup_ready_states = [
            TradingState.CLEANUP_CASH,
            TradingState.CLEANUP_MIXED,
            TradingState.CLEANUP_CONFLICT,
        ]
        
        for state in cleanup_ready_states:
            sm = StateMachine(initial_state=TradingState.WARMING_UP)
            sm.transition_to(state, reason="test")
            
            self.assertTrue(sm.is_ready_to_trade())
            self.assertTrue(sm.is_in_cleanup())
    
    def test_holding_position_and_can_initiate_swap(self):
        """Test HOLDING_WAITING is both holding position and can initiate swap."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        sm.set_current_stock(StockHeld.TICKER_A)
        
        self.assertTrue(sm.is_holding_position())
        self.assertTrue(sm.can_initiate_swap())
    
    def test_pending_states_not_ready_to_trade(self):
        """Test PENDING states are not ready to trade."""
        sm_buy = StateMachine(initial_state=TradingState.WARMING_UP)
        sm_buy.transition_to(TradingState.CASH, reason="setup")
        sm_buy.transition_to(TradingState.PENDING_BUY, reason="test")
        
        self.assertTrue(sm_buy.is_order_pending())
        self.assertFalse(sm_buy.is_ready_to_trade())
        
        sm_sell = StateMachine(initial_state=TradingState.WARMING_UP)
        sm_sell.transition_to(TradingState.HOLDING_WAITING, reason="setup")
        sm_sell.transition_to(TradingState.HOLDING_TRIGGERED, reason="setup")
        sm_sell.transition_to(TradingState.PENDING_SELL, reason="test")
        
        self.assertTrue(sm_sell.is_order_pending())
        self.assertFalse(sm_sell.is_ready_to_trade())
    
    def test_error_state_all_helpers_return_expected_values(self):
        """Test all helper methods return expected values for ERROR state."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.force_error_state(reason="test")
        
        self.assertTrue(sm.is_in_error())
        self.assertFalse(sm.is_ready_to_trade())
        self.assertFalse(sm.is_warming_up())
        self.assertFalse(sm.has_reached_daily_limit())
        self.assertFalse(sm.can_initiate_swap())
        self.assertFalse(sm.is_holding_position())
        self.assertFalse(sm.is_in_cleanup())
        self.assertFalse(sm.is_order_pending())


class TestHelperMethodEdgeCases(unittest.TestCase):
    """Test helper method edge cases."""
    
    def test_is_holding_stock_when_in_cash_state(self):
        """Test is_holding_stock() returns False when in CASH state."""
        sm = StateMachine(initial_state=TradingState.CASH)
        sm.set_current_stock(StockHeld.NONE)
        
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_A))
        self.assertFalse(sm.is_holding_stock(StockHeld.TICKER_B))
    
    def test_is_holding_position_when_current_stock_is_none(self):
        """Test is_holding_position() when current_stock is NONE."""
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        sm.set_current_stock(StockHeld.NONE)
        
        # is_holding_position() checks state, not current_stock value
        # So it returns True for HOLDING_WAITING regardless of current_stock
        self.assertTrue(sm.is_holding_position())
    
    def test_helper_methods_after_state_transitions(self):
        """Test helper methods return correct values after transitions."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Initially warming up
        self.assertTrue(sm.is_warming_up())
        self.assertFalse(sm.is_ready_to_trade())
        
        # Transition to CASH
        sm.transition_to(TradingState.CASH, reason="MA ready")
        self.assertFalse(sm.is_warming_up())
        self.assertTrue(sm.is_ready_to_trade())
        
        # Transition to PENDING_BUY
        sm.transition_to(TradingState.PENDING_BUY, reason="buying")
        self.assertTrue(sm.is_order_pending())
        self.assertFalse(sm.is_ready_to_trade())
        
        # Transition to HOLDING_WAITING
        sm.transition_to(TradingState.HOLDING_WAITING, reason="filled")
        sm.set_current_stock(StockHeld.TICKER_A)
        self.assertFalse(sm.is_order_pending())
        self.assertTrue(sm.is_ready_to_trade())
        self.assertTrue(sm.is_holding_position())
        self.assertTrue(sm.can_initiate_swap())


def run_tests():
    """Run the test suite."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()

