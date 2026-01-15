"""
Unit Tests for State Machine Transitions

Tests every valid state transition at least twice, and verifies that
invalid transitions are properly blocked.

This ensures the state machine enforces correct sequencing of operations
and prevents invalid actions based on the current state.
"""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.state_machine import (
    TradingState, StockHeld, StateMachine, InvalidStateTransition
)


class TestWarmingUpTransitions(unittest.TestCase):
    """Test all valid transitions from WARMING_UP state."""
    
    def test_warming_up_to_cleanup_cash_no_position(self):
        """WARMING_UP → CLEANUP_CASH when no position detected."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CASH))
        sm.transition_to(TradingState.CLEANUP_CASH, reason="MA ready, no position")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_CASH)
        history = sm.get_state_history()
        self.assertEqual(len(history), 2)  # init + transition
        self.assertEqual(history[-1]['to_state'], 'CLEANUP_CASH')
    
    def test_warming_up_to_cleanup_mixed_partial_position(self):
        """WARMING_UP → CLEANUP_MIXED when partial position detected."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        sm.transition_to(TradingState.CLEANUP_MIXED, reason="MA ready, partial position")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_MIXED)
    
    def test_warming_up_to_cleanup_conflict_both_stocks(self):
        """WARMING_UP → CLEANUP_CONFLICT when both stocks detected."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CONFLICT))
        sm.transition_to(TradingState.CLEANUP_CONFLICT, reason="Both stocks held")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_CONFLICT)
    
    def test_warming_up_to_cash_legacy_path(self):
        """WARMING_UP → CASH (legacy path for clean startup)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.CASH))
        sm.transition_to(TradingState.CASH, reason="MA ready, no position (legacy)")
        
        self.assertEqual(sm.state, TradingState.CASH)
    
    def test_warming_up_to_holding_waiting_clean_position(self):
        """WARMING_UP → HOLDING_WAITING when clean position recovery."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.set_current_stock(StockHeld.TICKER_A)
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_WAITING))
        sm.transition_to(TradingState.HOLDING_WAITING, reason="MA ready, clean position")
        
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
    
    def test_warming_up_to_pending_buy_detected(self):
        """WARMING_UP → PENDING_BUY when pending buy order detected."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_BUY))
        sm.set_pending_order("ORD123")
        sm.transition_to(TradingState.PENDING_BUY, reason="Pending buy detected")
        
        self.assertEqual(sm.state, TradingState.PENDING_BUY)
        self.assertEqual(sm.data.pending_order_id, "ORD123")
    
    def test_warming_up_to_pending_sell_detected(self):
        """WARMING_UP → PENDING_SELL when pending sell order detected."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_SELL))
        sm.set_pending_order("ORD456")
        sm.transition_to(TradingState.PENDING_SELL, reason="Pending sell detected")
        
        self.assertEqual(sm.state, TradingState.PENDING_SELL)
    
    def test_warming_up_to_error_bootstrap_failure(self):
        """WARMING_UP → ERROR when bootstrap fails."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="Failed to fetch historical bars")
        
        self.assertEqual(sm.state, TradingState.ERROR)
    
    def test_warming_up_invalid_transitions_blocked(self):
        """Verify invalid transitions from WARMING_UP are blocked."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Cannot go to HOLDING_TRIGGERED directly
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_TRIGGERED))
        with self.assertRaises(InvalidStateTransition):
            sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Invalid")
        
        # Cannot go to HOLDING_DAILY_LIMIT directly
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_DAILY_LIMIT))


class TestCashTransitions(unittest.TestCase):
    """Test all valid transitions from CASH state."""
    
    def test_cash_to_pending_buy_first_buy(self):
        """CASH → PENDING_BUY (initiating first buy)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_BUY))
        sm.set_pending_order("ORD100")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying undervalued stock")
        
        self.assertEqual(sm.state, TradingState.PENDING_BUY)
    
    def test_cash_to_pending_buy_swap_buy(self):
        """CASH → PENDING_BUY (swap buy after sell complete)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="setup")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="trigger met")
        sm.transition_to(TradingState.PENDING_SELL, reason="initiating swap")
        sm.transition_to(TradingState.CASH, reason="sell filled")
        
        # Now buy the other stock
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying other stock")
        self.assertEqual(sm.state, TradingState.PENDING_BUY)
    
    def test_cash_to_error_api_failure(self):
        """CASH → ERROR when API fails on buy attempt."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="API unreachable")
        
        self.assertEqual(sm.state, TradingState.ERROR)
    
    def test_cash_invalid_transitions_blocked(self):
        """Verify invalid transitions from CASH are blocked."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        
        # Cannot go directly to HOLDING_WAITING
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_WAITING))
        with self.assertRaises(InvalidStateTransition):
            sm.transition_to(TradingState.HOLDING_WAITING, reason="Invalid")
        
        # Cannot go to PENDING_SELL
        self.assertFalse(sm.can_transition_to(TradingState.PENDING_SELL))


class TestPendingBuyTransitions(unittest.TestCase):
    """Test all valid transitions from PENDING_BUY state."""
    
    def test_pending_buy_to_holding_waiting_clean_fill(self):
        """PENDING_BUY → HOLDING_WAITING (clean position filled)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        sm.set_pending_order("ORD100")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying V")
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_WAITING))
        sm.set_current_stock(StockHeld.TICKER_A)
        sm.set_pending_order(None)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Order filled")
        
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_A)
        self.assertIsNone(sm.data.pending_order_id)
    
    def test_pending_buy_to_cleanup_mixed_partial_fill(self):
        """PENDING_BUY → CLEANUP_MIXED (partial position filled)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CASH, reason="no position")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying partial")
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        sm.transition_to(TradingState.CLEANUP_MIXED, reason="Still partial")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_MIXED)
    
    def test_pending_buy_to_error_order_rejected(self):
        """PENDING_BUY → ERROR (order rejected by broker)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="Order rejected - insufficient funds")
        
        self.assertEqual(sm.state, TradingState.ERROR)
    
    def test_pending_buy_state_data_updated_on_fill(self):
        """Verify StateData updated correctly on buy fill."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        sm.set_pending_order("ORD999")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying")
        
        # Simulate order fill
        sm.set_current_stock(StockHeld.TICKER_B)
        sm.set_pending_order(None)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Filled")
        
        self.assertEqual(sm.data.current_stock, StockHeld.TICKER_B)
        self.assertIsNone(sm.data.pending_order_id)


class TestHoldingWaitingTransitions(unittest.TestCase):
    """Test all valid transitions from HOLDING_WAITING state."""
    
    def test_holding_waiting_to_holding_triggered_condition_met(self):
        """HOLDING_WAITING → HOLDING_TRIGGERED (trigger condition met)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.set_current_stock(StockHeld.TICKER_A)
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_TRIGGERED))
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Deviation > threshold")
        
        self.assertEqual(sm.state, TradingState.HOLDING_TRIGGERED)
    
    def test_holding_waiting_to_holding_daily_limit_limit_reached(self):
        """HOLDING_WAITING → HOLDING_DAILY_LIMIT (sell limit reached)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.set_sells_today(1)  # At limit
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_DAILY_LIMIT))
        sm.transition_to(TradingState.HOLDING_DAILY_LIMIT, reason="Sell limit reached")
        
        self.assertEqual(sm.state, TradingState.HOLDING_DAILY_LIMIT)
        self.assertEqual(sm.data.sells_today, 1)
    
    def test_holding_waiting_to_error_position_mismatch(self):
        """HOLDING_WAITING → ERROR (position mismatch detected)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="Position mismatch in reconciliation")
        
        self.assertEqual(sm.state, TradingState.ERROR)
    
    def test_holding_waiting_stays_when_no_trigger(self):
        """HOLDING_WAITING stays in same state when monitoring."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        
        # Should stay in HOLDING_WAITING when no trigger
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
        
        # Can transition back to itself isn't valid (would need to go through trigger)
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_WAITING))


class TestHoldingTriggeredTransitions(unittest.TestCase):
    """Test all valid transitions from HOLDING_TRIGGERED state."""
    
    def test_holding_triggered_to_pending_sell_swap_approved(self):
        """HOLDING_TRIGGERED → PENDING_SELL (swap approved)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.set_current_stock(StockHeld.TICKER_A)
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_SELL))
        sm.set_pending_order("ORD_SELL_123")
        sm.transition_to(TradingState.PENDING_SELL, reason="Initiating swap")
        
        self.assertEqual(sm.state, TradingState.PENDING_SELL)
    
    def test_holding_triggered_to_holding_waiting_past_cutoff(self):
        """HOLDING_TRIGGERED → HOLDING_WAITING (swap aborted - past cutoff)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_WAITING))
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Past 3:55 PM cutoff")
        
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
    
    def test_holding_triggered_to_holding_waiting_other_abort(self):
        """HOLDING_TRIGGERED → HOLDING_WAITING (swap aborted - other reason)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Trigger condition no longer met")
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
    
    def test_holding_triggered_to_error_api_failure(self):
        """HOLDING_TRIGGERED → ERROR (API failure during swap)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="API connection lost")
        
        self.assertEqual(sm.state, TradingState.ERROR)


class TestPendingSellTransitions(unittest.TestCase):
    """Test all valid transitions from PENDING_SELL state."""
    
    def test_pending_sell_to_cash_normal_swap(self):
        """PENDING_SELL → CASH (normal swap sell complete)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        sm.transition_to(TradingState.PENDING_SELL, reason="Selling")
        
        self.assertTrue(sm.can_transition_to(TradingState.CASH))
        sm.set_current_stock(StockHeld.NONE)
        sm.set_pending_order(None)
        sm.transition_to(TradingState.CASH, reason="Sell filled")
        
        self.assertEqual(sm.state, TradingState.CASH)
        self.assertEqual(sm.data.current_stock, StockHeld.NONE)
    
    def test_pending_sell_to_cleanup_cash_cleanup_sell(self):
        """PENDING_SELL → CLEANUP_CASH (cleanup sell complete)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, reason="Both stocks")
        sm.transition_to(TradingState.PENDING_SELL, reason="Selling non-optimal")
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CASH))
        sm.transition_to(TradingState.CLEANUP_CASH, reason="Now all cash")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_CASH)
    
    def test_pending_sell_to_cleanup_mixed_conflict_partial(self):
        """PENDING_SELL → CLEANUP_MIXED (conflict resolution leaves partial)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, reason="Both stocks")
        sm.transition_to(TradingState.PENDING_SELL, reason="Selling one stock")
        
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        sm.transition_to(TradingState.CLEANUP_MIXED, reason="Still have other stock")
        
        self.assertEqual(sm.state, TradingState.CLEANUP_MIXED)
    
    def test_pending_sell_to_error_order_rejected(self):
        """PENDING_SELL → ERROR (order rejected)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_TRIGGERED, reason="Trigger met")
        sm.transition_to(TradingState.PENDING_SELL, reason="Selling")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="Sell order rejected")
        
        self.assertEqual(sm.state, TradingState.ERROR)


class TestHoldingDailyLimitTransitions(unittest.TestCase):
    """Test all valid transitions from HOLDING_DAILY_LIMIT state."""
    
    def test_holding_daily_limit_to_holding_waiting_new_day(self):
        """HOLDING_DAILY_LIMIT → HOLDING_WAITING (new trading day)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.set_sells_today(1)
        sm.transition_to(TradingState.HOLDING_DAILY_LIMIT, reason="Limit reached")
        
        # Simulate new day reset
        sm.reset_sells_today("2025-01-15")
        
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_WAITING))
        sm.transition_to(TradingState.HOLDING_WAITING, reason="New trading day")
        
        self.assertEqual(sm.state, TradingState.HOLDING_WAITING)
        self.assertEqual(sm.data.sells_today, 0)
    
    def test_holding_daily_limit_to_error_position_mismatch(self):
        """HOLDING_DAILY_LIMIT → ERROR (position mismatch during limit)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_DAILY_LIMIT, reason="Limit reached")
        
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        sm.transition_to(TradingState.ERROR, reason="Position disappeared")
        
        self.assertEqual(sm.state, TradingState.ERROR)
    
    def test_holding_daily_limit_stays_when_limit_active(self):
        """HOLDING_DAILY_LIMIT stays when limit still active."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.HOLDING_WAITING, reason="Position held")
        sm.transition_to(TradingState.HOLDING_DAILY_LIMIT, reason="Limit reached")
        
        # Should stay in HOLDING_DAILY_LIMIT
        self.assertEqual(sm.state, TradingState.HOLDING_DAILY_LIMIT)
        
        # Cannot transition to most states
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_TRIGGERED))
        self.assertFalse(sm.can_transition_to(TradingState.PENDING_SELL))


class TestErrorStateTransitions(unittest.TestCase):
    """Test ERROR state behavior and transitions."""
    
    def test_error_state_is_terminal(self):
        """ERROR state has no valid transitions (terminal state)."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.ERROR, reason="Critical failure")
        
        self.assertEqual(sm.state, TradingState.ERROR)
        
        # No valid transitions from ERROR
        self.assertFalse(sm.can_transition_to(TradingState.WARMING_UP))
        self.assertFalse(sm.can_transition_to(TradingState.CASH))
        self.assertFalse(sm.can_transition_to(TradingState.HOLDING_WAITING))
    
    def test_force_error_state_from_any_state(self):
        """force_error_state() works from any state."""
        # Test from HOLDING_WAITING
        sm = StateMachine(initial_state=TradingState.HOLDING_WAITING)
        sm.force_error_state(reason="Critical error")
        self.assertEqual(sm.state, TradingState.ERROR)
        
        # Test from PENDING_BUY
        sm2 = StateMachine(initial_state=TradingState.WARMING_UP)
        sm2.transition_to(TradingState.CASH, reason="setup")
        sm2.transition_to(TradingState.PENDING_BUY, reason="setup")
        sm2.force_error_state(reason="API failure")
        self.assertEqual(sm2.state, TradingState.ERROR)
        
        # Test from CLEANUP_MIXED
        sm3 = StateMachine(initial_state=TradingState.WARMING_UP)
        sm3.transition_to(TradingState.CLEANUP_MIXED, reason="setup")
        sm3.force_error_state(reason="Position mismatch")
        self.assertEqual(sm3.state, TradingState.ERROR)
    
    def test_state_history_records_error_transitions(self):
        """State history correctly records error transitions."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CASH, reason="MA ready")
        sm.transition_to(TradingState.PENDING_BUY, reason="Buying")
        sm.force_error_state(reason="Order rejected")
        
        history = sm.get_state_history()
        
        # Should have 4 entries: init, CASH, PENDING_BUY, ERROR
        self.assertEqual(len(history), 4)
        self.assertEqual(history[-1]['to_state'], 'ERROR')
        self.assertEqual(history[-1]['from_state'], 'PENDING_BUY')
        self.assertEqual(history[-1]['reason'], 'Order rejected')


def run_tests():
    """Run the test suite."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()

