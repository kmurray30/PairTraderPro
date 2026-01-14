"""
Unit Tests for Cleanup States

Tests all edge cases for the three cleanup states:
- CLEANUP_CASH: All cash on startup
- CLEANUP_MIXED: Partial position
- CLEANUP_CONFLICT: Both stocks held

These states ensure robust startup recovery regardless of account state.
"""

import unittest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.state_machine import (
    TradingState, StockHeld, StateMachine, InvalidStateTransition
)
from live_trading.reconciliation import (
    Reconciler, ReconciliationResult, RecoveryAction, Position
)


class TestCleanupStates(unittest.TestCase):
    """Test cleanup state enum values and transitions."""
    
    def test_cleanup_states_exist(self):
        """Verify all three cleanup states exist in enum."""
        self.assertEqual(TradingState.CLEANUP_CASH.value, 1)
        self.assertEqual(TradingState.CLEANUP_MIXED.value, 2)
        self.assertEqual(TradingState.CLEANUP_CONFLICT.value, 3)
    
    def test_warming_up_can_transition_to_cleanup_states(self):
        """WARMING_UP can transition to all cleanup states."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        
        # Can transition to CLEANUP_CASH
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CASH))
        
        # Can transition to CLEANUP_MIXED
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        
        # Can transition to CLEANUP_CONFLICT
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CONFLICT))
    
    def test_cleanup_states_valid_transitions(self):
        """Test valid transitions from each cleanup state."""
        # CLEANUP_CASH transitions
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CASH, "test")
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_BUY))
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        self.assertFalse(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        
        # CLEANUP_MIXED transitions
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_MIXED, "test")
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_BUY))
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_SELL))
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        self.assertFalse(sm.can_transition_to(TradingState.CLEANUP_CASH))
        
        # CLEANUP_CONFLICT transitions
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, "test")
        self.assertTrue(sm.can_transition_to(TradingState.PENDING_SELL))
        self.assertTrue(sm.can_transition_to(TradingState.ERROR))
        self.assertFalse(sm.can_transition_to(TradingState.PENDING_BUY))
    
    def test_pending_buy_can_return_to_cleanup_mixed(self):
        """PENDING_BUY can transition back to CLEANUP_MIXED."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CASH, "test")
        sm.transition_to(TradingState.PENDING_BUY, "test")
        
        # After buy fills, can go to CLEANUP_MIXED if still partial
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        self.assertTrue(sm.can_transition_to(TradingState.HOLDING_WAITING))
    
    def test_pending_sell_can_go_to_cleanup_states(self):
        """PENDING_SELL can transition to cleanup states."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, "test")
        sm.transition_to(TradingState.PENDING_SELL, "test")
        
        # After sell fills, can go to various states
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_CASH))
        self.assertTrue(sm.can_transition_to(TradingState.CLEANUP_MIXED))
        self.assertTrue(sm.can_transition_to(TradingState.CASH))


class TestReconciliationCleanupDetection(unittest.TestCase):
    """Test reconciliation logic for detecting cleanup states."""
    
    def setUp(self):
        """Set up mock API and reconciler."""
        self.mock_api = Mock()
        self.mock_api.account = Mock()
        self.mock_api.orders = Mock()
        
        self.reconciler = Reconciler(
            api=self.mock_api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            logger=None,
            allocated_cash=1000
        )
    
    def test_no_position_recommends_cleanup_cash(self):
        """No position should recommend CLEANUP_CASH."""
        # Mock empty positions
        self.mock_api.account.get_positions.return_value = {
            'Positions': []
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(result.current_stock, StockHeld.NONE)
        self.assertEqual(result.action_needed, RecoveryAction.BUY_INITIAL)
    
    def test_partial_position_under_80_recommends_cleanup_mixed(self):
        """Partial position (under 80%) should recommend CLEANUP_MIXED."""
        # Mock partial position: V worth $600 (60% of $1000 allocated)
        self.mock_api.account.get_positions.return_value = {
            'Positions': [{
                'Symbol': 'V',
                'Quantity': 2,
                'AveragePrice': 300.0,
                'MarketValue': 600.0,
                'UnrealizedProfitLoss': 0.0
            }]
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        self.mock_api.account.get_balances.return_value = {
            'Balances': [{
                'BuyingPower': 1000.0,
                'Equity': 1600.0
            }]
        }
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_MIXED)
        self.assertEqual(result.current_stock, StockHeld.TICKER_A)
        self.assertEqual(result.action_needed, RecoveryAction.RESOLVE_MISMATCH)
    
    def test_clean_position_over_80_recommends_holding_waiting(self):
        """Clean position (over 80%) should recommend HOLDING_WAITING."""
        # Mock clean position: V worth $900 (90% of $1000 allocated)
        self.mock_api.account.get_positions.return_value = {
            'Positions': [{
                'Symbol': 'V',
                'Quantity': 3,
                'AveragePrice': 300.0,
                'MarketValue': 900.0,
                'UnrealizedProfitLoss': 0.0
            }]
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        self.mock_api.account.get_balances.return_value = {
            'Balances': [{
                'BuyingPower': 1000.0,
                'Equity': 1900.0
            }]
        }
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.HOLDING_WAITING)
        self.assertEqual(result.current_stock, StockHeld.TICKER_A)
        self.assertEqual(result.action_needed, RecoveryAction.NONE)
    
    def test_both_stocks_recommends_cleanup_conflict(self):
        """Positions in both stocks should recommend CLEANUP_CONFLICT."""
        # Mock positions in both V and MA
        self.mock_api.account.get_positions.return_value = {
            'Positions': [
                {
                    'Symbol': 'V',
                    'Quantity': 2,
                    'AveragePrice': 300.0,
                    'MarketValue': 600.0,
                    'UnrealizedProfitLoss': 0.0
                },
                {
                    'Symbol': 'MA',
                    'Quantity': 1,
                    'AveragePrice': 500.0,
                    'MarketValue': 500.0,
                    'UnrealizedProfitLoss': 0.0
                }
            ]
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CONFLICT)
        self.assertEqual(result.current_stock, StockHeld.NONE)
        self.assertEqual(result.action_needed, RecoveryAction.RESOLVE_MISMATCH)
        self.assertFalse(result.is_consistent)
    
    def test_oversized_position_still_recommends_holding_waiting(self):
        """Oversized position (150%) should still recommend HOLDING_WAITING."""
        # Mock oversized position: V worth $1500 (150% of $1000 allocated)
        # Per requirements, we ignore this and just treat as clean
        self.mock_api.account.get_positions.return_value = {
            'Positions': [{
                'Symbol': 'V',
                'Quantity': 5,
                'AveragePrice': 300.0,
                'MarketValue': 1500.0,
                'UnrealizedProfitLoss': 0.0
            }]
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        self.mock_api.account.get_balances.return_value = {
            'Balances': [{
                'BuyingPower': 1000.0,
                'Equity': 2500.0
            }]
        }
        
        result = self.reconciler.check_state()
        
        # Should be treated as clean position (no cleanup needed)
        self.assertEqual(result.recommended_state, TradingState.HOLDING_WAITING)
        self.assertEqual(result.current_stock, StockHeld.TICKER_A)
        self.assertEqual(result.action_needed, RecoveryAction.NONE)
    
    def test_partial_position_without_allocated_cash(self):
        """Test partial position detection when allocated_cash=0."""
        # Create reconciler without allocated_cash
        reconciler_no_alloc = Reconciler(
            api=self.mock_api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            logger=None,
            allocated_cash=0  # Use full account
        )
        
        # Mock position: V worth $500 with buying power $800
        # Position is 62.5% of buying power (under 80%)
        self.mock_api.account.get_positions.return_value = {
            'Positions': [{
                'Symbol': 'V',
                'Quantity': 2,
                'AveragePrice': 250.0,
                'MarketValue': 500.0,
                'UnrealizedProfitLoss': 0.0
            }]
        }
        self.mock_api.orders.get_orders.return_value = {
            'Orders': []
        }
        self.mock_api.account.get_balances.return_value = {
            'Balances': [{
                'BuyingPower': 800.0,
                'Equity': 1300.0
            }]
        }
        
        result = reconciler_no_alloc.check_state()
        
        # Should detect as partial (500 < 80% of 800 = 640)
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_MIXED)
        self.assertEqual(result.current_stock, StockHeld.TICKER_A)


class TestPriceTrackerUndervaluedDetection(unittest.TestCase):
    """Test get_undervalued_stock() method."""
    
    def test_negative_deviation_returns_ticker_a(self):
        """Negative deviation means ticker_a undervalued."""
        from live_trading.price_tracker import PriceTracker, PriceSnapshot, Quote
        from datetime import datetime
        
        mock_api = Mock()
        tracker = PriceTracker(
            api=mock_api,
            ticker_a="V",
            ticker_b="MA",
            ma_window_minutes=240,
            trigger_percent=0.4
        )
        
        # Create snapshot with negative deviation
        snapshot = PriceSnapshot(
            ticker_a_quote=Quote("V", 300.0, 299.0, 301.0, 100, 100, 1000000, datetime.now()),
            ticker_b_quote=Quote("MA", 500.0, 499.0, 501.0, 100, 100, 1000000, datetime.now()),
            ratio=0.600,
            ratio_ma=0.610,
            deviation_percent=-1.64,  # Negative
            timestamp=datetime.now()
        )
        
        result = tracker.get_undervalued_stock(snapshot)
        self.assertEqual(result, "ticker_a")
    
    def test_positive_deviation_returns_ticker_b(self):
        """Positive deviation means ticker_b undervalued."""
        from live_trading.price_tracker import PriceTracker, PriceSnapshot, Quote
        from datetime import datetime
        
        mock_api = Mock()
        tracker = PriceTracker(
            api=mock_api,
            ticker_a="V",
            ticker_b="MA",
            ma_window_minutes=240,
            trigger_percent=0.4
        )
        
        # Create snapshot with positive deviation
        snapshot = PriceSnapshot(
            ticker_a_quote=Quote("V", 300.0, 299.0, 301.0, 100, 100, 1000000, datetime.now()),
            ticker_b_quote=Quote("MA", 500.0, 499.0, 501.0, 100, 100, 1000000, datetime.now()),
            ratio=0.620,
            ratio_ma=0.610,
            deviation_percent=+1.64,  # Positive
            timestamp=datetime.now()
        )
        
        result = tracker.get_undervalued_stock(snapshot)
        self.assertEqual(result, "ticker_b")
    
    def test_zero_deviation_returns_ticker_b(self):
        """Zero deviation uses >= 0, so returns ticker_b (favors b)."""
        from live_trading.price_tracker import PriceTracker, PriceSnapshot, Quote
        from datetime import datetime
        
        mock_api = Mock()
        tracker = PriceTracker(
            api=mock_api,
            ticker_a="V",
            ticker_b="MA",
            ma_window_minutes=240,
            trigger_percent=0.4
        )
        
        # Create snapshot with exactly zero deviation
        snapshot = PriceSnapshot(
            ticker_a_quote=Quote("V", 300.0, 299.0, 301.0, 100, 100, 1000000, datetime.now()),
            ticker_b_quote=Quote("MA", 500.0, 499.0, 501.0, 100, 100, 1000000, datetime.now()),
            ratio=0.610,
            ratio_ma=0.610,
            deviation_percent=0.0,  # Exactly zero
            timestamp=datetime.now()
        )
        
        result = tracker.get_undervalued_stock(snapshot)
        self.assertEqual(result, "ticker_b")  # >= 0 favors ticker_b


class TestStateMachineHelpers(unittest.TestCase):
    """Test state machine helper methods for cleanup states."""
    
    def test_is_in_cleanup(self):
        """Test is_in_cleanup() helper method."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        self.assertFalse(sm.is_in_cleanup())
        
        sm.transition_to(TradingState.CLEANUP_CASH, "test")
        self.assertTrue(sm.is_in_cleanup())
        
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_MIXED, "test")
        self.assertTrue(sm.is_in_cleanup())
        
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, "test")
        self.assertTrue(sm.is_in_cleanup())
        
        sm.transition_to(TradingState.PENDING_SELL, "test")
        self.assertFalse(sm.is_in_cleanup())
    
    def test_is_ready_to_trade_includes_cleanup(self):
        """is_ready_to_trade() should include cleanup states."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CASH, "test")
        self.assertTrue(sm.is_ready_to_trade())
        
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_MIXED, "test")
        self.assertTrue(sm.is_ready_to_trade())
        
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CONFLICT, "test")
        self.assertTrue(sm.is_ready_to_trade())
    
    def test_is_holding_position_includes_cleanup_mixed(self):
        """is_holding_position() should include CLEANUP_MIXED."""
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_MIXED, "test")
        self.assertTrue(sm.is_holding_position())
        
        sm = StateMachine(initial_state=TradingState.WARMING_UP)
        sm.transition_to(TradingState.CLEANUP_CASH, "test")
        self.assertFalse(sm.is_holding_position())


def run_tests():
    """Run the test suite."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == "__main__":
    run_tests()

