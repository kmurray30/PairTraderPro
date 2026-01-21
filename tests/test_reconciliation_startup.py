"""
Unit Tests for Reconciliation Startup Order Detection

Tests comprehensive startup scenarios for all order statuses to ensure
the reconciliation module correctly identifies pending orders and handles
edge cases properly.
"""

import unittest
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_trading.reconciliation import Reconciler, RecoveryAction, ReconciliationResult
from live_trading.state_machine import TradingState, StockHeld
from live_trading.order_executor import OrderStatus


class MockAPI:
    """Mock TradeStation API for testing."""
    
    def __init__(self):
        self.account = Mock()
        self.orders = Mock()


class TestReconciliationStartupPendingStatuses(unittest.TestCase):
    """Test startup with all 4 pending order statuses."""
    
    def setUp(self):
        """Set up mock API and reconciler."""
        self.api = MockAPI()
        self.reconciler = Reconciler(
            api=self.api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            allocated_cash=10000
        )
    
    def test_startup_with_ack_buy_order(self):
        """Startup with ACK (Received) BUY order should transition to PENDING_BUY."""
        # Mock positions: none
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Mock orders: one ACK BUY order
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD123',
                'Symbol': 'MA',
                'TradeAction': 'BUY',
                'Quantity': 19,
                'FilledQuantity': 0,
                'Status': 'ACK'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertTrue(result.is_consistent)
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        self.assertEqual(result.current_stock, StockHeld.NONE)
        self.assertEqual(result.action_needed, RecoveryAction.WAIT_FOR_FILL)
        self.assertEqual(len(result.pending_orders), 1)
        self.assertEqual(result.pending_orders[0].order_id, 'ORD123')
    
    def test_startup_with_opn_buy_order(self):
        """Startup with OPN (Sent) BUY order should transition to PENDING_BUY."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD456',
                'Symbol': 'V',
                'TradeAction': 'BUY',
                'Quantity': 30,
                'FilledQuantity': 0,
                'Status': 'OPN'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertTrue(result.is_consistent)
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        self.assertEqual(result.action_needed, RecoveryAction.WAIT_FOR_FILL)
    
    def test_startup_with_fpr_partial_fill_alive(self):
        """Startup with FPR (Partial Fill Alive) should transition to PENDING_BUY."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD789',
                'Symbol': 'MA',
                'TradeAction': 'BUY',
                'Quantity': 20,
                'FilledQuantity': 10,  # Partially filled
                'Status': 'FPR'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertTrue(result.is_consistent)
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        self.assertEqual(result.action_needed, RecoveryAction.WAIT_FOR_FILL)
        self.assertEqual(result.pending_orders[0].filled_quantity, 10)
    
    def test_startup_with_flp_partial_fill_out(self):
        """Startup with FLP (Partial Fill UROut) should transition to PENDING_BUY."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD101',
                'Symbol': 'V',
                'TradeAction': 'BUY',
                'Quantity': 25,
                'FilledQuantity': 15,
                'Status': 'FLP'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertTrue(result.is_consistent)
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        self.assertEqual(result.action_needed, RecoveryAction.WAIT_FOR_FILL)
    
    def test_startup_with_pending_sell_order(self):
        """Startup with pending SELL order should transition to PENDING_SELL."""
        # Mock position: holding V
        self.api.account.get_positions = Mock(return_value={
            'Positions': [{
                'Symbol': 'V',
                'Quantity': 30,
                'AveragePrice': 320.0,
                'MarketValue': 9600.0,
                'UnrealizedProfitLoss': 0.0
            }]
        })
        
        # Mock pending SELL order
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD202',
                'Symbol': 'V',
                'TradeAction': 'SELL',
                'Quantity': 30,
                'FilledQuantity': 0,
                'Status': 'OPN'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertTrue(result.is_consistent)
        self.assertEqual(result.recommended_state, TradingState.PENDING_SELL)
        self.assertEqual(result.current_stock, StockHeld.TICKER_A)
        self.assertEqual(result.action_needed, RecoveryAction.WAIT_FOR_FILL)


class TestReconciliationStartupTerminalStatuses(unittest.TestCase):
    """Test startup with terminal order statuses (should be ignored)."""
    
    def setUp(self):
        """Set up mock API and reconciler."""
        self.api = MockAPI()
        self.reconciler = Reconciler(
            api=self.api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            allocated_cash=10000
        )
        # Mock balances for buying power checks
        self.api.account.get_balances = Mock(return_value={
            'Balances': [{'BuyingPower': 10000.0}]
        })
    
    def test_startup_with_fll_filled_order(self):
        """Startup with FLL (Filled) order should ignore it and proceed."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Filled order should be ignored (not pending)
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD999',
                'Symbol': 'MA',
                'TradeAction': 'BUY',
                'Quantity': 19,
                'FilledQuantity': 19,
                'Status': 'FLL'
            }]
        })
        
        result = self.reconciler.check_state()
        
        # Should proceed to CLEANUP_CASH since no pending orders
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(result.action_needed, RecoveryAction.BUY_INITIAL)
        self.assertEqual(len(result.pending_orders), 0)
    
    def test_startup_with_rej_rejected_order(self):
        """Startup with REJ (Rejected) order should ignore it."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD888',
                'Symbol': 'V',
                'TradeAction': 'BUY',
                'Quantity': 30,
                'FilledQuantity': 0,
                'Status': 'REJ'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 0)
    
    def test_startup_with_can_canceled_order(self):
        """Startup with CAN (Canceled) order should ignore it."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD777',
                'Symbol': 'MA',
                'TradeAction': 'BUY',
                'Quantity': 20,
                'FilledQuantity': 0,
                'Status': 'CAN'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 0)
    
    def test_startup_with_exp_expired_order(self):
        """Startup with EXP (Expired) order should ignore it."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD666',
                'Symbol': 'V',
                'TradeAction': 'BUY',
                'Quantity': 25,
                'FilledQuantity': 0,
                'Status': 'EXP'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 0)
    
    def test_startup_with_out_urout_order(self):
        """Startup with OUT (UROut) order should ignore it."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD555',
                'Symbol': 'MA',
                'TradeAction': 'BUY',
                'Quantity': 18,
                'FilledQuantity': 0,
                'Status': 'OUT'
            }]
        })
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 0)


class TestReconciliationStartupUnknownStatus(unittest.TestCase):
    """Test startup with unknown order status."""
    
    def setUp(self):
        """Set up mock API and reconciler."""
        self.api = MockAPI()
        self.reconciler = Reconciler(
            api=self.api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            allocated_cash=10000
        )
        self.api.account.get_balances = Mock(return_value={
            'Balances': [{'BuyingPower': 10000.0}]
        })
    
    def test_startup_with_unknown_status(self):
        """Startup with unknown status code should log error and ignore order."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Unknown status code
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD444',
                'Symbol': 'V',
                'TradeAction': 'BUY',
                'Quantity': 30,
                'FilledQuantity': 0,
                'Status': 'XYZ'  # Unknown status
            }]
        })
        
        result = self.reconciler.check_state()
        
        # Should proceed to CLEANUP_CASH since unknown status is not pending
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 0)


class TestReconciliationStartupEdgeCases(unittest.TestCase):
    """Test edge cases in startup reconciliation."""
    
    def setUp(self):
        """Set up mock API and reconciler."""
        self.api = MockAPI()
        self.reconciler = Reconciler(
            api=self.api,
            account_id="TEST123",
            ticker_a="V",
            ticker_b="MA",
            allocated_cash=10000
        )
        self.api.account.get_balances = Mock(return_value={
            'Balances': [{'BuyingPower': 10000.0}]
        })
    
    def test_startup_with_multiple_pending_orders(self):
        """Startup with multiple pending orders should pick the first one."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Multiple pending orders
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [
                {
                    'OrderID': 'ORD111',
                    'Symbol': 'V',
                    'TradeAction': 'BUY',
                    'Quantity': 30,
                    'FilledQuantity': 0,
                    'Status': 'ACK'
                },
                {
                    'OrderID': 'ORD222',
                    'Symbol': 'MA',
                    'TradeAction': 'BUY',
                    'Quantity': 19,
                    'FilledQuantity': 0,
                    'Status': 'OPN'
                }
            ]
        })
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        self.assertEqual(len(result.pending_orders), 2)
        # Should use first pending order
        self.assertEqual(result.pending_orders[0].order_id, 'ORD111')
    
    def test_startup_with_pending_order_wrong_symbol(self):
        """Startup with pending order for wrong symbol should ignore it."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Pending order for symbol not in trading pair
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [{
                'OrderID': 'ORD333',
                'Symbol': 'AAPL',  # Not in our V/MA pair
                'TradeAction': 'BUY',
                'Quantity': 10,
                'FilledQuantity': 0,
                'Status': 'ACK'
            }]
        })
        
        result = self.reconciler.check_state()
        
        # Should proceed to CLEANUP_CASH since no relevant pending orders
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(len(result.pending_orders), 1)  # All orders tracked
        # But no pending orders for our pair, so state is CLEANUP_CASH
    
    def test_startup_with_no_orders(self):
        """Startup with no orders should proceed to CLEANUP_CASH."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        self.api.orders.get_orders = Mock(return_value={'Orders': []})
        
        result = self.reconciler.check_state()
        
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
        self.assertEqual(result.action_needed, RecoveryAction.BUY_INITIAL)
        self.assertEqual(len(result.pending_orders), 0)
    
    def test_startup_api_error_fetching_orders(self):
        """Startup with API error should handle gracefully."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # API error when fetching orders
        self.api.orders.get_orders = Mock(side_effect=Exception("API error"))
        
        result = self.reconciler.check_state()
        
        # Should still return a result with empty pending orders
        self.assertEqual(len(result.pending_orders), 0)
        # Should proceed to CLEANUP_CASH based on no position
        self.assertEqual(result.recommended_state, TradingState.CLEANUP_CASH)
    
    def test_startup_mixed_pending_and_terminal_orders(self):
        """Startup with mix of pending and terminal orders should only process pending."""
        self.api.account.get_positions = Mock(return_value={'Positions': []})
        
        # Mix of statuses
        self.api.orders.get_orders = Mock(return_value={
            'Orders': [
                {
                    'OrderID': 'ORD001',
                    'Symbol': 'V',
                    'TradeAction': 'BUY',
                    'Quantity': 30,
                    'FilledQuantity': 30,
                    'Status': 'FLL'  # Terminal
                },
                {
                    'OrderID': 'ORD002',
                    'Symbol': 'MA',
                    'TradeAction': 'BUY',
                    'Quantity': 19,
                    'FilledQuantity': 0,
                    'Status': 'ACK'  # Pending
                },
                {
                    'OrderID': 'ORD003',
                    'Symbol': 'V',
                    'TradeAction': 'BUY',
                    'Quantity': 25,
                    'FilledQuantity': 0,
                    'Status': 'CAN'  # Terminal
                }
            ]
        })
        
        result = self.reconciler.check_state()
        
        # Should identify pending order
        self.assertEqual(result.recommended_state, TradingState.PENDING_BUY)
        # pending_orders only contains pending orders (not terminal ones)
        self.assertEqual(len(result.pending_orders), 1)  # Only the ACK order
        self.assertEqual(result.pending_orders[0].order_id, 'ORD002')


if __name__ == '__main__':
    unittest.main()
