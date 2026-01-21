"""
Unit tests for OrderExecutor with mock TradeStation API.

Tests the allocated_cash logic to ensure correct share quantities are purchased.
"""

import unittest
from typing import Dict, Any, List, Optional
from datetime import datetime


class MockTradeStationAPI:
    """
    Mock TradeStation API that simulates account state, positions, and orders.
    
    This mock maintains internal state for:
    - Cash balance
    - Positions (symbol -> quantity, avg_price)
    - Orders (pending and filled)
    - Buying power (may be reduced by positions/margin)
    """
    
    def __init__(self, initial_cash: float = 30000.0, buying_power: Optional[float] = None):
        """
        Initialize mock API.
        
        Args:
            initial_cash: Starting cash balance
            buying_power: Available buying power (defaults to initial_cash)
        """
        self.cash_balance = initial_cash
        self.buying_power_override = buying_power  # Can be set to simulate reduced buying power
        self.positions: Dict[str, Dict[str, Any]] = {}  # symbol -> {quantity, avg_price, market_value}
        self.orders_list: List[Dict[str, Any]] = []  # Store orders in orders_list, not orders
        self.order_counter = 1000
        
        # Mock account ID
        self.account_id = "TEST123456"
        
        # Create nested structure to match real API
        self.account = self._AccountAPI(self)
        self.orders = self._OrdersAPI(self)
        self.config = self._Config(self)
    
    class _Config:
        def __init__(self, parent):
            self.account_id = parent.account_id
    
    class _AccountAPI:
        """Mock account API methods."""
        
        def __init__(self, parent):
            self.parent = parent
        
        def get_balances(self, account_id: str) -> Dict[str, Any]:
            """Get account balances."""
            # Calculate equity (cash + position values)
            position_value = sum(p['market_value'] for p in self.parent.positions.values())
            equity = self.parent.cash_balance + position_value
            
            # Determine buying power
            if self.parent.buying_power_override is not None:
                buying_power = self.parent.buying_power_override
            else:
                buying_power = self.parent.cash_balance
            
            return {
                'Balances': [{
                    'AccountID': account_id,
                    'CashBalance': self.parent.cash_balance,
                    'BuyingPower': buying_power,
                    'Equity': equity,
                    'EquityWithLoanValue': equity
                }]
            }
        
        def get_positions(self, account_id: str) -> Dict[str, Any]:
            """Get current positions."""
            positions_list = []
            
            for symbol, pos_data in self.parent.positions.items():
                if pos_data['quantity'] > 0:
                    positions_list.append({
                        'Symbol': symbol,
                        'Quantity': pos_data['quantity'],
                        'AveragePrice': pos_data['avg_price'],
                        'MarketValue': pos_data['market_value'],
                        'UnrealizedProfitLoss': pos_data['market_value'] - (pos_data['quantity'] * pos_data['avg_price'])
                    })
            
            return {
                'Positions': positions_list
            }
    
    class _OrdersAPI:
        """Mock orders API methods."""
        
        def __init__(self, parent):
            self.parent = parent
        
        def place_order(
            self,
            account_id: str,
            symbol: str,
            quantity: int,
            action: str,  # BUY or SELL
            order_type: str,
            time_in_force: str
        ) -> Dict[str, Any]:
            """Place an order (immediately fills it for testing)."""
            order_id = f"ORD{self.parent.order_counter}"
            self.parent.order_counter += 1
            
            # Get current price (simulate with a lookup)
            price = self.parent._get_mock_price(symbol)
            
            # Execute the order immediately (simulate fill)
            if action == 'BUY':
                cost = quantity * price
                if cost > self.parent.cash_balance:
                    return {
                        'Message': f'Insufficient funds: need ${cost:.2f}, have ${self.parent.cash_balance:.2f}'
                    }
                
                # Update cash
                self.parent.cash_balance -= cost
                
                # Update position
                if symbol in self.parent.positions:
                    old_qty = self.parent.positions[symbol]['quantity']
                    old_avg = self.parent.positions[symbol]['avg_price']
                    new_qty = old_qty + quantity
                    new_avg = ((old_qty * old_avg) + (quantity * price)) / new_qty
                    self.parent.positions[symbol] = {
                        'quantity': new_qty,
                        'avg_price': new_avg,
                        'market_value': new_qty * price
                    }
                else:
                    self.parent.positions[symbol] = {
                        'quantity': quantity,
                        'avg_price': price,
                        'market_value': quantity * price
                    }
            
            elif action == 'SELL':
                # Check if we have the position
                if symbol not in self.parent.positions or self.parent.positions[symbol]['quantity'] < quantity:
                    return {
                        'Message': f'Insufficient shares: trying to sell {quantity}, have {self.parent.positions.get(symbol, {}).get("quantity", 0)}'
                    }
                
                # Update cash
                proceeds = quantity * price
                self.parent.cash_balance += proceeds
                
                # Update position
                self.parent.positions[symbol]['quantity'] -= quantity
                if self.parent.positions[symbol]['quantity'] == 0:
                    del self.parent.positions[symbol]
                else:
                    self.parent.positions[symbol]['market_value'] = self.parent.positions[symbol]['quantity'] * price
            
            # Create order record
            order = {
                'OrderID': order_id,
                'Symbol': symbol,
                'Quantity': quantity,
                'TradeAction': action,
                'Status': 'FLL',  # TradeStation uses 3-letter codes: FLL = Filled
                'FilledQuantity': quantity,
                'AveragePrice': price
            }
            
            self.parent.orders_list.append(order)
            
            return {
                'Orders': [order]
            }
        
        def get_order(self, order_id: str) -> Dict[str, Any]:
            """Get order status by ID."""
            for order in self.parent.orders_list:
                if order['OrderID'] == order_id:
                    return order
            
            return {
                'Message': f'Order {order_id} not found'
            }
        
        def get_orders(self, account_id: str) -> Dict[str, Any]:
            """Get all orders."""
            return {
                'Orders': self.parent.orders_list
            }
    
    def _get_mock_price(self, symbol: str) -> float:
        """Get mock price for a symbol."""
        # Simple price lookup for testing
        prices = {
            'V': 300.0,
            'MA': 500.0
        }
        return prices.get(symbol, 100.0)
    
    def set_buying_power(self, buying_power: float):
        """Override the buying power (to simulate reduced availability)."""
        self.buying_power_override = buying_power


class TestOrderExecutorAllocatedCash(unittest.TestCase):
    """Test cases for OrderExecutor with allocated_cash."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Import here to avoid circular imports
        from live_trading.order_executor import OrderExecutor
        
        self.OrderExecutor = OrderExecutor
    
    def test_allocated_cash_initial_buy_buys_correct_shares(self):
        """
        Test that with allocated_cash=1000 and V price=$300,
        the initial buy purchases 3 shares (floor(1000/300)).
        
        This test should FAIL initially, reproducing the bug where only 1 share is bought.
        """
        # Setup: Account with $30,000, allocated_cash=1000
        mock_api = MockTradeStationAPI(initial_cash=30000.0, buying_power=30000.0)
        
        # Create OrderExecutor with allocated_cash=1000
        executor = self.OrderExecutor(
            api=mock_api,
            account_id=mock_api.account_id,
            logger=None,
            allocated_cash=1000
        )
        
        # Execute initial buy of V at $300
        result = executor.execute_initial_buy(
            symbol='V',
            current_price=300.0,
            current_state='CASH'
        )
        
        # Assert: Should buy 3 shares
        self.assertIsNotNone(result, "Order result should not be None")
        self.assertTrue(result.is_filled, "Order should be filled")
        self.assertEqual(result.filled_quantity, 3, 
                        f"Should buy 3 shares with allocated_cash=1000 and price=$300, but bought {result.filled_quantity}")
        
        # Verify position
        positions = mock_api.account.get_positions(mock_api.account_id)
        self.assertEqual(len(positions['Positions']), 1, "Should have 1 position")
        self.assertEqual(positions['Positions'][0]['Symbol'], 'V')
        self.assertEqual(positions['Positions'][0]['Quantity'], 3)
    
    def test_allocated_cash_swap_buys_correct_shares(self):
        """
        Test that swapping from V to MA with allocated_cash=1000
        buys 2 shares of MA (floor(1000/500)).
        """
        # Setup: Account holding 3 shares of V
        mock_api = MockTradeStationAPI(initial_cash=29100.0, buying_power=29100.0)
        mock_api.positions['V'] = {
            'quantity': 3,
            'avg_price': 300.0,
            'market_value': 900.0
        }
        
        # Create OrderExecutor with allocated_cash=1000
        executor = self.OrderExecutor(
            api=mock_api,
            account_id=mock_api.account_id,
            logger=None,
            allocated_cash=1000
        )
        
        # Execute swap: sell V, buy MA
        sell_result, buy_result = executor.execute_swap(
            sell_symbol='V',
            sell_quantity=3,
            buy_symbol='MA',
            current_price_sell=300.0,
            current_price_buy=500.0,
            current_state='HOLDING_WAITING'
        )
        
        # Assert: Should sell all V and buy 2 shares of MA
        self.assertIsNotNone(sell_result, "Sell result should not be None")
        self.assertTrue(sell_result.is_filled, "Sell order should be filled")
        self.assertEqual(sell_result.filled_quantity, 3, "Should sell all 3 shares")
        
        self.assertIsNotNone(buy_result, "Buy result should not be None")
        self.assertTrue(buy_result.is_filled, "Buy order should be filled")
        self.assertEqual(buy_result.filled_quantity, 2,
                        f"Should buy 2 shares of MA with allocated_cash=1000 and price=$500, but bought {buy_result.filled_quantity}")
        
        # Verify final position
        positions = mock_api.account.get_positions(mock_api.account_id)
        self.assertEqual(len(positions['Positions']), 1, "Should have 1 position")
        self.assertEqual(positions['Positions'][0]['Symbol'], 'MA')
        self.assertEqual(positions['Positions'][0]['Quantity'], 2)
    
    def test_allocated_cash_zero_uses_full_buying_power(self):
        """
        Test that with allocated_cash=0, the executor uses
        full account buying power ($30,000).
        """
        # Setup: Account with $30,000, allocated_cash=0
        mock_api = MockTradeStationAPI(initial_cash=30000.0, buying_power=30000.0)
        
        # Create OrderExecutor with allocated_cash=0 (unlimited mode)
        executor = self.OrderExecutor(
            api=mock_api,
            account_id=mock_api.account_id,
            logger=None,
            allocated_cash=0
        )
        
        # Execute initial buy of V at $300
        result = executor.execute_initial_buy(
            symbol='V',
            current_price=300.0,
            current_state='CASH'
        )
        
        # Assert: Should buy 100 shares (floor(30000/300))
        self.assertIsNotNone(result, "Order result should not be None")
        self.assertTrue(result.is_filled, "Order should be filled")
        self.assertEqual(result.filled_quantity, 100,
                        f"Should buy 100 shares with full buying power $30,000 and price=$300, but bought {result.filled_quantity}")
    
    def test_get_buying_power_with_allocated_cash(self):
        """
        Test that get_buying_power() returns allocated_cash
        even when API reports lower buying power.
        
        This directly tests the bug scenario where API returns
        reduced buying power due to other positions.
        """
        # Setup: Account with $30,000 total, but only $300 buying power available
        # (simulating other positions using up margin)
        mock_api = MockTradeStationAPI(initial_cash=30000.0, buying_power=300.0)
        
        # Create OrderExecutor with allocated_cash=1000
        executor = self.OrderExecutor(
            api=mock_api,
            account_id=mock_api.account_id,
            logger=None,
            allocated_cash=1000
        )
        
        # Get buying power
        buying_power = executor.get_buying_power()
        
        # Assert: Should return 1000, not 300
        self.assertEqual(buying_power, 1000.0,
                        f"get_buying_power() should return allocated_cash (1000), not API buying power (300). Got {buying_power}")
    
    def test_get_buying_power_without_allocated_cash(self):
        """
        Test that get_buying_power() returns API buying power
        when allocated_cash is 0.
        """
        # Setup: Account with $25,000 buying power
        mock_api = MockTradeStationAPI(initial_cash=30000.0, buying_power=25000.0)
        
        # Create OrderExecutor with allocated_cash=0
        executor = self.OrderExecutor(
            api=mock_api,
            account_id=mock_api.account_id,
            logger=None,
            allocated_cash=0
        )
        
        # Get buying power
        buying_power = executor.get_buying_power()
        
        # Assert: Should return API buying power (25000)
        self.assertEqual(buying_power, 25000.0,
                        f"get_buying_power() should return API buying power (25000) when allocated_cash=0. Got {buying_power}")


def run_tests():
    """Run the test suite."""
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()

