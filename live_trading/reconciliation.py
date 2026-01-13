"""
Reconciliation Module - Position Verification and Startup Recovery

This module handles:
    - Position reconciliation: Verify internal state matches TradeStation API
    - Startup recovery: Determine initial state from API on restart
    - Pending order detection: Handle orders that may be in-flight

Reconciliation is critical for maintaining consistency between the algorithm's
internal state and the actual state at the broker. Mismatches can occur due to:
    - Application crashes mid-trade
    - Network issues causing missed confirmations
    - Manual interventions in the TradeStation interface
    - Bugs in state management

On startup, the algorithm:
    1. Fetches current positions from API
    2. Fetches pending orders from API
    3. Determines appropriate initial state
    4. Verifies consistency before proceeding
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .state_machine import TradingState, StockHeld, StateData

# Import centralized logger
from .logger import logger


@dataclass
class Position:
    """
    A position in a single security.
    
    Attributes:
        symbol: Stock symbol
        quantity: Number of shares (positive = long, negative = short)
        average_price: Average entry price
        market_value: Current market value
        unrealized_pnl: Unrealized profit/loss
    """
    symbol: str
    quantity: int
    average_price: float
    market_value: float
    unrealized_pnl: float


@dataclass
class PendingOrder:
    """
    A pending order not yet filled.
    
    Attributes:
        order_id: TradeStation order ID
        symbol: Stock symbol
        action: BUY, SELL, etc.
        quantity: Order quantity
        filled_quantity: Quantity already filled
        status: Order status string
    """
    order_id: str
    symbol: str
    action: str
    quantity: int
    filled_quantity: int
    status: str


class RecoveryAction(Enum):
    """Actions that may be needed on startup recovery."""
    NONE = "none"                    # No action needed, state is valid
    WAIT_FOR_FILL = "wait_for_fill"  # Wait for pending order to fill
    BUY_INITIAL = "buy_initial"      # Need to execute initial buy
    RESOLVE_MISMATCH = "resolve"     # State mismatch needs resolution
    ERROR = "error"                  # Unrecoverable error


@dataclass
class ReconciliationResult:
    """
    Result of a reconciliation check.
    
    Attributes:
        is_consistent: True if internal state matches API state
        recommended_state: What state we should be in based on API
        current_stock: Which stock is held (if any)
        positions: List of positions found
        pending_orders: List of pending orders found
        action_needed: What action (if any) is needed
        error_message: Error description if inconsistent
    """
    is_consistent: bool
    recommended_state: TradingState
    current_stock: StockHeld
    positions: List[Position]
    pending_orders: List[PendingOrder]
    action_needed: RecoveryAction
    error_message: Optional[str] = None


class Reconciler:
    """
    Handles position reconciliation and startup recovery.
    
    This class queries the TradeStation API to determine the actual state
    and compares it with the algorithm's internal state. It's used:
        1. On startup: To recover from crashes and determine initial state
        2. Periodically: To verify ongoing consistency
        3. After trades: To confirm expected positions
    
    Usage:
        reconciler = Reconciler(api, account_id, "V", "MA", logger)
        
        # On startup
        result = reconciler.check_state()
        if result.action_needed == RecoveryAction.BUY_INITIAL:
            # Execute initial buy
        
        # Periodic check
        result = reconciler.verify_position(expected_stock="ticker_a", expected_shares=100)
        if not result.is_consistent:
            # Handle mismatch
    
    Attributes:
        api: TradeStation API instance
        account_id: Trading account ID
        ticker_a: First ticker symbol
        ticker_b: Second ticker symbol
        logger: TradingLogger for logging
    """
    
    def __init__(
        self,
        api,  # TradeStationAPI instance
        account_id: str,
        ticker_a: str,
        ticker_b: str,
        logger=None,  # TradingLogger
        allocated_cash: float = 0
    ):
        """
        Initialize the reconciler.
        
        Args:
            api: TradeStation API instance
            account_id: Trading account ID
            ticker_a: First ticker symbol
            ticker_b: Second ticker symbol
            logger: TradingLogger for logging
            allocated_cash: Maximum cash to use for trading (0 = use full account)
        """
        self.api = api
        self.account_id = account_id
        self.ticker_a = ticker_a
        self.ticker_b = ticker_b
        self.logger = logger
        self.allocated_cash = allocated_cash
    
    def fetch_positions(self) -> List[Position]:
        """
        Fetch current positions from TradeStation API.
        
        Returns:
            List of Position objects
        """
        try:
            response = self.api.account.get_positions(self.account_id)
            
            positions = []
            for pos_data in response.get('Positions', []):
                position = Position(
                    symbol=pos_data.get('Symbol', ''),
                    quantity=int(pos_data.get('Quantity', 0)),
                    average_price=float(pos_data.get('AveragePrice', 0)),
                    market_value=float(pos_data.get('MarketValue', 0)),
                    unrealized_pnl=float(pos_data.get('UnrealizedProfitLoss', 0))
                )
                positions.append(position)
            
            # Print positions found
            if positions:
                logger.info(f"Positions fetched: {len(positions)} position(s)")
                for pos in positions:
                    logger.verbose(f"  {pos.symbol}: {pos.quantity} shares @ ${pos.average_price:.2f}, Value: ${pos.market_value:.2f}")
            else:
                logger.verbose("Positions fetched: No positions found")
            
            return positions
            
        except Exception as exception:
            logger.error(f"Failed to fetch positions: {exception}")
            return []
    
    def fetch_pending_orders(self) -> List[PendingOrder]:
        """
        Fetch pending orders from TradeStation API.
        
        Returns:
            List of PendingOrder objects for orders not yet filled
        """
        try:
            response = self.api.orders.get_orders(self.account_id)
            
            pending_orders = []
            for order_data in response.get('Orders', []):
                status = order_data.get('Status', '')
                
                # Only include orders that are still pending
                if status in ['Received', 'Sent', 'PartiallyFilled']:
                    order = PendingOrder(
                        order_id=order_data.get('OrderID', ''),
                        symbol=order_data.get('Symbol', ''),
                        action=order_data.get('TradeAction', ''),
                        quantity=int(order_data.get('Quantity', 0)),
                        filled_quantity=int(order_data.get('FilledQuantity', 0)),
                        status=status
                    )
                    pending_orders.append(order)
            
            # Print pending orders found
            if pending_orders:
                logger.warning(f"⚠️  Pending orders found: {len(pending_orders)} order(s)")
                for order in pending_orders:
                    logger.warning(f"  Order {order.order_id}: {order.action} {order.quantity} {order.symbol} ({order.status})")
            else:
                logger.verbose("No pending orders")
            
            return pending_orders
            
        except Exception as exception:
            logger.error(f"Failed to fetch orders: {exception}")
            return []
    
    def check_state(self) -> ReconciliationResult:
        """
        Check current state by querying positions and orders.
        
        This is the main method for determining what state the algorithm
        should be in based on actual API state. Used on startup and for
        periodic verification.
        
        Returns:
            ReconciliationResult with recommended state and any actions needed
        """
        logger.info("🔍 Running state reconciliation check...")
        positions = self.fetch_positions()
        pending_orders = self.fetch_pending_orders()
        
        # Filter to just our trading pair
        pair_positions = [
            p for p in positions 
            if p.symbol in (self.ticker_a, self.ticker_b)
        ]
        pair_pending = [
            o for o in pending_orders 
            if o.symbol in (self.ticker_a, self.ticker_b)
        ]
        
        # Check for unexpected positions (stocks not in our pair)
        unexpected_positions = [
            p for p in positions 
            if p.symbol not in (self.ticker_a, self.ticker_b) and p.quantity != 0
        ]
        
        if unexpected_positions:
            # Log warning but don't fail - just ignore other positions
            for pos in unexpected_positions:
                logger.warning(f"⚠️  WARNING: Found position in {pos.symbol} (not in trading pair)")
        
        # Determine state based on what we found
        result = self._determine_state(pair_positions, pair_pending, positions, pending_orders)
        
        # Print result
        if result.is_consistent:
            logger.info(f"✓ State check complete: Recommended state = {result.recommended_state.name}, Stock = {result.current_stock.value}, Action = {result.action_needed.name}")
        else:
            logger.warning(f"⚠️  State inconsistency: {result.error_message}")
        
        return result
    
    def _determine_state(
        self,
        pair_positions: List[Position],
        pair_pending: List[PendingOrder],
        all_positions: List[Position],
        all_pending: List[PendingOrder]
    ) -> ReconciliationResult:
        """
        Determine the appropriate state based on positions and orders.
        
        Decision logic:
            - Pending order exists → PENDING_BUY or PENDING_SELL
            - Position in ticker_a → HOLDING_WAITING (holding A)
            - Position in ticker_b → HOLDING_WAITING (holding B)
            - No position, no pending → CASH
        """
        # Check for pending orders first
        if pair_pending:
            # There's a pending order - determine type
            order = pair_pending[0]  # Take the first one
            
            if order.action == 'BUY':
                return ReconciliationResult(
                    is_consistent=True,
                    recommended_state=TradingState.PENDING_BUY,
                    current_stock=StockHeld.NONE,
                    positions=all_positions,
                    pending_orders=all_pending,
                    action_needed=RecoveryAction.WAIT_FOR_FILL,
                    error_message=None
                )
            elif order.action == 'SELL':
                # Determine which stock we're selling
                if order.symbol == self.ticker_a:
                    current_stock = StockHeld.TICKER_A
                else:
                    current_stock = StockHeld.TICKER_B
                
                return ReconciliationResult(
                    is_consistent=True,
                    recommended_state=TradingState.PENDING_SELL,
                    current_stock=current_stock,
                    positions=all_positions,
                    pending_orders=all_pending,
                    action_needed=RecoveryAction.WAIT_FOR_FILL,
                    error_message=None
                )
        
        # No pending orders - check positions
        ticker_a_position = next(
            (p for p in pair_positions if p.symbol == self.ticker_a and p.quantity > 0),
            None
        )
        ticker_b_position = next(
            (p for p in pair_positions if p.symbol == self.ticker_b and p.quantity > 0),
            None
        )
        
        # Check for conflicting positions (shouldn't happen)
        if ticker_a_position and ticker_b_position:
            return ReconciliationResult(
                is_consistent=False,
                recommended_state=TradingState.ERROR,
                current_stock=StockHeld.NONE,
                positions=all_positions,
                pending_orders=all_pending,
                action_needed=RecoveryAction.ERROR,
                error_message=f"Conflicting positions: {ticker_a_position.quantity} {self.ticker_a} "
                             f"AND {ticker_b_position.quantity} {self.ticker_b}"
            )
        
        # Single position in ticker_a
        if ticker_a_position:
            return ReconciliationResult(
                is_consistent=True,
                recommended_state=TradingState.HOLDING_WAITING,
                current_stock=StockHeld.TICKER_A,
                positions=all_positions,
                pending_orders=all_pending,
                action_needed=RecoveryAction.NONE,
                error_message=None
            )
        
        # Single position in ticker_b
        if ticker_b_position:
            return ReconciliationResult(
                is_consistent=True,
                recommended_state=TradingState.HOLDING_WAITING,
                current_stock=StockHeld.TICKER_B,
                positions=all_positions,
                pending_orders=all_pending,
                action_needed=RecoveryAction.NONE,
                error_message=None
            )
        
        # No position - we're in cash
        return ReconciliationResult(
            is_consistent=True,
            recommended_state=TradingState.CASH,
            current_stock=StockHeld.NONE,
            positions=all_positions,
            pending_orders=all_pending,
            action_needed=RecoveryAction.BUY_INITIAL,
            error_message=None
        )
    
    def verify_position(
        self,
        expected_stock: str,  # "ticker_a" or "ticker_b"
        expected_shares: int
    ) -> ReconciliationResult:
        """
        Verify that actual position matches expected.
        
        Use this after trades complete to confirm the position
        was established correctly.
        
        Args:
            expected_stock: Which stock we expect to hold
            expected_shares: Number of shares expected
        
        Returns:
            ReconciliationResult with consistency check
        """
        result = self.check_state()
        
        # Check if position matches expectation
        expected_symbol = self.ticker_a if expected_stock == "ticker_a" else self.ticker_b
        
        actual_position = next(
            (p for p in result.positions if p.symbol == expected_symbol),
            None
        )
        
        if not actual_position:
            result.is_consistent = False
            result.error_message = f"Expected position in {expected_symbol}, found none"
            result.action_needed = RecoveryAction.RESOLVE_MISMATCH
            
            if self.logger:
                self.logger.log_position_mismatch(
                    expected=f"{expected_shares} {expected_symbol}",
                    actual="no position",
                    state=result.recommended_state.name
                )
            
            return result
        
        # Check quantity (allow small differences due to partial fills)
        actual_shares = actual_position.quantity
        
        if actual_shares != expected_shares:
            # Log warning but don't necessarily fail
            # Small differences might be acceptable
            diff_pct = abs(actual_shares - expected_shares) / expected_shares * 100
            
            if diff_pct > 5:  # More than 5% difference
                result.is_consistent = False
                result.error_message = (
                    f"Position mismatch: expected {expected_shares} {expected_symbol}, "
                    f"found {actual_shares}"
                )
                result.action_needed = RecoveryAction.RESOLVE_MISMATCH
                
                if self.logger:
                    self.logger.log_position_mismatch(
                        expected=f"{expected_shares} {expected_symbol}",
                        actual=f"{actual_shares} {expected_symbol}",
                        state=result.recommended_state.name
                    )
            else:
                # Small difference - warn but continue
                print(f"WARNING: Position quantity differs slightly: "
                      f"expected {expected_shares}, actual {actual_shares}")
        
        return result
    
    def get_position_quantity(self, stock: str) -> int:
        """
        Get the current position quantity for a stock.
        
        Args:
            stock: "ticker_a" or "ticker_b"
        
        Returns:
            Number of shares held (0 if no position)
        """
        symbol = self.ticker_a if stock == "ticker_a" else self.ticker_b
        positions = self.fetch_positions()
        
        position = next(
            (p for p in positions if p.symbol == symbol),
            None
        )
        
        return position.quantity if position else 0
    
    def get_buying_power(self) -> float:
        """
        Get current available buying power.
        
        When allocated_cash is set (> 0), always returns allocated_cash to ensure
        consistent position sizing. When allocated_cash=0, uses the account's
        full buying power for exponential growth.
        
        Returns:
            Buying power in dollars
        """
        # If allocated_cash is set, use it directly regardless of API buying power
        if self.allocated_cash > 0:
            logger.verbose(f"Buying power check: ${self.allocated_cash:.2f} (allocated_cash mode)")
            return self.allocated_cash
        
        # Otherwise, query API for actual buying power
        try:
            balances = self.api.account.get_balances(self.account_id)
            
            # Extract the first balance object from the Balances array
            if 'Balances' in balances and len(balances['Balances']) > 0:
                balance = balances['Balances'][0]
            else:
                logger.error("No balance data in API response")
                return 0.0
            
            # Try various fields that might contain buying power
            buying_power = balance.get('BuyingPower', 0)
            if buying_power == 0:
                buying_power = balance.get('CashBalance', 0)
            
            api_buying_power = float(buying_power)
            
            logger.verbose(f"Buying power check: ${api_buying_power:.2f}")
            return api_buying_power
            
        except Exception as exception:
            logger.error(f"Failed to get buying power: {exception}")
            return 0.0
    
    def get_portfolio_value(self) -> float:
        """
        Get current total portfolio value.
        
        Returns:
            Total equity value in dollars
        """
        try:
            balances = self.api.account.get_balances(self.account_id)
            
            # Extract the first balance object from the Balances array
            if 'Balances' in balances and len(balances['Balances']) > 0:
                balance = balances['Balances'][0]
            else:
                logger.error("No balance data in API response")
                return 0.0
            
            # Try various fields
            equity = balance.get('Equity', 0)
            if equity == 0:
                equity = balance.get('AccountBalance', 0)
            if equity == 0:
                # Fall back to cash + position values
                cash = float(balance.get('CashBalance', 0))
                positions = self.fetch_positions()
                position_value = sum(p.market_value for p in positions)
                equity = cash + position_value
            
            portfolio_value = float(equity)
            logger.verbose(f"Portfolio value check: ${portfolio_value:.2f}")
            return portfolio_value
            
        except Exception as exception:
            logger.error(f"Failed to get portfolio value: {exception}")
            return 0.0

