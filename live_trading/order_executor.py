"""
Order Executor Module - Order Placement and Fill Verification

This module handles all order-related operations for the pairs trading algorithm:
    - Order placement (market orders only)
    - Fill verification and polling
    - Slippage tracking (expected vs actual)
    - Sequential sell-then-buy execution for swaps

Order Flow for Swaps:
    1. Calculate shares to sell (entire position)
    2. Place SELL market order
    3. Poll order status until Filled
    4. Log actual fill price and slippage
    5. Get updated buying power
    6. Calculate shares to buy (floor of cash / price)
    7. Place BUY market order
    8. Poll order status until Filled
    9. Verify final position

Order Status Values (from TradeStation API):
    - Received: Order accepted by TradeStation
    - Sent: Order sent to exchange
    - Filled: Order completely filled
    - PartiallyFilled: Order partially filled (wait for full fill)
    - Rejected: Order rejected (CRITICAL - halt app)
    - Canceled: Order canceled
"""

import time
import math
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Import centralized logger
from .logger import logger


# Market hours constants (Eastern Time)
EASTERN_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)


class OrderStatus(Enum):
    """Order status values from TradeStation API."""
    RECEIVED = "Received"
    SENT = "Sent"
    FILLED = "Filled"
    PARTIALLY_FILLED = "PartiallyFilled"
    REJECTED = "Rejected"
    CANCELED = "Canceled"
    UNKNOWN = "Unknown"


@dataclass
class OrderResult:
    """
    Result of an order placement and fill.
    
    Attributes:
        order_id: TradeStation order ID
        symbol: Stock symbol
        action: BUY or SELL
        quantity: Number of shares
        expected_price: Price at time of order placement
        actual_price: Average fill price
        status: Final order status
        filled_quantity: Number of shares actually filled
        timestamp: Time of fill completion
        error_message: Error message if order failed
    """
    order_id: str
    symbol: str
    action: str
    quantity: int
    expected_price: float
    actual_price: float
    status: OrderStatus
    filled_quantity: int
    timestamp: datetime
    error_message: Optional[str] = None
    
    @property
    def slippage_percent(self) -> float:
        """Calculate slippage as a percentage."""
        if self.expected_price <= 0:
            return 0.0
        return ((self.actual_price - self.expected_price) / self.expected_price) * 100
    
    @property
    def is_filled(self) -> bool:
        """Check if order was completely filled."""
        return self.status == OrderStatus.FILLED
    
    @property
    def is_rejected(self) -> bool:
        """Check if order was rejected."""
        return self.status == OrderStatus.REJECTED


class OrderExecutor:
    """
    Handles order placement and verification for the pairs trading algorithm.
    
    This class provides methods for:
        - Placing market orders (buy and sell)
        - Polling for order fills
        - Calculating share quantities
        - Tracking expected vs actual slippage
        - Executing complete swap sequences
    
    IMPORTANT: This executor uses MARKET ORDERS ONLY.
    All orders are placed as DAY orders.
    
    Usage:
        executor = OrderExecutor(api, account_id, logger)
        
        # Single order
        result = executor.place_and_wait_for_fill(
            symbol="V",
            action="BUY",
            quantity=100,
            expected_price=280.50
        )
        
        # Complete swap sequence
        buy_result = executor.execute_swap(
            sell_symbol="V",
            sell_quantity=100,
            buy_symbol="MA",
            current_price_sell=280.50,
            current_price_buy=520.00
        )
    
    Attributes:
        api: TradeStation API instance
        account_id: Trading account ID
        logger: TradingLogger for structured logging
        poll_interval: Seconds between order status checks
        max_poll_attempts: Maximum attempts before giving up
    """
    
    def __init__(
        self,
        api,  # TradeStationAPI instance
        account_id: str,
        logger=None,  # TradingLogger instance
        poll_interval: float = 0.5,
        max_poll_attempts: int = 120,  # 60 seconds max wait
        allocated_cash: float = 0,
        sell_counter_manager=None,  # SellCounterManager instance
        sells_per_day_limit: int = 1
    ):
        """
        Initialize the order executor.
        
        Args:
            api: TradeStation API instance
            account_id: Trading account ID (from config)
            logger: TradingLogger for structured logging (optional)
            poll_interval: Seconds between order status polls
            max_poll_attempts: Max polls before timeout
            allocated_cash: Maximum cash to use for trading (0 = use full account)
            sell_counter_manager: SellCounterManager for tracking daily sells (optional)
            sells_per_day_limit: Maximum sells allowed per day (default: 1)
        """
        self.api = api
        self.account_id = account_id
        self.logger = logger
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts
        self.allocated_cash = allocated_cash
        self.sell_counter_manager = sell_counter_manager
        self.sells_per_day_limit = sells_per_day_limit
    
    def _is_market_open(self) -> bool:
        """
        Check if market is currently open (9:30 AM - 4:00 PM ET).
        
        Returns:
            True if within market hours, False otherwise
        """
        now_et = datetime.now(EASTERN_TZ)
        current_time = now_et.time()
        return MARKET_OPEN <= current_time < MARKET_CLOSE
    
    def get_buying_power(self) -> float:
        """
        Get current available buying power.
        
        When allocated_cash is set (> 0), always returns allocated_cash to ensure
        consistent position sizing. When allocated_cash=0, uses the account's
        full buying power for exponential growth.
        
        Returns:
            Available buying power in dollars
        """
        # If allocated_cash is set, use it directly regardless of API buying power
        if self.allocated_cash > 0:
            logger.verbose(f"Buying power: ${self.allocated_cash:.2f} (allocated_cash mode)")
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
            
            # The specific field depends on account type
            # Try several common fields
            buying_power = balance.get('BuyingPower', 0)
            if buying_power == 0:
                buying_power = balance.get('CashBalance', 0)
            if buying_power == 0:
                buying_power = balance.get('EquityWithLoanValue', 0)
            
            api_buying_power = float(buying_power)
            
            logger.verbose(f"Buying power: ${api_buying_power:.2f}")
            return api_buying_power
            
        except Exception as exception:
            logger.error(f"Failed to get buying power: {exception}")
            return 0.0
    
    def calculate_shares_to_buy(self, price: float, available_cash: float) -> int:
        """
        Calculate maximum whole shares that can be purchased.
        
        Args:
            price: Current stock price
            available_cash: Available buying power
        
        Returns:
            Number of whole shares (rounded down)
        """
        if price <= 0:
            return 0
        
        # Calculate maximum shares and round down to whole number
        max_shares = math.floor(available_cash / price)
        return max(0, max_shares)
    
    def place_order(
        self,
        symbol: str,
        action: str,  # "BUY" or "SELL"
        quantity: int
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Place a market order.
        
        Args:
            symbol: Stock symbol
            action: "BUY" or "SELL"
            quantity: Number of shares
        
        Returns:
            Tuple of (order_id, error_message)
            order_id is None if placement failed
        """
        try:
            response = self.api.orders.place_order(
                account_id=self.account_id,
                symbol=symbol,
                quantity=quantity,
                action=action,
                order_type='Market',
                time_in_force='DAY'
            )
            
            # Extract order ID from response
            # TradeStation returns: {"Orders": [{"OrderID": "...", ...}]}
            orders = response.get('Orders', [])
            if orders and len(orders) > 0:
                order_id = orders[0].get('OrderID')
                status = orders[0].get('Status', '')
                
                if order_id:
                    logger.info(f"Order placed: {action} {quantity} {symbol}, OrderID: {order_id}, Status: {status}")
                    return order_id, None
            
            # Order placement failed
            error_msg = response.get('Message', 'Unknown error placing order')
            logger.error(f"Order placement failed: {error_msg}")
            return None, error_msg
            
        except Exception as exception:
            error_msg = str(exception)
            logger.error(f"Failed to place order: {error_msg}")
            return None, error_msg
    
    def get_order_status(self, order_id: str) -> Tuple[OrderStatus, Dict[str, Any]]:
        """
        Get the current status of an order.
        
        Args:
            order_id: TradeStation order ID
        
        Returns:
            Tuple of (OrderStatus, full_order_data)
        """
        try:
            response = self.api.orders.get_order(order_id)
            
            status_str = response.get('Status', 'Unknown')
            
            # Map string to enum
            try:
                status = OrderStatus(status_str)
            except ValueError:
                logger.warning(f"Unknown order status '{status_str}' for order {order_id}")
                status = OrderStatus.UNKNOWN
            
            logger.verbose(f"Order {order_id} status: {status_str}")
            return status, response
            
        except Exception as exception:
            # ERROR level - this is a real problem
            logger.error(f"Failed to get order status for {order_id}: {exception}")
            logger.error(f"Exception type: {type(exception).__name__}")
            # Log full traceback at debug level
            logger.debug("Full traceback:", exc_info=True)
            return OrderStatus.UNKNOWN, {}
    
    def wait_for_fill(
        self,
        order_id: str,
        symbol: str,
        action: str,
        quantity: int,
        expected_price: float,
        current_state: str = ""
    ) -> OrderResult:
        """
        Poll order status until filled, rejected, or timeout.
        
        This method polls the order status at regular intervals until:
            - Order is filled (success)
            - Order is rejected (failure - CRITICAL)
            - Max attempts reached (failure)
        
        Args:
            order_id: TradeStation order ID
            symbol: Stock symbol
            action: BUY or SELL
            quantity: Expected quantity
            expected_price: Price at order placement
            current_state: Current state for logging
        
        Returns:
            OrderResult with fill details or error information
        """
        attempts = 0
        
        while attempts < self.max_poll_attempts:
            status, order_data = self.get_order_status(order_id)
            
            if status == OrderStatus.FILLED:
                # Order completely filled
                filled_qty = int(order_data.get('FilledQuantity', quantity))
                avg_price = float(order_data.get('AveragePrice', expected_price))
                
                result = OrderResult(
                    order_id=order_id,
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    expected_price=expected_price,
                    actual_price=avg_price,
                    status=OrderStatus.FILLED,
                    filled_quantity=filled_qty,
                    timestamp=datetime.now()
                )
                
                # Log the fill
                if self.logger:
                    self.logger.log_order_filled(
                        order_id=order_id,
                        action=action,
                        symbol=symbol,
                        quantity=filled_qty,
                        expected_price=expected_price,
                        actual_price=avg_price,
                        state=current_state
                    )
                
                return result
            
            elif status == OrderStatus.REJECTED:
                # Order rejected - this is CRITICAL
                error_msg = order_data.get('Message', 'Order rejected')
                
                result = OrderResult(
                    order_id=order_id,
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    expected_price=expected_price,
                    actual_price=0.0,
                    status=OrderStatus.REJECTED,
                    filled_quantity=0,
                    timestamp=datetime.now(),
                    error_message=error_msg
                )
                
                # Log the rejection (CRITICAL)
                if self.logger:
                    self.logger.log_order_rejected(
                        order_id=order_id,
                        symbol=symbol,
                        reason=error_msg,
                        state=current_state
                    )
                
                return result
            
            elif status == OrderStatus.CANCELED:
                # Order was canceled
                result = OrderResult(
                    order_id=order_id,
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    expected_price=expected_price,
                    actual_price=0.0,
                    status=OrderStatus.CANCELED,
                    filled_quantity=0,
                    timestamp=datetime.now(),
                    error_message="Order was canceled"
                )
                return result
            
            elif status in {OrderStatus.RECEIVED, OrderStatus.SENT, 
                           OrderStatus.PARTIALLY_FILLED}:
                # Still pending - continue polling
                if status == OrderStatus.PARTIALLY_FILLED:
                    filled_so_far = order_data.get('FilledQuantity', 0)
                    logger.verbose(f"Partial fill: {filled_so_far}/{quantity} shares")
                
                # Log periodic updates every 20 attempts (20 seconds)
                if attempts > 0 and attempts % 20 == 0:
                    logger.info(
                        f"Still waiting for order {order_id} to fill... "
                        f"Status: {status.value} (attempt {attempts}/{self.max_poll_attempts})"
                    )
                
                time.sleep(self.poll_interval)
                attempts += 1
            
            else:
                # Unknown status - continue polling
                # Log periodic updates for unknown status too
                if attempts > 0 and attempts % 20 == 0:
                    logger.warning(
                        f"Order {order_id} in unknown status (attempt {attempts}/{self.max_poll_attempts})"
                    )
                
                time.sleep(self.poll_interval)
                attempts += 1
        
        # Timeout - max attempts reached
        # Log comprehensive timeout information with context
        last_status_str = order_data.get('Status', 'Unknown')
        market_open = self._is_market_open()
        market_status = "during market hours" if market_open else "after hours"
        
        # Log timeout details at appropriate level
        logger.error(
            f"Order {order_id} timeout after {self.max_poll_attempts} attempts "
            f"({self.max_poll_attempts * self.poll_interval:.0f}s). "
            f"Final status: {last_status_str}. Market: {market_status}"
        )
        logger.error(f"Order details: {action} {quantity} {symbol} @ ${expected_price:.2f}")
        
        # Provide actionable guidance based on market status
        if not market_open:
            logger.warning(
                f"Order placed after hours - may fill when market opens. "
                f"Consider: (1) Cancel and retry at open, or (2) Wait patiently"
            )
        else:
            logger.error(
                f"Order failed to fill during market hours - likely a broker issue. "
                f"Entering ERROR state. Check order {order_id} in TradeStation."
            )
        
        # Log full order data at verbose level for debugging
        logger.verbose(f"Full order data: {order_data}")
        
        result = OrderResult(
            order_id=order_id,
            symbol=symbol,
            action=action,
            quantity=quantity,
            expected_price=expected_price,
            actual_price=0.0,
            status=OrderStatus.UNKNOWN,
            filled_quantity=0,
            timestamp=datetime.now(),
            error_message=f"Timeout waiting for fill after {self.max_poll_attempts} attempts. "
                         f"Final status: {last_status_str}. Market: {market_status}"
        )
        return result
    
    def place_and_wait_for_fill(
        self,
        symbol: str,
        action: str,
        quantity: int,
        expected_price: float,
        current_state: str = ""
    ) -> OrderResult:
        """
        Place an order and wait for it to fill.
        
        This is the main method for executing a single order. It:
            1. Check sell counter (if action is SELL)
            2. Increment and persist counter (if action is SELL)
            3. Places the market order
            4. Logs the order placement
            5. Polls until filled or failed
            6. Returns the result with slippage info
        
        Args:
            symbol: Stock symbol
            action: "BUY" or "SELL"
            quantity: Number of shares
            expected_price: Current price for slippage calculation
            current_state: Current state for logging
        
        Returns:
            OrderResult with complete fill information
            Returns rejected OrderResult if sell not allowed
        """
        return self._place_and_wait_internal(
            symbol=symbol,
            action=action,
            quantity=quantity,
            expected_price=expected_price,
            current_state=current_state,
            skip_counter_check=False
        )
    
    def _place_and_wait_internal(
        self,
        symbol: str,
        action: str,
        quantity: int,
        expected_price: float,
        current_state: str = "",
        skip_counter_check: bool = False
    ) -> OrderResult:
        """
        Internal method to place order and wait for fill.
        
        Args:
            skip_counter_check: If True, skip the counter check (used when execute_swap already checked)
        """
        # Check sell counter for SELL orders (prevents Good Faith Violations)
        if action == "SELL" and self.sell_counter_manager and not skip_counter_check:
            if not self.sell_counter_manager.can_sell(self.sells_per_day_limit):
                logger.error(
                    f"🚨 SELL BLOCKED: Daily sell limit reached "
                    f"({self.sell_counter_manager.get_counter()}/{self.sells_per_day_limit})"
                )
                logger.error(
                    "Sell order aborted to prevent Good Faith Violation"
                )
                return OrderResult(
                    order_id="",
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    expected_price=expected_price,
                    actual_price=0.0,
                    status=OrderStatus.REJECTED,
                    filled_quantity=0,
                    timestamp=datetime.now(),
                    error_message="Daily sell limit reached - blocked to prevent GFV"
                )
            
            # Increment and persist counter BEFORE executing sell
            try:
                new_count = self.sell_counter_manager.increment_and_persist()
                logger.info(f"✓ Sell counter incremented and persisted: {new_count}")
            except RuntimeError as error:
                logger.error(f"Sell counter persistence FAILED: {error}")
                logger.error("ABORTING SELL - order will NOT be placed")
                return OrderResult(
                    order_id="",
                    symbol=symbol,
                    action=action,
                    quantity=quantity,
                    expected_price=expected_price,
                    actual_price=0.0,
                    status=OrderStatus.REJECTED,
                    filled_quantity=0,
                    timestamp=datetime.now(),
                    error_message=f"Sell counter persistence failed: {error}"
                )
        
        # Log order placement
        if self.logger:
            self.logger.log_order_placed(
                order_id="pending",
                action=action,
                symbol=symbol,
                quantity=quantity,
                state=current_state
            )
        
        # Place the order
        order_id, error = self.place_order(symbol, action, quantity)
        
        if not order_id:
            # Order placement failed
            result = OrderResult(
                order_id="",
                symbol=symbol,
                action=action,
                quantity=quantity,
                expected_price=expected_price,
                actual_price=0.0,
                status=OrderStatus.REJECTED,
                filled_quantity=0,
                timestamp=datetime.now(),
                error_message=error or "Failed to place order"
            )
            
            if self.logger:
                self.logger.log_order_rejected(
                    order_id="N/A",
                    symbol=symbol,
                    reason=error or "Failed to place order",
                    state=current_state
                )
            
            return result
        
        # Log the actual order ID
        if self.logger:
            self.logger.log_order_placed(
                order_id=order_id,
                action=action,
                symbol=symbol,
                quantity=quantity,
                state=current_state
            )
        
        # Wait for fill
        return self.wait_for_fill(
            order_id=order_id,
            symbol=symbol,
            action=action,
            quantity=quantity,
            expected_price=expected_price,
            current_state=current_state
        )
    
    def execute_swap(
        self,
        sell_symbol: str,
        sell_quantity: int,
        buy_symbol: str,
        current_price_sell: float,
        current_price_buy: float,
        current_state: str = ""
    ) -> Tuple[Optional[OrderResult], Optional[OrderResult]]:
        """
        Execute a complete swap: sell one stock, buy another.
        
        This method executes the two-leg swap sequence:
            1. Check sell counter - ABORT if at limit
            2. Increment and persist sell counter
            3. SELL all shares of current stock
            4. Wait for sell to fill completely
            5. Get updated buying power
            6. Calculate shares to buy
            7. BUY as many shares as possible of new stock
            8. Wait for buy to fill completely
        
        The sell MUST complete before the buy is initiated.
        The sell counter is incremented and persisted BEFORE the sell executes
        to prevent Good Faith Violations in case of app crash.
        
        Args:
            sell_symbol: Symbol to sell
            sell_quantity: Shares to sell (usually all)
            buy_symbol: Symbol to buy
            current_price_sell: Expected sell price
            current_price_buy: Expected buy price
            current_state: Current state for logging
        
        Returns:
            Tuple of (sell_result, buy_result)
            Returns (None, None) if sell not allowed (limit reached)
            buy_result is None if sell failed
        """
        logger.info(f"🔄 Executing swap: SELL {sell_quantity} {sell_symbol} -> BUY {buy_symbol}")
        
        # Step 1: Check if sell is allowed (under daily limit)
        if self.sell_counter_manager:
            if not self.sell_counter_manager.can_sell(self.sells_per_day_limit):
                logger.error(
                    f"🚨 SELL BLOCKED: Daily sell limit reached "
                    f"({self.sell_counter_manager.get_counter()}/{self.sells_per_day_limit})"
                )
                logger.error(
                    "Attempting to sell would risk Good Faith Violation. "
                    "Swap aborted - no orders placed."
                )
                return None, None
            
            # Step 2: Increment and persist counter BEFORE executing sell
            try:
                new_count = self.sell_counter_manager.increment_and_persist()
                logger.info(f"✓ Sell counter incremented and persisted: {new_count}")
            except RuntimeError as error:
                logger.error(f"Sell counter persistence FAILED: {error}")
                logger.error("ABORTING SWAP - sell order will NOT be placed")
                return None, None
        
        # Step 3: Execute SELL order (counter already incremented, so use internal method)
        sell_result = self._place_and_wait_internal(
            symbol=sell_symbol,
            action="SELL",
            quantity=sell_quantity,
            expected_price=current_price_sell,
            current_state=current_state,
            skip_counter_check=True  # Already checked and incremented above
        )
        
        # Check if sell was successful
        if not sell_result.is_filled:
            logger.error(f"SELL failed: {sell_result.error_message}")
            return sell_result, None
        
        logger.info(f"✓ SELL complete: {sell_result.filled_quantity} @ ${sell_result.actual_price:.2f}")
        
        # Step 2: Get updated buying power
        # Small delay to let balances update
        time.sleep(0.5)
        available_cash = self.get_buying_power()
        
        if available_cash <= 0:
            logger.error("No buying power available after sell")
            return sell_result, None
        
        # Step 3: Calculate shares to buy
        shares_to_buy = self.calculate_shares_to_buy(current_price_buy, available_cash)
        
        if shares_to_buy <= 0:
            logger.error(f"Cannot afford any shares of {buy_symbol} (price ${current_price_buy:.2f}, cash ${available_cash:.2f})")
            return sell_result, None
        
        logger.info(f"Buying {shares_to_buy} shares of {buy_symbol} (${available_cash:.2f} / ${current_price_buy:.2f})")
        
        # Step 4: Execute BUY order
        buy_result = self.place_and_wait_for_fill(
            symbol=buy_symbol,
            action="BUY",
            quantity=shares_to_buy,
            expected_price=current_price_buy,
            current_state=current_state
        )
        
        if buy_result.is_filled:
            logger.info(f"✓ BUY complete: {buy_result.filled_quantity} @ ${buy_result.actual_price:.2f}")
        else:
            logger.error(f"BUY failed: {buy_result.error_message}")
        
        return sell_result, buy_result
    
    def execute_initial_buy(
        self,
        symbol: str,
        current_price: float,
        current_state: str = ""
    ) -> Optional[OrderResult]:
        """
        Execute the initial buy when starting from cash.
        
        This calculates how many shares to buy based on available
        buying power and executes the order.
        
        Args:
            symbol: Symbol to buy
            current_price: Current stock price
            current_state: Current state for logging
        
        Returns:
            OrderResult with fill details, or None on failure
        """
        # Get available buying power
        available_cash = self.get_buying_power()
        
        if available_cash <= 0:
            logger.error("No buying power available")
            return None
        
        # Calculate shares
        shares_to_buy = self.calculate_shares_to_buy(current_price, available_cash)
        
        if shares_to_buy <= 0:
            logger.error(f"Cannot afford any shares of {symbol}")
            return None
        
        logger.info(f"Initial buy: {shares_to_buy} shares of {symbol} (${available_cash:.2f} / ${current_price:.2f})")
        
        # Execute the buy
        return self.place_and_wait_for_fill(
            symbol=symbol,
            action="BUY",
            quantity=shares_to_buy,
            expected_price=current_price,
            current_state=current_state
        )
    
    def get_position_quantity(self, symbol: str) -> int:
        """
        Get the current position quantity for a symbol.
        
        Args:
            symbol: Stock symbol
        
        Returns:
            Number of shares held (0 if no position)
        """
        try:
            positions = self.api.account.get_positions(self.account_id)
            
            for position in positions.get('Positions', []):
                if position.get('Symbol') == symbol:
                    return int(position.get('Quantity', 0))
            
            return 0
            
        except Exception as exception:
            print(f"ERROR getting position: {exception}")
            return 0

