"""
Live Pairs Trading Algorithm - Main Orchestrator

This is the main entry point for the live pairs trading algorithm.
It orchestrates all components:
    - State machine for algorithm flow control
    - Price tracker for quotes and moving average
    - Order executor for trade execution
    - Performance tracker for metrics and logging
    - Reconciler for position verification
    - Observability for Prometheus/Loki/CSV

SAFETY FEATURES:
    - Environment controlled by settings.yaml (default: sim)
    - ENFORCE_ONE_TRADE_PER_DAY flag prevents more than 1 swap per day
    - Production mode shows prominent warning banner

Usage:
    python -m live_trading.live_pairs_trader
    
    Or:
    
    from live_trading.live_pairs_trader import LivePairsTrader
    trader = LivePairsTrader()
    trader.run()

Configuration:
    All settings are loaded from settings.yaml in this directory.
    Account credentials come from .env at project root.
"""

import sys
import time
import yaml
from pathlib import Path
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tradestation.api import TradeStationAPI

from .state_machine import (
    StateMachine, TradingState, StockHeld, InvalidStateTransition
)
from .price_tracker import PriceTracker, PriceSnapshot
from .order_executor import OrderExecutor, OrderResult, OrderStatus
from .performance_tracker import PerformanceTracker, SlippageTracker
from .reconciliation import Reconciler, RecoveryAction
from .observability import Observability


# Eastern timezone for market hours
EASTERN_TZ = ZoneInfo("America/New_York")

# Market hours (9:30 AM - 4:00 PM ET)
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)



def load_settings() -> dict:
    """Load settings from settings.yaml."""
    settings_path = Path(__file__).parent / "settings.yaml"
    
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")
    
    with open(settings_path, 'r') as settings_file:
        return yaml.safe_load(settings_file)


class LivePairsTrader:
    """
    Main orchestrator for the live pairs trading algorithm.
    
    This class coordinates all components to implement the pairs trading
    strategy in real-time. It manages:
        - State transitions based on market conditions
        - Quote polling and MA updates
        - Order execution when triggers are met
        - Performance tracking and logging
        - Position reconciliation
    
    The algorithm flow:
        1. Bootstrap MA from historical data
        2. Recover state from API (positions, orders)
        3. Enter main loop:
            a. Poll quotes
            b. Update MA (every minute)
            c. Check market hours
            d. Execute state-specific logic
            e. Periodic reconciliation
    
    Attributes:
        settings: Configuration dictionary from settings.yaml
        api: TradeStation API instance (HARDCODED to simulation)
        state_machine: Algorithm state manager
        price_tracker: Quote and MA handler
        order_executor: Trade execution handler
        performance_tracker: Return calculation
        reconciler: Position verification
        observability: Metrics and logging
    """
    
    def __init__(self):
        """Initialize the live pairs trader."""
        print("=" * 60)
        print("Live Pairs Trading Algorithm")
        print("=" * 60)
        
        # Load configuration
        self.settings = load_settings()
        
        # Extract key settings
        self.ticker_a = self.settings['ticker_a']
        self.ticker_b = self.settings['ticker_b']
        self.trigger_percent = self.settings['trigger_percent']
        self.ma_window = self.settings['moving_average_window_minutes']
        self.trades_per_day_limit = self.settings['trades_per_day_limit']
        self.poll_interval = self.settings['poll_interval_seconds']
        self.reconciliation_interval = self.settings['reconciliation_interval_seconds']
        self.swap_cutoff_minutes = self.settings['swap_cutoff_minutes_before_close']
        self.retry_attempts = self.settings['retry_attempts']
        self.retry_backoff = self.settings['retry_backoff_seconds']
        self.performance_timeframes = self.settings['performance_timeframes']
        self.slippage_settings = self.settings.get('slippage', {})
        self.enforce_one_trade_per_day = self.settings.get('enforce_one_trade_per_day', True)
        self.allocated_cash = self.settings.get('allocated_cash', 0)
        
        # Get environment setting
        self.environment = self.settings.get('environment', 'sim')
        
        # Validate environment
        if self.environment not in ('sim', 'prod'):
            raise ValueError(f"Invalid environment '{self.environment}'. Must be 'sim' or 'prod'.")
        
        print(f"\nConfiguration:")
        print(f"  Environment: {self.environment.upper()}")
        print(f"  Ticker A: {self.ticker_a}")
        print(f"  Ticker B: {self.ticker_b}")
        print(f"  Trigger: {self.trigger_percent}%")
        print(f"  MA Window: {self.ma_window} minutes")
        print(f"  Daily Trade Limit: {self.trades_per_day_limit}")
        print(f"  Swap Cutoff: {self.swap_cutoff_minutes} minutes before close")
        print(f"  Enforce 1 Trade/Day: {self.enforce_one_trade_per_day}")
        if self.allocated_cash > 0:
            print(f"  Allocated Cash: ${self.allocated_cash:,.2f}")
        else:
            print(f"  Allocated Cash: Full account balance")
        
        # =====================================================================
        # Environment Selection
        # =====================================================================
        if self.environment == 'prod':
            print("\n" + "=" * 60)
            print("⚠️  WARNING: PRODUCTION MODE - REAL MONEY AT RISK ⚠️")
            print("=" * 60)
            print("You are about to run the algorithm with REAL money.")
            print("Make sure you understand the risks and have tested in sim mode.")
            print("=" * 60)
        else:
            print("\n✓ SIMULATION MODE - No real money at risk")
        
        self.api = TradeStationAPI(self.environment)
        self.account_id = self.api.config.account_id
        print(f"  Account: {self.account_id}")
        
        # Initialize observability (metrics, logging, CSV)
        self.observability = Observability(
            ticker_a=self.ticker_a,
            ticker_b=self.ticker_b
        )
        
        # Initialize state machine with logging callback
        self.state_machine = StateMachine(
            initial_state=TradingState.WARMING_UP,
            on_transition=self._on_state_transition
        )
        
        # Initialize price tracker
        self.price_tracker = PriceTracker(
            api=self.api,
            ticker_a=self.ticker_a,
            ticker_b=self.ticker_b,
            ma_window_minutes=self.ma_window,
            trigger_percent=self.trigger_percent
        )
        
        # Initialize order executor
        self.order_executor = OrderExecutor(
            api=self.api,
            account_id=self.account_id,
            logger=self.observability.logger,
            allocated_cash=self.allocated_cash
        )
        
        # Initialize performance tracker
        self.performance_tracker = PerformanceTracker(
            timeframes=self.performance_timeframes
        )
        
        # Initialize slippage tracker
        self.slippage_tracker = SlippageTracker()
        
        # Initialize reconciler
        self.reconciler = Reconciler(
            api=self.api,
            account_id=self.account_id,
            ticker_a=self.ticker_a,
            ticker_b=self.ticker_b,
            logger=self.observability.logger,
            allocated_cash=self.allocated_cash
        )
        
        # Track last reconciliation time
        self._last_reconciliation = datetime.now()
        
        # Track current trading day for daily limit reset
        self._current_trading_day: Optional[str] = None
        
        # Track shares held (for sell quantity)
        self._shares_held: int = 0
        
        # Track last heartbeat minute for once-per-minute logging
        self._last_heartbeat_minute: Optional[int] = None
        
        # Track market open/close state for event logging
        self._was_market_open: bool = False
        
        # Track swap cutoff state for event logging
        self._was_past_cutoff: bool = False
        
        print("\nInitialization complete.")
    
    def _on_state_transition(
        self,
        from_state: TradingState,
        to_state: TradingState,
        reason: str
    ) -> None:
        """Callback invoked on every state transition."""
        self.observability.logger.log_state_change(
            from_state=from_state.name if from_state else "None",
            to_state=to_state.name,
            reason=reason
        )
        self.observability.metrics.record_state(to_state.value)
    
    def _is_market_open(self) -> bool:
        """Check if market is currently open (9:30 AM - 4:00 PM ET)."""
        now_et = datetime.now(EASTERN_TZ)
        current_time = now_et.time()
        return MARKET_OPEN <= current_time < MARKET_CLOSE
    
    def _is_past_swap_cutoff(self) -> bool:
        """Check if we're past the swap cutoff time."""
        now_et = datetime.now(EASTERN_TZ)
        # Calculate cutoff time
        cutoff_hour = 15  # 3 PM
        cutoff_minute = 60 - self.swap_cutoff_minutes
        if cutoff_minute >= 60:
            cutoff_hour = 16
            cutoff_minute = cutoff_minute - 60
        
        cutoff_time = dt_time(cutoff_hour, cutoff_minute)
        return now_et.time() >= cutoff_time
    
    def _minutes_until_market_close(self) -> int:
        """
        Calculate minutes until market close.
        
        Returns:
            Positive number if market is open (minutes until 4:00 PM ET)
            Negative number if market is closed (minutes since close or until open)
        """
        now_et = datetime.now(EASTERN_TZ)
        current_time = now_et.time()
        
        # If we're between open and close
        if MARKET_OPEN <= current_time < MARKET_CLOSE:
            # Calculate minutes until 4:00 PM
            now_minutes = current_time.hour * 60 + current_time.minute
            close_minutes = MARKET_CLOSE.hour * 60 + MARKET_CLOSE.minute
            return close_minutes - now_minutes
        else:
            # Market closed - return negative
            if current_time >= MARKET_CLOSE:
                # After close - minutes since close
                now_minutes = current_time.hour * 60 + current_time.minute
                close_minutes = MARKET_CLOSE.hour * 60 + MARKET_CLOSE.minute
                return -(now_minutes - close_minutes)
            else:
                # Before open - minutes until open (negative)
                now_minutes = current_time.hour * 60 + current_time.minute
                open_minutes = MARKET_OPEN.hour * 60 + MARKET_OPEN.minute
                return -(open_minutes - now_minutes)
    
    def _check_new_trading_day(self) -> bool:
        """Check if we've entered a new trading day (reset daily counters)."""
        now_et = datetime.now(EASTERN_TZ)
        today = now_et.strftime("%Y-%m-%d")
        
        if self._current_trading_day != today:
            old_day = self._current_trading_day
            self._current_trading_day = today
            
            if old_day is not None:
                # Reset daily trade counter
                self.state_machine.reset_trades_today(today)
                self.observability.logger.log_daily_reset(today, self.state_machine.state_name)
                
                # Transition out of HOLDING_DAILY_LIMIT if applicable
                if self.state_machine.state == TradingState.HOLDING_DAILY_LIMIT:
                    self.state_machine.transition_to(
                        TradingState.HOLDING_WAITING,
                        reason="New trading day"
                    )
                
                return True
        
        return False
    
    def _check_sufficient_buying_power(self, min_price: float) -> bool:
        """
        Check if we have sufficient buying power to buy at least 1 share.
        
        This is a recoverable error - we log it but continue running.
        """
        buying_power = self.reconciler.get_buying_power()
        
        if buying_power < min_price:
            self.observability.logger.log_insufficient_buying_power(
                required=min_price,
                available=buying_power,
                state=self.state_machine.state_name
            )
            return False
        
        return True
    
    def _bootstrap_moving_average(self) -> bool:
        """Bootstrap the moving average from historical data."""
        print("\n" + "=" * 60)
        print("Bootstrapping Moving Average...")
        print("=" * 60)
        
        success = self.price_tracker.bootstrap_moving_average()
        
        if success:
            print("MA bootstrap successful!")
            return True
        else:
            print("ERROR: MA bootstrap failed!")
            return False
    
    def _recover_state(self) -> bool:
        """Recover state from API on startup."""
        print("\n" + "=" * 60)
        print("Recovering State from API...")
        print("=" * 60)
        
        result = self.reconciler.check_state()
        
        print(f"  Recommended state: {result.recommended_state.name}")
        print(f"  Current stock: {result.current_stock.value}")
        print(f"  Action needed: {result.action_needed.name}")
        
        if result.error_message:
            print(f"  ERROR: {result.error_message}")
        
        # Update state machine based on recovery result
        if result.action_needed == RecoveryAction.ERROR:
            self.state_machine.force_error_state(result.error_message or "Recovery error")
            return False
        
        # Set the appropriate state
        if result.recommended_state == TradingState.HOLDING_WAITING:
            self.state_machine.transition_to(
                TradingState.HOLDING_WAITING,
                reason=f"Recovery: holding {result.current_stock.value}"
            )
            self.state_machine.set_current_stock(result.current_stock)
            
            # Get current position quantity
            stock_name = "ticker_a" if result.current_stock == StockHeld.TICKER_A else "ticker_b"
            self._shares_held = self.reconciler.get_position_quantity(stock_name)
            print(f"  Shares held: {self._shares_held}")
            
        elif result.recommended_state == TradingState.CASH:
            self.state_machine.transition_to(
                TradingState.CASH,
                reason="Recovery: no position (cash)"
            )
            
        elif result.recommended_state in (TradingState.PENDING_BUY, TradingState.PENDING_SELL):
            # There's a pending order - we need to wait for it
            self.state_machine.transition_to(
                result.recommended_state,
                reason="Recovery: pending order detected"
            )
            
            if result.pending_orders:
                order = result.pending_orders[0]
                self.state_machine.set_pending_order(order.order_id)
                print(f"  Pending order: {order.order_id} ({order.action} {order.symbol})")
        
        # Initialize performance tracking period
        portfolio_value = self.reconciler.get_portfolio_value()
        snapshot = self.price_tracker.get_price_snapshot()
        
        if snapshot:
            self.performance_tracker.start_new_period(
                portfolio_value=portfolio_value,
                ticker_a_price=snapshot.ticker_a_quote.last,
                ticker_b_price=snapshot.ticker_b_quote.last
            )
        
        return True
    
    def _execute_initial_buy(self, snapshot: PriceSnapshot) -> bool:
        """Execute the initial buy when starting from cash."""
        # Determine which stock to buy (undervalued one)
        stock_to_buy = self.price_tracker.get_undervalued_stock(snapshot)
        
        if stock_to_buy == "ticker_a":
            symbol = self.ticker_a
            price = snapshot.ticker_a_quote.last
            stock_enum = StockHeld.TICKER_A
        else:
            symbol = self.ticker_b
            price = snapshot.ticker_b_quote.last
            stock_enum = StockHeld.TICKER_B
        
        print(f"\nInitiating initial buy: {symbol} (undervalued)")
        
        # Transition to PENDING_BUY
        self.state_machine.transition_to(
            TradingState.PENDING_BUY,
            reason=f"Initial buy of {symbol}"
        )
        
        # Record portfolio value at start of trade
        portfolio_value_before = self.reconciler.get_portfolio_value()
        self.state_machine.set_portfolio_value_at_trade_start(portfolio_value_before)
        
        # Execute the buy
        result = self.order_executor.execute_initial_buy(
            symbol=symbol,
            current_price=price,
            current_state=self.state_machine.state_name
        )
        
        if result and result.is_filled:
            # Success - transition to HOLDING_WAITING
            self.state_machine.transition_to(
                TradingState.HOLDING_WAITING,
                reason=f"Initial buy filled: {result.filled_quantity} {symbol}"
            )
            self.state_machine.set_current_stock(stock_enum)
            self._shares_held = result.filled_quantity
            
            # Record trade in performance tracker
            portfolio_value_after = self.reconciler.get_portfolio_value()
            
            trade_record = self.performance_tracker.record_trade(
                trade_id=self.observability.logger.generate_trade_id(),
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                ticker_a_price_before=snapshot.ticker_a_quote.last,
                ticker_b_price_before=snapshot.ticker_b_quote.last,
                ticker_a_price_after=snapshot.ticker_a_quote.last,
                ticker_b_price_after=snapshot.ticker_b_quote.last
            )
            
            # Record slippage
            expected_slippage = self.price_tracker.calculate_expected_slippage(
                result.filled_quantity, self.slippage_settings
            ) * 100
            self.slippage_tracker.record_slippage(
                trade_id=trade_record.trade_id,
                symbol=symbol,
                action="BUY",
                expected_slippage_pct=expected_slippage,
                actual_slippage_pct=result.slippage_percent,
                shares=result.filled_quantity,
                price=price
            )
            
            # Log and emit metrics
            self._emit_trade_metrics(trade_record, portfolio_value_after)
            
            # Log to CSV
            metrics = self.performance_tracker.get_metrics(portfolio_value_after)
            self.observability.csv_logger.log_trade(
                trade_id=trade_record.trade_id,
                action="BUY",
                symbol=symbol,
                quantity=result.filled_quantity,
                expected_price=price,
                actual_price=result.actual_price,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                period_return_pct=trade_record.period_return_percent,
                compound_return_all_time=metrics['return_multiplier_all_time'],
                algo_return_7d=metrics.get('return_multiplier_7d', 1.0),
                algo_return_60d=metrics.get('return_multiplier_60d', 1.0),
                relative_perf_7d=metrics.get('relative_perf_7d', 1.0),
                relative_perf_60d=metrics.get('relative_perf_60d', 1.0),
                relative_perf_all_time=metrics.get('relative_perf_all_time', 1.0)
            )
            
            # Count this trade toward daily limit
            self.state_machine.increment_trades_today()
            self.observability.metrics.record_trades_today(
                self.state_machine.data.trades_today
            )
            
            return True
        
        else:
            # Buy failed - this is CRITICAL
            self.state_machine.force_error_state(
                f"Initial buy failed: {result.error_message if result else 'No result'}"
            )
            return False
    
    def _execute_swap(self, snapshot: PriceSnapshot, direction: str) -> bool:
        """Execute a swap from one stock to the other."""
        # Determine sell and buy symbols
        if direction == "to_ticker_a":
            sell_symbol = self.ticker_b
            buy_symbol = self.ticker_a
            sell_price = snapshot.ticker_b_quote.last
            buy_price = snapshot.ticker_a_quote.last
            new_stock = StockHeld.TICKER_A
        else:
            sell_symbol = self.ticker_a
            buy_symbol = self.ticker_b
            sell_price = snapshot.ticker_a_quote.last
            buy_price = snapshot.ticker_b_quote.last
            new_stock = StockHeld.TICKER_B
        
        print(f"\nExecuting swap: {sell_symbol} -> {buy_symbol}")
        
        # Log trigger
        ratio, ratio_ma = self.price_tracker.get_current_ratio_ma()
        self.observability.logger.log_trigger_met(
            ratio=ratio,
            ratio_ma=ratio_ma,
            deviation_pct=snapshot.deviation_percent,
            direction=direction,
            state=self.state_machine.state_name
        )
        
        # Transition to HOLDING_TRIGGERED then PENDING_SELL
        self.state_machine.transition_to(
            TradingState.HOLDING_TRIGGERED,
            reason=f"Swap trigger met: {direction}"
        )
        self.state_machine.transition_to(
            TradingState.PENDING_SELL,
            reason=f"Initiating sell of {sell_symbol}"
        )
        
        # Record portfolio value at start
        portfolio_value_before = self.reconciler.get_portfolio_value()
        self.state_machine.set_portfolio_value_at_trade_start(portfolio_value_before)
        
        # Get period start values for performance calculation
        period_start = self.performance_tracker.get_period_start_prices()
        ticker_a_price_before = period_start.get('ticker_a', snapshot.ticker_a_quote.last)
        ticker_b_price_before = period_start.get('ticker_b', snapshot.ticker_b_quote.last)
        
        # Execute the swap (sell then buy)
        sell_result, buy_result = self.order_executor.execute_swap(
            sell_symbol=sell_symbol,
            sell_quantity=self._shares_held,
            buy_symbol=buy_symbol,
            current_price_sell=sell_price,
            current_price_buy=buy_price,
            current_state=self.state_machine.state_name
        )
        
        # Handle sell completion
        if sell_result and sell_result.is_filled:
            self.state_machine.transition_to(
                TradingState.CASH,
                reason=f"Sell complete: {sell_result.filled_quantity} {sell_symbol}"
            )
            self.state_machine.set_current_stock(StockHeld.NONE)
        else:
            # Sell failed - CRITICAL
            self.state_machine.force_error_state(
                f"Sell failed: {sell_result.error_message if sell_result else 'No result'}"
            )
            return False
        
        # Handle buy
        if not buy_result:
            # Buy was not attempted (maybe insufficient funds)
            self.state_machine.force_error_state("Buy not executed after sell")
            return False
        
        self.state_machine.transition_to(
            TradingState.PENDING_BUY,
            reason=f"Initiating buy of {buy_symbol}"
        )
        
        if buy_result.is_filled:
            # Success - transition to HOLDING_WAITING
            self.state_machine.transition_to(
                TradingState.HOLDING_WAITING,
                reason=f"Buy filled: {buy_result.filled_quantity} {buy_symbol}"
            )
            self.state_machine.set_current_stock(new_stock)
            self._shares_held = buy_result.filled_quantity
            
            # Record trade in performance tracker
            portfolio_value_after = self.reconciler.get_portfolio_value()
            
            trade_id = self.observability.logger.generate_trade_id()
            trade_record = self.performance_tracker.record_trade(
                trade_id=trade_id,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                ticker_a_price_before=ticker_a_price_before,
                ticker_b_price_before=ticker_b_price_before,
                ticker_a_price_after=snapshot.ticker_a_quote.last,
                ticker_b_price_after=snapshot.ticker_b_quote.last
            )
            
            # Record slippage for both legs
            for result, action, symbol, price in [
                (sell_result, "SELL", sell_symbol, sell_price),
                (buy_result, "BUY", buy_symbol, buy_price)
            ]:
                expected_slippage = self.price_tracker.calculate_expected_slippage(
                    result.filled_quantity, self.slippage_settings
                ) * 100
                self.slippage_tracker.record_slippage(
                    trade_id=trade_id,
                    symbol=symbol,
                    action=action,
                    expected_slippage_pct=expected_slippage,
                    actual_slippage_pct=result.slippage_percent,
                    shares=result.filled_quantity,
                    price=price
                )
                self.observability.metrics.record_slippage(expected_slippage, result.slippage_percent)
            
            # Log trade completion
            self.observability.logger.log_trade_complete(
                trade_id=trade_id,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                period_return_pct=trade_record.period_return_percent,
                compound_return_all_time=self.performance_tracker.get_metrics()['return_multiplier_all_time'],
                state=self.state_machine.state_name
            )
            
            # Log flip completion (swap-specific)
            self.observability.logger.log_flip_complete(
                from_stock=sell_symbol,
                to_stock=buy_symbol,
                deviation_at_flip=snapshot.deviation_percent,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                state=self.state_machine.state_name
            )
            
            # Emit metrics
            self._emit_trade_metrics(trade_record, portfolio_value_after)
            
            # Log to CSV
            metrics = self.performance_tracker.get_metrics(portfolio_value_after)
            self.observability.csv_logger.log_trade(
                trade_id=trade_id,
                action=f"SWAP_{sell_symbol}_TO_{buy_symbol}",
                symbol=buy_symbol,
                quantity=buy_result.filled_quantity,
                expected_price=buy_price,
                actual_price=buy_result.actual_price,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after,
                period_return_pct=trade_record.period_return_percent,
                compound_return_all_time=metrics['return_multiplier_all_time'],
                algo_return_7d=metrics.get('return_multiplier_7d', 1.0),
                algo_return_60d=metrics.get('return_multiplier_60d', 1.0),
                relative_perf_7d=metrics.get('relative_perf_7d', 1.0),
                relative_perf_60d=metrics.get('relative_perf_60d', 1.0),
                relative_perf_all_time=metrics.get('relative_perf_all_time', 1.0)
            )
            
            # Print performance table
            print("\n" + self.performance_tracker.format_metrics_table(metrics))
            
            # Count trade toward daily limit
            self.state_machine.increment_trades_today()
            self.observability.metrics.record_trades_today(
                self.state_machine.data.trades_today
            )
            
            # Record flip count (same as trades_today for swaps)
            self.observability.metrics.record_flips_today(
                self.state_machine.data.trades_today
            )
            
            return True
        
        else:
            # Buy failed after sell succeeded - CRITICAL
            self.state_machine.force_error_state(
                f"Buy failed after sell: {buy_result.error_message}"
            )
            return False
    
    def _emit_trade_metrics(self, trade_record, portfolio_value: float) -> None:
        """Emit metrics after a trade completes."""
        metrics = self.performance_tracker.get_metrics(portfolio_value)
        
        # Portfolio value
        self.observability.metrics.record_portfolio_value(portfolio_value)
        
        # Return multipliers
        self.observability.metrics.record_return_multipliers(
            multiplier_7d=metrics.get('return_multiplier_7d', 1.0),
            multiplier_60d=metrics.get('return_multiplier_60d', 1.0),
            multiplier_all_time=metrics.get('return_multiplier_all_time', 1.0)
        )
        
        # Relative performance
        self.observability.metrics.record_relative_performance(
            relative_7d=metrics.get('relative_perf_7d', 1.0),
            relative_60d=metrics.get('relative_perf_60d', 1.0),
            relative_all_time=metrics.get('relative_perf_all_time', 1.0)
        )
    
    def _run_main_loop(self) -> None:
        """Main trading loop."""
        print("\n" + "=" * 60)
        print("Starting Main Trading Loop")
        print("=" * 60)
        
        while True:
            try:
                # Check for new trading day
                self._check_new_trading_day()
                
                # Skip if in error state (frozen)
                if self.state_machine.is_in_error():
                    time.sleep(self.poll_interval)
                    continue
                
                # Get current price snapshot
                snapshot = self.price_tracker.get_price_snapshot()
                
                if not snapshot:
                    print("WARNING: Failed to get price snapshot")
                    time.sleep(self.poll_interval)
                    continue
                
                # Update MA if minute changed
                if self.price_tracker.check_minute_changed():
                    self.price_tracker.update_moving_average()
                
                # Record price metrics
                self.observability.metrics.record_ratio_metrics(
                    ratio=snapshot.ratio,
                    ratio_ma=snapshot.ratio_ma,
                    deviation_percent=snapshot.deviation_percent
                )
                
                # Record trigger proximity and bands
                trigger_proximity = abs(snapshot.deviation_percent) / self.trigger_percent if self.trigger_percent > 0 else 0.0
                self.observability.metrics.record_trigger_proximity(trigger_proximity)
                self.observability.metrics.record_trigger_bands(
                    upper=snapshot.ratio_ma * (1 + self.trigger_percent/100),
                    lower=snapshot.ratio_ma * (1 - self.trigger_percent/100)
                )
                
                # Record individual prices
                self.observability.metrics.record_prices(
                    ticker_a_price=snapshot.ticker_a_quote.last,
                    ticker_b_price=snapshot.ticker_b_quote.last
                )
                
                # Record which stock we're holding
                if self.state_machine.data.current_stock == StockHeld.TICKER_A:
                    holding_stock = "ticker_a"
                elif self.state_machine.data.current_stock == StockHeld.TICKER_B:
                    holding_stock = "ticker_b"
                else:
                    holding_stock = "none"
                self.observability.metrics.record_holding_indicator(holding_stock)
                
                # Check market hours and log transitions
                market_open = self._is_market_open()
                if market_open and not self._was_market_open:
                    # Market just opened
                    self.observability.logger.log_market_open(self.state_machine.state_name)
                elif not market_open and self._was_market_open:
                    # Market just closed
                    self.observability.logger.log_market_close(self.state_machine.state_name)
                self._was_market_open = market_open
                
                if not market_open:
                    time.sleep(self.poll_interval)
                    continue
                
                # Check swap cutoff and log transitions
                past_cutoff = self._is_past_swap_cutoff()
                if past_cutoff and not self._was_past_cutoff:
                    # Just entered cutoff period
                    self.observability.logger.log_swap_cutoff_entered(
                        minutes_before_close=self.swap_cutoff_minutes,
                        state=self.state_machine.state_name
                    )
                self._was_past_cutoff = past_cutoff
                
                # State-specific logic
                current_state = self.state_machine.state
                
                if current_state == TradingState.CASH:
                    # In cash - execute initial buy
                    min_price = min(snapshot.ticker_a_quote.last, snapshot.ticker_b_quote.last)
                    if self._check_sufficient_buying_power(min_price):
                        self._execute_initial_buy(snapshot)
                
                elif current_state == TradingState.HOLDING_WAITING:
                    # Check if we should swap
                    current_stock = self.state_machine.data.current_stock
                    stock_name = "ticker_a" if current_stock == StockHeld.TICKER_A else "ticker_b"
                    
                    should_swap, direction = self.price_tracker.should_swap(snapshot, stock_name)
                    
                    # Check conditions for swap
                    trades_today = self.state_machine.data.trades_today
                    
                    # Check #1: Configurable daily limit from settings.yaml
                    config_allows_trade = trades_today < self.trades_per_day_limit or self.trades_per_day_limit == 0
                    
                    # Check #2: Secondary safety check from settings
                    # This is a failsafe to prevent runaway trading
                    safety_allows_trade = not self.enforce_one_trade_per_day or trades_today < 1

                    if config_allows_trade and not safety_allows_trade:
                        self.state_machine.force_error_state(
                            f"Safety violation: Primary limit allowed trade but safety blocked it. "
                            f"trades_per_day_limit={self.trades_per_day_limit} is too high! "
                            f"trades_today={trades_today}"
                        )
                        return
                    
                    # Both checks must pass
                    can_trade = config_allows_trade and safety_allows_trade
                    
                    if should_swap and can_trade and not past_cutoff:
                        self._execute_swap(snapshot, direction)
                    elif not can_trade and current_state != TradingState.HOLDING_DAILY_LIMIT:
                        self.state_machine.transition_to(
                            TradingState.HOLDING_DAILY_LIMIT,
                            reason=f"Daily limit reached ({trades_today} trades)"
                        )
                
                elif current_state == TradingState.HOLDING_DAILY_LIMIT:
                    # Just wait - daily reset will transition us back
                    pass
                
                elif current_state in (TradingState.PENDING_BUY, TradingState.PENDING_SELL):
                    # Check order status
                    order_id = self.state_machine.data.pending_order_id
                    if order_id:
                        status, order_data = self.order_executor.get_order_status(order_id)
                        
                        if status == OrderStatus.FILLED:
                            # Order filled - reconcile and update state
                            result = self.reconciler.check_state()
                            if result.recommended_state == TradingState.HOLDING_WAITING:
                                self.state_machine.transition_to(
                                    TradingState.HOLDING_WAITING,
                                    reason="Pending order filled"
                                )
                                self.state_machine.set_current_stock(result.current_stock)
                            elif result.recommended_state == TradingState.CASH:
                                self.state_machine.transition_to(
                                    TradingState.CASH,
                                    reason="Pending order filled (now in cash)"
                                )
                
                # Periodic reconciliation
                now = datetime.now()
                if (now - self._last_reconciliation).total_seconds() >= self.reconciliation_interval:
                    self._last_reconciliation = now
                    result = self.reconciler.check_state()
                    
                    if not result.is_consistent:
                        self.observability.logger.log_position_mismatch(
                            expected=self.state_machine.state_name,
                            actual=result.recommended_state.name,
                            state=self.state_machine.state_name
                        )
                        self.state_machine.force_error_state(
                            f"Reconciliation mismatch: {result.error_message}"
                        )
                
                # Record current portfolio value and cash
                portfolio_value = self.reconciler.get_portfolio_value()
                cash_available = self.reconciler.get_buying_power()
                self.observability.metrics.record_portfolio_value(portfolio_value)
                self.observability.metrics.record_cash_available(cash_available)
                
                # Calculate and record minutes until close
                minutes_until_close = self._minutes_until_market_close()
                self.observability.metrics.record_minutes_until_close(minutes_until_close)
                
                # Emit heartbeat log once per minute
                current_minute = datetime.now().minute
                if current_minute != self._last_heartbeat_minute:
                    self._last_heartbeat_minute = current_minute
                    
                    # Emit heartbeat with all vital signs
                    self.observability.logger.log_heartbeat(
                        state=self.state_machine.state_name,
                        holding_stock=holding_stock,
                        ratio=snapshot.ratio,
                        ratio_ma=snapshot.ratio_ma,
                        deviation_pct=snapshot.deviation_percent,
                        trigger_proximity=trigger_proximity,
                        portfolio_value=portfolio_value,
                        trades_today=self.state_machine.data.trades_today,
                        minutes_until_close=minutes_until_close
                    )
                
                # Sleep until next poll
                time.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                print("\n\nShutting down...")
                break
            
            except Exception as exception:
                print(f"\nERROR in main loop: {exception}")
                self.observability.logger.error(
                    f"Main loop error: {exception}",
                    state=self.state_machine.state_name
                )
                # Don't crash on single errors - log and continue
                time.sleep(self.poll_interval)
    
    def run(self) -> None:
        """Run the live pairs trading algorithm."""
        try:
            # Step 1: Bootstrap moving average
            if not self._bootstrap_moving_average():
                print("FATAL: Failed to bootstrap MA. Exiting.")
                return
            
            # Step 2: Recover state from API
            if not self._recover_state():
                print("FATAL: Failed to recover state. Exiting.")
                return
            
            # Step 3: Enter main loop
            self._run_main_loop()
            
        finally:
            # Cleanup
            print("\nFlushing metrics and logs...")
            self.observability.flush()
            self.observability.shutdown()
            print("Shutdown complete.")


def main():
    """Main entry point."""
    trader = LivePairsTrader()
    trader.run()


if __name__ == "__main__":
    main()

