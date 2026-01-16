"""
Observability Module - Prometheus Metrics and Loki Logging

This module provides centralized observability infrastructure for the live
pairs trading algorithm. It configures:
    - Prometheus metrics for Grafana dashboards
    - Structured Loki logs for querying and alerting
    - CSV trade logging for permanent record keeping

All metrics and logs are emitted to Grafana Cloud via OpenTelemetry (OTLP).

CRITICAL Logs:
    The following log patterns indicate errors that FREEZE the application
    and require manual intervention. Set up alerts on these:
    
    - ORDER_REJECTED: Order was rejected by TradeStation
    - POSITION_MISMATCH: Internal state doesn't match API positions
    - API_FAILURE_EXHAUSTED: Retries exhausted on API calls
    - INSUFFICIENT_BUYING_POWER: Can't afford minimum 1 share
    - INVALID_STATE_TRANSITION: State machine logic error
    - UNEXPECTED_POSITION: Holding stock not in configured pair
"""

import os
import logging
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any
from enum import Enum
from dotenv import load_dotenv

# OpenTelemetry imports for Prometheus metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# OpenTelemetry imports for Loki logs
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

# Import centralized logger
from .logger import logger


class AlertLevel(Enum):
    """Log levels that determine alert severity."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"  # These freeze the app


class TradingMetrics:
    """
    Prometheus metrics for the pairs trading algorithm.
    
    This class creates and manages all Prometheus metrics emitted by the
    algorithm. Metrics are sent to Grafana Cloud via OTLP exporter.
    
    Metrics Categories:
        - Portfolio: Value, cash available
        - Performance: Return multipliers by timeframe
        - Trading: State, ratio, deviation, trades today
        - Slippage: Expected vs actual comparison
    """
    
    def __init__(self, meter_provider: MeterProvider):
        """
        Initialize all trading metrics.
        
        Args:
            meter_provider: OpenTelemetry MeterProvider for creating metrics
        """
        # Get a meter (instrument factory) for our service
        self.meter = meter_provider.get_meter("PairTraderPro")
        
        # =====================================================================
        # Portfolio Metrics
        # =====================================================================
        
        # Current total portfolio value in dollars
        self.portfolio_value = self.meter.create_gauge(
            name="pairs_trader_portfolio_value",
            description="Current portfolio value in dollars",
            unit="USD"
        )
        
        # Available cash/buying power
        self.cash_available = self.meter.create_gauge(
            name="pairs_trader_cash_available",
            description="Current available buying power",
            unit="USD"
        )
        
        # =====================================================================
        # Performance Metrics (Compound Return Multipliers)
        # =====================================================================
        
        # Return multiplier for 7-day window
        self.return_multiplier_7d = self.meter.create_gauge(
            name="pairs_trader_return_multiplier_7d",
            description="7-day compound return multiplier",
            unit="ratio"
        )
        
        # Return multiplier for 60-day window
        self.return_multiplier_60d = self.meter.create_gauge(
            name="pairs_trader_return_multiplier_60d",
            description="60-day compound return multiplier",
            unit="ratio"
        )
        
        # Return multiplier for all-time
        self.return_multiplier_all_time = self.meter.create_gauge(
            name="pairs_trader_return_multiplier_all_time",
            description="All-time compound return multiplier",
            unit="ratio"
        )
        
        # Relative performance vs market (7d)
        self.relative_perf_7d = self.meter.create_gauge(
            name="pairs_trader_relative_perf_7d",
            description="7-day relative performance vs market",
            unit="ratio"
        )
        
        # Relative performance vs market (60d)
        self.relative_perf_60d = self.meter.create_gauge(
            name="pairs_trader_relative_perf_60d",
            description="60-day relative performance vs market",
            unit="ratio"
        )
        
        # Relative performance vs market (all-time)
        self.relative_perf_all_time = self.meter.create_gauge(
            name="pairs_trader_relative_perf_all_time",
            description="All-time relative performance vs market",
            unit="ratio"
        )
        
        # =====================================================================
        # Trading State Metrics
        # =====================================================================
        
        # Current algorithm state (as numeric enum for graphing)
        self.state = self.meter.create_gauge(
            name="pairs_trader_state",
            description="Current algorithm state (0=WARMING_UP, 1=CASH, 2=PENDING_BUY, 3=HOLDING_WAITING, 4=HOLDING_TRIGGERED, 5=PENDING_SELL, 6=HOLDING_DAILY_LIMIT, 7=ERROR)",
            unit="state"
        )
        
        # Number of trades executed today
        self.trades_today = self.meter.create_gauge(
            name="pairs_trader_trades_today",
            description="Number of trades executed today",
            unit="trades"
        )
        
        # =====================================================================
        # Price Ratio Metrics
        # =====================================================================
        
        # Current price ratio (ticker_a / ticker_b)
        self.ratio = self.meter.create_gauge(
            name="pairs_trader_ratio",
            description="Current price ratio (ticker_a / ticker_b)",
            unit="ratio"
        )
        
        # Moving average of ratio
        self.ratio_ma = self.meter.create_gauge(
            name="pairs_trader_ratio_ma",
            description="Moving average of price ratio",
            unit="ratio"
        )
        
        # Percent deviation from MA
        self.ratio_deviation = self.meter.create_gauge(
            name="pairs_trader_ratio_deviation",
            description="Percent deviation of ratio from MA",
            unit="percent"
        )
        
        # =====================================================================
        # Trigger Proximity and Band Metrics
        # =====================================================================
        
        # Trigger proximity (0 = no deviation, 1.0 = at trigger, >1 = past trigger)
        self.trigger_proximity = self.meter.create_gauge(
            name="pairs_trader_trigger_proximity",
            description="Proximity to trigger threshold (1.0 = at trigger, >1.0 = past)",
            unit="ratio"
        )
        
        # Trigger bands for charting
        self.trigger_upper = self.meter.create_gauge(
            name="pairs_trader_trigger_upper",
            description="Upper trigger threshold (ratio_ma * (1 + trigger_pct/100))",
            unit="ratio"
        )
        
        self.trigger_lower = self.meter.create_gauge(
            name="pairs_trader_trigger_lower",
            description="Lower trigger threshold (ratio_ma * (1 - trigger_pct/100))",
            unit="ratio"
        )
        
        # =====================================================================
        # Individual Price Metrics
        # =====================================================================
        
        # Current prices for both tickers (useful for charting)
        self.price_ticker_a = self.meter.create_gauge(
            name="pairs_trader_price_ticker_a",
            description="Current price of ticker A",
            unit="USD"
        )
        
        self.price_ticker_b = self.meter.create_gauge(
            name="pairs_trader_price_ticker_b",
            description="Current price of ticker B",
            unit="USD"
        )
        
        # =====================================================================
        # Flip Tracking Metrics
        # =====================================================================
        
        # Number of flips (swaps) executed today
        self.flips_today = self.meter.create_gauge(
            name="pairs_trader_flips_today",
            description="Swaps executed today (should be max 1)",
            unit="flips"
        )
        
        # =====================================================================
        # Time and State Awareness Metrics
        # =====================================================================
        
        # Minutes until market close (negative if market closed)
        self.minutes_until_close = self.meter.create_gauge(
            name="pairs_trader_minutes_until_close",
            description="Minutes until market close (negative if closed)",
            unit="minutes"
        )
        
        # Which stock we're holding: 1 = ticker_a, -1 = ticker_b, 0 = none
        self.holding_indicator = self.meter.create_gauge(
            name="pairs_trader_holding_indicator",
            description="Current holding: 1=ticker_a, -1=ticker_b, 0=none",
            unit="stock"
        )
        
        # =====================================================================
        # Slippage Metrics
        # =====================================================================
        
        # Expected slippage (from model)
        self.slippage_expected = self.meter.create_histogram(
            name="pairs_trader_slippage_expected",
            description="Expected slippage from model",
            unit="percent"
        )
        
        # Actual slippage (from execution)
        self.slippage_actual = self.meter.create_histogram(
            name="pairs_trader_slippage_actual",
            description="Actual slippage from execution",
            unit="percent"
        )
    
    def record_portfolio_value(self, value: float) -> None:
        """Record current portfolio value."""
        self.portfolio_value.set(value)
        # Suppress print - too verbose for metrics recording
    
    def record_cash_available(self, cash: float) -> None:
        """Record current available cash/buying power."""
        self.cash_available.set(cash)
        # Suppress print - too verbose
    
    def record_return_multipliers(
        self,
        multiplier_7d: float,
        multiplier_60d: float,
        multiplier_all_time: float
    ) -> None:
        """Record return multipliers for all timeframes."""
        self.return_multiplier_7d.set(multiplier_7d)
        self.return_multiplier_60d.set(multiplier_60d)
        self.return_multiplier_all_time.set(multiplier_all_time)
        # Suppress print - logged elsewhere in trade completion
    
    def record_relative_performance(
        self,
        relative_7d: float,
        relative_60d: float,
        relative_all_time: float
    ) -> None:
        """Record relative performance vs market for all timeframes."""
        self.relative_perf_7d.set(relative_7d)
        self.relative_perf_60d.set(relative_60d)
        self.relative_perf_all_time.set(relative_all_time)
        # Suppress print - logged elsewhere
    
    def record_state(self, state_value: int) -> None:
        """Record current algorithm state as numeric value."""
        self.state.set(state_value)
        # State changes are logged elsewhere, no print here to avoid duplication
    
    def record_trades_today(self, count: int) -> None:
        """Record number of trades executed today."""
        self.trades_today.set(count)
        # Suppress print - too verbose
    
    def record_ratio_metrics(
        self,
        ratio: float,
        ratio_ma: float,
        deviation_percent: float
    ) -> None:
        """Record price ratio metrics."""
        self.ratio.set(ratio)
        self.ratio_ma.set(ratio_ma)
        self.ratio_deviation.set(deviation_percent)
        # Suppress print - logged in main loop verbose output
    
    def record_slippage(self, expected: float, actual: float) -> None:
        """Record expected and actual slippage for a trade."""
        self.slippage_expected.record(expected)
        self.slippage_actual.record(actual)
        # Logged by order executor, suppress duplicate
    
    def record_trigger_proximity(self, proximity: float) -> None:
        """Record proximity to trigger threshold."""
        self.trigger_proximity.set(proximity)
        # Suppress print - in verbose poll output
    
    def record_trigger_bands(self, upper: float, lower: float) -> None:
        """Record upper and lower trigger bands."""
        self.trigger_upper.set(upper)
        self.trigger_lower.set(lower)
        # Don't print bands every time, they're constant once set
    
    def record_prices(self, ticker_a_price: float, ticker_b_price: float) -> None:
        """Record current prices for both tickers."""
        self.price_ticker_a.set(ticker_a_price)
        self.price_ticker_b.set(ticker_b_price)
        # Suppress print - in verbose quote fetch
    
    def record_flips_today(self, count: int) -> None:
        """Record number of flips (swaps) executed today."""
        self.flips_today.set(count)
        # Suppress print - same as trades_today
    
    def record_minutes_until_close(self, minutes: int) -> None:
        """Record minutes until market close (negative if closed)."""
        self.minutes_until_close.set(minutes)
        # Suppress print - in heartbeat
    
    def record_holding_indicator(self, stock_held: str) -> None:
        """
        Record which stock is currently held.
        
        Args:
            stock_held: "ticker_a", "ticker_b", or "none"
        """
        if stock_held == "ticker_a":
            self.holding_indicator.set(1)
        elif stock_held == "ticker_b":
            self.holding_indicator.set(-1)
        else:
            self.holding_indicator.set(0)
        # Suppress print - in verbose poll output


class TradingLogger:
    """
    Structured logging for the pairs trading algorithm.
    
    This class provides logging methods that emit structured JSON logs
    to Grafana Loki via OpenTelemetry. All logs include standard context
    fields for easy filtering and correlation.
    
    Critical Logs:
        Logs with level=CRITICAL freeze the application and require
        manual intervention. Set up alerts on these patterns:
        - ORDER_REJECTED
        - POSITION_MISMATCH
        - API_FAILURE_EXHAUSTED
        - INSUFFICIENT_BUYING_POWER
        - INVALID_STATE_TRANSITION
        - UNEXPECTED_POSITION
    """
    
    def __init__(
        self,
        logger: logging.Logger,
        ticker_a: str,
        ticker_b: str
    ):
        """
        Initialize the trading logger.
        
        Args:
            logger: Python logger configured with Loki handler
            ticker_a: First ticker symbol for context
            ticker_b: Second ticker symbol for context
        """
        self.logger = logger
        self.ticker_a = ticker_a
        self.ticker_b = ticker_b
        self.trade_id_counter = 0
    
    def _get_base_context(self, state: str) -> Dict[str, Any]:
        """Get base context fields included in all logs."""
        return {
            "ticker_a": self.ticker_a,
            "ticker_b": self.ticker_b,
            "state": state,
        }
    
    def _log(
        self,
        level: AlertLevel,
        message: str,
        state: str,
        **extra_fields
    ) -> None:
        """
        Internal logging method with structured context.
        
        Args:
            level: Log level (INFO, WARNING, ERROR, CRITICAL)
            message: Log message
            state: Current algorithm state
            **extra_fields: Additional context fields
        """
        context = self._get_base_context(state)
        context.update(extra_fields)
        
        log_method = getattr(self.logger, level.value)
        log_method(message, extra=context)
    
    def info(self, message: str, state: str, **kwargs) -> None:
        """Log an info message."""
        self._log(AlertLevel.INFO, message, state, **kwargs)
    
    def warning(self, message: str, state: str, **kwargs) -> None:
        """Log a warning message."""
        self._log(AlertLevel.WARNING, message, state, **kwargs)
    
    def error(self, message: str, state: str, **kwargs) -> None:
        """Log an error message."""
        self._log(AlertLevel.ERROR, message, state, **kwargs)
    
    def critical(self, message: str, state: str, **kwargs) -> None:
        """
        Log a critical message that freezes the application.
        
        ALERT: Set up monitoring alerts for CRITICAL level logs!
        These indicate the application has frozen and needs manual intervention.
        """
        self._log(AlertLevel.CRITICAL, message, state, **kwargs)
    
    # =========================================================================
    # Specific Event Logging Methods
    # =========================================================================
    
    def log_state_change(
        self,
        from_state: str,
        to_state: str,
        reason: str
    ) -> None:
        """Log a state machine transition."""
        self.info(
            f"State transition: {from_state} -> {to_state}",
            state=to_state,
            from_state=from_state,
            to_state=to_state,
            reason=reason
        )
        logger.info(f"State transition: {from_state} -> {to_state} (Reason: {reason})")
    
    def log_order_placed(
        self,
        order_id: str,
        action: str,
        symbol: str,
        quantity: int,
        state: str
    ) -> None:
        """Log an order placement."""
        self.info(
            f"Order placed: {action} {quantity} {symbol}",
            state=state,
            order_id=order_id,
            action=action,
            symbol=symbol,
            quantity=quantity
        )
        logger.info(f"Order placed: {action} {quantity} {symbol} (Order ID: {order_id})")
    
    def log_order_filled(
        self,
        order_id: str,
        action: str,
        symbol: str,
        quantity: int,
        expected_price: float,
        actual_price: float,
        state: str
    ) -> None:
        """Log an order fill with slippage information."""
        slippage_pct = ((actual_price - expected_price) / expected_price) * 100
        self.info(
            f"Order filled: {action} {quantity} {symbol} @ ${actual_price:.2f} (expected ${expected_price:.2f}, slippage {slippage_pct:.3f}%)",
            state=state,
            order_id=order_id,
            action=action,
            symbol=symbol,
            quantity=quantity,
            expected_price=expected_price,
            actual_price=actual_price,
            slippage_pct=slippage_pct
        )
        logger.info(f"Order filled: {action} {quantity} {symbol} @ ${actual_price:.2f} (expected ${expected_price:.2f}, slippage {slippage_pct:+.3f}%)")
    
    def log_trade_complete(
        self,
        trade_id: str,
        portfolio_value_before: float,
        portfolio_value_after: float,
        period_return_pct: float,
        compound_return_all_time: float,
        state: str
    ) -> None:
        """Log completion of a full trade (swap or initial buy)."""
        self.info(
            f"Trade complete: Portfolio ${portfolio_value_before:.2f} -> ${portfolio_value_after:.2f} ({period_return_pct:+.2f}%)",
            state=state,
            trade_id=trade_id,
            portfolio_value_before=portfolio_value_before,
            portfolio_value_after=portfolio_value_after,
            period_return_pct=period_return_pct,
            compound_return_all_time=compound_return_all_time
        )
        logger.info(f"Trade complete: Portfolio ${portfolio_value_before:.2f} -> ${portfolio_value_after:.2f} ({period_return_pct:+.2f}%) | All-time: {compound_return_all_time:.4f}x")
    
    def log_trigger_met(
        self,
        ratio: float,
        ratio_ma: float,
        deviation_pct: float,
        direction: str,
        state: str
    ) -> None:
        """Log when a swap trigger condition is met."""
        self.info(
            f"Trigger met: ratio={ratio:.4f}, MA={ratio_ma:.4f}, deviation={deviation_pct:+.3f}%, direction={direction}",
            state=state,
            ratio=ratio,
            ratio_ma=ratio_ma,
            deviation_pct=deviation_pct,
            direction=direction
        )
        logger.info(f"🔔 TRIGGER MET: ratio={ratio:.5f}, MA={ratio_ma:.5f}, deviation={deviation_pct:+.3f}%, direction={direction}")
    
    def log_daily_reset(self, new_day: str, state: str) -> None:
        """Log daily trade counter reset."""
        self.info(
            f"Daily reset: New trading day {new_day}, trade counter reset",
            state=state,
            trading_day=new_day
        )
        logger.info(f"📅 Daily reset: New trading day {new_day}, trade counter reset")
    
    def log_heartbeat(
        self,
        state: str,
        holding_stock: str,
        shares_held: int,
        ticker_a_symbol: str,
        ticker_b_symbol: str,
        ratio: float,
        ratio_ma: float,
        deviation_pct: float,
        trigger_percent: float,
        trigger_proximity: float,
        portfolio_value: float,
        trades_today: int,
        minutes_until_close: int
    ) -> None:
        """
        Periodic heartbeat log with system vitals (emit once per minute).
        
        This is the primary debugging log - grep for HEARTBEAT to see
        the system's vital signs over time.
        """
        # Build position string
        if holding_stock == "ticker_a":
            position_str = f"{ticker_a_symbol} x{shares_held}"
        elif holding_stock == "ticker_b":
            position_str = f"{ticker_b_symbol} x{shares_held}"
        else:
            position_str = "CASH (no position)"
        
        # Build trigger deviation indicator - show what we're WAITING for
        if holding_stock == "ticker_a":
            # Holding ticker_a, waiting for positive deviation
            trigger_dev = f"over +{trigger_percent}%"
        elif holding_stock == "ticker_b":
            # Holding ticker_b, waiting for negative deviation
            trigger_dev = f"under -{trigger_percent}%"
        else:
            # In cash - can trigger in either direction
            trigger_dev = f"±{trigger_percent}%"
        
        self.info(
            f"HEARTBEAT: state={state}, position={position_str}, "
            f"ratio={ratio:.5f}, MA={ratio_ma:.5f}, dev={deviation_pct:+.3f}%, "
            f"trigger_dev={trigger_dev}, proximity={trigger_proximity:.2f}, "
            f"value=${portfolio_value:.2f}, trades={trades_today}, mins_to_close={minutes_until_close}",
            state=state,
            position=position_str,
            ratio=ratio,
            ratio_ma=ratio_ma,
            deviation_pct=deviation_pct,
            trigger_dev=trigger_dev,
            trigger_proximity=trigger_proximity,
            portfolio_value=portfolio_value,
            trades_today=trades_today,
            minutes_until_close=minutes_until_close,
            event_type="HEARTBEAT"
        )
        # Print formatted heartbeat to terminal
        logger.info("═" * 60)
        logger.info(f"💓 HEARTBEAT {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"  State: {state} | Position: {position_str} | Trades Today: {trades_today}")
        logger.info(f"  Ratio: {ratio:.5f} | MA: {ratio_ma:.5f} | Deviation: {deviation_pct:+.3f}%")
        logger.info(f"  Trigger Dev: {trigger_dev} | Proximity: {trigger_proximity:.2f}")
        logger.info(f"  Portfolio: ${portfolio_value:.2f} | Minutes to Close: {minutes_until_close}")
        logger.info("═" * 60)
    
    def log_flip_complete(
        self,
        from_stock: str,
        to_stock: str,
        deviation_at_flip: float,
        portfolio_value_before: float,
        portfolio_value_after: float,
        state: str
    ) -> None:
        """
        Log completion of a stock swap (flip).
        
        This is distinct from log_trade_complete - a flip is specifically
        a swap from one stock to another (max 1 per day typically).
        """
        profit_loss = portfolio_value_after - portfolio_value_before
        profit_loss_pct = (profit_loss / portfolio_value_before) * 100 if portfolio_value_before > 0 else 0.0
        
        self.info(
            f"FLIP_COMPLETE: {from_stock} -> {to_stock}, deviation={deviation_at_flip:+.3f}%, "
            f"P&L=${profit_loss:+.2f} ({profit_loss_pct:+.3f}%)",
            state=state,
            from_stock=from_stock,
            to_stock=to_stock,
            deviation_at_flip=deviation_at_flip,
            portfolio_value_before=portfolio_value_before,
            portfolio_value_after=portfolio_value_after,
            profit_loss=profit_loss,
            profit_loss_pct=profit_loss_pct,
            event_type="FLIP_COMPLETE"
        )
        logger.info(f"🔄 FLIP COMPLETE: {from_stock} -> {to_stock} | Deviation at flip: {deviation_at_flip:+.3f}% | P&L: ${profit_loss:+.2f} ({profit_loss_pct:+.3f}%)")
    
    def log_market_open(self, state: str) -> None:
        """Log market open event."""
        self.info(
            "MARKET_OPEN: Trading session started",
            state=state,
            event_type="MARKET_OPEN"
        )
        logger.info("🔔 MARKET OPEN: Trading session started")
    
    def log_market_close(self, state: str) -> None:
        """Log market close event."""
        self.info(
            "MARKET_CLOSE: Trading session ended",
            state=state,
            event_type="MARKET_CLOSE"
        )
        logger.info("🔕 MARKET CLOSE: Trading session ended")
    
    def log_swap_cutoff_entered(self, minutes_before_close: int, state: str) -> None:
        """Log entering the swap cutoff period (no new swaps allowed)."""
        self.warning(
            f"SWAP_CUTOFF: Entered no-swap zone ({minutes_before_close} mins before close)",
            state=state,
            minutes_before_close=minutes_before_close,
            event_type="SWAP_CUTOFF"
        )
        logger.warning(f"⚠️  SWAP CUTOFF: Entered no-swap zone ({minutes_before_close} mins before close)")
    
    # =========================================================================
    # Critical Alert Logging Methods (APP FREEZES ON THESE)
    # =========================================================================
    
    def log_order_rejected(
        self,
        order_id: str,
        symbol: str,
        reason: str,
        state: str
    ) -> None:
        """
        CRITICAL: Log an order rejection. App will freeze.
        Alert pattern: ORDER_REJECTED
        """
        self.critical(
            f"ORDER_REJECTED: Order {order_id} for {symbol} rejected: {reason}",
            state=state,
            order_id=order_id,
            symbol=symbol,
            rejection_reason=reason,
            alert_type="ORDER_REJECTED"
        )
        logger.error(f"❌ CRITICAL - ORDER_REJECTED: Order {order_id} for {symbol} rejected: {reason}")
    
    def log_position_mismatch(
        self,
        expected: str,
        actual: str,
        state: str
    ) -> None:
        """
        CRITICAL: Log a position mismatch. App will freeze.
        Alert pattern: POSITION_MISMATCH
        """
        self.critical(
            f"POSITION_MISMATCH: Expected {expected}, found {actual}",
            state=state,
            expected_position=expected,
            actual_position=actual,
            alert_type="POSITION_MISMATCH"
        )
        logger.error(f"❌ CRITICAL - POSITION_MISMATCH: Expected {expected}, found {actual}")
    
    def log_api_failure_exhausted(
        self,
        endpoint: str,
        attempts: int,
        last_error: str,
        state: str
    ) -> None:
        """
        CRITICAL: Log API failure after retries exhausted. App will freeze.
        Alert pattern: API_FAILURE_EXHAUSTED
        """
        self.critical(
            f"API_FAILURE_EXHAUSTED: {endpoint} failed after {attempts} attempts: {last_error}",
            state=state,
            endpoint=endpoint,
            attempts=attempts,
            last_error=last_error,
            alert_type="API_FAILURE_EXHAUSTED"
        )
        logger.error(f"❌ CRITICAL - API_FAILURE_EXHAUSTED: {endpoint} failed after {attempts} attempts: {last_error}")
    
    def log_insufficient_buying_power(
        self,
        required: float,
        available: float,
        state: str
    ) -> None:
        """
        ERROR (recoverable): Log insufficient buying power.
        Alert pattern: INSUFFICIENT_BUYING_POWER
        """
        self.error(
            f"INSUFFICIENT_BUYING_POWER: Need ${required:.2f}, have ${available:.2f}",
            state=state,
            required=required,
            available=available,
            alert_type="INSUFFICIENT_BUYING_POWER"
        )
        logger.warning(f"⚠️  INSUFFICIENT_BUYING_POWER: Need ${required:.2f}, have ${available:.2f}")
    
    def log_invalid_state_transition(
        self,
        from_state: str,
        to_state: str,
        reason: str
    ) -> None:
        """
        CRITICAL: Log an invalid state transition. App will freeze.
        Alert pattern: INVALID_STATE_TRANSITION
        """
        self.critical(
            f"INVALID_STATE_TRANSITION: Cannot transition from {from_state} to {to_state}: {reason}",
            state=from_state,
            from_state=from_state,
            attempted_state=to_state,
            reason=reason,
            alert_type="INVALID_STATE_TRANSITION"
        )
        logger.error(f"❌ CRITICAL - INVALID_STATE_TRANSITION: Cannot transition from {from_state} to {to_state}: {reason}")
    
    def log_unexpected_position(
        self,
        symbol: str,
        quantity: int,
        state: str
    ) -> None:
        """
        CRITICAL: Log an unexpected position. App will freeze.
        Alert pattern: UNEXPECTED_POSITION
        """
        self.critical(
            f"UNEXPECTED_POSITION: Found {quantity} shares of {symbol} (not in configured pair)",
            state=state,
            symbol=symbol,
            quantity=quantity,
            alert_type="UNEXPECTED_POSITION"
        )
        logger.error(f"❌ CRITICAL - UNEXPECTED_POSITION: Found {quantity} shares of {symbol} (not in configured pair)")
    
    def generate_trade_id(self) -> str:
        """Generate a unique trade ID."""
        self.trade_id_counter += 1
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        return f"T{timestamp}_{self.trade_id_counter:04d}"


class CSVTradeLogger:
    """
    CSV file logger for permanent trade record keeping.
    
    This class appends trade records to a CSV file for post-hoc analysis,
    tax reporting, and performance auditing.
    """
    
    HEADERS = [
        "trade_id",
        "timestamp",
        "action",
        "symbol",
        "quantity",
        "expected_price",
        "actual_price",
        "slippage_pct",
        "portfolio_value_before",
        "portfolio_value_after",
        "period_return_pct",
        "compound_return_all_time",
        "algo_return_7d",
        "algo_return_60d",
        "relative_perf_7d",
        "relative_perf_60d",
        "relative_perf_all_time"
    ]
    
    def __init__(self, csv_path: Path):
        """
        Initialize CSV logger.
        
        Args:
            csv_path: Path to the CSV file for trade logging
        """
        self.csv_path = csv_path
        
        # Create the file with headers if it doesn't exist
        if not self.csv_path.exists():
            self._write_headers()
    
    def _write_headers(self) -> None:
        """Write CSV headers to a new file."""
        with open(self.csv_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(self.HEADERS)
    
    def log_trade(
        self,
        trade_id: str,
        action: str,
        symbol: str,
        quantity: int,
        expected_price: float,
        actual_price: float,
        portfolio_value_before: float,
        portfolio_value_after: float,
        period_return_pct: float,
        compound_return_all_time: float,
        algo_return_7d: float,
        algo_return_60d: float,
        relative_perf_7d: float,
        relative_perf_60d: float,
        relative_perf_all_time: float
    ) -> None:
        """
        Log a trade to the CSV file.
        
        Args:
            trade_id: Unique identifier for this trade
            action: BUY or SELL
            symbol: Stock symbol
            quantity: Number of shares
            expected_price: Expected execution price
            actual_price: Actual execution price
            portfolio_value_before: Portfolio value before trade
            portfolio_value_after: Portfolio value after trade
            period_return_pct: Return since last trade
            compound_return_all_time: All-time compound return
            algo_return_7d: 7-day algo return
            algo_return_60d: 60-day algo return
            relative_perf_7d: 7-day relative performance
            relative_perf_60d: 60-day relative performance
            relative_perf_all_time: All-time relative performance
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        slippage_pct = ((actual_price - expected_price) / expected_price) * 100
        
        row = [
            trade_id,
            timestamp,
            action,
            symbol,
            quantity,
            f"{expected_price:.4f}",
            f"{actual_price:.4f}",
            f"{slippage_pct:.4f}",
            f"{portfolio_value_before:.2f}",
            f"{portfolio_value_after:.2f}",
            f"{period_return_pct:.4f}",
            f"{compound_return_all_time:.4f}",
            f"{algo_return_7d:.4f}",
            f"{algo_return_60d:.4f}",
            f"{relative_perf_7d:.4f}",
            f"{relative_perf_60d:.4f}",
            f"{relative_perf_all_time:.4f}"
        ]
        
        with open(self.csv_path, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(row)


class Observability:
    """
    Main observability class that provides unified access to all
    monitoring infrastructure: metrics, logging, and CSV recording.
    
    Usage:
        obs = Observability(ticker_a="V", ticker_b="MA")
        obs.metrics.record_portfolio_value(10000.0)
        obs.logger.log_state_change("CASH", "PENDING_BUY", "Initial buy")
        obs.csv_logger.log_trade(...)
    """
    
    def __init__(
        self,
        ticker_a: str,
        ticker_b: str,
        csv_path: Optional[Path] = None
    ):
        """
        Initialize all observability infrastructure.
        
        Args:
            ticker_a: First ticker symbol
            ticker_b: Second ticker symbol
            csv_path: Path for CSV trade log (defaults to live_trading/trades.csv)
        """
        # Load environment variables for OTLP configuration
        load_dotenv()
        
        # Get OTLP configuration from environment
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        headers_raw = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "")
        
        # Parse headers from comma-separated format
        headers = {}
        for part in headers_raw.split(","):
            if "=" in part:
                key, value = part.split("=", 1)
                headers[key.strip()] = value.strip()
        
        # Create OpenTelemetry resource to identify this service
        resource = Resource.create({
            "service.name": "PairTraderPro",
            "service.version": "1.0.0",
            "deployment.environment": "live_trading",
            "trading.ticker_a": ticker_a,
            "trading.ticker_b": ticker_b
        })
        
        # =====================================================================
        # Set up Prometheus Metrics
        # =====================================================================
        if endpoint:
            metric_exporter = OTLPMetricExporter(
                endpoint=f"{endpoint.rstrip('/')}/v1/metrics",
                headers=headers
            )
            metric_reader = PeriodicExportingMetricReader(
                metric_exporter,
                export_interval_millis=5000  # Export every 5 seconds
            )
            self.meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[metric_reader]
            )
        else:
            # No OTLP endpoint configured - create a no-op meter provider
            self.meter_provider = MeterProvider(resource=resource)
        
        self.metrics = TradingMetrics(self.meter_provider)
        
        # =====================================================================
        # Set up Loki Logging
        # =====================================================================
        if endpoint:
            log_exporter = OTLPLogExporter(
                endpoint=f"{endpoint.rstrip('/')}/v1/logs",
                headers=headers
            )
            self.logger_provider = LoggerProvider(resource=resource)
            self.logger_provider.add_log_record_processor(
                BatchLogRecordProcessor(log_exporter)
            )
            
            # Create Python logger with OpenTelemetry handler
            handler = LoggingHandler(logger_provider=self.logger_provider)
            python_logger = logging.getLogger("PairTraderPro")
            python_logger.addHandler(handler)
            python_logger.setLevel(logging.INFO)
        else:
            # No OTLP endpoint - use standard Python logging
            self.logger_provider = None
            python_logger = logging.getLogger("PairTraderPro")
            python_logger.setLevel(logging.INFO)
            
            # Add console handler if not already present
            if not python_logger.handlers:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(
                    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                )
                python_logger.addHandler(console_handler)
        
        self.logger = TradingLogger(python_logger, ticker_a, ticker_b)
        
        # =====================================================================
        # Set up CSV Logging
        # =====================================================================
        if csv_path is None:
            csv_path = Path(__file__).parent / "trades.csv"
        
        self.csv_logger = CSVTradeLogger(csv_path)
    
    def flush(self) -> None:
        """Force flush all metrics and logs."""
        self.meter_provider.force_flush()
        if self.logger_provider:
            self.logger_provider.force_flush()
    
    def shutdown(self) -> None:
        """Gracefully shutdown observability infrastructure."""
        self.flush()
        self.meter_provider.shutdown()
        if self.logger_provider:
            self.logger_provider.shutdown()

