"""
Performance Tracker Module - Return Calculation and Logging

This module tracks and calculates performance metrics for the pairs trading algorithm:
    - Compound return multipliers (allocation-change-resistant)
    - Absolute portfolio value tracking
    - Relative performance vs market (holding both stocks)
    - Trade-by-trade performance logging

Performance Calculation Philosophy:
    Returns are tracked as MULTIPLIERS that compound correctly regardless of
    deposits/withdrawals. Each "period" (between trades) has a multiplier:
    
        period_multiplier = end_value / start_value
    
    All-time return is the product of all period multipliers:
    
        all_time_multiplier = product(all_period_multipliers)
    
    This ensures accurate performance measurement even when allocation changes.

Timeframe Windows:
    Performance is calculated for configurable timeframes (e.g., 7d, 60d).
    Only trades within the window contribute to that timeframe's multiplier.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import re


@dataclass
class TradeRecord:
    """
    Record of a completed trade for performance tracking.
    
    A "trade" here means a complete action (initial buy or swap).
    Each trade marks the end of a performance period.
    
    Attributes:
        trade_id: Unique identifier
        timestamp: When the trade completed
        portfolio_value_before: Value at period start
        portfolio_value_after: Value at period end
        period_multiplier: Return multiplier for this period
        ticker_a_price_before: Price of ticker_a at period start
        ticker_b_price_before: Price of ticker_b at period start
        ticker_a_price_after: Price of ticker_a at period end
        ticker_b_price_after: Price of ticker_b at period end
        market_multiplier: What market returned (avg of both stocks)
    """
    trade_id: str
    timestamp: datetime
    portfolio_value_before: float
    portfolio_value_after: float
    period_multiplier: float
    ticker_a_price_before: float
    ticker_b_price_before: float
    ticker_a_price_after: float
    ticker_b_price_after: float
    market_multiplier: float
    
    @property
    def period_return_percent(self) -> float:
        """Return as percentage for this period."""
        return (self.period_multiplier - 1) * 100
    
    @property
    def relative_multiplier(self) -> float:
        """Algorithm multiplier relative to market."""
        if self.market_multiplier <= 0:
            return 1.0
        return self.period_multiplier / self.market_multiplier


def parse_timeframe(timeframe: str) -> timedelta:
    """
    Parse a timeframe string into a timedelta.
    
    Args:
        timeframe: String like "7d", "60d", "24h"
    
    Returns:
        timedelta representing the duration
    
    Raises:
        ValueError: If format is invalid
    """
    match = re.match(r'^(\d+)([dh])$', timeframe.lower())
    if not match:
        raise ValueError(f"Invalid timeframe format: {timeframe}. Use '7d', '60d', '24h', etc.")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    if unit == 'd':
        return timedelta(days=value)
    elif unit == 'h':
        return timedelta(hours=value)
    else:
        raise ValueError(f"Unknown time unit: {unit}")


class PerformanceTracker:
    """
    Tracks performance metrics for the pairs trading algorithm.
    
    This class maintains a history of trades and calculates:
        - Compound return multipliers for various timeframes
        - Absolute portfolio value over time
        - Relative performance vs market benchmark
    
    The tracker uses COMPOUND MULTIPLIERS to handle allocation changes correctly.
    Each trade period's return is: end_value / start_value
    Total return is: product(all period returns)
    
    Usage:
        tracker = PerformanceTracker(["7d", "60d"])
        
        # Record a trade
        tracker.record_trade(
            trade_id="T001",
            portfolio_value_before=10000,
            portfolio_value_after=10200,
            ticker_a_price_before=280.00,
            ticker_b_price_before=520.00,
            ticker_a_price_after=282.00,
            ticker_b_price_after=518.00
        )
        
        # Get metrics
        metrics = tracker.get_metrics()
    
    Attributes:
        timeframes: List of timeframe strings (e.g., ["7d", "60d"])
        trade_history: List of all trade records
    """
    
    def __init__(self, timeframes: List[str]):
        """
        Initialize the performance tracker.
        
        Args:
            timeframes: List of timeframe strings for metric calculation
                       e.g., ["7d", "60d"]
        """
        self.timeframes = timeframes
        self.trade_history: List[TradeRecord] = []
        
        # Parse timeframes into timedeltas for filtering
        self._timeframe_deltas: Dict[str, timedelta] = {}
        for tf in timeframes:
            self._timeframe_deltas[tf] = parse_timeframe(tf)
        
        # Track current period start values (for next trade calculation)
        self._period_start_value: float = 0.0
        self._period_start_prices: Dict[str, float] = {}
        self._period_start_time: Optional[datetime] = None
    
    def start_new_period(
        self,
        portfolio_value: float,
        ticker_a_price: float,
        ticker_b_price: float
    ) -> None:
        """
        Mark the start of a new performance period.
        
        Call this after each trade completes to set the baseline for
        the next period's return calculation.
        
        Args:
            portfolio_value: Current portfolio value
            ticker_a_price: Current price of ticker_a
            ticker_b_price: Current price of ticker_b
        """
        self._period_start_value = portfolio_value
        self._period_start_prices = {
            'ticker_a': ticker_a_price,
            'ticker_b': ticker_b_price
        }
        self._period_start_time = datetime.now(timezone.utc)
    
    def record_trade(
        self,
        trade_id: str,
        portfolio_value_before: float,
        portfolio_value_after: float,
        ticker_a_price_before: float,
        ticker_b_price_before: float,
        ticker_a_price_after: float,
        ticker_b_price_after: float
    ) -> TradeRecord:
        """
        Record a completed trade and calculate period performance.
        
        Args:
            trade_id: Unique trade identifier
            portfolio_value_before: Value at period start
            portfolio_value_after: Value at period end (after trade)
            ticker_a_price_before: Ticker A price at period start
            ticker_b_price_before: Ticker B price at period start
            ticker_a_price_after: Ticker A price at period end
            ticker_b_price_after: Ticker B price at period end
        
        Returns:
            TradeRecord with calculated metrics
        """
        # Calculate algorithm period multiplier
        if portfolio_value_before > 0:
            period_multiplier = portfolio_value_after / portfolio_value_before
        else:
            period_multiplier = 1.0
        
        # Calculate market multiplier (average return of both stocks)
        # Market benchmark: what if you just held 50/50 of each stock
        ticker_a_return = ticker_a_price_after / ticker_a_price_before if ticker_a_price_before > 0 else 1.0
        ticker_b_return = ticker_b_price_after / ticker_b_price_before if ticker_b_price_before > 0 else 1.0
        market_multiplier = (ticker_a_return + ticker_b_return) / 2
        
        # Create trade record
        record = TradeRecord(
            trade_id=trade_id,
            timestamp=datetime.now(timezone.utc),
            portfolio_value_before=portfolio_value_before,
            portfolio_value_after=portfolio_value_after,
            period_multiplier=period_multiplier,
            ticker_a_price_before=ticker_a_price_before,
            ticker_b_price_before=ticker_b_price_before,
            ticker_a_price_after=ticker_a_price_after,
            ticker_b_price_after=ticker_b_price_after,
            market_multiplier=market_multiplier
        )
        
        self.trade_history.append(record)
        
        # Update period start for next trade
        self.start_new_period(
            portfolio_value=portfolio_value_after,
            ticker_a_price=ticker_a_price_after,
            ticker_b_price=ticker_b_price_after
        )
        
        return record
    
    def get_compound_return(
        self,
        timeframe: Optional[str] = None
    ) -> Tuple[float, float, float]:
        """
        Calculate compound returns for a timeframe.
        
        Args:
            timeframe: Timeframe string (e.g., "7d") or None for all-time
        
        Returns:
            Tuple of (algo_multiplier, market_multiplier, relative_multiplier)
            All values are multipliers (1.0 = no change, 1.10 = +10%)
        """
        if not self.trade_history:
            return 1.0, 1.0, 1.0
        
        # Filter trades by timeframe
        if timeframe:
            cutoff = datetime.now(timezone.utc) - self._timeframe_deltas[timeframe]
            trades = [t for t in self.trade_history if t.timestamp >= cutoff]
        else:
            trades = self.trade_history
        
        if not trades:
            return 1.0, 1.0, 1.0
        
        # Compound all period multipliers
        algo_multiplier = 1.0
        market_multiplier = 1.0
        
        for trade in trades:
            algo_multiplier *= trade.period_multiplier
            market_multiplier *= trade.market_multiplier
        
        # Calculate relative performance
        if market_multiplier > 0:
            relative_multiplier = algo_multiplier / market_multiplier
        else:
            relative_multiplier = algo_multiplier
        
        return algo_multiplier, market_multiplier, relative_multiplier
    
    def get_metrics(self, current_portfolio_value: float = 0.0) -> Dict[str, float]:
        """
        Get all performance metrics.
        
        Args:
            current_portfolio_value: Current portfolio value for absolute tracking
        
        Returns:
            Dictionary with all metric values:
            {
                'portfolio_value': current_portfolio_value,
                'return_multiplier_7d': ...,
                'return_multiplier_60d': ...,
                'return_multiplier_all_time': ...,
                'relative_perf_7d': ...,
                'relative_perf_60d': ...,
                'relative_perf_all_time': ...,
                'last_trade_return_pct': ...,
                'trades_count': ...
            }
        """
        metrics = {
            'portfolio_value': current_portfolio_value,
            'trades_count': len(self.trade_history)
        }
        
        # All-time metrics
        algo_all, market_all, relative_all = self.get_compound_return(None)
        metrics['return_multiplier_all_time'] = algo_all
        metrics['market_multiplier_all_time'] = market_all
        metrics['relative_perf_all_time'] = relative_all
        
        # Timeframe-specific metrics
        for tf in self.timeframes:
            algo, market, relative = self.get_compound_return(tf)
            metrics[f'return_multiplier_{tf}'] = algo
            metrics[f'market_multiplier_{tf}'] = market
            metrics[f'relative_perf_{tf}'] = relative
        
        # Last trade metrics
        if self.trade_history:
            last_trade = self.trade_history[-1]
            metrics['last_trade_return_pct'] = last_trade.period_return_percent
            metrics['last_trade_relative'] = last_trade.relative_multiplier
        else:
            metrics['last_trade_return_pct'] = 0.0
            metrics['last_trade_relative'] = 1.0
        
        return metrics
    
    def get_period_start_value(self) -> float:
        """Get the portfolio value at the start of the current period."""
        return self._period_start_value
    
    def get_period_start_prices(self) -> Dict[str, float]:
        """Get the stock prices at the start of the current period."""
        return self._period_start_prices.copy()
    
    def get_latest_trade(self) -> Optional[TradeRecord]:
        """Get the most recent trade record."""
        if self.trade_history:
            return self.trade_history[-1]
        return None
    
    def get_trade_count_in_timeframe(self, timeframe: str) -> int:
        """
        Count trades within a timeframe.
        
        Args:
            timeframe: Timeframe string (e.g., "7d")
        
        Returns:
            Number of trades in the timeframe
        """
        cutoff = datetime.now(timezone.utc) - self._timeframe_deltas[timeframe]
        return sum(1 for t in self.trade_history if t.timestamp >= cutoff)
    
    def format_metrics_table(self, metrics: Dict[str, float]) -> str:
        """
        Format metrics as a readable table (for console output).
        
        Args:
            metrics: Metrics dictionary from get_metrics()
        
        Returns:
            Formatted string table
        """
        lines = [
            "+------------+------------+------------+------------+",
            "| Timeframe  | Algo       | Market     | Relative   |",
            "+------------+------------+------------+------------+"
        ]
        
        # Add each timeframe
        for tf in self.timeframes:
            algo = metrics.get(f'return_multiplier_{tf}', 1.0)
            market = metrics.get(f'market_multiplier_{tf}', 1.0)
            relative = metrics.get(f'relative_perf_{tf}', 1.0)
            
            algo_pct = (algo - 1) * 100
            market_pct = (market - 1) * 100
            relative_pct = (relative - 1) * 100
            
            lines.append(
                f"| {tf:<10} | {algo_pct:>+9.2f}% | {market_pct:>+9.2f}% | {relative_pct:>+9.2f}% |"
            )
        
        # Add all-time
        algo = metrics.get('return_multiplier_all_time', 1.0)
        market = metrics.get('market_multiplier_all_time', 1.0)
        relative = metrics.get('relative_perf_all_time', 1.0)
        
        algo_pct = (algo - 1) * 100
        market_pct = (market - 1) * 100
        relative_pct = (relative - 1) * 100
        
        lines.append("+------------+------------+------------+------------+")
        lines.append(
            f"| {'All-time':<10} | {algo_pct:>+9.2f}% | {market_pct:>+9.2f}% | {relative_pct:>+9.2f}% |"
        )
        lines.append("+------------+------------+------------+------------+")
        
        # Add portfolio value and trade count
        pv = metrics.get('portfolio_value', 0)
        tc = int(metrics.get('trades_count', 0))
        lines.append(f"Portfolio Value: ${pv:,.2f} | Trades: {tc}")
        
        return '\n'.join(lines)


class SlippageTracker:
    """
    Tracks expected vs actual slippage for analysis.
    
    This class maintains a history of slippage events for comparison
    and analysis. It calculates statistics and provides data for
    the slippage comparison chart.
    """
    
    def __init__(self, max_history: int = 1000):
        """
        Initialize slippage tracker.
        
        Args:
            max_history: Maximum number of events to keep
        """
        self.history: deque = deque(maxlen=max_history)
    
    def record_slippage(
        self,
        trade_id: str,
        symbol: str,
        action: str,
        expected_slippage_pct: float,
        actual_slippage_pct: float,
        shares: int,
        price: float
    ) -> None:
        """
        Record a slippage event.
        
        Args:
            trade_id: Trade identifier
            symbol: Stock symbol
            action: BUY or SELL
            expected_slippage_pct: Estimated slippage from model
            actual_slippage_pct: Actual slippage from execution
            shares: Number of shares traded
            price: Expected price
        """
        self.history.append({
            'trade_id': trade_id,
            'timestamp': datetime.now(timezone.utc),
            'symbol': symbol,
            'action': action,
            'expected_pct': expected_slippage_pct,
            'actual_pct': actual_slippage_pct,
            'shares': shares,
            'price': price,
            'difference_pct': actual_slippage_pct - expected_slippage_pct
        })
    
    def get_statistics(self) -> Dict[str, float]:
        """
        Get slippage statistics.
        
        Returns:
            Dictionary with:
                - avg_expected: Average expected slippage
                - avg_actual: Average actual slippage
                - avg_difference: Average difference (actual - expected)
                - count: Number of events
        """
        if not self.history:
            return {
                'avg_expected': 0.0,
                'avg_actual': 0.0,
                'avg_difference': 0.0,
                'count': 0
            }
        
        expected = [e['expected_pct'] for e in self.history]
        actual = [e['actual_pct'] for e in self.history]
        diff = [e['difference_pct'] for e in self.history]
        
        return {
            'avg_expected': sum(expected) / len(expected),
            'avg_actual': sum(actual) / len(actual),
            'avg_difference': sum(diff) / len(diff),
            'count': len(self.history)
        }
    
    def get_recent_events(self, count: int = 10) -> List[Dict]:
        """Get the most recent slippage events."""
        return list(self.history)[-count:]

