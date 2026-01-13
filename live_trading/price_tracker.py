"""
Price Tracker Module - Quote Polling and Moving Average Calculation

This module handles all price data operations for the pairs trading algorithm:
    - Real-time quote polling (1-second intervals)
    - Historical bar fetching (for MA bootstrap and updates)
    - Moving average calculation over configurable window
    - Ratio and deviation calculations
    - Trigger condition detection

The price tracker maintains a rolling window of 1-minute bars for calculating
the moving average of the price ratio between the two stocks.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, field
from collections import deque
import statistics

# Import terminal color utilities
from .terminal_colors import print_grey, print_white, format_currency, format_ratio


@dataclass
class Quote:
    """
    Real-time quote data for a single symbol.
    
    Attributes:
        symbol: Stock ticker symbol
        last: Last trade price (used for ratio calculation)
        bid: Current bid price
        ask: Current ask price
        bid_size: Size at bid
        ask_size: Size at ask
        volume: Today's trading volume
        timestamp: Time of quote
    """
    symbol: str
    last: float
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    volume: int
    timestamp: datetime


@dataclass
class Bar:
    """
    OHLCV bar data for a single time period.
    
    Attributes:
        symbol: Stock ticker symbol
        timestamp: Bar timestamp
        open: Opening price
        high: High price
        low: Low price
        close: Closing price
        volume: Trading volume
    """
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class PriceSnapshot:
    """
    Current price state for the trading pair.
    
    Attributes:
        ticker_a_quote: Quote for ticker A
        ticker_b_quote: Quote for ticker B
        ratio: Current price ratio (ticker_a / ticker_b)
        ratio_ma: Moving average of ratio
        deviation_percent: Percent deviation from MA
        timestamp: Snapshot timestamp
    """
    ticker_a_quote: Quote
    ticker_b_quote: Quote
    ratio: float
    ratio_ma: float
    deviation_percent: float
    timestamp: datetime
    
    @property
    def should_buy_a(self) -> bool:
        """True if ticker_a is undervalued (ratio below MA)."""
        return self.deviation_percent < 0
    
    @property
    def should_buy_b(self) -> bool:
        """True if ticker_b is undervalued (ratio above MA)."""
        return self.deviation_percent > 0


class PriceTracker:
    """
    Tracks prices and calculates moving averages for the pairs trading algorithm.
    
    This class is responsible for:
        1. Fetching real-time quotes via the TradeStation API
        2. Fetching historical bars for MA bootstrap
        3. Maintaining a rolling window of bars for MA calculation
        4. Computing the price ratio and its deviation from MA
        5. Detecting trigger conditions for swaps
    
    Usage:
        tracker = PriceTracker(api, "V", "MA", ma_window=240, trigger_pct=0.4)
        await tracker.bootstrap_moving_average()
        
        while True:
            snapshot = tracker.get_price_snapshot()
            if tracker.should_swap(snapshot, "ticker_a"):
                # Initiate swap
            time.sleep(1)
    
    Attributes:
        api: TradeStation API instance
        ticker_a: First ticker symbol
        ticker_b: Second ticker symbol
        ma_window_minutes: Moving average window in minutes
        trigger_percent: Deviation threshold for triggering swaps
    """
    
    def __init__(
        self,
        api,  # TradeStationAPI instance
        ticker_a: str,
        ticker_b: str,
        ma_window_minutes: int = 240,
        trigger_percent: float = 0.4
    ):
        """
        Initialize the price tracker.
        
        Args:
            api: TradeStation API instance for market data
            ticker_a: First ticker symbol (numerator of ratio)
            ticker_b: Second ticker symbol (denominator of ratio)
            ma_window_minutes: Number of 1-minute bars for MA calculation
            trigger_percent: Minimum deviation from MA to trigger swap
        """
        self.api = api
        self.ticker_a = ticker_a
        self.ticker_b = ticker_b
        self.ma_window_minutes = ma_window_minutes
        self.trigger_percent = trigger_percent
        self.trigger_threshold = trigger_percent / 100.0
        
        # Rolling window of ratio values (one per minute)
        # Using deque with maxlen for automatic oldest-removal
        self.ratio_history: deque = deque(maxlen=ma_window_minutes)
        
        # Cache for latest quotes
        self._last_quote_a: Optional[Quote] = None
        self._last_quote_b: Optional[Quote] = None
        self._last_snapshot_time: Optional[datetime] = None
        
        # Track last minute for bar updates
        self._last_bar_minute: Optional[int] = None
        
        # Flag indicating if MA is ready (has enough history)
        self._ma_ready: bool = False
    
    @property
    def ma_ready(self) -> bool:
        """Check if moving average has enough data to be valid."""
        return self._ma_ready and len(self.ratio_history) >= self.ma_window_minutes
    
    def bootstrap_moving_average(self) -> bool:
        """
        Bootstrap the moving average by fetching historical bars.
        
        This method fetches the required number of historical 1-minute bars
        for both tickers and populates the ratio history. This must be called
        before the algorithm can start trading.
        
        Returns:
            True if bootstrap was successful, False otherwise
        
        Raises:
            Exception: If API calls fail after retries
        """
        print(f"Bootstrapping MA with {self.ma_window_minutes} minutes of history...")
        
        # Fetch historical bars for both tickers
        # We need ma_window_minutes bars, so request a bit more to be safe
        bars_to_fetch = self.ma_window_minutes + 10
        
        bars_a = self._fetch_historical_bars(self.ticker_a, bars_to_fetch)
        bars_b = self._fetch_historical_bars(self.ticker_b, bars_to_fetch)
        
        if not bars_a or not bars_b:
            print("ERROR: Failed to fetch historical bars")
            return False
        
        # Match bars by timestamp and calculate ratios
        # Create lookup dict for ticker_b bars by timestamp (minute precision)
        bars_b_lookup: Dict[str, Bar] = {}
        for bar in bars_b:
            # Key by timestamp string for easy matching
            key = bar.timestamp.strftime("%Y-%m-%d %H:%M")
            bars_b_lookup[key] = bar
        
        # Calculate ratio for each minute where we have both bars
        ratios_with_timestamps: List[Tuple[datetime, float]] = []
        for bar_a in bars_a:
            key = bar_a.timestamp.strftime("%Y-%m-%d %H:%M")
            bar_b = bars_b_lookup.get(key)
            if bar_b:
                # Use close price for ratio (matches simulation's "close" setting)
                ratio = bar_a.close / bar_b.close
                ratios_with_timestamps.append((bar_a.timestamp, ratio))
        
        # Sort by timestamp and take the most recent ma_window_minutes
        ratios_with_timestamps.sort(key=lambda x: x[0])
        recent_ratios = ratios_with_timestamps[-self.ma_window_minutes:]
        
        # Populate the ratio history
        self.ratio_history.clear()
        for timestamp, ratio in recent_ratios:
            self.ratio_history.append(ratio)
        
        if len(self.ratio_history) >= self.ma_window_minutes:
            self._ma_ready = True
            current_ma = statistics.mean(self.ratio_history)
            print(f"MA bootstrap complete: {len(self.ratio_history)} bars loaded")
            print(f"Initial MA: {current_ma:.6f}")
            return True
        else:
            print(f"WARNING: Only got {len(self.ratio_history)} bars, "
                  f"need {self.ma_window_minutes}")
            return False
    
    def _fetch_historical_bars(self, symbol: str, bars_back: int) -> List[Bar]:
        """
        Fetch historical 1-minute bars for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            bars_back: Number of bars to fetch
        
        Returns:
            List of Bar objects, or empty list on failure
        """
        try:
            response = self.api.market_data.get_bars(
                symbol=symbol,
                interval=1,
                unit='Minute',
                bars_back=bars_back
            )
            
            bars = []
            for bar_data in response.get('Bars', []):
                # Parse timestamp from ISO format
                timestamp_str = bar_data.get('TimeStamp', '')
                try:
                    # TradeStation returns timestamps like "2026-01-12T14:30:00Z"
                    timestamp = datetime.fromisoformat(
                        timestamp_str.replace('Z', '+00:00')
                    )
                except ValueError:
                    continue
                
                bar = Bar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(bar_data.get('Open', 0)),
                    high=float(bar_data.get('High', 0)),
                    low=float(bar_data.get('Low', 0)),
                    close=float(bar_data.get('Close', 0)),
                    volume=int(bar_data.get('TotalVolume', 0))
                )
                bars.append(bar)
            
            return bars
            
        except Exception as exception:
            print(f"ERROR fetching bars for {symbol}: {exception}")
            return []
    
    def fetch_quotes(self) -> Tuple[Optional[Quote], Optional[Quote]]:
        """
        Fetch current quotes for both tickers.
        
        Returns:
            Tuple of (quote_a, quote_b), either may be None on failure
        """
        try:
            # Fetch quotes for both symbols in one call
            symbols = f"{self.ticker_a},{self.ticker_b}"
            response = self.api.market_data.get_quote(symbols)
            
            quote_a = None
            quote_b = None
            
            for quote_data in response.get('Quotes', []):
                symbol = quote_data.get('Symbol', '')
                
                quote = Quote(
                    symbol=symbol,
                    last=float(quote_data.get('Last', 0)),
                    bid=float(quote_data.get('Bid', 0)),
                    ask=float(quote_data.get('Ask', 0)),
                    bid_size=int(quote_data.get('BidSize', 0)),
                    ask_size=int(quote_data.get('AskSize', 0)),
                    volume=int(quote_data.get('Volume', 0)),
                    timestamp=datetime.now()
                )
                
                if symbol == self.ticker_a:
                    quote_a = quote
                elif symbol == self.ticker_b:
                    quote_b = quote
            
            # Cache the quotes
            if quote_a:
                self._last_quote_a = quote_a
            if quote_b:
                self._last_quote_b = quote_b
            
            # Print quote fetch in grey
            if quote_a and quote_b:
                print_grey(f"Quotes fetched: {self.ticker_a}={format_currency(quote_a.last)}, {self.ticker_b}={format_currency(quote_b.last)}")
            
            return quote_a, quote_b
            
        except Exception as exception:
            print(f"ERROR fetching quotes: {exception}")
            return None, None
    
    def update_moving_average(self) -> bool:
        """
        Update the moving average with the latest bar data.
        
        This should be called once per minute to add the latest ratio
        to the rolling window. Uses the most recent quote prices.
        
        Returns:
            True if update was successful, False otherwise
        """
        if not self._last_quote_a or not self._last_quote_b:
            return False
        
        # Calculate current ratio from latest quotes
        current_ratio = self._last_quote_a.last / self._last_quote_b.last
        
        # Add to rolling window (deque handles size limit automatically)
        self.ratio_history.append(current_ratio)
        
        # Print MA update in white
        new_ma = statistics.mean(self.ratio_history) if len(self.ratio_history) > 0 else current_ratio
        print_white(f"MA updated: New ratio={format_ratio(current_ratio)}, Updated MA={format_ratio(new_ma)}, History length={len(self.ratio_history)}")
        
        return True
    
    def check_minute_changed(self) -> bool:
        """
        Check if we've entered a new minute since last check.
        
        Returns:
            True if minute changed (should update MA), False otherwise
        """
        current_minute = datetime.now().minute
        
        if self._last_bar_minute is None:
            self._last_bar_minute = current_minute
            return False
        
        if current_minute != self._last_bar_minute:
            self._last_bar_minute = current_minute
            return True
        
        return False
    
    def get_price_snapshot(self) -> Optional[PriceSnapshot]:
        """
        Get the current price snapshot with ratio and MA calculations.
        
        This is the main method called in the trading loop to get current
        price state and determine if action should be taken.
        
        Returns:
            PriceSnapshot with current state, or None if data unavailable
        """
        # Fetch fresh quotes
        quote_a, quote_b = self.fetch_quotes()
        
        if not quote_a or not quote_b:
            return None
        
        if quote_a.last <= 0 or quote_b.last <= 0:
            return None
        
        # Calculate current ratio
        current_ratio = quote_a.last / quote_b.last
        
        # Calculate moving average (if ready)
        if self.ma_ready:
            ratio_ma = statistics.mean(self.ratio_history)
        else:
            # Use current ratio as MA if not ready (no trigger will fire)
            ratio_ma = current_ratio
        
        # Calculate deviation from MA
        if ratio_ma > 0:
            deviation_percent = ((current_ratio / ratio_ma) - 1) * 100
        else:
            deviation_percent = 0.0
        
        self._last_snapshot_time = datetime.now()
        
        return PriceSnapshot(
            ticker_a_quote=quote_a,
            ticker_b_quote=quote_b,
            ratio=current_ratio,
            ratio_ma=ratio_ma,
            deviation_percent=deviation_percent,
            timestamp=datetime.now()
        )
    
    def should_swap(
        self,
        snapshot: PriceSnapshot,
        currently_holding: str  # "ticker_a" or "ticker_b"
    ) -> Tuple[bool, str]:
        """
        Determine if a swap should be triggered based on current prices.
        
        The swap logic:
            - If holding ticker_a and ratio is HIGH (above MA + trigger):
              ticker_a is overvalued, swap to ticker_b
            - If holding ticker_b and ratio is LOW (below MA - trigger):
              ticker_b is overvalued, swap to ticker_a
        
        Args:
            snapshot: Current price snapshot
            currently_holding: Which stock we're holding ("ticker_a" or "ticker_b")
        
        Returns:
            Tuple of (should_swap: bool, direction: str)
            direction is "to_ticker_a" or "to_ticker_b" if swap should happen
        """
        if not self.ma_ready:
            return False, ""
        
        # Convert percentage to decimal for comparison
        deviation = snapshot.deviation_percent / 100.0
        
        if currently_holding == "ticker_a":
            # Check if ticker_a is overvalued (ratio above MA + trigger)
            # This means: sell ticker_a, buy ticker_b
            if deviation > self.trigger_threshold:
                return True, "to_ticker_b"
        
        elif currently_holding == "ticker_b":
            # Check if ticker_b is overvalued (ratio below MA - trigger)
            # This means: sell ticker_b, buy ticker_a
            if deviation < -self.trigger_threshold:
                return True, "to_ticker_a"
        
        return False, ""
    
    def get_undervalued_stock(self, snapshot: PriceSnapshot) -> str:
        """
        Determine which stock is currently undervalued.
        
        Used when starting from cash to decide which stock to buy first.
        
        Args:
            snapshot: Current price snapshot
        
        Returns:
            "ticker_a" or "ticker_b" depending on which is undervalued
        """
        # If ratio is below MA, ticker_a is undervalued (buy ticker_a)
        # If ratio is above MA, ticker_b is undervalued (buy ticker_b)
        if snapshot.deviation_percent < 0:
            return "ticker_a"
        else:
            return "ticker_b"
    
    def calculate_expected_slippage(self, shares: int, settings: dict) -> float:
        """
        Calculate expected slippage using the market impact model.
        
        This uses the same formula as the simulation:
        market_impact = volatility * impact_coefficient * sqrt(shares / adv)
        
        Args:
            shares: Number of shares being traded
            settings: Slippage settings dict with volatility, adv, impact_coefficient
        
        Returns:
            Expected slippage as a decimal (e.g., 0.001 for 0.1%)
        """
        import math
        
        volatility = settings.get('volatility', 0.15)
        adv = settings.get('average_daily_volume', 6000000)
        impact_coefficient = settings.get('impact_coefficient', 0.0055)
        
        market_impact = volatility * impact_coefficient * math.sqrt(shares / adv)
        return market_impact
    
    def get_current_ratio_ma(self) -> Tuple[float, float]:
        """
        Get the current ratio and moving average values.
        
        Returns:
            Tuple of (current_ratio, moving_average)
        """
        if not self._last_quote_a or not self._last_quote_b:
            return 0.0, 0.0
        
        current_ratio = self._last_quote_a.last / self._last_quote_b.last
        
        if self.ma_ready:
            ratio_ma = statistics.mean(self.ratio_history)
        else:
            ratio_ma = current_ratio
        
        return current_ratio, ratio_ma

