"""
TradeStation Market Data Module

This module provides access to TradeStation's market data endpoints.
It handles retrieving historical and real-time price data, symbol information,
and streaming market data feeds.

Market Data Types:
    - Bar Charts: OHLCV data at various time intervals
    - Quotes: Real-time bid/ask and last trade information
    - Symbol Details: Contract specifications, expirations, metadata
    - Streaming Data: Continuous real-time updates

Symbol Notation (important for TradeStation):
    - Stocks: Use ticker symbol (e.g., 'AAPL', 'MSFT')
    - Futures: Use @ prefix for root symbol (e.g., '@ES', '@NQ')
    - Specific Contracts: Use full contract code (e.g., 'ESZ24' for Dec 2024 E-mini S&P)
    - Indices: Use index symbols (e.g., '$SPX', '$NDX')

TradeStation API Endpoints Used (from OpenAPI spec):
    - GET /v3/marketdata/barcharts/{symbol}
    - GET /v3/marketdata/stream/barcharts/{symbol}
    - GET /v3/marketdata/symbols/{symbol}
    - GET /v3/marketdata/quotes/{symbols}
    - GET /v3/marketdata/stream/quotes/{symbols}
    - GET /v2/stream/tickbars/{symbol}/{interval}/{barsBack}
"""

from typing import Dict, Any, Optional, Iterator, Literal
import json
from .client import TradeStationClient


class MarketData:
    """
    Market data operations for TradeStation API.
    
    This class provides methods for accessing historical and real-time market data.
    All methods use the underlying TradeStationClient for authentication and HTTP requests.
    
    Data Categories:
        - Historical Bars: OHLCV data at various time intervals
        - Real-time Streams: Continuous updates as new data arrives
        - Symbol Information: Contract specs, expirations, metadata
        - Quotes: Bid/ask/last price information
    
    All market data endpoints require authentication (per OpenAPI spec),
    even for data that might be considered "public" on other platforms.
    
    Attributes:
        client: TradeStationClient for making authenticated API requests
    """
    
    def __init__(self, client: TradeStationClient):
        """
        Initialize market data handler.
        
        Args:
            client: TradeStationClient instance with authentication configured
        
        Note:
            No API calls are made during initialization. Methods are called
            lazily only when market data is requested.
        """
        # Store the client for making authenticated API requests
        self.client = client
    
    def get_bars(
        self,
        symbol: str,
        interval: int,
        unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Minute',
        bars_back: int = 10,
        first_date: Optional[str] = None,
        last_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get historical bar chart data (snapshot, not streaming).
        
        This retrieves OHLCV (Open, High, Low, Close, Volume) data for a symbol
        at the specified time interval. It returns a complete snapshot of data
        and closes the connection.
        
        Use this for:
            - Historical data analysis
            - Backtesting strategies
            - One-time data retrieval
            - Building charts
        
        For real-time updates, use stream_bars() instead.
        
        Args:
            symbol: Symbol to get data for. Format varies by asset class:
                   - Stocks: 'AAPL', 'MSFT', 'TSLA'
                   - Futures root: '@ES', '@NQ', '@CL'
                   - Specific futures contract: 'ESZ24', 'NQH25'
                   - Indices: '$SPX', '$NDX'
            
            interval: Bar interval size (integer)
                     e.g., 1, 5, 15, 60 (for minutes)
                           1 (for daily/weekly/monthly)
                     Combined with 'unit' to define bar size:
                     - interval=5, unit='Minute' = 5-minute bars
                     - interval=1, unit='Daily' = daily bars
            
            unit: Time unit for bars (default: 'Minute')
                 Options: 'Minute', 'Daily', 'Weekly', 'Monthly'
                 Must be capitalized exactly as shown (per API spec)
            
            bars_back: Number of bars to retrieve (default: 10)
                      Works backward from the most recent bar
                      e.g., bars_back=100 gets last 100 bars
                      Max value depends on data availability
            
            first_date: Optional start date for date range query
                       Format: 'mm-dd-yyyy' or 'mm-dd-yyyy hh:mm:ss'
                       e.g., '01-15-2024' or '01-15-2024 09:30:00'
                       If provided, overrides bars_back for the start
            
            last_date: Optional end date for date range query
                      Format: 'mm-dd-yyyy' or 'mm-dd-yyyy hh:mm:ss'
                      If provided, returns bars up to this date
                      If omitted, returns bars up to current time
        
        Returns:
            Dictionary containing bar chart data with structure:
            {
                'Bars': [
                    {
                        'TimeStamp': '2024-01-15T09:35:00Z',
                        'Open': 4800.50,
                        'High': 4802.25,
                        'Low': 4799.75,
                        'Close': 4801.00,
                        'TotalVolume': 12345,
                        'DownVolume': 5432,
                        'UpVolume': 6913,
                        'BarStatus': 'Closed'
                    },
                    ...
                ],
                'NextToken': '...'  # For pagination if applicable
            }
        
        Example:
            >>> # Get last 10 5-minute bars for S&P E-mini futures
            >>> bars = market_data.get_bars('@ES', interval=5, unit='Minute', bars_back=10)
            >>> latest_bar = bars['Bars'][-1]
            >>> print(f"Close: {latest_bar['Close']}")
            
            >>> # Get daily bars for a date range
            >>> bars = market_data.get_bars(
            ...     'AAPL',
            ...     interval=1,
            ...     unit='Daily',
            ...     first_date='01-01-2024',
            ...     last_date='01-31-2024'
            ... )
        
        Note:
            The API endpoint is: GET /v3/marketdata/barcharts/{symbol}
            Query parameters: unit, interval, barsback (and optional date filters)
        """
        # Build query parameters for the API request
        # All values must be strings (per TradeStation API requirements)
        params = {
            'unit': unit,                    # Time unit (Minute, Daily, etc.)
            'interval': str(interval),       # Bar interval (convert int to string)
            'barsback': str(bars_back)       # Number of bars (convert int to string)
        }
        
        # Add optional date filters if provided
        # These override bars_back for determining the data range
        if first_date:
            params['firstdate'] = first_date
        if last_date:
            params['lastdate'] = last_date
        
        # Construct the API endpoint with the symbol
        # Format: /v3/marketdata/barcharts/AAPL or /v3/marketdata/barcharts/@ES
        endpoint = f"/v3/marketdata/barcharts/{symbol}"
        
        # Make the GET request and return the parsed JSON response
        return self.client.get(endpoint, params=params)
    
    def stream_bars(
        self,
        symbol: str,
        interval: int,
        unit: Literal['Minute', 'Daily', 'Weekly', 'Monthly'] = 'Minute',
        bars_back: int = 10
    ) -> Iterator[bytes]:
        """
        Stream real-time bar chart data (continuous updates).
        
        This opens a persistent connection and streams bar data as it becomes available.
        It first sends historical bars (bars_back), then continues streaming real-time
        updates as new bars form and close.
        
        Use this for:
            - Real-time trading systems
            - Live price monitoring
            - Continuous data feeds
            - Building live charts
        
        For one-time historical data, use get_bars() instead.
        
        Args:
            symbol: Symbol to stream (same notation as get_bars)
                   e.g., '@ES', 'AAPL', 'ESZ24'
            
            interval: Bar interval size (integer)
                     e.g., 1, 5, 15 for 1-min, 5-min, 15-min bars
            
            unit: Time unit for bars (default: 'Minute')
                 Options: 'Minute', 'Daily', 'Weekly', 'Monthly'
            
            bars_back: Number of historical bars to send initially (default: 10)
                      The stream first sends this many historical bars,
                      then continues with real-time updates
        
        Yields:
            Lines of bar data (as bytes)
            Each line is a JSON object representing a bar
            Parse with json.loads() to get dictionary
        
        Stream Format:
            Each line is a JSON object with bar data:
            {
                'TimeStamp': '2024-01-15T09:35:00Z',
                'Open': 4800.50,
                'High': 4802.25,
                'Low': 4799.75,
                'Close': 4801.00,
                'TotalVolume': 12345,
                'BarStatus': 'Open' or 'Closed',
                ...
            }
        
        Example:
            >>> # Stream 5-minute bars for S&P futures
            >>> import json
            >>> stream = market_data.stream_bars('@ES', interval=5, bars_back=5)
            >>> 
            >>> # Process streaming data
            >>> for line in stream:
            ...     bar = json.loads(line)
            ...     print(f"Time: {bar['TimeStamp']}, Close: {bar['Close']}")
            ...     
            ...     # Check if bar is closed (complete)
            ...     if bar['BarStatus'] == 'Closed':
            ...         print(f"Bar closed at {bar['Close']}")
            ...     
            ...     # Press Ctrl+C to stop
        
        Note:
            - The stream runs indefinitely until you stop iterating
            - Handle KeyboardInterrupt to gracefully close the stream
            - The connection auto-closes when you break from the loop
            - API endpoint: GET /v3/marketdata/stream/barcharts/{symbol}
        """
        # Build query parameters (same as get_bars but for streaming endpoint)
        params = {
            'unit': unit,
            'interval': str(interval),
            'barsback': str(bars_back)
        }
        
        # Construct the streaming endpoint
        # Note the '/stream/' in the path - this is what makes it stream
        endpoint = f"/v3/marketdata/stream/barcharts/{symbol}"
        
        # Return the streaming iterator
        # The client.stream() method yields lines as they arrive
        return self.client.stream(endpoint, params=params)
    
    def get_symbol_details(self, symbol: str) -> Dict[str, Any]:
        """
        Get detailed information about a symbol.
        
        This retrieves comprehensive metadata about a symbol including:
        - Asset class and type
        - Trading exchange
        - Contract specifications (for futures/options)
        - Expiration dates (for derivatives)
        - Price formatting (tick size, decimal places)
        - Currency and country
        - For futures: root symbol and available contracts
        
        Use this to:
            - Validate symbols before trading
            - Get contract specifications
            - Find available futures months
            - Determine proper price formatting
            - Get exchange information
        
        Args:
            symbol: Symbol to get details for. Can be:
                   - Stock ticker: 'AAPL', 'MSFT'
                   - Futures root: 'ES', '@ES' (with or without @)
                   - Specific contract: 'ESZ24'
                   - Index: 'SPX', '$SPX'
        
        Returns:
            Dictionary containing symbol details. Structure varies by asset type.
            
            For futures root symbol (e.g., 'ES'):
            {
                'Name': 'ES',
                'Description': 'E-mini S&P 500',
                'Exchange': 'CME',
                'Category': 'Future',
                'Country': 'United States',
                'Currency': 'USD',
                'Symbols': [
                    {
                        'Name': 'ESZ24',
                        'Description': 'E-mini S&P 500 Dec 24',
                        'Underlying': '@ES',
                        'ExpirationDate': '2024-12-20',
                        ...
                    },
                    ...
                ]
            }
            
            For stocks:
            {
                'Name': 'AAPL',
                'Description': 'Apple Inc',
                'Exchange': 'NASDAQ',
                'Category': 'Stock',
                'Country': 'United States',
                'Currency': 'USD',
                ...
            }
        
        Example:
            >>> # Get info about E-mini S&P 500 futures
            >>> details = market_data.get_symbol_details('ES')
            >>> print(f"Description: {details['Description']}")
            >>> 
            >>> # Get available contract months
            >>> contracts = details.get('Symbols', [])
            >>> for contract in contracts:
            ...     print(f"{contract['Name']}: Exp {contract['ExpirationDate']}")
            
            >>> # Get stock details
            >>> details = market_data.get_symbol_details('AAPL')
            >>> print(f"Exchange: {details['Exchange']}")
        
        Note:
            - API endpoint: GET /v3/marketdata/symbols/{symbol}
            - No query parameters required
            - Response structure varies by asset class
            - For futures root symbols, includes list of all available contracts
        """
        # Construct the endpoint with the symbol
        # The API accepts symbols with or without special prefixes (@ or $)
        endpoint = f"/v3/marketdata/symbols/{symbol}"
        
        # Make the GET request and return symbol metadata
        return self.client.get(endpoint)
    
    def stream_tick_bars(
        self,
        symbol: str,
        interval: int,
        bars_back: int = 5
    ) -> Iterator[bytes]:
        """
        Stream tick-based bar data (bars based on trade count, not time).
        
        Unlike time-based bars (e.g., 5-minute bars), tick bars aggregate a fixed
        number of trades. This provides more consistent data during different market
        conditions (high/low activity).
        
        Tick Bar vs Time Bar:
            - Time Bar: Fixed time period (e.g., every 5 minutes)
            - Tick Bar: Fixed number of trades (e.g., every 100 trades)
        
        Advantages of Tick Bars:
            - More consistent bar sizes (by trade count)
            - Better for markets with varying activity levels
            - Faster updates during high volatility
            - Slower updates during quiet periods
        
        Use this for:
            - High-frequency trading
            - Scalping strategies
            - Markets with irregular trading activity
            - Volume-based analysis
        
        Args:
            symbol: Symbol to stream (e.g., '@ES', 'AAPL')
                   Best for actively traded instruments
            
            interval: Number of ticks (trades) per bar
                     e.g., 100 = each bar represents 100 trades
                     Smaller values = more frequent bars
                     Larger values = less frequent bars
            
            bars_back: Number of historical tick bars to send initially (default: 5)
                      The stream first sends this many historical bars,
                      then continues with real-time tick bars
        
        Yields:
            Lines of tick bar data (as bytes)
            Each line is a JSON object representing a tick bar
        
        Example:
            >>> # Stream 100-tick bars for S&P futures
            >>> import json
            >>> stream = market_data.stream_tick_bars('@ES', interval=100, bars_back=5)
            >>> 
            >>> for line in stream:
            ...     bar = json.loads(line)
            ...     print(f"Bar with {bar.get('TickCount')} ticks: Close={bar['Close']}")
            ...     # Press Ctrl+C to stop
        
        Note:
            - This uses the v2 API (older version)
            - API endpoint: GET /v2/stream/tickbars/{symbol}/{interval}/{barsBack}
            - Parameters are in the path, not query string
            - Different from time-based bars (v3 API)
        """
        # Construct the v2 streaming endpoint
        # Note: This API version uses path parameters instead of query params
        # Format: /v2/stream/tickbars/@ES/100/5
        endpoint = f"/v2/stream/tickbars/{symbol}/{interval}/{bars_back}"
        
        # Return the streaming iterator
        # No query parameters needed - all params are in the path
        return self.client.stream(endpoint)
    
    def get_quote(self, symbols: str) -> Dict[str, Any]:
        """
        Get real-time quote data for one or more symbols (snapshot).
        
        This retrieves current bid/ask/last price information. It returns a
        single snapshot and closes the connection.
        
        Quote data includes:
            - Bid price and size
            - Ask price and size
            - Last trade price, size, and time
            - Daily high/low
            - Open and close prices
            - Volume
        
        Use this for:
            - Getting current prices
            - Checking bid/ask spreads
            - One-time price lookups
            - Building watchlists
        
        For continuous updates, use stream_quotes() instead.
        
        Args:
            symbols: Comma-separated list of symbols
                    e.g., 'AAPL,MSFT,@ES'
                    No spaces after commas
                    Can mix different asset types
        
        Returns:
            Dictionary containing quote data:
            {
                'Quotes': [
                    {
                        'Symbol': 'AAPL',
                        'Bid': 150.25,
                        'Ask': 150.27,
                        'Last': 150.26,
                        'BidSize': 100,
                        'AskSize': 200,
                        'High': 151.50,
                        'Low': 149.80,
                        'Open': 150.00,
                        'Close': 149.75,  # Previous close
                        'Volume': 1234567,
                        'TradeTime': '2024-01-15T10:30:45Z',
                        ...
                    },
                    ...
                ]
            }
        
        Example:
            >>> # Get quotes for multiple symbols
            >>> quotes = market_data.get_quote('AAPL,MSFT,@ES')
            >>> for quote in quotes['Quotes']:
            ...     print(f"{quote['Symbol']}: Last={quote['Last']}, Bid={quote['Bid']}, Ask={quote['Ask']}")
            
            >>> # Get single quote
            >>> quotes = market_data.get_quote('@ES')
            >>> quote = quotes['Quotes'][0]
            >>> spread = quote['Ask'] - quote['Bid']
            >>> print(f"Spread: {spread}")
        
        Note:
            - API endpoint: GET /v3/marketdata/quotes/{symbols}
            - Returns snapshot data only (not streaming)
            - Maximum symbols per request varies (check API docs)
        """
        # Construct the endpoint with comma-separated symbols
        # Format: /v3/marketdata/quotes/AAPL,MSFT,@ES
        endpoint = f"/v3/marketdata/quotes/{symbols}"
        
        # Make the GET request and return quote data
        # No query parameters needed
        return self.client.get(endpoint)
    
    def stream_quotes(self, symbols: str) -> Iterator[bytes]:
        """
        Stream real-time quote data for one or more symbols (continuous updates).
        
        This opens a persistent connection and streams quote updates as they occur.
        Updates are sent whenever bid, ask, or last price changes.
        
        Use this for:
            - Real-time price monitoring
            - Live trading systems
            - Tracking multiple symbols simultaneously
            - Building real-time watchlists
        
        For one-time price lookups, use get_quote() instead.
        
        Args:
            symbols: Comma-separated list of symbols
                    e.g., 'AAPL,MSFT,@ES'
                    No spaces after commas
                    Can monitor multiple symbols in one stream
        
        Yields:
            Lines of quote data (as bytes)
            Each line is a JSON object representing a quote update
            Updates sent on any price change
        
        Stream Format:
            Each line is a JSON object:
            {
                'Symbol': 'AAPL',
                'Bid': 150.25,
                'Ask': 150.27,
                'Last': 150.26,
                'BidSize': 100,
                'AskSize': 200,
                'TradeTime': '2024-01-15T10:30:45Z',
                ...
            }
        
        Example:
            >>> # Stream quotes for multiple symbols
            >>> import json
            >>> stream = market_data.stream_quotes('AAPL,MSFT,@ES')
            >>> 
            >>> for line in stream:
            ...     quote = json.loads(line)
            ...     symbol = quote['Symbol']
            ...     last = quote['Last']
            ...     spread = quote['Ask'] - quote['Bid']
            ...     print(f"{symbol}: Last={last}, Spread={spread:.2f}")
            ...     # Press Ctrl+C to stop
        
        Note:
            - API endpoint: GET /v3/marketdata/stream/quotes/{symbols}
            - Stream runs indefinitely until stopped
            - Updates sent on price changes (not on fixed interval)
            - More efficient than polling get_quote() repeatedly
        """
        # Construct the streaming endpoint with comma-separated symbols
        # Note the '/stream/' in the path
        endpoint = f"/v3/marketdata/stream/quotes/{symbols}"
        
        # Return the streaming iterator
        # Updates will arrive as prices change
        return self.client.stream(endpoint)
