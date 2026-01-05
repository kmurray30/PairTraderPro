"""
TradeStation Account Module

This module provides access to account-related endpoints including balances,
positions, and account information. It handles both snapshot and streaming data
for monitoring account status in real-time.

Account Information Types:
    - Account List: All accounts associated with the user
    - Balances: Cash, equity, buying power, margin
    - Positions: Current holdings (long/short)
    - BOD Balances: Beginning-of-day snapshot for comparison

Account ID Format (important):
    - Simulation: Starts with 'SIM' (e.g., 'SIM2977785M')
    - Production: Real account number
    - The account ID determines whether trades are real or simulated
    - Same API credentials work for both sim and prod accounts

TradeStation API Endpoints Used (from OpenAPI spec):
    - GET /v3/brokerage/accounts
    - GET /v3/brokerage/accounts/{accountIds}/balances
    - GET /v3/brokerage/accounts/{accountIds}/bodbalances
    - GET /v3/brokerage/accounts/{accountIds}/positions
    - GET /v3/brokerage/stream/accounts/{accountIds}/positions
"""

from typing import Dict, Any, Optional, Iterator
from .client import TradeStationClient


class Account:
    """
    Account operations for TradeStation API.
    
    This class provides methods for accessing account information, balances,
    and positions. It supports both snapshot (one-time) and streaming (real-time)
    data retrieval.
    
    All account endpoints require authentication and return data only for
    accounts accessible by the current user's credentials.
    
    Data Categories:
        - Account Metadata: Account IDs, types, status
        - Balances: Cash, equity, buying power, margin
        - Positions: Holdings with P&L and market values
        - Streams: Real-time updates as positions change
    
    Attributes:
        client: TradeStationClient for making authenticated API requests
    """
    
    def __init__(self, client: TradeStationClient):
        """
        Initialize account handler.
        
        Args:
            client: TradeStationClient instance with authentication configured
        
        Note:
            No API calls are made during initialization. Methods are called
            lazily only when account data is requested.
        """
        # Store the client for making authenticated API requests
        self.client = client
    
    def get_accounts(self) -> Dict[str, Any]:
        """
        Get list of all accounts associated with the user.
        
        This retrieves basic information about all accounts the user has access to.
        Use this to discover account IDs and determine which accounts to query
        for detailed information.
        
        The response includes:
            - Account IDs
            - Account names
            - Account types (e.g., Cash, Margin, Futures)
            - Account status (Active, Closed, etc.)
        
        Use this to:
            - Discover available account IDs
            - Check account status
            - Determine account types
            - Select which account to use for trading
        
        Returns:
            Dictionary containing list of accounts:
            {
                'Accounts': [
                    {
                        'AccountID': 'SIM2977785M',
                        'Name': 'Simulation Account',
                        'AccountType': 'Margin',
                        'Status': 'Active',
                        'Currency': 'USD',
                        ...
                    },
                    ...
                ]
            }
        
        Example:
            >>> # Get all accounts
            >>> accounts = account.get_accounts()
            >>> for acct in accounts['Accounts']:
            ...     print(f"Account: {acct['AccountID']} - {acct['Name']}")
            ...     print(f"  Type: {acct['AccountType']}, Status: {acct['Status']}")
            
            >>> # Find simulation accounts
            >>> sim_accounts = [a for a in accounts['Accounts'] 
            ...                 if a['AccountID'].startswith('SIM')]
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts
            - No parameters required
            - Returns all accounts accessible with current credentials
            - Use the AccountID from response in other account methods
        """
        # Construct the accounts endpoint
        # This endpoint lists all accounts for the authenticated user
        endpoint = "/v3/brokerage/accounts"
        
        # Make the GET request and return account list
        return self.client.get(endpoint)
    
    def get_balances(self, account_id: str) -> Dict[str, Any]:
        """
        Get real-time account balances and financial metrics.
        
        This retrieves current account balance information including cash,
        equity, buying power, and margin details. The values are real-time
        and reflect current market prices and positions.
        
        Balance Types Returned:
            - Cash Balance: Available cash in the account
            - Equity: Total account value (cash + positions)
            - Buying Power: Amount available for new positions
            - Margin Used: Amount of margin currently in use
            - Unrealized P&L: Profit/loss on open positions
            - Realized P&L: Profit/loss from closed trades today
        
        Use this for:
            - Checking available funds before trading
            - Monitoring account health
            - Calculating position sizes
            - Risk management
            - Performance tracking
        
        Args:
            account_id: Account ID to get balances for
                       Format: 'SIM2977785M' for simulation
                              or real account number for production
                       Get from get_accounts() or config.account_id
        
        Returns:
            Dictionary containing real-time balance information:
            {
                'CashBalance': 100000.00,
                'Equity': 105432.50,
                'BuyingPower': 200000.00,
                'MarginBalance': 50000.00,
                'UnrealizedProfitLoss': 5432.50,
                'RealizedProfitLoss': 1250.75,
                'DayTradesBuyingPower': 400000.00,
                'DayTradesCount': 2,
                'OptionBuyingPower': 50000.00,
                ...
            }
        
        Example:
            >>> # Get current balances
            >>> balances = account.get_balances('SIM2977785M')
            >>> 
            >>> print(f"Cash: ${balances['CashBalance']:,.2f}")
            >>> print(f"Equity: ${balances['Equity']:,.2f}")
            >>> print(f"Buying Power: ${balances['BuyingPower']:,.2f}")
            >>> 
            >>> # Check if we have enough buying power for a trade
            >>> required_capital = 10000
            >>> if balances['BuyingPower'] >= required_capital:
            ...     print("Sufficient buying power")
            >>> 
            >>> # Calculate current day's profit
            >>> day_pl = balances['RealizedProfitLoss'] + balances['UnrealizedProfitLoss']
            >>> print(f"Today's P&L: ${day_pl:,.2f}")
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}/balances
            - Values update in real-time with market prices
            - For beginning-of-day snapshot, use get_bod_balances()
            - Buying power calculations depend on account type and regulations
        """
        # Construct the balances endpoint with the account ID
        # Format: /v3/brokerage/accounts/SIM2977785M/balances
        endpoint = f"/v3/brokerage/accounts/{account_id}/balances"
        
        # Make the GET request and return real-time balance data
        return self.client.get(endpoint)
    
    def get_bod_balances(self, account_id: str) -> Dict[str, Any]:
        """
        Get beginning-of-day (BOD) account balances.
        
        This retrieves a snapshot of account balances as they were at the
        start of the trading day. Use this as a baseline for comparing
        current balances to calculate daily performance.
        
        BOD balances include:
            - Cash balance at market open
            - Equity at market open
            - Margin information at market open
            - Position values at previous close
        
        Use this for:
            - Calculating daily profit/loss
            - Comparing current vs starting balances
            - Daily performance metrics
            - End-of-day reconciliation
            - Reporting and record keeping
        
        Args:
            account_id: Account ID to get BOD balances for
                       Same format as get_balances()
        
        Returns:
            Dictionary containing beginning-of-day balances:
            {
                'CashBalance': 100000.00,
                'Equity': 100000.00,
                'MarginBalance': 0.00,
                'RealizedProfitLoss': 0.00,
                ...
            }
        
        Example:
            >>> # Compare current vs BOD to calculate daily P&L
            >>> bod_balances = account.get_bod_balances('SIM2977785M')
            >>> current_balances = account.get_balances('SIM2977785M')
            >>> 
            >>> # Calculate today's change in equity
            >>> daily_pl = current_balances['Equity'] - bod_balances['Equity']
            >>> print(f"Today's Equity Change: ${daily_pl:,.2f}")
            >>> 
            >>> # Calculate percentage return
            >>> daily_return_pct = (daily_pl / bod_balances['Equity']) * 100
            >>> print(f"Daily Return: {daily_return_pct:.2f}%")
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}/bodbalances
            - BOD values are fixed until next trading day
            - Updates occur at market open
            - Useful for daily performance tracking
        """
        # Construct the BOD balances endpoint
        # Format: /v3/brokerage/accounts/SIM2977785M/bodbalances
        endpoint = f"/v3/brokerage/accounts/{account_id}/bodbalances"
        
        # Make the GET request and return beginning-of-day balance snapshot
        return self.client.get(endpoint)
    
    def get_positions(self, account_id: str) -> Dict[str, Any]:
        """
        Get current positions in the account (snapshot).
        
        This retrieves all open positions including stocks, options, and futures.
        Each position includes quantity, entry price, current price, and P&L.
        
        Position Information Returned:
            - Symbol and asset type
            - Quantity (positive for long, negative for short)
            - Average entry price
            - Current market price
            - Market value
            - Unrealized P&L
            - Today's P&L
        
        Quantity Convention (important):
            - Positive quantity = Long position
            - Negative quantity = Short position
            - Zero quantity = No position (shouldn't appear)
        
        Use this for:
            - Monitoring open positions
            - Risk management
            - Position sizing calculations
            - Portfolio analysis
            - Closing positions
        
        Args:
            account_id: Account ID to get positions for
        
        Returns:
            Dictionary containing current positions:
            {
                'Positions': [
                    {
                        'Symbol': 'AAPL',
                        'Quantity': 100,          # Long 100 shares
                        'AveragePrice': 150.25,
                        'Last': 152.50,
                        'MarketValue': 15250.00,
                        'UnrealizedProfitLoss': 225.00,
                        'UnrealizedProfitLossPercent': 1.50,
                        'TodaysProfitLoss': 125.00,
                        'AssetType': 'Stock',
                        ...
                    },
                    {
                        'Symbol': '@ES',
                        'Quantity': -2,           # Short 2 contracts
                        'AveragePrice': 4800.00,
                        'Last': 4795.00,
                        'UnrealizedProfitLoss': 500.00,  # Profit on short
                        ...
                    },
                    ...
                ]
            }
        
        Example:
            >>> # Get all positions
            >>> positions = account.get_positions('SIM2977785M')
            >>> 
            >>> # Display positions
            >>> for pos in positions['Positions']:
            ...     symbol = pos['Symbol']
            ...     qty = pos['Quantity']
            ...     direction = "LONG" if qty > 0 else "SHORT"
            ...     pnl = pos['UnrealizedProfitLoss']
            ...     print(f"{symbol}: {direction} {abs(qty)} | P&L: ${pnl:,.2f}")
            >>> 
            >>> # Calculate total unrealized P&L
            >>> total_pnl = sum(p['UnrealizedProfitLoss'] for p in positions['Positions'])
            >>> print(f"Total Unrealized P&L: ${total_pnl:,.2f}")
            >>> 
            >>> # Find positions with losses
            >>> losing_positions = [p for p in positions['Positions'] 
            ...                     if p['UnrealizedProfitLoss'] < 0]
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}/positions
            - Returns snapshot data (for real-time updates, use stream_positions)
            - Empty list if no positions
            - Quantities are signed (positive=long, negative=short)
        """
        # Construct the positions endpoint
        # Format: /v3/brokerage/accounts/SIM2977785M/positions
        endpoint = f"/v3/brokerage/accounts/{account_id}/positions"
        
        # Make the GET request and return position snapshot
        return self.client.get(endpoint)
    
    def stream_positions(self, account_id: str) -> Iterator[bytes]:
        """
        Stream real-time position updates.
        
        This opens a persistent connection and streams position updates as they
        occur. Updates are sent whenever a position changes (new trades, price
        changes affecting P&L, positions closed, etc.).
        
        Use this for:
            - Real-time portfolio monitoring
            - Live P&L tracking
            - Immediate notification of position changes
            - Building live dashboards
            - Risk monitoring systems
        
        Stream Triggers:
            - New position opened
            - Position quantity changed (partial fills, new trades)
            - Position closed
            - P&L changed due to price movement
            - End of day reconciliation
        
        Args:
            account_id: Account ID to stream positions for
        
        Yields:
            Lines of position data (as bytes)
            Each line is a JSON object representing a position update
        
        Stream Format:
            Each line is a JSON object similar to get_positions() response:
            {
                'Symbol': 'AAPL',
                'Quantity': 100,
                'AveragePrice': 150.25,
                'Last': 152.75,
                'UnrealizedProfitLoss': 250.00,
                'UpdateType': 'PositionUpdate',  # Type of update
                ...
            }
        
        Example:
            >>> # Stream real-time position updates
            >>> import json
            >>> stream = account.stream_positions('SIM2977785M')
            >>> 
            >>> print("Monitoring positions in real-time...")
            >>> for line in stream:
            ...     position = json.loads(line)
            ...     symbol = position['Symbol']
            ...     qty = position['Quantity']
            ...     pnl = position['UnrealizedProfitLoss']
            ...     
            ...     print(f"Update: {symbol} | Qty: {qty} | P&L: ${pnl:,.2f}")
            ...     
            ...     # Alert on significant losses
            ...     if pnl < -1000:
            ...         print(f"⚠️  WARNING: Large loss on {symbol}")
            ...     
            ...     # Press Ctrl+C to stop
        
        Note:
            - API endpoint: GET /v3/brokerage/stream/accounts/{accountId}/positions
            - Stream runs indefinitely until stopped
            - Updates sent as positions change (not on fixed interval)
            - More efficient than polling get_positions() repeatedly
            - Handle KeyboardInterrupt for graceful shutdown
        """
        # Construct the streaming positions endpoint
        # Note the '/stream/' in the path
        endpoint = f"/v3/brokerage/stream/accounts/{account_id}/positions"
        
        # Return the streaming iterator
        # Position updates will arrive as changes occur
        return self.client.stream(endpoint)
    
    def get_account_info(self, account_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific account.
        
        This retrieves comprehensive account metadata beyond just balances.
        It includes account configuration, status, and other details.
        
        Account Information May Include:
            - Account name and alias
            - Account type (Cash, Margin, Futures, etc.)
            - Account status (Active, Restricted, Closed)
            - Currency
            - Account features and permissions
            - Day trading status
            - Pattern day trader flag
        
        Use this for:
            - Verifying account status
            - Checking account permissions
            - Determining account capabilities
            - Account configuration details
        
        Args:
            account_id: Account ID to get information for
        
        Returns:
            Dictionary containing detailed account information:
            {
                'AccountID': 'SIM2977785M',
                'Name': 'Simulation Trading Account',
                'AccountType': 'Margin',
                'Status': 'Active',
                'Currency': 'USD',
                'PatternDayTrader': false,
                ...
            }
        
        Example:
            >>> # Get account details
            >>> info = account.get_account_info('SIM2977785M')
            >>> 
            >>> print(f"Account: {info['Name']}")
            >>> print(f"Type: {info['AccountType']}")
            >>> print(f"Status: {info['Status']}")
            >>> 
            >>> # Check if pattern day trader
            >>> if info.get('PatternDayTrader'):
            ...     print("Account is flagged as Pattern Day Trader")
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}
            - Returns static account configuration (doesn't change frequently)
            - For balance and position data, use get_balances() and get_positions()
        """
        # Construct the account info endpoint
        # Format: /v3/brokerage/accounts/SIM2977785M
        endpoint = f"/v3/brokerage/accounts/{account_id}"
        
        # Make the GET request and return account information
        return self.client.get(endpoint)
