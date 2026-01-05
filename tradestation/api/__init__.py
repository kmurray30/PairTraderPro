"""
TradeStation API Library

A comprehensive Python library for interacting with the TradeStation API.
Provides clean, modular access to market data, account information, and
order execution with built-in environment management for safe trading.

Key Features:
    - Environment-based configuration (sim/dev/prod)
    - Automatic OAuth token management
    - Modular design (market data, account, orders)
    - Type hints for IDE support
    - Streaming data support
    - Comprehensive error handling

Design Philosophy:
    - Safety first: Always defaults to simulation environment
    - Explicit over implicit: Require explicit environment selection
    - Clean abstractions: Hide complexity, expose simplicity
    - Fail fast: Validate early and provide clear error messages

Quick Start:
    ```python
    from tradestation.api import TradeStationAPI
    
    # Initialize API (defaults to simulation)
    api = TradeStationAPI('sim')  # or just TradeStationAPI()
    
    # Get market data
    bars = api.market_data.get_bars('@ES', interval=5, bars_back=10)
    
    # Check account balance
    balances = api.account.get_balances(api.config.account_id)
    
    # Confirm an order (dry-run, doesn't place)
    confirmation = api.orders.confirm_order(
        account_id=api.config.account_id,
        symbol='ESZ24',
        quantity=1,
        action='BUY'
    )
    ```

Module Organization:
    - config: Environment and credentials management
    - auth: OAuth 2.0 authentication
    - client: HTTP client with token management
    - market_data: Market data and streaming
    - account: Account info, balances, positions
    - orders: Order execution and management

Environment Setup:
    Create a single .env file in the project root:
    - .env: Contains all credentials for both sim and prod
    
    The file should contain:
    - TRADESTATION_API_KEY
    - TRADESTATION_SECRET
    - REFRESH_TOKEN
    - ACCOUNT_ID
    
    Environment selection ('sim' vs 'prod') just determines which
    API URL is used - the credentials are the same.

Safety Features:
    - Single .env file (simpler, less confusion)
    - Default to 'sim' environment
    - Production warning banner
    - Explicit environment selection required
    - No silent fallbacks

For detailed documentation, see:
    - README.md: Library overview and setup
    - USAGE.md: Comprehensive usage guide
    - example.py: Working code examples
"""

# Import core components from submodules
# These provide the building blocks for the main API class
from .config import TradeStationConfig, get_config, EnvironmentType
from .auth import TradeStationAuth
from .client import TradeStationClient
from .market_data import MarketData
from .account import Account
from .orders import Orders


class TradeStationAPI:
    """
    Main API client for TradeStation.
    
    This is the primary entry point for the library. It provides unified access
    to all TradeStation API functionality through organized modules.
    
    The API uses a composition pattern where the main client contains
    specialized modules for different API categories:
        - market_data: For market data and quotes
        - account: For account information and positions
        - orders: For order execution and management
    
    Each module uses a shared HTTP client (client) that handles authentication
    and request/response management automatically.
    
    Architecture:
        TradeStationAPI
        ├── config: Configuration and environment settings
        ├── client: HTTP client with authentication
        │   └── auth: OAuth token management
        ├── market_data: Market data module
        ├── account: Account module
        └── orders: Orders module
    
    All modules share the same authenticated client, ensuring consistent
    token management and error handling across the entire library.
    
    Attributes:
        config: TradeStationConfig with environment and credentials
        client: TradeStationClient for HTTP requests
        auth: TradeStationAuth for OAuth (alias to client.auth)
        market_data: MarketData module for market data operations
        account: Account module for account operations
        orders: Orders module for order operations
    """
    
    def __init__(self, environment: EnvironmentType = 'sim'):
        """
        Initialize TradeStation API client.
        
        This sets up the complete API client with all modules configured
        and ready to use. No API calls are made during initialization -
        calls happen lazily when you use the modules.
        
        The initialization process:
        1. Load configuration (credentials from .env, select API URL)
        2. Create authenticated HTTP client
        3. Initialize specialized modules (market_data, account, orders)
        4. All modules share the same client for authentication
        
        Args:
            environment: Either 'sim' or 'prod' (default: 'sim')
                        This only determines the API URL - credentials
                        come from the single .env file.
                        
                        'sim':  Paper trading at sim-api.tradestation.com
                        'prod': Live trading at api.tradestation.com (REAL MONEY ⚠️)
        
        Raises:
            ValueError: If environment is invalid or .env file missing
                       If required credentials are not set
        
        Example:
            >>> # Simulation (default, safe)
            >>> api = TradeStationAPI('sim')
            >>> api = TradeStationAPI()  # Same as above
            >>> 
            >>> # Production (REAL MONEY)
            >>> api = TradeStationAPI('prod')
            >>> 
            >>> # Access modules
            >>> bars = api.market_data.get_bars('@ES', interval=5, bars_back=10)
            >>> balances = api.account.get_balances(api.config.account_id)
            >>> orders_data = api.orders.get_orders(api.config.account_id)
        
        Note:
            - No API calls during __init__ (fast initialization)
            - All modules are ready to use immediately
            - Token refresh happens automatically on first API call
            - Safe to create multiple instances (each has its own token cache)
        """
        # ============================================================
        # STEP 1: Initialize Configuration
        # ============================================================
        # Load environment-specific configuration including:
        # - API credentials (key, secret, refresh token)
        # - Account ID
        # - API base URL (sim-api or api based on environment)
        # This may print a warning if using production environment
        self.config = TradeStationConfig(environment)
        
        # ============================================================
        # STEP 2: Initialize Base Client
        # ============================================================
        # Create the HTTP client that all modules will share
        # The client handles:
        # - OAuth token management (automatic refresh)
        # - HTTP requests (GET, POST, DELETE)
        # - Streaming connections
        # - Error handling
        self.client = TradeStationClient(self.config)
        
        # ============================================================
        # STEP 3: Initialize API Modules
        # ============================================================
        # Create specialized modules for different API categories
        # All modules share the same client for consistent authentication
        
        # Market Data Module: Bar charts, quotes, streaming data
        self.market_data = MarketData(self.client)
        
        # Account Module: Balances, positions, account info
        self.account = Account(self.client)
        
        # Orders Module: Order placement, tracking, cancellation
        self.orders = Orders(self.client)
        
        # ============================================================
        # STEP 4: Expose Authentication Handler
        # ============================================================
        # Provide direct access to auth for advanced use cases
        # (e.g., getting authorization URL for initial setup)
        # This is an alias to client.auth for convenience
        self.auth = self.client.auth
    
    def get_authorization_url(self) -> str:
        """
        Get the authorization URL for initial OAuth setup.
        
        This is a convenience method for the initial OAuth flow. Use this when
        setting up the library for the first time to get a refresh token.
        
        OAuth Setup Process:
        1. Call this method to get authorization URL
        2. Open URL in browser and log in to TradeStation
        3. Copy authorization code from redirect URL
        4. Exchange code for tokens using auth.exchange_code_for_tokens()
        5. Save refresh token to your .env file
        
        After initial setup, you don't need this method - the library will
        use the refresh token from your .env file automatically.
        
        Returns:
            Authorization URL to visit in browser for authentication
        
        Example:
            >>> # Initial setup: Get authorization URL
            >>> api = TradeStationAPI('sim')
            >>> auth_url = api.get_authorization_url()
            >>> print(f"Visit this URL:\n{auth_url}")
            >>> 
            >>> # User logs in via browser, gets redirected with code
            >>> # URL will be: http://localhost:3000?code=ABC123...
            >>> 
            >>> # Exchange the code for tokens
            >>> code = 'ABC123...'  # From redirect URL
            >>> tokens = api.auth.exchange_code_for_tokens(code)
            >>> print(f"Refresh Token: {tokens['refresh_token']}")
            >>> print("Save this to your .env.sim file as REFRESH_TOKEN")
        
        Note:
            - This is only needed for initial setup
            - After saving refresh token to .env, you don't need this
            - Refresh tokens last indefinitely (until revoked)
            - Same OAuth flow works for sim and prod environments
        """
        # Delegate to the auth module's method
        # This generates the OAuth authorization URL with all required parameters
        return self.auth.get_authorization_url()
    
    def __repr__(self) -> str:
        """
        String representation of the API client.
        
        Provides a readable summary of the API configuration without
        exposing sensitive credentials.
        
        Returns:
            String showing environment, base URL, and account ID
        
        Example:
            >>> api = TradeStationAPI('sim')
            >>> print(api)
            TradeStationAPI(TradeStationConfig(environment='sim', 
                          base_url='https://sim-api.tradestation.com', 
                          account_id='SIM2977785M'))
        """
        # Show the config details (config.__repr__ safely excludes secrets)
        return f"TradeStationAPI({self.config})"


# ============================================================
# Public API Exports
# ============================================================
# Define what gets exported when someone does:
# from tradestation.api import *
#
# We export both high-level (TradeStationAPI) and low-level
# components for different use cases:
#
# High-level (most users):
#   - TradeStationAPI: Complete API client
#
# Low-level (advanced users):
#   - TradeStationConfig: Configuration management
#   - TradeStationAuth: OAuth authentication
#   - TradeStationClient: HTTP client
#   - MarketData, Account, Orders: Individual modules
#
# Utilities:
#   - get_config: Convenience function
#   - EnvironmentType: Type hint for environments
#
__all__ = [
    # Main API class (recommended for most users)
    'TradeStationAPI',
    
    # Configuration and authentication
    'TradeStationConfig',      # Environment and credentials management
    'TradeStationAuth',        # OAuth 2.0 authentication handler
    'TradeStationClient',      # HTTP client with auto-token-refresh
    
    # Specialized API modules
    'MarketData',              # Market data and streaming
    'Account',                 # Account info and positions
    'Orders',                  # Order execution and management
    
    # Utilities
    'get_config',              # Convenience function for creating config
    'EnvironmentType'          # Type hint: Literal['sim', 'dev', 'prod']
]
