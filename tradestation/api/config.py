"""
TradeStation API Configuration Module

This module manages configuration for the TradeStation API. It loads credentials
from a single .env file and allows environment selection (sim vs prod) to determine
which API endpoint to use.

Key Features:
    - Single .env file for all credentials
    - Environment parameter selects API URL (sim-api vs api)
    - Automatic credential validation on load
    - Safety-first defaults (always 'sim' unless explicitly overridden)

TradeStation API Requirements:
    - All API calls require OAuth 2.0 bearer token authentication
    - Same credentials work for both simulation and production APIs
    - The only difference between sim and prod is the base URL

Environment Selection:
    - 'sim': Paper trading at https://sim-api.tradestation.com
    - 'prod': Live trading at https://api.tradestation.com (REAL MONEY)
"""

import os
from typing import Literal
from pathlib import Path
import dotenv


# Type hint for environment names - restricts to valid options
# Removed 'dev' since it's redundant with 'sim'
EnvironmentType = Literal['sim', 'prod']


class TradeStationConfig:
    """
    Configuration manager for TradeStation API.
    
    This class loads credentials from a single .env file and provides the
    appropriate API base URL based on the environment parameter. Since
    TradeStation uses the same credentials for both sim and prod, we only
    need one .env file - the environment parameter just changes the URL.
    
    Attributes:
        environment: Current environment name ('sim' or 'prod')
        client_id: TradeStation API key (from .env file)
        client_secret: TradeStation API secret (from .env file)
        refresh_token: OAuth refresh token (lasts indefinitely)
        account_id: TradeStation account ID (e.g., 'SIM2977785M' or real ID)
        base_url: API base URL (sim-api or api based on environment)
    """
    
    def __init__(self, environment: EnvironmentType = 'sim'):
        """
        Initialize configuration with environment selection.
        
        The initialization process:
        1. Store environment selection (defaults to 'sim' for safety)
        2. Load credentials from .env file
        3. Extract and validate required credentials
        4. Set the appropriate API base URL based on environment
        5. Display warning if using production environment
        
        Args:
            environment: Either 'sim' or 'prod' (default: 'sim')
                        
                        'sim'  - Paper trading (simulation API)
                        'prod' - Live trading (production API) ⚠️ REAL MONEY
        
        Raises:
            ValueError: If environment is invalid or .env file missing
                       or required credentials are not set.
        
        Example:
            >>> # Simulation (safe, default)
            >>> config = TradeStationConfig('sim')
            >>> config = TradeStationConfig()  # Also defaults to 'sim'
            
            >>> # Production (REAL MONEY)
            >>> config = TradeStationConfig('prod')
        """
        # ============================================================
        # STEP 1: Validate and Store Environment
        # ============================================================
        # Default to 'sim' if not provided (safest option)
        if environment is None:
            environment = 'sim'
        
        # Validate that the environment is one of the allowed values
        # This prevents typos like 'production' instead of 'prod'
        if environment not in ('sim', 'prod'):
            raise ValueError(
                f"Invalid environment '{environment}'. Must be 'sim' or 'prod'."
            )
        
        # Store the environment name - this determines the API URL
        self.environment = environment
        
        # ============================================================
        # STEP 2: Load Credentials from .env File
        # ============================================================
        # Load credentials from the single .env file
        # This populates os.environ with the values from the file
        self._load_environment_file()
        
        # ============================================================
        # STEP 3: Extract and Validate Credentials
        # ============================================================
        # These are required for OAuth authentication with TradeStation
        # All values come from the .env file we just loaded
        
        # API Key - Get from TradeStation Developer Portal
        self.client_id = self._get_required_env('TRADESTATION_API_KEY')
        
        # API Secret - Get from TradeStation Developer Portal
        self.client_secret = self._get_required_env('TRADESTATION_SECRET')
        
        # Refresh Token - Obtained via OAuth flow (lasts indefinitely by default)
        # See auth.py for how to get this token initially
        self.refresh_token = self._get_required_env('REFRESH_TOKEN')
        
        # Account ID - Your TradeStation account identifier
        # Format: 'SIM#######M' for simulation, real ID for production
        if self.environment == 'sim':
            self.account_id = self._get_required_env('ACCOUNT_ID_SIM')
        else:
            self.account_id = self._get_required_env('ACCOUNT_ID_PROD')
        
        # ============================================================
        # STEP 4: Set API Base URL Based on Environment
        # ============================================================
        # The environment parameter determines which API endpoint to use
        self.base_url = self._get_base_url()
        
        # ============================================================
        # STEP 5: Safety Warning for Production
        # ============================================================
        # If using production environment, display a prominent warning
        # This helps prevent accidentally running test code with real money
        if self.environment == 'prod':
            print("=" * 80)
            print("⚠️  WARNING: RUNNING IN PRODUCTION MODE - REAL MONEY AT RISK ⚠️")
            print("=" * 80)
    
    def _load_environment_file(self) -> None:
        """
        Load the .env file into os.environ.
        
        This method loads the single .env file from the project root.
        Since TradeStation uses the same credentials for both sim and prod,
        we only need one .env file - the environment parameter determines
        which API URL to use.
        
        File Structure:
            project_root/
            ├── .env          <- Single credentials file
            └── tradestation/
                └── api/
                    └── config.py  <- We are here
        
        Raises:
            ValueError: If the .env file doesn't exist. This typically means
                       the user hasn't set up their credentials yet.
        
        Note:
            The .env file is ignored by git (via .gitignore) to prevent
            accidental credential exposure. Only .env.example is checked into git.
        """
        # Calculate the project root directory path
        # __file__ is this file (config.py)
        # .parent gets the directory containing this file (api/)
        # .parent.parent gets two levels up (project root)
        project_root = Path(__file__).parent.parent.parent
        
        # Construct the full path to the .env file
        # There's only one .env file now (simplified from .env.sim, .env.dev, .env.prod)
        env_file = project_root / '.env'
        
        # Check if the file exists before trying to load it
        # This gives us a chance to provide a helpful error message
        if not env_file.exists():
            raise ValueError(
                f"Environment file '{env_file}' does not exist.\n"
                f"Please create it based on .env.example template.\n"
                f"The same .env file is used for both 'sim' and 'prod' environments."
            )
        
        # Load the environment file into os.environ
        # After this call, os.getenv() will return values from the file
        dotenv.load_dotenv(env_file)
        
        # Confirm successful load to the user
        print(f"✓ Loaded configuration from: {env_file.name}")
        print(f"✓ Environment: {self.environment}")
    
    def _get_required_env(self, key: str) -> str:
        """
        Get a required environment variable with validation.
        
        This method retrieves an environment variable and ensures it's set and non-empty.
        If the variable is missing or empty, it raises a clear error message directing
        the user to check their .env file.
        
        Args:
            key: Environment variable name (e.g., 'TRADESTATION_API_KEY')
        
        Returns:
            The environment variable value (guaranteed to be non-empty)
        
        Raises:
            ValueError: If the environment variable is not set or empty.
                       Error message includes the variable name and file to check.
        
        Note:
            This method is called after _load_environment_file(), so the environment
            variables should be populated from the .env file at this point.
        """
        # Try to get the environment variable
        # Returns None if not set
        value = os.getenv(key)
        
        # Check if the value exists and is not empty
        # Empty strings are falsy in Python, so this catches both None and ""
        if not value:
            raise ValueError(
                f"Required environment variable '{key}' is not set in .env\n"
                f"Please check your .env file."
            )
        
        return value
    
    def _get_base_url(self) -> str:
        """
        Get the appropriate API base URL for the current environment.
        
        TradeStation provides two API endpoints:
        - Production API: https://api.tradestation.com
          Used for live trading with real money
        
        - Simulation API: https://sim-api.tradestation.com
          Used for paper trading (no real money)
        
        The simulation API mirrors the production API functionality but routes
        orders to a paper trading account. It's safe for testing and development.
        
        Returns:
            Base URL for TradeStation API (without trailing slash)
        
        Note:
            The same OAuth credentials work for both sim and prod APIs.
            The account ID determines whether trades are simulated or real.
        """
        if self.environment == 'prod':
            # Production API - REAL MONEY
            return "https://api.tradestation.com"
        else:
            # Simulation API - PAPER TRADING
            # This is the default (when environment='sim')
            return "https://sim-api.tradestation.com"
    
    def __repr__(self) -> str:
        """
        String representation of the config (safely excludes secrets).
        
        This method provides a readable representation of the config object
        without exposing sensitive credentials (API key, secret, refresh token).
        
        Returns:
            String representation showing environment, base URL, and account ID
        """
        return (
            f"TradeStationConfig("
            f"environment='{self.environment}', "
            f"base_url='{self.base_url}', "
            f"account_id='{self.account_id}')"
        )


def get_config(environment: EnvironmentType = 'sim') -> TradeStationConfig:
    """
    Convenience function to get a TradeStation configuration.
    
    This is a simple wrapper around TradeStationConfig() that can be used
    when you want a more functional style of creating config objects.
    
    Args:
        environment: Either 'sim' or 'prod' (default: 'sim')
    
    Returns:
        TradeStationConfig instance with loaded credentials
    
    Example:
        >>> config = get_config('sim')
        >>> print(config.base_url)
        https://sim-api.tradestation.com
    """
    return TradeStationConfig(environment)
