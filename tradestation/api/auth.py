"""
TradeStation API Authentication Module

This module handles OAuth 2.0 authentication for the TradeStation API.
It manages the complete authentication flow from initial authorization
through token refresh for ongoing API access.

OAuth 2.0 Flow Overview:
    1. Initial Setup (one-time):
       - User visits authorization URL in browser
       - User logs in with TradeStation credentials
       - TradeStation redirects back with authorization code
       - Exchange authorization code for access + refresh tokens
       - Store refresh token in .env file
    
    2. Regular Usage:
       - Use refresh token to get new access tokens
       - Access tokens expire after 20 minutes
       - Refresh tokens last indefinitely (until revoked)

TradeStation OAuth Endpoints (from API spec):
    - Authorization: https://signin.tradestation.com/authorize
    - Token Exchange: https://signin.tradestation.com/oauth/token
    - Token Revoke: https://signin.tradestation.com/oauth/revoke

Security Notes:
    - Access tokens are short-lived (20 minutes) for security
    - Refresh tokens persist until explicitly revoked
    - Never expose refresh tokens in logs or version control
    - Same OAuth flow works for both sim and prod environments
"""

import requests
from typing import Dict, Any
from .config import TradeStationConfig


class TradeStationAuth:
    """
    OAuth 2.0 authentication handler for TradeStation API.
    
    This class manages the complete authentication lifecycle:
    - Generating authorization URLs for initial setup
    - Exchanging authorization codes for tokens
    - Refreshing access tokens as they expire
    
    The TradeStation API uses OAuth 2.0 with bearer tokens (per OpenAPI spec).
    All API requests must include a valid access token in the Authorization header.
    
    Token Lifetimes:
        - Access Token: 20 minutes (short-lived for security)
        - Refresh Token: Indefinite (until manually revoked)
    
    Attributes:
        config: TradeStationConfig with API credentials
        _access_token: Cached access token (private, auto-refreshed)
    """
    
    # OAuth endpoints from TradeStation's authentication service
    # These are the same for both simulation and production APIs
    AUTH_URL = "https://signin.tradestation.com/authorize"
    TOKEN_URL = "https://signin.tradestation.com/oauth/token"
    
    # Redirect URI for OAuth flow
    # After authentication, TradeStation redirects to this URL with the auth code
    # localhost:3000 is a common choice for local development
    REDIRECT_URI = "http://localhost:3000"
    
    def __init__(self, config: TradeStationConfig):
        """
        Initialize authentication handler with configuration.
        
        Args:
            config: TradeStationConfig instance containing:
                   - client_id: API key from TradeStation Developer Portal
                   - client_secret: API secret from TradeStation Developer Portal
                   - refresh_token: OAuth refresh token (from initial setup)
        
        Note:
            No API calls are made during initialization. Tokens are only
            requested when explicitly needed via get_access_token().
        """
        # Store the config for accessing credentials
        self.config = config
        
        # Cache for access token to avoid unnecessary token refreshes
        # None means no token cached yet (will be fetched on first use)
        self._access_token = None
    
    def get_authorization_url(self) -> str:
        """
        Generate the authorization URL for initial OAuth flow.
        
        This is Step 1 of the OAuth flow, used only during initial setup.
        Open this URL in a browser to authenticate with TradeStation.
        After successful login, TradeStation will redirect to REDIRECT_URI
        with an authorization code in the URL parameters.
        
        Example Flow:
            1. Get URL: auth_url = auth.get_authorization_url()
            2. Open in browser: User logs in to TradeStation
            3. Redirected to: http://localhost:3000?code=ABC123...
            4. Extract code: authorization_code = 'ABC123...'
            5. Exchange: tokens = auth.exchange_code_for_tokens(authorization_code)
        
        Returns:
            Authorization URL to visit in browser
        
        OAuth Scopes Requested:
            - openid: Basic authentication
            - MarketData: Access to market data
            - profile: User profile information
            - ReadAccount: Read account balances and positions
            - Trade: Place and manage orders
            - offline_access: Get refresh tokens (allows token refresh)
            - Matrix: Advanced order types
            - OptionSpreads: Options trading
        """
        # Define the OAuth scopes we need for the API
        # Each scope grants access to specific API endpoints
        scopes = [
            "openid",           # Basic OpenID Connect authentication
            "MarketData",       # Access to market data endpoints
            "profile",          # User profile information
            "ReadAccount",      # Read account info (balances, positions)
            "Trade",            # Place and manage orders
            "offline_access",   # Get refresh tokens (crucial for long-term access)
            "Matrix",           # Matrix-style order entry
            "OptionSpreads"     # Options spread trading
        ]
        
        # Build the OAuth authorization URL parameters
        # These follow the standard OAuth 2.0 authorization code flow
        params = {
            "response_type": "code",                        # We want an authorization code
            "client_id": self.config.client_id,            # Our API key
            "audience": "https://api.tradestation.com",    # Target API audience
            "redirect_uri": self.REDIRECT_URI,             # Where to send the auth code
            "scope": " ".join(scopes)                      # Space-separated list of scopes
        }
        
        # Build query string manually to handle URL encoding properly
        # TradeStation requires specific encoding for audience and redirect_uri
        query_parts = []
        for key, value in params.items():
            if key == "audience":
                # Manually encode the audience URL
                # :// becomes %3A%2F%2F and / becomes %2F
                # This ensures TradeStation correctly parses the audience parameter
                query_parts.append(f"{key}={value.replace('://', '%3A%2F%2F').replace('/', '%2F')}")
            elif key == "redirect_uri":
                # Same manual encoding for redirect_uri
                # TradeStation needs this specific encoding format
                query_parts.append(f"{key}={value.replace('://', '%3A%2F%2F').replace('/', '%2F')}")
            else:
                # Other parameters don't need special encoding
                query_parts.append(f"{key}={value}")
        
        # Construct the full authorization URL
        # Format: https://signin.tradestation.com/authorize?param1=value1&param2=value2...
        return f"{self.AUTH_URL}?{'&'.join(query_parts)}"
    
    def exchange_code_for_tokens(self, authorization_code: str) -> Dict[str, Any]:
        """
        Exchange authorization code for access and refresh tokens.
        
        This is Step 2 of the OAuth flow, used only during initial setup.
        After the user authenticates via browser, TradeStation provides an
        authorization code. This method exchanges that code for actual tokens.
        
        The response includes:
            - access_token: Short-lived token for API requests (20 min)
            - refresh_token: Long-lived token to get new access tokens
            - token_type: Should be "Bearer"
            - expires_in: Seconds until access_token expires (typically 1200)
        
        Args:
            authorization_code: The code received from the redirect URL after
                              the user authenticated. This is a one-time-use code
                              that must be exchanged within a few minutes.
        
        Returns:
            Dictionary containing tokens and metadata:
            {
                'access_token': 'eyJhbGci...',
                'refresh_token': 'def50200...',
                'token_type': 'Bearer',
                'expires_in': 1200,
                'id_token': '...'
            }
        
        Raises:
            Exception: If the token exchange fails. Common errors include:
                      - invalid_grant: Authorization code expired or already used
                      - invalid_client: Wrong API key or secret
                      - invalid_request: Malformed request
        
        Important:
            Save the refresh_token to your .env file! This is what you'll use
            for all future authentication. The authorization code can only be
            used once.
        
        Example:
            >>> code = 'abc123def456...'  # From redirect URL
            >>> tokens = auth.exchange_code_for_tokens(code)
            >>> print(f"Save this: {tokens['refresh_token']}")
        """
        # Build the token exchange request payload
        # This follows the OAuth 2.0 authorization code grant flow
        # Content-Type must be application/x-www-form-urlencoded (per OpenAPI spec)
        payload = (
            f"grant_type=authorization_code&"           # Type of OAuth grant
            f"client_id={self.config.client_id}&"      # Our API key
            f"client_secret={self.config.client_secret}&"  # Our API secret
            f"code={authorization_code}&"               # One-time authorization code
            f"redirect_uri={self.REDIRECT_URI}"        # Must match the one from authorize
        )
        
        # Set the required content type for OAuth token requests
        # TradeStation expects form-urlencoded, not JSON
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        # Make the token exchange request to TradeStation
        response = requests.post(self.TOKEN_URL, headers=headers, data=payload)
        response_data = response.json()
        
        # Check if the OAuth server returned an error
        # Errors come back as JSON with an "error" field
        if "error" in response_data:
            # Extract error details for a helpful exception message
            error_type = response_data.get('error')
            error_desc = response_data.get('error_description', 'No description')
            
            raise Exception(
                f"Token exchange failed: {error_type} - {error_desc}"
            )
        
        # Return the full token response
        # The caller should save refresh_token to their .env file
        return response_data
    
    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        This is the method used for regular API access. It handles token
        refresh automatically, so callers don't need to worry about token
        expiration. Access tokens expire after 20 minutes, so we refresh
        them using the long-lived refresh token.
        
        Token Caching Strategy:
            - First call: Fetches new token from TradeStation
            - Subsequent calls: Returns cached token (no network call)
            - force_refresh=True: Always fetches fresh token
        
        Note: For simplicity, we don't track token expiration time.
        A production system might cache the token and only refresh when
        it's about to expire (expires_in - buffer). For now, we just
        cache it and let the API tell us if it's expired (via 401 error).
        
        Args:
            force_refresh: If True, always fetch a new token even if one is cached.
                          Useful if you suspect the cached token is expired or invalid.
        
        Returns:
            Valid access token (JWT format) to use in API request headers
        
        Raises:
            Exception: If token refresh fails. Common errors include:
                      - invalid_grant: Refresh token is invalid or revoked
                      - invalid_client: Wrong API key or secret
        
        Example:
            >>> # Normal usage (with caching)
            >>> token = auth.get_access_token()
            >>> headers = {'Authorization': f'Bearer {token}'}
            
            >>> # Force refresh (ignore cache)
            >>> token = auth.get_access_token(force_refresh=True)
        """
        # Check if we have a cached token and force_refresh is False
        # If so, return the cached token without making a network call
        if not force_refresh and self._access_token:
            return self._access_token
        
        # Build the token refresh request payload
        # This uses the refresh_token grant type to get a new access token
        # The refresh token itself doesn't expire (unless revoked)
        payload = (
            f"grant_type=refresh_token&"                # OAuth grant type for refresh
            f"client_id={self.config.client_id}&"       # Our API key
            f"client_secret={self.config.client_secret}&"  # Our API secret
            f"refresh_token={self.config.refresh_token}"   # Long-lived refresh token
        )
        
        # Set the required content type for OAuth token requests
        # Must be application/x-www-form-urlencoded per OAuth 2.0 spec
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        # Request a new access token from TradeStation's OAuth server
        response = requests.post(self.TOKEN_URL, headers=headers, data=payload)
        response_data = response.json()
        
        # Check if the OAuth server returned an error
        # Common error: invalid_grant means the refresh token is no longer valid
        if "error" in response_data:
            error_type = response_data.get('error')
            error_desc = response_data.get('error_description', 'No description')
            
            raise Exception(
                f"Token refresh failed: {error_type} - {error_desc}"
            )
        
        # Extract and cache the new access token
        # The response also contains expires_in (1200 seconds = 20 minutes)
        # but we don't track it for simplicity
        self._access_token = response_data["access_token"]
        
        # Return the fresh access token
        return self._access_token
    
    def clear_token_cache(self) -> None:
        """
        Clear the cached access token, forcing a refresh on next use.
        
        This is useful if you know the cached token is invalid (e.g., after
        receiving a 401 Unauthorized response from the API). After clearing
        the cache, the next call to get_access_token() will fetch a fresh token.
        
        Example:
            >>> # If API returns 401 Unauthorized
            >>> auth.clear_token_cache()
            >>> token = auth.get_access_token()  # Fetches fresh token
        """
        self._access_token = None
