"""
TradeStation API Base Client

This module provides the core HTTP client for making requests to the TradeStation API.
It handles authentication, request formatting, response parsing, and error handling,
abstracting away the complexity of API communication.

Key Features:
    - Automatic access token management (refresh when needed)
    - Consistent error handling across all endpoints
    - Support for both regular and streaming endpoints
    - Clean HTTP method abstractions (GET, POST, DELETE)
    - JSON request/response handling

The client uses the requests library and follows REST API patterns.
All API endpoints require bearer token authentication (per OpenAPI spec).
"""

import requests
from typing import Dict, Any, Optional, Iterator
from .config import TradeStationConfig
from .auth import TradeStationAuth


class TradeStationClient:
    """
    Base HTTP client for TradeStation API with automatic token management.
    
    This class serves as the foundation for all API interactions. It provides
    low-level HTTP methods (GET, POST, DELETE, stream) that handle authentication
    and error checking automatically.
    
    The higher-level modules (MarketData, Account, Orders) use this client
    to make API calls without worrying about authentication or error handling.
    
    Responsibilities:
        - Manage authentication (get fresh tokens as needed)
        - Construct full API URLs from endpoints
        - Set required headers (Authorization, Content-Type)
        - Parse JSON responses
        - Handle HTTP errors consistently
        - Support streaming endpoints for real-time data
    
    Attributes:
        config: TradeStationConfig with environment and credentials
        auth: TradeStationAuth for token management
    """
    
    def __init__(self, config: TradeStationConfig = None):
        """
        Initialize the TradeStation API client.
        
        Args:
            config: TradeStationConfig instance with credentials and base URL.
                   If None, creates a default config (which loads from .env.sim)
        
        Note:
            No API calls are made during initialization. The client is ready
            to use immediately, and authentication happens automatically on
            the first API request.
        """
        # If no config provided, create one with default settings
        # This will load .env.sim by default (safest option)
        if config is None:
            config = TradeStationConfig()
        
        # Store the configuration (contains base URL and account info)
        self.config = config
        
        # Create authentication handler for managing OAuth tokens
        # This handles getting and refreshing access tokens as needed
        self.auth = TradeStationAuth(config)
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authorization headers with a fresh access token.
        
        This private method is called before every API request to ensure we
        always have a valid access token. If the cached token is present, it's
        reused. If not, a new one is fetched via the refresh token.
        
        The TradeStation API requires bearer token authentication (per OpenAPI spec).
        All requests must include: Authorization: Bearer <access_token>
        
        Returns:
            Dictionary with Authorization header:
            {'Authorization': 'Bearer eyJhbGci...'}
        
        Note:
            Access tokens expire after 20 minutes, but the auth module
            handles caching and refresh automatically. This method always
            returns a valid token (or raises an exception if refresh fails).
        """
        # Get a valid access token (from cache or via refresh)
        # The auth module handles expiration automatically
        access_token = self.auth.get_access_token()
        
        # Format the Authorization header per OAuth 2.0 bearer token spec
        # Format: "Bearer <token>" (note the space after "Bearer")
        return {"Authorization": f"Bearer {access_token}"}
    
    def _handle_response(self, response: requests.Response) -> Any:
        """
        Handle API response and parse JSON, with consistent error handling.
        
        This private method processes HTTP responses from the TradeStation API.
        It checks for errors, extracts error messages, and parses JSON responses.
        
        Error Handling:
            - HTTP 4xx/5xx: Raises exception with error details
            - Tries to parse error as JSON first (structured errors)
            - Falls back to raw text if JSON parsing fails
            - Handles empty responses gracefully (some endpoints return no body)
        
        Args:
            response: requests.Response object from an API call
        
        Returns:
            Parsed JSON response (typically a dict or list)
            Returns None if the response body is empty
        
        Raises:
            Exception: If the response indicates an error (non-2xx status code)
                      Exception message includes status code and error details
        
        Common HTTP Status Codes:
            - 200 OK: Success
            - 400 Bad Request: Invalid parameters
            - 401 Unauthorized: Invalid/expired token
            - 404 Not Found: Endpoint or resource doesn't exist
            - 500 Internal Server Error: TradeStation server error
        """
        # ============================================================
        # Check for HTTP Errors (non-2xx status codes)
        # ============================================================
        # response.ok is True only for 2xx status codes
        if not response.ok:
            # Try to extract a structured error message from JSON response
            try:
                error_data = response.json()
                # TradeStation returns errors in different formats
                # Try 'Message' first (common in API errors), then 'error'
                error_msg = error_data.get('Message', error_data.get('error', 'Unknown error'))
            except:
                # If JSON parsing fails, use raw text or just the status code
                error_msg = response.text or f"HTTP {response.status_code}"
            
            # Raise a descriptive exception with status code and message
            raise Exception(
                f"API request failed ({response.status_code}): {error_msg}"
            )
        
        # ============================================================
        # Parse JSON Response
        # ============================================================
        # Try to parse the response as JSON
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            # Some endpoints return empty responses (e.g., successful DELETE)
            # This is normal - return None instead of raising an error
            return None
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Make a GET request to the API.
        
        GET requests are used for retrieving data without side effects.
        They're the most common type of API request.
        
        Use GET for:
            - Fetching market data
            - Getting account balances
            - Retrieving positions
            - Listing orders
        
        Args:
            endpoint: API endpoint path (e.g., '/v3/brokerage/accounts')
                     Should start with / and include the version (v2 or v3)
            params: Optional query parameters as a dictionary
                   e.g., {'symbol': 'AAPL', 'interval': '5'}
                   These are URL-encoded and appended to the URL
        
        Returns:
            Parsed JSON response from the API
        
        Example:
            >>> # Get account balances
            >>> response = client.get('/v3/brokerage/accounts/SIM123/balances')
            >>> print(response['CashBalance'])
            
            >>> # Get bars with query parameters
            >>> params = {'unit': 'Minute', 'interval': '5', 'barsback': '10'}
            >>> response = client.get('/v3/marketdata/barcharts/@ES', params=params)
        """
        # Construct the full URL by combining base URL with endpoint
        # e.g., "https://sim-api.tradestation.com" + "/v3/brokerage/accounts"
        url = f"{self.config.base_url}{endpoint}"
        
        # Get authorization headers with a valid access token
        headers = self._get_auth_headers()
        
        # Make the GET request
        # params are automatically URL-encoded by requests library
        response = requests.get(url, headers=headers, params=params)
        
        # Parse and return the response (or raise exception if error)
        return self._handle_response(response)
    
    def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Make a POST request to the API.
        
        POST requests are used for creating resources or performing actions.
        They typically include a request body with data.
        
        Use POST for:
            - Placing orders
            - Confirming orders (dry-run)
            - Creating resources
        
        Content Types:
            - If json_data is provided: Content-Type is application/json
            - If data is provided: Content-Type defaults to form-urlencoded
        
        Args:
            endpoint: API endpoint path (e.g., '/v3/orderexecution/orders')
            data: Optional form data (for form-urlencoded requests)
                 Typically not used with TradeStation API
            json_data: Optional JSON data (for JSON requests)
                      This is the most common format for TradeStation
                      e.g., {'AccountID': 'SIM123', 'Symbol': 'AAPL', ...}
        
        Returns:
            Parsed JSON response from the API
        
        Example:
            >>> # Place a market order
            >>> order_data = {
            ...     'AccountID': 'SIM123',
            ...     'Symbol': 'AAPL',
            ...     'Quantity': '10',
            ...     'OrderType': 'Market',
            ...     'TradeAction': 'BUY'
            ... }
            >>> response = client.post('/v3/orderexecution/orders', json_data=order_data)
        """
        # Construct the full URL
        url = f"{self.config.base_url}{endpoint}"
        
        # Get authorization headers with a valid access token
        headers = self._get_auth_headers()
        
        # Determine request format and make the request
        if json_data is not None:
            # JSON request - most common for TradeStation API
            # Set Content-Type header to application/json
            headers["Content-Type"] = "application/json"
            
            # requests.post with json= parameter automatically:
            # 1. Serializes the dict to JSON
            # 2. Sets Content-Type to application/json
            # 3. Sends as request body
            response = requests.post(url, headers=headers, json=json_data)
        else:
            # Form data request (less common with TradeStation)
            # Content-Type will be application/x-www-form-urlencoded
            response = requests.post(url, headers=headers, data=data)
        
        # Parse and return the response (or raise exception if error)
        return self._handle_response(response)
    
    def delete(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Make a DELETE request to the API.
        
        DELETE requests are used for removing resources.
        They typically don't include a request body.
        
        Use DELETE for:
            - Canceling orders
            - Removing resources
        
        Args:
            endpoint: API endpoint path (e.g., '/v3/orderexecution/orders/12345')
                     Often includes a resource ID in the path
            params: Optional query parameters (rarely used with DELETE)
        
        Returns:
            Parsed JSON response from the API
            May return None if the response body is empty (common with DELETE)
        
        Example:
            >>> # Cancel an order
            >>> order_id = '123456'
            >>> response = client.delete(f'/v3/orderexecution/orders/{order_id}')
        """
        # Construct the full URL
        url = f"{self.config.base_url}{endpoint}"
        
        # Get authorization headers with a valid access token
        headers = self._get_auth_headers()
        
        # Make the DELETE request
        response = requests.delete(url, headers=headers, params=params)
        
        # Parse and return the response (or raise exception if error)
        # DELETE often returns an empty body, which will be None
        return self._handle_response(response)
    
    def stream(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Iterator[bytes]:
        """
        Make a streaming GET request to the API.
        
        Streaming endpoints provide real-time data updates. Instead of returning
        a complete response at once, they keep the connection open and send data
        as it becomes available.
        
        TradeStation Streaming Endpoints (from OpenAPI spec):
            - Market data streams: /v3/marketdata/stream/...
            - Position updates: /v3/brokerage/stream/accounts/{id}/positions
            - Order updates: /v3/brokerage/stream/accounts/{id}/orders
            - Tick bars: /v2/stream/tickbars/...
        
        How Streaming Works:
            1. Request is made with stream=True
            2. Connection stays open indefinitely
            3. Server sends data line by line as events occur
            4. We yield each line to the caller
            5. Connection closes when caller stops iterating
        
        Use streaming for:
            - Real-time market data
            - Live position updates
            - Order status changes
            - Continuous data feeds
        
        Args:
            endpoint: API endpoint path (typically starts with '/stream/')
                     e.g., '/v3/marketdata/stream/barcharts/@ES'
            params: Optional query parameters
                   e.g., {'unit': 'Minute', 'interval': '5'}
        
        Yields:
            Lines of data from the stream (as bytes)
            Each line is typically a JSON object representing an event
        
        Raises:
            Exception: If the stream connection fails immediately
        
        Example:
            >>> # Stream real-time bar data
            >>> params = {'unit': 'Minute', 'interval': '5', 'barsback': '10'}
            >>> stream = client.stream('/v3/marketdata/stream/barcharts/@ES', params=params)
            >>> 
            >>> # Process streaming data
            >>> for line in stream:
            ...     data = json.loads(line)
            ...     print(f"New bar: Close={data['Close']}")
            ...     # Press Ctrl+C to stop
        
        Note:
            Streaming connections can run indefinitely. Make sure to handle
            KeyboardInterrupt or implement a stop condition to close the stream.
            The connection automatically closes when you stop iterating.
        """
        # Construct the full URL
        url = f"{self.config.base_url}{endpoint}"
        
        # Get authorization headers with a valid access token
        headers = self._get_auth_headers()
        
        # Make the GET request with stream=True
        # This tells requests to keep the connection open and not download everything at once
        response = requests.get(url, headers=headers, params=params, stream=True)
        
        # ============================================================
        # Check for Immediate Errors
        # ============================================================
        # If the stream fails to establish, we get an error right away
        # Check for this before trying to read from the stream
        if not response.ok:
            try:
                error_data = response.json()
                error_msg = error_data.get('Message', error_data.get('error', 'Unknown error'))
            except:
                error_msg = response.text or f"HTTP {response.status_code}"
            
            raise Exception(
                f"Stream request failed ({response.status_code}): {error_msg}"
            )
        
        # ============================================================
        # Yield Lines from the Stream
        # ============================================================
        # iter_lines() reads the response line by line as data arrives
        # Each line is typically a JSON object (you need to parse it)
        for line in response.iter_lines():
            # Skip empty lines (keepalive pings, etc.)
            if line:
                yield line
