"""
TradeStation Orders Module

This module provides access to order execution and management endpoints.
It handles the complete order lifecycle from validation through placement,
monitoring, and cancellation.

⚠️ CRITICAL: Orders placed through this module can execute real trades
with real money if using a production account. Always test with simulation
accounts first.

Order Types Supported:
    - Market: Execute immediately at best available price
    - Limit: Execute only at specified price or better
    - StopMarket: Trigger market order when stop price hit
    - StopLimit: Trigger limit order when stop price hit

Trade Actions:
    - BUY: Open long position or add to existing long
    - SELL: Close long position or reduce existing long
    - SELLSHORT: Open short position or add to existing short
    - BUYTOCOVER: Close short position or reduce existing short

TradeStation API Endpoints Used (from OpenAPI spec):
    - POST /v3/orderexecution/orderconfirm (validate without placing)
    - POST /v3/orderexecution/orders (place order)
    - DELETE /v3/orderexecution/orders/{orderId} (cancel order)
    - GET /v3/brokerage/accounts/{accountId}/orders (current orders)
    - GET /v3/brokerage/accounts/{accountId}/historicalorders (history)
    - GET /v3/brokerage/stream/accounts/{accountId}/orders (real-time)
    - GET /v3/orderexecution/routes (available routes)
"""

from typing import Dict, Any, Optional, Iterator, Literal
from .client import TradeStationClient


class Orders:
    """
    Order execution and management operations for TradeStation API.
    
    This class provides methods for the complete order lifecycle:
    - Validation (confirm without placing)
    - Placement (send order to market)
    - Monitoring (track order status)
    - Cancellation (cancel pending orders)
    - History (review past orders)
    
    ⚠️ WARNING: Methods in this class can place real trades. Always:
    - Use simulation accounts for testing
    - Confirm orders before placing (use confirm_order)
    - Verify account_id is correct
    - Double-check quantity and symbol
    - Understand the order type and time-in-force
    
    Attributes:
        client: TradeStationClient for making authenticated API requests
    """
    
    def __init__(self, client: TradeStationClient):
        """
        Initialize orders handler.
        
        Args:
            client: TradeStationClient instance with authentication configured
        
        Note:
            No API calls are made during initialization. Order methods are
            called explicitly when you want to interact with orders.
        """
        # Store the client for making authenticated API requests
        self.client = client
    
    def confirm_order(
        self,
        account_id: str,
        symbol: str,
        quantity: int,
        action: Literal['BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'],
        order_type: Literal['Market', 'Limit', 'StopMarket', 'StopLimit'] = 'Market',
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: Literal['DAY', 'GTC', 'GTD', 'IOC', 'FOK'] = 'DAY',
        route: str = 'Intelligent'
    ) -> Dict[str, Any]:
        """
        Confirm order without placing it (dry-run validation).
        
        This validates an order and returns estimated costs WITHOUT actually
        placing the order. Use this before place_order() to verify:
        - Order is valid
        - Sufficient buying power
        - Estimated commissions
        - Routing information
        
        This is a SAFE operation - it will NOT place a trade.
        Always confirm complex orders before placing them.
        
        Args:
            account_id: Account ID to validate order for
                       e.g., 'SIM2977785M' for simulation
                       Get from config.account_id or account.get_accounts()
            
            symbol: Symbol to trade
                   - Stocks: 'AAPL', 'MSFT', 'TSLA'
                   - Futures: 'ESZ24', 'NQH25'
                   - ETFs: 'SPY', 'QQQ'
            
            quantity: Number of shares/contracts to trade
                     Must be positive integer
                     e.g., 100 shares, 1 contract
            
            action: Trade action (MUST be uppercase):
                   - 'BUY': Buy to open or add to long position
                   - 'SELL': Sell to close or reduce long position
                   - 'SELLSHORT': Sell short to open or add to short position
                   - 'BUYTOCOVER': Buy to close or reduce short position
            
            order_type: Type of order (default: 'Market'):
                       - 'Market': Execute immediately at market price
                       - 'Limit': Execute at limit_price or better
                       - 'StopMarket': Trigger market order when stop_price hit
                       - 'StopLimit': Trigger limit order when stop_price hit
            
            limit_price: Limit price (required for Limit and StopLimit orders)
                        Price you want or better
                        For buy limit: maximum price you'll pay
                        For sell limit: minimum price you'll accept
            
            stop_price: Stop price (required for StopMarket and StopLimit)
                       Price that triggers the order
                       For buy stop: triggers when market >= stop_price
                       For sell stop: triggers when market <= stop_price
            
            time_in_force: How long order remains active (default: 'DAY'):
                          - 'DAY': Good for trading day (cancels at market close)
                          - 'GTC': Good til canceled (stays active until filled/canceled)
                          - 'GTD': Good til date (specify expiration)
                          - 'IOC': Immediate or cancel (fill immediately or cancel)
                          - 'FOK': Fill or kill (fill completely or cancel)
            
            route: Order routing destination (default: 'Intelligent')
                  'Intelligent' uses TradeStation's smart routing
                  Get available routes with get_routes()
        
        Returns:
            Dictionary containing order validation results:
            {
                'EstimatedCost': 15025.00,
                'EstimatedCommission': 0.00,
                'EstimatedPrice': 150.25,
                'Route': 'Intelligent',
                'AccountID': 'SIM2977785M',
                'Symbol': 'AAPL',
                'Quantity': '100',
                'ProductInfo': {...},
                ...
            }
        
        Example:
            >>> # Confirm a market order before placing
            >>> confirmation = orders.confirm_order(
            ...     account_id='SIM2977785M',
            ...     symbol='AAPL',
            ...     quantity=100,
            ...     action='BUY'
            ... )
            >>> print(f"Estimated Cost: ${confirmation['EstimatedCost']:,.2f}")
            >>> print(f"Commission: ${confirmation['EstimatedCommission']:,.2f}")
            >>> 
            >>> # Confirm a limit order
            >>> confirmation = orders.confirm_order(
            ...     account_id='SIM2977785M',
            ...     symbol='ESZ24',
            ...     quantity=1,
            ...     action='BUY',
            ...     order_type='Limit',
            ...     limit_price=4800.00
            ... )
        
        Note:
            - API endpoint: POST /v3/orderexecution/orderconfirm
            - This does NOT place the order (safe to call)
            - Use the same parameters for place_order() after confirming
            - Validation doesn't guarantee order will fill at estimated price
        """
        # Build the order payload
        # TradeStation API requires specific field names and formats (per OpenAPI spec)
        payload = {
            # Account ID must match exactly
            'AccountID': account_id,
            
            # Symbol - no prefix needed (API handles it)
            'Symbol': symbol,
            
            # Quantity must be a string (per TradeStation API requirement)
            # Even though it's numeric, the API expects it as a string
            'Quantity': str(quantity),
            
            # Order type determines how the order executes
            'OrderType': order_type,
            
            # Trade action - must be uppercase
            'TradeAction': action,
            
            # Time in force is a nested object
            # The API requires this specific structure
            'TimeInForce': {'Duration': time_in_force},
            
            # Order routing - how order is sent to exchange
            'Route': route
        }
        
        # Add limit price if provided
        # Required for Limit and StopLimit orders
        # Must be a string (per API spec)
        if limit_price is not None:
            payload['LimitPrice'] = str(limit_price)
        
        # Add stop price if provided
        # Required for StopMarket and StopLimit orders
        # Must be a string (per API spec)
        if stop_price is not None:
            payload['StopPrice'] = str(stop_price)
        
        # Construct the order confirmation endpoint
        # This endpoint validates without placing
        endpoint = "/v3/orderexecution/orderconfirm"
        
        # Make the POST request with JSON payload
        # Returns validation results without placing the order
        return self.client.post(endpoint, json_data=payload)
    
    def place_order(
        self,
        account_id: str,
        symbol: str,
        quantity: int,
        action: Literal['BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'],
        order_type: Literal['Market', 'Limit', 'StopMarket', 'StopLimit'] = 'Market',
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: Literal['DAY', 'GTC', 'GTD', 'IOC', 'FOK'] = 'DAY',
        route: str = 'Intelligent'
    ) -> Dict[str, Any]:
        """
        Place an order (ACTUAL TRADE - USE WITH CAUTION).
        
        ⚠️ WARNING: This method places a REAL order that can execute immediately.
        
        Before calling this method:
        1. Verify you're using the correct account (sim vs prod)
        2. Confirm the order with confirm_order() first
        3. Double-check symbol, quantity, and action
        4. Understand the order type and its risks
        5. Ensure you have sufficient buying power
        
        For production accounts, this WILL place real trades with real money.
        
        Args:
            account_id: Account ID to place order in
            symbol: Symbol to trade
            quantity: Number of shares/contracts (positive integer)
            action: Trade action (BUY, SELL, BUYTOCOVER, SELLSHORT)
            order_type: Type of order (default: Market)
            limit_price: Limit price (required for Limit and StopLimit)
            stop_price: Stop price (required for StopMarket and StopLimit)
            time_in_force: How long order remains active (default: DAY)
            route: Order routing (default: Intelligent)
        
        Returns:
            Dictionary containing order placement result:
            {
                'OrderID': '123456789',
                'Status': 'Received',
                'Message': 'Order successfully received',
                'Orders': [{
                    'OrderID': '123456789',
                    'Symbol': 'AAPL',
                    'Quantity': '100',
                    'OrderType': 'Market',
                    'Status': 'Received',
                    ...
                }]
            }
        
        Order Status Values:
            - 'Received': Order accepted by TradeStation
            - 'Sent': Order sent to exchange
            - 'Filled': Order completely filled
            - 'PartiallyFilled': Order partially filled
            - 'Rejected': Order rejected
            - 'Canceled': Order canceled
        
        Example:
            >>> # Place a market order (executes immediately)
            >>> result = orders.place_order(
            ...     account_id='SIM2977785M',
            ...     symbol='AAPL',
            ...     quantity=100,
            ...     action='BUY'
            ... )
            >>> order_id = result['OrderID']
            >>> print(f"Order placed: {order_id}")
            >>> 
            >>> # Place a limit order
            >>> result = orders.place_order(
            ...     account_id='SIM2977785M',
            ...     symbol='ESZ24',
            ...     quantity=1,
            ...     action='BUY',
            ...     order_type='Limit',
            ...     limit_price=4800.00
            ... )
            >>> 
            >>> # Place a stop-loss order
            >>> result = orders.place_order(
            ...     account_id='SIM2977785M',
            ...     symbol='AAPL',
            ...     quantity=100,
            ...     action='SELL',
            ...     order_type='StopMarket',
            ...     stop_price=145.00  # Sell if price drops to $145
            ... )
        
        Note:
            - API endpoint: POST /v3/orderexecution/orders
            - Market orders typically execute within seconds
            - Limit orders may not fill if price isn't reached
            - Save the OrderID to track/cancel the order later
            - Use stream_orders() to monitor order status in real-time
        """
        # Build the order payload
        # IDENTICAL structure to confirm_order - only the endpoint differs
        # This ensures confirmed orders match placed orders exactly
        payload = {
            'AccountID': account_id,
            'Symbol': symbol,
            'Quantity': str(quantity),          # String per API spec
            'OrderType': order_type,
            'TradeAction': action,
            'TimeInForce': {'Duration': time_in_force},
            'Route': route
        }
        
        # Add limit price if specified
        # Required for Limit and StopLimit orders
        if limit_price is not None:
            payload['LimitPrice'] = str(limit_price)
        
        # Add stop price if specified
        # Required for StopMarket and StopLimit orders
        if stop_price is not None:
            payload['StopPrice'] = str(stop_price)
        
        # Construct the order placement endpoint
        # This is the REAL endpoint that places trades
        endpoint = "/v3/orderexecution/orders"
        
        # Make the POST request
        # ⚠️ This actually places the order
        return self.client.post(endpoint, json_data=payload)
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order.
        
        Cancels a pending order by its OrderID. Only works on orders that
        haven't been filled yet. You cannot cancel filled orders.
        
        Cancellation is NOT guaranteed - the order might fill before
        the cancel request is processed (race condition).
        
        Cancellable Order States:
            - Received: Order accepted but not yet sent
            - Sent: Order sent to exchange
            - PartiallyFilled: Can cancel remaining quantity
        
        Non-Cancellable Order States:
            - Filled: Already completely filled
            - Rejected: Already rejected
            - Canceled: Already canceled
        
        Args:
            order_id: ID of the order to cancel
                     Get from place_order() response or get_orders()
                     Format: Numeric string (e.g., '123456789')
        
        Returns:
            Dictionary containing cancellation result:
            {
                'OrderID': '123456789',
                'Status': 'Canceled',
                'Message': 'Order canceled successfully'
            }
        
        Example:
            >>> # Place an order
            >>> result = orders.place_order(
            ...     account_id='SIM2977785M',
            ...     symbol='AAPL',
            ...     quantity=100,
            ...     action='BUY',
            ...     order_type='Limit',
            ...     limit_price=150.00
            ... )
            >>> order_id = result['OrderID']
            >>> 
            >>> # Cancel the order
            >>> cancel_result = orders.cancel_order(order_id)
            >>> print(f"Cancellation status: {cancel_result['Status']}")
            >>> 
            >>> # Cancel all open orders for an account
            >>> current_orders = orders.get_orders('SIM2977785M')
            >>> for order in current_orders['Orders']:
            ...     if order['Status'] in ['Received', 'Sent']:
            ...         orders.cancel_order(order['OrderID'])
        
        Note:
            - API endpoint: DELETE /v3/orderexecution/orders/{orderId}
            - May fail if order already filled
            - Market orders often fill too fast to cancel
            - Check order status after cancel to confirm
        """
        # Construct the cancel endpoint with the order ID
        # Format: /v3/orderexecution/orders/123456789
        endpoint = f"/v3/orderexecution/orders/{order_id}"
        
        # Make the DELETE request to cancel the order
        return self.client.delete(endpoint)
    
    def get_orders(
        self,
        account_id: str,
        since: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get current orders (today's orders plus all active orders).
        
        This retrieves:
        - All orders placed today (regardless of status)
        - All active orders from previous days (not yet filled/canceled)
        
        Use this to:
            - Check order status
            - Find orders to cancel
            - Monitor today's trading activity
            - Get order IDs for cancellation
        
        Args:
            account_id: Account ID to get orders for
            
            since: Optional start date filter (format: 'YYYY-MM-DD')
                  e.g., '2024-01-15'
                  If provided, returns orders from this date forward
                  If omitted, returns today's orders + active orders
        
        Returns:
            Dictionary containing current orders:
            {
                'Orders': [
                    {
                        'OrderID': '123456789',
                        'Symbol': 'AAPL',
                        'Quantity': '100',
                        'FilledQuantity': '50',
                        'OrderType': 'Limit',
                        'TradeAction': 'BUY',
                        'Status': 'PartiallyFilled',
                        'LimitPrice': '150.00',
                        'TimeInForce': {'Duration': 'DAY'},
                        'OrderedAt': '2024-01-15T09:30:00Z',
                        ...
                    },
                    ...
                ]
            }
        
        Example:
            >>> # Get today's orders
            >>> orders_data = orders.get_orders('SIM2977785M')
            >>> for order in orders_data['Orders']:
            ...     print(f"{order['OrderID']}: {order['Symbol']} {order['Status']}")
            >>> 
            >>> # Get orders since a specific date
            >>> orders_data = orders.get_orders('SIM2977785M', since='2024-01-01')
            >>> 
            >>> # Find pending orders
            >>> pending = [o for o in orders_data['Orders'] 
            ...            if o['Status'] in ['Received', 'Sent']]
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}/orders
            - Returns recent and active orders only
            - For older orders, use get_historical_orders()
            - Order history limited to recent period (typically 30-90 days)
        """
        # Construct the orders endpoint for the account
        endpoint = f"/v3/brokerage/accounts/{account_id}/orders"
        
        # Build query parameters if 'since' is provided
        # Format: {'since': '2024-01-15'}
        params = {'since': since} if since else None
        
        # Make the GET request and return current orders
        return self.client.get(endpoint, params=params)
    
    def get_historical_orders(
        self,
        account_id: str,
        since: Optional[str] = None,
        page_size: int = 100,
        next_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get historical orders with pagination support.
        
        This retrieves past orders going back further than get_orders().
        Results are paginated - use next_token to get additional pages.
        
        Use this for:
            - Reviewing past trades
            - Analyzing trading history
            - Generating reports
            - Performance analysis
            - Tax reporting
        
        Args:
            account_id: Account ID to get historical orders for
            
            since: Start date for historical orders (format: 'YYYY-MM-DD')
                  e.g., '2024-01-01'
                  Orders from this date forward will be returned
                  Defaults to last 30 days if not specified
            
            page_size: Number of orders per page (default: 100)
                      Max value depends on API limits
                      Smaller values = more API calls needed
            
            next_token: Token for pagination (from previous response)
                       Used to get the next page of results
                       First call: omit this parameter
                       Subsequent calls: use token from previous response
        
        Returns:
            Dictionary containing historical orders and pagination:
            {
                'Orders': [
                    {
                        'OrderID': '123456789',
                        'Symbol': 'AAPL',
                        'Quantity': '100',
                        'FilledQuantity': '100',
                        'Status': 'Filled',
                        'AveragePrice': '150.25',
                        'OrderType': 'Market',
                        'OrderedAt': '2024-01-15T09:30:00Z',
                        'FilledAt': '2024-01-15T09:30:05Z',
                        ...
                    },
                    ...
                ],
                'NextToken': 'abc123...'  # Use for next page, null if last page
            }
        
        Example:
            >>> # Get first page of historical orders
            >>> history = orders.get_historical_orders(
            ...     account_id='SIM2977785M',
            ...     since='2024-01-01',
            ...     page_size=50
            ... )
            >>> 
            >>> # Process first page
            >>> for order in history['Orders']:
            ...     print(f"{order['Symbol']}: {order['Status']}")
            >>> 
            >>> # Get next page if available
            >>> if history.get('NextToken'):
            ...     next_page = orders.get_historical_orders(
            ...         account_id='SIM2977785M',
            ...         since='2024-01-01',
            ...         page_size=50,
            ...         next_token=history['NextToken']
            ...     )
            >>> 
            >>> # Get all historical orders (pagination loop)
            >>> all_orders = []
            >>> next_token = None
            >>> while True:
            ...     page = orders.get_historical_orders(
            ...         account_id='SIM2977785M',
            ...         since='2024-01-01',
            ...         next_token=next_token
            ...     )
            ...     all_orders.extend(page['Orders'])
            ...     next_token = page.get('NextToken')
            ...     if not next_token:
            ...         break
        
        Note:
            - API endpoint: GET /v3/brokerage/accounts/{accountId}/historicalorders
            - Results are paginated (use NextToken for more pages)
            - Historical data retention varies by account type
            - For recent orders, use get_orders() instead (faster)
        """
        # Construct the historical orders endpoint
        endpoint = f"/v3/brokerage/accounts/{account_id}/historicalorders"
        
        # Build query parameters
        # All values must be strings per API spec
        params = {}
        if since:
            params['since'] = since
        if page_size:
            params['pageSize'] = str(page_size)
        if next_token:
            params['nextToken'] = next_token
        
        # Make the GET request with query parameters
        # Return None for params if empty dict (cleaner URL)
        return self.client.get(endpoint, params=params if params else None)
    
    def stream_orders(self, account_id: str) -> Iterator[bytes]:
        """
        Stream real-time order updates.
        
        This opens a persistent connection and streams order updates as they
        occur. You receive immediate notifications when:
        - New orders are placed
        - Order status changes
        - Orders are partially filled
        - Orders are completely filled
        - Orders are canceled or rejected
        
        Use this for:
            - Real-time order monitoring
            - Automated trade management
            - Immediate fill notifications
            - Building trading dashboards
            - Order execution analytics
        
        Args:
            account_id: Account ID to stream orders for
        
        Yields:
            Lines of order data (as bytes)
            Each line is a JSON object representing an order update
        
        Stream Format:
            Each line is a JSON object:
            {
                'OrderID': '123456789',
                'Symbol': 'AAPL',
                'Status': 'Filled',
                'FilledQuantity': '100',
                'AveragePrice': '150.30',
                'UpdateType': 'OrderUpdate',
                ...
            }
        
        Example:
            >>> # Stream real-time order updates
            >>> import json
            >>> stream = orders.stream_orders('SIM2977785M')
            >>> 
            >>> print("Monitoring orders in real-time...")
            >>> for line in stream:
            ...     order = json.loads(line)
            ...     order_id = order['OrderID']
            ...     status = order['Status']
            ...     symbol = order.get('Symbol', 'Unknown')
            ...     
            ...     print(f"Order {order_id}: {symbol} - {status}")
            ...     
            ...     # Alert on filled orders
            ...     if status == 'Filled':
            ...         filled_qty = order['FilledQuantity']
            ...         avg_price = order['AveragePrice']
            ...         print(f"✓ Filled: {filled_qty} @ ${avg_price}")
            ...     
            ...     # Alert on rejections
            ...     elif status == 'Rejected':
            ...         print(f"⚠️  Order rejected!")
            ...     
            ...     # Press Ctrl+C to stop
        
        Note:
            - API endpoint: GET /v3/brokerage/stream/accounts/{accountId}/orders
            - Stream runs indefinitely until stopped
            - Updates sent immediately as order status changes
            - More efficient than polling get_orders() repeatedly
            - Handle KeyboardInterrupt for graceful shutdown
        """
        # Construct the streaming orders endpoint
        # Note the '/stream/' in the path
        endpoint = f"/v3/brokerage/stream/accounts/{account_id}/orders"
        
        # Return the streaming iterator
        # Order updates will arrive as status changes
        return self.client.stream(endpoint)
    
    def get_routes(self) -> Dict[str, Any]:
        """
        Get available order routing options.
        
        This retrieves the list of routing destinations available for your account.
        Routes determine how your order is sent to the market.
        
        Route Types:
            - Intelligent: TradeStation's smart order routing (recommended)
            - Direct: Route to specific exchange
            - ECN: Electronic Communication Network routes
        
        Use this to:
            - Discover available routing options
            - Verify route names before placing orders
            - Understand routing capabilities
            - Select optimal routing for your strategy
        
        Returns:
            Dictionary containing available routes:
            {
                'Routes': [
                    {
                        'Route': 'Intelligent',
                        'Description': 'Smart order routing',
                        ...
                    },
                    {
                        'Route': 'ARCA',
                        'Description': 'NYSE Arca',
                        ...
                    },
                    ...
                ]
            }
        
        Example:
            >>> # Get available routes
            >>> routes = orders.get_routes()
            >>> for route in routes['Routes']:
            ...     print(f"{route['Route']}: {route.get('Description', 'No description')}")
            >>> 
            >>> # Check if a specific route is available
            >>> route_names = [r['Route'] for r in routes['Routes']]
            >>> if 'Intelligent' in route_names:
            ...     print("Intelligent routing available")
        
        Note:
            - API endpoint: GET /v3/orderexecution/routes
            - Available routes depend on account type and permissions
            - Most users should use 'Intelligent' routing
            - Route availability may change (call periodically to refresh)
        """
        # Construct the routes endpoint
        # This endpoint has no parameters
        endpoint = "/v3/orderexecution/routes"
        
        # Make the GET request and return available routes
        return self.client.get(endpoint)
    
    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Get details of a specific order by OrderID.
        
        This retrieves complete information about a single order including
        its current status, fill details, and all order parameters.
        
        Use this to:
            - Check order status
            - Get fill price and quantity
            - Verify order parameters
            - Track specific orders
        
        Args:
            order_id: Order ID to get details for
                     Format: Numeric string (e.g., '123456789')
                     Get from place_order() or get_orders()
        
        Returns:
            Dictionary containing complete order details:
            {
                'OrderID': '123456789',
                'Symbol': 'AAPL',
                'Quantity': '100',
                'FilledQuantity': '100',
                'OrderType': 'Market',
                'TradeAction': 'BUY',
                'Status': 'Filled',
                'AveragePrice': '150.25',
                'LimitPrice': null,
                'StopPrice': null,
                'TimeInForce': {'Duration': 'DAY'},
                'OrderedAt': '2024-01-15T09:30:00Z',
                'FilledAt': '2024-01-15T09:30:05Z',
                'CommissionFee': '0.00',
                ...
            }
        
        Example:
            >>> # Get order details by ID
            >>> order = orders.get_order('123456789')
            >>> print(f"Status: {order['Status']}")
            >>> print(f"Filled: {order['FilledQuantity']}/{order['Quantity']}")
            >>> if order['Status'] == 'Filled':
            ...     print(f"Average Price: ${order['AveragePrice']}")
            >>> 
            >>> # Check if order is still active
            >>> if order['Status'] in ['Received', 'Sent', 'PartiallyFilled']:
            ...     print("Order is still active")
        
        Note:
            - API endpoint: GET /v3/orderexecution/orders/{orderId}
            - Returns current status (snapshot, not streaming)
            - For real-time updates, use stream_orders()
            - May return error if order ID doesn't exist
        """
        # Construct the order detail endpoint with the order ID
        # Format: /v3/orderexecution/orders/123456789
        endpoint = f"/v3/orderexecution/orders/{order_id}"
        
        # Make the GET request and return order details
        return self.client.get(endpoint)
