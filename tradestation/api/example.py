"""
TradeStation API Library - Complete Usage Example

This script demonstrates the core functionality of the TradeStation API library.
It shows how to work with market data, account information, and order management
in a safe simulation environment.

What This Example Covers:
    1. API initialization with environment selection
    2. Market data retrieval (bars and symbol details)
    3. Account information (balances and positions)
    4. Order validation (confirm without placing)
    5. Order monitoring (listing current orders)

Safety Note:
    This example uses the simulation environment ('sim'), which means:
    - No real money is at risk
    - Orders go to paper trading accounts
    - Safe for testing and learning
    - Identical API to production (just different account IDs)

Prerequisites:
    - .env.sim file configured with your credentials
    - TradeStation API key and secret
    - Valid refresh token
    - Simulation account ID

To Run:
    python -m tradestation.api.example

Or from the api directory:
    python example.py
"""

import os
from tradestation.api import TradeStationAPI


def main():
    """
    Main function demonstrating TradeStation API usage.
    
    This function walks through common API operations in a logical order:
    1. Setup and initialization
    2. Market data queries
    3. Account information queries
    4. Order management operations
    
    All operations include error handling to gracefully handle API issues.
    """
    
    # ============================================================
    # INITIALIZATION
    # ============================================================
    # Initialize the API client with the simulation environment
    # This will load credentials from .env and use the simulation API
    # No need to set os.environ['ENV'] anymore - just pass the parameter!
    print("Initializing TradeStation API...")
    api = TradeStationAPI('sim')  # Explicitly use simulation
    
    # Get account ID from the loaded configuration
    # This comes from the ACCOUNT_ID variable in .env
    # For simulation, it should start with 'SIM' (e.g., 'SIM2977785M')
    account_id = api.config.account_id
    print(f"Using account: {account_id}")
    print()
    
    # ============================================================
    # MARKET DATA EXAMPLES
    # ============================================================
    # Market data endpoints provide historical and real-time price information
    # These are read-only operations (safe to call)
    print("=" * 60)
    print("MARKET DATA")
    print("=" * 60)
    
    # ----------------------------------------------------------
    # Example 1: Get Historical Bar Chart Data
    # ----------------------------------------------------------
    # Retrieves OHLCV (Open, High, Low, Close, Volume) data
    # This is a snapshot request - returns data and closes connection
    print("\n1. Getting bar chart data for @ES (S&P 500 E-mini futures)...")
    try:
        bars = api.market_data.get_bars(
            symbol='@ES',          # S&P 500 E-mini futures
            interval=5,            # 5-minute bars
            unit='Minute',         # Time unit (Minute, Daily, Weekly, Monthly)
            bars_back=3            # Get last 3 bars
        )
        
        # Check if we got data back
        # The response contains a 'Bars' array with OHLCV data
        bar_list = bars.get('Bars', [])
        print(f"   Retrieved {len(bar_list)} bars")
        
        # Display the most recent bar
        if bar_list:
            latest_bar = bar_list[-1]  # Last element is most recent
            close_price = latest_bar.get('Close')
            volume = latest_bar.get('TotalVolume')
            print(f"   Latest bar: Close={close_price}, Volume={volume}")
    except Exception as error:
        # Catch any API errors (auth failures, network issues, etc.)
        print(f"   Error: {error}")
    
    # ----------------------------------------------------------
    # Example 2: Get Symbol Details
    # ----------------------------------------------------------
    # Retrieves metadata about a symbol including contract specs,
    # expiration dates (for futures), exchange info, etc.
    print("\n2. Getting symbol details for ES...")
    try:
        symbol_info = api.market_data.get_symbol_details('ES')
        
        # The response contains comprehensive symbol information
        # For futures, this includes available contract months
        symbol_name = symbol_info.get('Name')
        description = symbol_info.get('Description')
        print(f"   Symbol: {symbol_name}")
        print(f"   Description: {description}")
        
        # Optional: Show available contract months (for futures)
        # symbols = symbol_info.get('Symbols', [])
        # if symbols:
        #     print(f"   Available contracts: {len(symbols)}")
    except Exception as error:
        print(f"   Error: {error}")
    
    # ============================================================
    # ACCOUNT INFORMATION EXAMPLES
    # ============================================================
    # Account endpoints provide balance and position information
    # These are read-only operations (safe to call)
    print("\n" + "=" * 60)
    print("ACCOUNT INFORMATION")
    print("=" * 60)
    
    # ----------------------------------------------------------
    # Example 3: Get Account Balances
    # ----------------------------------------------------------
    # Retrieves real-time account balance information including
    # cash, equity, buying power, and P&L
    print("\n3. Getting account balances...")
    try:
        balances = api.account.get_balances(account_id)
        
        # Extract key balance metrics
        # All values are in the account's currency (typically USD)
        cash_balance = balances.get('CashBalance', 0)
        equity = balances.get('Equity', 0)
        buying_power = balances.get('BuyingPower', 0)
        
        # Display formatted balances
        # The :,.2f format adds thousand separators and 2 decimal places
        print(f"   Cash Balance: ${cash_balance:,.2f}")
        print(f"   Equity: ${equity:,.2f}")
        print(f"   Buying Power: ${buying_power:,.2f}")
        
        # Optional: Show additional balance info
        # unrealized_pl = balances.get('UnrealizedProfitLoss', 0)
        # realized_pl = balances.get('RealizedProfitLoss', 0)
        # print(f"   Unrealized P&L: ${unrealized_pl:,.2f}")
        # print(f"   Realized P&L: ${realized_pl:,.2f}")
    except Exception as error:
        print(f"   Error: {error}")
    
    # ----------------------------------------------------------
    # Example 4: Get Current Positions
    # ----------------------------------------------------------
    # Retrieves all open positions in the account
    # Each position includes symbol, quantity, entry price, and P&L
    print("\n4. Getting current positions...")
    try:
        positions = api.account.get_positions(account_id)
        
        # Extract the positions array from response
        position_list = positions.get('Positions', [])
        
        if position_list:
            # We have open positions - display them
            print(f"   Found {len(position_list)} position(s)")
            
            # Iterate through each position
            for position in position_list:
                symbol = position.get('Symbol')
                quantity = position.get('Quantity')  # Positive=long, negative=short
                avg_price = position.get('AveragePrice')
                
                # Display position details
                # Note: Quantity sign indicates direction (+ = long, - = short)
                print(f"   - {symbol}: {quantity} @ ${avg_price}")
                
                # Optional: Show P&L for this position
                # unrealized_pl = position.get('UnrealizedProfitLoss', 0)
                # print(f"      P&L: ${unrealized_pl:,.2f}")
        else:
            # No open positions (empty account or all positions closed)
            print("   No open positions")
    except Exception as error:
        print(f"   Error: {error}")
    
    # ============================================================
    # ORDER MANAGEMENT EXAMPLES
    # ============================================================
    # Order endpoints allow placing, confirming, and managing orders
    # IMPORTANT: confirm_order is safe (doesn't place), place_order is real
    print("\n" + "=" * 60)
    print("ORDER MANAGEMENT")
    print("=" * 60)
    
    # ----------------------------------------------------------
    # Example 5: Confirm Order (Dry-Run)
    # ----------------------------------------------------------
    # This validates an order WITHOUT placing it
    # Use this to check:
    # - Order is valid
    # - You have sufficient buying power
    # - Estimated costs and commissions
    # SAFE TO RUN: This does NOT place an actual order
    print("\n5. Confirming an order (dry-run, not placing)...")
    try:
        confirmation = api.orders.confirm_order(
            account_id=account_id,
            symbol='ESZ24',        # E-mini S&P 500 December 2024 contract
            quantity=1,            # 1 contract
            action='BUY',          # Opening long position
            order_type='Market'    # Execute at current market price
        )
        
        # The confirmation response includes estimated costs
        # This helps you verify the order before actually placing it
        print(f"   Order confirmation successful")
        
        estimated_cost = confirmation.get('EstimatedCost', 0)
        route = confirmation.get('Route', 'Unknown')
        
        print(f"   Estimated cost: ${estimated_cost:,.2f}")
        print(f"   Route: {route}")
        
        # NOTE: To actually place this order, you would call:
        # api.orders.place_order() with the same parameters
        # But we DON'T do that in this example (safety first!)
    except Exception as error:
        print(f"   Error: {error}")
    
    # ----------------------------------------------------------
    # Example 6: Get Current Orders
    # ----------------------------------------------------------
    # Retrieves today's orders plus any active orders from previous days
    # Use this to monitor order status or find orders to cancel
    print("\n6. Getting current orders...")
    try:
        orders = api.orders.get_orders(account_id)
        
        # Extract the orders array from response
        order_list = orders.get('Orders', [])
        
        if order_list:
            # We have orders - display them
            print(f"   Found {len(order_list)} order(s)")
            
            # Iterate through each order
            for order in order_list:
                symbol = order.get('Symbol')
                order_type = order.get('OrderType')      # Market, Limit, etc.
                trade_action = order.get('TradeAction')  # BUY, SELL, etc.
                quantity = order.get('Quantity')
                status = order.get('Status')             # Filled, Pending, etc.
                
                # Display order summary
                print(f"   - {symbol}: {order_type} {trade_action} {quantity}")
                
                # Optional: Show order status and fill details
                # print(f"      Status: {status}")
                # if order.get('FilledQuantity'):
                #     filled = order.get('FilledQuantity')
                #     avg_price = order.get('AveragePrice', 0)
                #     print(f"      Filled: {filled} @ ${avg_price}")
        else:
            # No current orders
            print("   No current orders")
    except Exception as error:
        print(f"   Error: {error}")
    
    # ============================================================
    # COMPLETION
    # ============================================================
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
    print()
    print("Next Steps:")
    print("  - Review USAGE.md for more detailed examples")
    print("  - Explore streaming endpoints for real-time data")
    print("  - Try placing orders in simulation (use place_order)")
    print("  - Check out the API modules for more functionality")


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================
# This standard Python pattern ensures main() only runs when
# the script is executed directly (not when imported)
if __name__ == '__main__':
    # Run the main example function
    # Any exceptions not caught above will display full traceback
    main()
