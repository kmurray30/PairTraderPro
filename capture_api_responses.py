"""
Capture API Response Formats from TradeStation SIM Account

This script makes real API calls to document the exact response format
for each endpoint. This helps prevent parsing bugs by having reference
responses to test against.

SAFETY: Only runs in SIM mode with minimal test quantities.
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from tradestation.api import TradeStationAPI


def save_response(response_data: dict, filename: str, output_dir: Path):
    """Save API response to JSON file."""
    filepath = output_dir / filename
    with open(filepath, 'w') as file:
        json.dump(response_data, file, indent=2)
    print(f"✓ Saved: {filename}")


def capture_all_responses():
    """Capture responses from all API endpoints."""
    print("=" * 70)
    print("API Response Format Capture - TradeStation SIM")
    print("=" * 70)
    print("\nInitializing API (SIM mode only)...")
    
    # SAFETY: Force SIM mode
    api = TradeStationAPI("sim")
    account_id = api.config.account_id
    
    # Verify we're in SIM mode
    if not account_id.startswith('SIM'):
        print("❌ ERROR: Not a SIM account! Aborting for safety.")
        return
    
    print(f"✓ Using SIM account: {account_id}")
    print(f"✓ Environment: {api.config.environment}")
    
    # Create output directory
    output_dir = Path(__file__).parent / "response_formats"
    output_dir.mkdir(exist_ok=True)
    print(f"✓ Output directory: {output_dir}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 70)
    print("PHASE 1: MARKET DATA APIs")
    print("=" * 70)
    
    # 1. Get Quote
    print("\n1. Capturing get_quote response (V and MA)...")
    try:
        quote_v = api.market_data.get_quote("V")
        save_response(quote_v, f"quote_V_{timestamp}.json", output_dir)
        
        quote_ma = api.market_data.get_quote("MA")
        save_response(quote_ma, f"quote_MA_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 2. Get Bars
    print("\n2. Capturing get_bars response (1-minute bars)...")
    try:
        bars = api.market_data.get_bars(
            symbol="V",
            interval=1,
            unit="Minute",
            bars_back=10
        )
        save_response(bars, f"bars_1min_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 3. Get Symbol Details
    print("\n3. Capturing get_symbol_details response...")
    try:
        symbol_details = api.market_data.get_symbol_details("V")
        save_response(symbol_details, f"symbol_details_V_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    print("\n" + "=" * 70)
    print("PHASE 2: ACCOUNT APIs")
    print("=" * 70)
    
    # 4. Get Accounts
    print("\n4. Capturing get_accounts response...")
    try:
        accounts = api.account.get_accounts()
        save_response(accounts, f"accounts_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 5. Get Balances
    print("\n5. Capturing get_balances response...")
    try:
        balances = api.account.get_balances(account_id)
        save_response(balances, f"balances_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 6. Get Positions
    print("\n6. Capturing get_positions response...")
    try:
        positions = api.account.get_positions(account_id)
        save_response(positions, f"positions_{timestamp}.json", output_dir)
        print(f"   Found {len(positions.get('Positions', []))} positions")
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 7. Get BOD Balances
    print("\n7. Capturing get_bod_balances response...")
    try:
        bod_balances = api.account.get_bod_balances(account_id)
        save_response(bod_balances, f"bod_balances_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    print("\n" + "=" * 70)
    print("PHASE 3: ORDER APIs")
    print("=" * 70)
    
    # 8. Get Orders (existing)
    print("\n8. Capturing get_orders response...")
    try:
        orders = api.orders.get_orders(account_id)
        save_response(orders, f"orders_list_{timestamp}.json", output_dir)
        print(f"   Found {len(orders.get('Orders', []))} recent orders")
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 9. Get Routes
    print("\n9. Capturing get_routes response...")
    try:
        routes = api.orders.get_routes()
        save_response(routes, f"routes_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 10. Confirm Order (dry-run)
    print("\n10. Capturing confirm_order response (BUY 1 V)...")
    try:
        confirm = api.orders.confirm_order(
            account_id=account_id,
            symbol="V",
            quantity=1,
            action="BUY",
            order_type="Market",
            time_in_force="DAY"
        )
        save_response(confirm, f"confirm_order_{timestamp}.json", output_dir)
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 11. Place Test Order (BUY 1 share)
    print("\n11. Placing test order (BUY 1 V)...")
    test_order_id = None
    try:
        place_response = api.orders.place_order(
            account_id=account_id,
            symbol="V",
            quantity=1,
            action="BUY",
            order_type="Market",
            time_in_force="DAY"
        )
        save_response(place_response, f"place_order_{timestamp}.json", output_dir)
        
        # Extract order ID for next steps
        orders_placed = place_response.get('Orders', [])
        if orders_placed:
            test_order_id = orders_placed[0].get('OrderID')
            print(f"   Order placed: {test_order_id}")
    except Exception as error:
        print(f"   ⚠️  Error: {error}")
    
    # 12. Get Order by ID
    if test_order_id:
        print(f"\n12. Capturing get_order response (order {test_order_id})...")
        try:
            # Wait a moment for order to process
            time.sleep(1)
            
            order_detail = api.orders.get_order(test_order_id)
            save_response(order_detail, f"get_order_{timestamp}.json", output_dir)
            print(f"   Order status: {order_detail.get('Status', 'Unknown')}")
        except Exception as error:
            print(f"   ⚠️  Error: {error}")
        
        # 13. Cancel Order
        print(f"\n13. Canceling test order {test_order_id}...")
        try:
            # Wait for order to be cancelable
            time.sleep(1)
            
            cancel_response = api.orders.cancel_order(test_order_id)
            save_response(cancel_response, f"cancel_order_{timestamp}.json", output_dir)
            print(f"   ✓ Order canceled")
        except Exception as error:
            print(f"   ⚠️  Error canceling: {error}")
            print(f"   Note: Order may have already filled (this is fine in SIM)")
    else:
        print("\n12-13. Skipping get_order and cancel_order (no test order placed)")
    
    # Create README
    print("\n" + "=" * 70)
    print("Creating README...")
    print("=" * 70)
    
    readme_content = f"""# API Response Format Reference

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Account: {account_id} (SIM)
Environment: {api.config.environment}

## Purpose

This directory contains actual API responses from TradeStation's SIM environment.
These serve as reference documentation to ensure our code correctly parses API responses.

## Files

### Market Data
- `quote_V_*.json` - Real-time quote for Visa (V)
- `quote_MA_*.json` - Real-time quote for Mastercard (MA)
- `bars_1min_*.json` - Historical 1-minute bars
- `symbol_details_V_*.json` - Symbol metadata

### Account
- `accounts_*.json` - List of available accounts
- `balances_*.json` - Current account balances
- `positions_*.json` - Current positions
- `bod_balances_*.json` - Beginning of day balances

### Orders
- `orders_list_*.json` - List of recent orders
- `routes_*.json` - Available order routes
- `confirm_order_*.json` - Order confirmation (dry-run)
- `place_order_*.json` - Response from placing order
- `get_order_*.json` - Single order details
- `cancel_order_*.json` - Response from canceling order

## Important Notes

### Nested Structures
Many responses use nested arrays. For example, balances:
```json
{{
  "Balances": [
    {{
      "AccountID": "...",
      "BuyingPower": "...",
      ...
    }}
  ]
}}
```

Always check for array wrappers before accessing fields!

### String Numbers
Many numeric fields come as strings (e.g., "1000000" instead of 1000000).
Always use float() or int() conversion.

### Case Sensitivity
Field names are case-sensitive. "BuyingPower" ≠ "buyingpower"

## Usage

When parsing API responses in code:
1. Check the corresponding file in this directory
2. Verify field names and structure
3. Test with empty/null cases
4. Handle type conversions properly
"""
    
    readme_path = output_dir / "README.md"
    with open(readme_path, 'w') as file:
        file.write(readme_content)
    print(f"✓ Created README.md")
    
    print("\n" + "=" * 70)
    print("✓ CAPTURE COMPLETE")
    print("=" * 70)
    print(f"\nAll responses saved to: {output_dir}")
    print("\nNext: Run audit to check code against these responses")


if __name__ == "__main__":
    try:
        capture_all_responses()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as error:
        print(f"\n\n❌ FATAL ERROR: {error}")
        import traceback
        traceback.print_exc()

