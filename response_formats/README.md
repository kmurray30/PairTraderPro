# API Response Format Reference

Generated: 2026-01-13 11:40:26
Account: SIM2977785M (SIM)
Environment: sim

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
{
  "Balances": [
    {
      "AccountID": "...",
      "BuyingPower": "...",
      ...
    }
  ]
}
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
