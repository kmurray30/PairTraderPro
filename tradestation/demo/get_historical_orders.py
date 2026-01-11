"""Get historical orders.

CLI Usage:
    python -m tradestation.demo.get_historical_orders
    python -m tradestation.demo.get_historical_orders --since 2024-01-01
    python -m tradestation.demo.get_historical_orders --since 2025-12-01 --environment sim
    python -m tradestation.demo.get_historical_orders --since 2026-01-01
"""


def get_historical_orders(api, account_id, since='2024-01-01'):
    """
    Get past orders (filled, canceled, rejected).
    
    This shows complete order history for the account.
    Useful for: reviewing past trades, calculating P&L, auditing.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID to query
        since: Date filter in YYYY-MM-DD format (optional)
    """
    historical_orders = api.orders.get_historical_orders(
        account_id=account_id,
        since=since
    )
    
    print(f"Historical Orders for {account_id}:")
    
    if 'Orders' in historical_orders and len(historical_orders['Orders']) > 0:
        print(f"\nFound {len(historical_orders['Orders'])} historical order(s):\n")
        
        # Show first 10 to avoid overwhelming output
        for order in historical_orders['Orders'][:10]:
            order_id = order.get('OrderID', 'N/A')
            symbol = order.get('Legs', [{}])[0].get('Symbol', 'N/A')
            action = order.get('Legs', [{}])[0].get('BuyOrSell', 'N/A')
            quantity = order.get('Legs', [{}])[0].get('QuantityOrdered', 0)
            status = order.get('Status', 'N/A')
            opened = order.get('OpenedDateTime', 'N/A')[:10] if order.get('OpenedDateTime') else 'N/A'
            
            print(f"  Order {order_id}:")
            print(f"    Date: {opened}")
            print(f"    Symbol: {symbol}")
            print(f"    Action: {action} {quantity}")
            print(f"    Status: {status}")
            print()
        
        if len(historical_orders['Orders']) > 10:
            print(f"  ... and {len(historical_orders['Orders']) - 10} more orders")
    else:
        print("  No historical orders found.")
    
    print("\nUse 'since' parameter to filter by date:")
    print("  Example: --since 2024-01-01")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get historical orders')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--since', default='2024-01-01', 
                        help='Filter orders since date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    get_historical_orders(api, account_id, args.since)


if __name__ == '__main__':
    main()

