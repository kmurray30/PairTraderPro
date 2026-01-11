"""Get current active orders.

CLI Usage:
    python -m tradestation.demo.get_orders
    python -m tradestation.demo.get_orders --environment sim
    python -m tradestation.demo.get_orders --environment prod
"""


def get_orders(api, account_id):
    """
    Get all current (active/open) orders for an account.
    
    This includes: pending, working, partially filled orders.
    Does NOT include: filled, canceled, or rejected orders.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID to query
    """
    orders = api.orders.get_orders(account_id)
    
    print(f"Current Orders for {account_id}:")
    
    if 'Orders' in orders and len(orders['Orders']) > 0:
        print(f"\nFound {len(orders['Orders'])} order(s):\n")
        
        for order in orders['Orders']:
            order_id = order.get('OrderID', 'N/A')
            symbol = order.get('Legs', [{}])[0].get('Symbol', 'N/A')
            action = order.get('Legs', [{}])[0].get('BuyOrSell', 'N/A')
            quantity = order.get('Legs', [{}])[0].get('QuantityOrdered', 0)
            status = order.get('Status', 'N/A')
            
            print(f"  Order ID: {order_id}")
            print(f"    Symbol: {symbol}")
            print(f"    Action: {action}")
            print(f"    Quantity: {quantity}")
            print(f"    Status: {status}")
            print()
    else:
        print("  No active orders found.")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get current orders')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    get_orders(api, account_id)


if __name__ == '__main__':
    main()

