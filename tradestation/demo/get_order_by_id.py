"""Get detailed information about a specific order.

CLI Usage:
    python -m tradestation.demo.get_order_by_id --order-id YOUR_ORDER_ID
    python -m tradestation.demo.get_order_by_id --order-id 12345678 --environment sim
    python -m tradestation.demo.get_order_by_id --order-id abc123def
"""

import json


def get_order_by_id(api, order_id):
    """
    Get detailed information about a specific order.
    
    Useful for tracking order status and fill information.
    
    Args:
        api: TradeStation API instance
        order_id: Order ID to query
    """
    order_details = api.orders.get_order(order_id)
    
    print(f"Order Details for {order_id}:")
    print(json.dumps(order_details, indent=2))
    
    # Extract key information
    if 'Orders' in order_details and len(order_details['Orders']) > 0:
        order = order_details['Orders'][0]
        
        print(f"\nOrder Summary:")
        print(f"  Order ID: {order.get('OrderID')}")
        print(f"  Status: {order.get('Status')}")
        print(f"  Symbol: {order.get('Legs', [{}])[0].get('Symbol', 'N/A')}")
        print(f"  Action: {order.get('Legs', [{}])[0].get('BuyOrSell', 'N/A')}")
        print(f"  Quantity Ordered: {order.get('Legs', [{}])[0].get('QuantityOrdered', 0)}")
        print(f"  Quantity Filled: {order.get('FilledQuantity', 0)}")
        print(f"  Fill Price: ${order.get('FilledPrice', 0)}")
    else:
        print("\n⚠️ Order not found or unexpected format")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get order by ID')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--order-id', required=True, help='Order ID to query')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    get_order_by_id(api, args.order_id)


if __name__ == '__main__':
    main()

