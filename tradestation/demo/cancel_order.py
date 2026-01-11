"""Cancel a pending order.

CLI Usage:
    python -m tradestation.demo.cancel_order --order-id YOUR_ORDER_ID
    python -m tradestation.demo.cancel_order --order-id 12345678 --environment sim
    python -m tradestation.demo.cancel_order --order-id abc123def --environment prod
"""

import json


def cancel_order(api, order_id):
    """
    Cancel a pending order by its Order ID.
    
    Note: Only works for orders that haven't been filled yet.
    
    Args:
        api: TradeStation API instance
        order_id: Order ID to cancel
    """
    cancel_response = api.orders.cancel_order(order_id)
    
    print("Order Cancellation Response:")
    print(json.dumps(cancel_response, indent=2))
    
    # Check cancellation status
    if 'Orders' in cancel_response and len(cancel_response['Orders']) > 0:
        canceled_order = cancel_response['Orders'][0]
        print(f"\n✓ Order {canceled_order.get('OrderID')} cancellation requested")
        print(f"✓ Status: {canceled_order.get('Status')}")
    else:
        print("\n⚠️ Order cancellation failed or returned unexpected format")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Cancel order')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--order-id', required=True, help='Order ID to cancel')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    cancel_order(api, args.order_id)


if __name__ == '__main__':
    main()

