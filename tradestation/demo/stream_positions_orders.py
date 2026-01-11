"""Stream positions and order updates.

⚠️ This runs indefinitely until interrupted with Ctrl+C

CLI Usage:
    python -m tradestation.demo.stream_positions_orders
    python -m tradestation.demo.stream_positions_orders --environment sim
    python -m tradestation.demo.stream_positions_orders --environment prod
"""

import json


def stream_positions_orders(api, account_id):
    """
    Stream real-time position and order updates.
    
    This monitors changes to positions and orders in real-time.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID to monitor
    """
    print(f"Streaming positions and orders for account: {account_id}")
    print("Press Ctrl+C to stop\n")
    
    # Note: This is a simplified example. In practice, you might want to
    # run these in separate threads or use asyncio for concurrent streaming.
    
    print("Streaming positions:")
    for position_update in api.account.stream_positions(account_id):
        print(f"Position update: {position_update}")
        # In a real implementation, you'd parse and handle the update
        # break after a few for demo purposes in interactive mode
    
    print("\nStreaming orders:")
    for order_update in api.orders.stream_orders(account_id):
        print(f"Order update: {order_update}")
        # In a real implementation, you'd parse and handle the update


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Stream positions and orders')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    
    print("\n⚠️  Streaming data - runs indefinitely until Ctrl+C")
    print("⚠️  Best used in standalone scripts, not notebooks")
    print("⚠️  This will stream both positions and orders")
    print("⚠️  Consider running them in separate processes for production\n")
    
    stream_positions_orders(api, account_id)


if __name__ == '__main__':
    main()

