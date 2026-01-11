"""Place an actual order (REAL TRADES!).

⚠️ WARNING: This places REAL orders!

CLI Usage:
    # Simulation account (paper trading)
    python -m tradestation.demo.place_order --symbol AAPL --quantity 1 --action BUY --environment sim
    python -m tradestation.demo.place_order --symbol SNAP --quantity 5 --action BUY --order-type Market
    
    # Production account (REAL MONEY - use with extreme caution!)
    python -m tradestation.demo.place_order --symbol AAPL --quantity 1 --action BUY --environment prod
"""

import json


def place_order(api, account_id, symbol, quantity, action, 
                order_type='Market', time_in_force='Day'):
    """
    Place an actual order - THIS WILL EXECUTE A REAL TRADE!
    
    ⚠️ WARNING: This WILL place a REAL order (or simulation order).
    Always confirm with confirm_order() first!
    
    Args:
        api: TradeStation API instance
        account_id: Account ID
        symbol: Stock/futures symbol
        quantity: Number of shares/contracts
        action: 'BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'
        order_type: 'Market', 'Limit', 'StopMarket', 'StopLimit'
        time_in_force: 'Day', 'GTC', 'GTD', 'IOC', 'FOK'
        
    Returns:
        str: Order ID if successful, None otherwise
    """
    print("⚠️  PLACING ACTUAL ORDER")
    print(f"⚠️  Symbol: {symbol}, Quantity: {quantity}, Action: {action}")
    
    order_response = api.orders.place_order(
        account_id=account_id,
        symbol=symbol,
        quantity=quantity,
        action=action,
        order_type=order_type,
        time_in_force=time_in_force
    )
    
    print("\nOrder Placed:")
    print(json.dumps(order_response, indent=2))
    
    # Extract the order ID for tracking/canceling
    if 'Orders' in order_response and len(order_response['Orders']) > 0:
        placed_order = order_response['Orders'][0]
        order_id = placed_order.get('OrderID')
        status = placed_order.get('Status')
        
        print(f"\n✓ Order ID: {order_id}")
        print(f"✓ Status: {status}")
        print(f"✓ Symbol: {placed_order.get('Symbol')}")
        print(f"✓ Quantity: {placed_order.get('Quantity')}")
        
        return order_id
    else:
        print("\n⚠️ Order placement failed or returned unexpected format")
        return None


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Place order (REAL TRADE!)')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', required=True, help='Stock/futures symbol')
    parser.add_argument('--quantity', type=int, required=True, help='Number of shares/contracts')
    parser.add_argument('--action', required=True,
                        choices=['BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'])
    parser.add_argument('--order-type', default='Market',
                        choices=['Market', 'Limit', 'StopMarket', 'StopLimit'])
    parser.add_argument('--time-in-force', default='Day',
                        choices=['Day', 'GTC', 'GTD', 'IOC', 'FOK'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    place_order(api, account_id, args.symbol, args.quantity, args.action,
                args.order_type, args.time_in_force)


if __name__ == '__main__':
    main()

