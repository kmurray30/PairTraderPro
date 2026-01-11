"""Confirm order without placing it (dry-run).

CLI Usage:
    python -m tradestation.demo.confirm_order
    python -m tradestation.demo.confirm_order --symbol SNAP --quantity 1 --action BUY
    python -m tradestation.demo.confirm_order --symbol AAPL --quantity 10 --action BUY --order-type Market
    python -m tradestation.demo.confirm_order --symbol MSFT --quantity 5 --action SELL --time-in-force GTC
"""

import json
from .utils import to_float


def confirm_order(api, account_id, symbol='SNAP', quantity=1, action='BUY', 
                  order_type='Market', time_in_force='Day'):
    """
    Confirm an order without placing it.
    
    This validates the order and shows estimated costs/margins.
    It does NOT actually place the order - safe to run.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID
        symbol: Stock/futures symbol
        quantity: Number of shares/contracts
        action: 'BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'
        order_type: 'Market', 'Limit', 'StopMarket', 'StopLimit'
        time_in_force: 'Day', 'GTC', 'GTD', 'IOC', 'FOK'
    """
    confirmation = api.orders.confirm_order(
        account_id=account_id,
        symbol=symbol,
        quantity=quantity,
        action=action,
        order_type=order_type,
        time_in_force=time_in_force
    )
    
    print("Order Confirmation (NOT placed):")
    print(json.dumps(confirmation, indent=2))
    
    # Check if confirmation was successful
    if 'Confirmations' in confirmation and len(confirmation['Confirmations']) > 0:
        print(f"\n✓ Order Confirmation Successful:")
        
        conf = confirmation['Confirmations'][0]
        
        print(f"  Route: {conf.get('Route', 'N/A')}")
        
        estimated_cost = to_float(conf.get('EstimatedCost', 0))
        print(f"  Estimated Cost: ${estimated_cost:,.2f}")
        
        summary = conf.get('SummaryMessage', 'N/A')
        print(f"  Summary: {summary}")
        
        if 'InitialMarginRequirement' in conf:
            margin = to_float(conf.get('InitialMarginRequirement', 0))
            print(f"  Initial Margin: ${margin:,.2f}")
    else:
        print("\n⚠️ Order confirmation failed or returned unexpected format")
        print("Check the full JSON output above for error details")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Confirm order (dry-run)')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='SNAP', help='Stock/futures symbol')
    parser.add_argument('--quantity', type=int, default=1, help='Number of shares/contracts')
    parser.add_argument('--action', default='BUY', 
                        choices=['BUY', 'SELL', 'BUYTOCOVER', 'SELLSHORT'])
    parser.add_argument('--order-type', default='Market',
                        choices=['Market', 'Limit', 'StopMarket', 'StopLimit'])
    parser.add_argument('--time-in-force', default='Day',
                        choices=['Day', 'GTC', 'GTD', 'IOC', 'FOK'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    confirm_order(api, account_id, args.symbol, args.quantity, args.action,
                  args.order_type, args.time_in_force)


if __name__ == '__main__':
    main()

