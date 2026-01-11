"""Get current positions.

CLI Usage:
    python -m tradestation.demo.get_positions
    python -m tradestation.demo.get_positions --environment sim
    python -m tradestation.demo.get_positions --environment prod
"""

from .utils import to_float


def get_positions(api, account_id):
    """
    Get all current positions for an account.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID to query
    """
    positions = api.account.get_positions(account_id)
    
    print(f"Current Positions for {account_id}:")
    
    if 'Positions' in positions and len(positions['Positions']) > 0:
        print(f"\nFound {len(positions['Positions'])} position(s):\n")
        
        total_unrealized_pnl = 0
        
        for pos in positions['Positions']:
            symbol = pos.get('Symbol', 'N/A')
            quantity = to_float(pos.get('Quantity', 0))
            avg_price = to_float(pos.get('AveragePrice', 0))
            last_price = to_float(pos.get('Last', 0))
            unrealized_pnl = to_float(pos.get('UnrealizedProfitLoss', 0))
            
            print(f"  {symbol}:")
            print(f"    Quantity: {quantity:.0f}")
            print(f"    Avg Price: ${avg_price:.2f}")
            print(f"    Last Price: ${last_price:.2f}")
            print(f"    Unrealized P&L: ${unrealized_pnl:.2f}")
            print()
            
            total_unrealized_pnl += unrealized_pnl
        
        print(f"Total Unrealized P&L: ${total_unrealized_pnl:.2f}")
    else:
        print("  No positions found.")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get current positions')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    get_positions(api, account_id)


if __name__ == '__main__':
    main()

