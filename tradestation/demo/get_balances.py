"""Get account balances and buying power.

CLI Usage:
    python -m tradestation.demo.get_balances
    python -m tradestation.demo.get_balances --environment sim
    python -m tradestation.demo.get_balances --environment prod
"""

import json
from .utils import to_float


def get_balances(api, account_id):
    """
    Get real-time account balances.
    
    Args:
        api: TradeStation API instance
        account_id: Account ID to query
    """
    balances = api.account.get_balances(account_id)
    
    print(f"Account Balances for {account_id}:")
    print(json.dumps(balances, indent=2))
    
    # Check if we got balance data in the response
    if 'Balances' in balances and len(balances['Balances']) > 0:
        balance = balances['Balances'][0]
        print(f"\nKey Metrics:")
        
        # Extract account value (use Equity if AccountValue not present)
        account_value = to_float(balance.get('AccountValue', balance.get('Equity', 0)))
        cash_balance = to_float(balance.get('CashBalance', 0))
        buying_power = to_float(balance.get('BuyingPower', 0))
        
        print(f"  Account Value: ${account_value:,.2f}")
        print(f"  Cash Balance: ${cash_balance:,.2f}")
        print(f"  Buying Power: ${buying_power:,.2f}")
    else:
        print("\n⚠️ No balance data returned")
        print("Check if the account ID is valid")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get account balances')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    args = parser.parse_args()
    
    api, account_id = setup_api(args.environment)
    get_balances(api, account_id)


if __name__ == '__main__':
    main()

