"""Get all accounts associated with the user.

This retrieves basic information about all accounts the user has access to,
including account IDs, names, types, and status.

CLI Usage:
    python -m tradestation.demo.get_accounts
    python -m tradestation.demo.get_accounts --environment sim
    python -m tradestation.demo.get_accounts --environment prod
"""

import json


def get_accounts(api):
    """
    Get list of all accounts associated with the user.
    
    Use this to discover available account IDs and determine which accounts
    to use for trading or querying.
    
    Args:
        api: TradeStation API instance
    """
    accounts_data = api.account.get_accounts()
    
    print("All Accounts:")
    print(json.dumps(accounts_data, indent=2))
    
    # Check if we got account data in the response
    if 'Accounts' in accounts_data and len(accounts_data['Accounts']) > 0:
        print(f"\n{'='*60}")
        print(f"Found {len(accounts_data['Accounts'])} account(s)")
        print(f"{'='*60}\n")
        
        for index, account in enumerate(accounts_data['Accounts'], 1):
            account_id = account.get('AccountID', 'N/A')
            account_name = account.get('Name', 'N/A')
            account_type = account.get('AccountType', 'N/A')
            account_status = account.get('Status', 'N/A')
            currency = account.get('Currency', 'USD')
            
            print(f"Account {index}:")
            print(f"  Account ID: {account_id}")
            print(f"  Name: {account_name}")
            print(f"  Type: {account_type}")
            print(f"  Status: {account_status}")
            print(f"  Currency: {currency}")
            
            # Determine if this is a simulation or production account
            if account_id.startswith('SIM'):
                print(f"  Environment: 🧪 SIMULATION")
            else:
                print(f"  Environment: 🔴 PRODUCTION")
            
            print()
    else:
        print("\n⚠️ No accounts found")
        print("This could indicate:")
        print("  - Authentication issue")
        print("  - No accounts associated with this user")
        print("  - API access not properly configured")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(
        description='Get all accounts associated with the user'
    )
    parser.add_argument(
        '--environment', 
        default='sim', 
        choices=['sim', 'prod'],
        help='Environment to use (affects which account is used by other demos, but this command shows all accounts)'
    )
    args = parser.parse_args()
    
    # Note: setup_api returns (api, account_id), but we only need the api instance
    # since get_accounts() doesn't take an account_id parameter
    api, _ = setup_api(args.environment)
    get_accounts(api)


if __name__ == '__main__':
    main()

