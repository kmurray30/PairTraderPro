"""Setup and initialize TradeStation API connection.

CLI Usage:
    python -m tradestation.demo.setup_api
    python -m tradestation.demo.setup_api --environment sim
    python -m tradestation.demo.setup_api --environment prod
"""

import sys
from pathlib import Path


def setup_api(environment='sim'):
    """
    Initialize TradeStation API and return (api, account_id).
    
    Args:
        environment: 'sim' for paper trading or 'prod' for live trading
        
    Returns:
        tuple: (api, account_id)
    """
    # Add parent directory to path so we can import the api module
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    from tradestation.api import TradeStationAPI
    
    api = TradeStationAPI(environment)
    account_id = api.config.account_id
    
    print(f"\n✓ API initialized successfully")
    print(f"✓ Base URL: {api.config.base_url}")
    print(f"✓ Account ID: {account_id}")
    
    return api, account_id


def main():
    """CLI entry point for API setup."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Setup TradeStation API')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'],
                        help='Trading environment (sim=paper trading, prod=live trading)')
    args = parser.parse_args()
    
    setup_api(args.environment)


if __name__ == '__main__':
    main()

