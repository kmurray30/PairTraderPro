"""Get detailed information about a symbol.

CLI Usage:
    python -m tradestation.demo.get_symbol_details
    python -m tradestation.demo.get_symbol_details --symbol ES
    python -m tradestation.demo.get_symbol_details --symbol AAPL
    python -m tradestation.demo.get_symbol_details --symbol @ES --environment sim
"""

import json


def get_symbol_details(api, symbol='ES'):
    """
    Get detailed information about a symbol.
    
    Args:
        api: TradeStation API instance
        symbol: Stock/futures symbol
    """
    details = api.market_data.get_symbol_details(symbol)
    
    print(f"Symbol Details for {symbol}:")
    print(json.dumps(details, indent=2))


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get symbol details')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='ES', help='Stock/futures symbol')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    get_symbol_details(api, args.symbol)


if __name__ == '__main__':
    main()

