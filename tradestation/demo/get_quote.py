"""Get real-time quote for a symbol.

CLI Usage:
    python -m tradestation.demo.get_quote
    python -m tradestation.demo.get_quote --symbol AAPL
    python -m tradestation.demo.get_quote --symbol MSFT --environment sim
    python -m tradestation.demo.get_quote --symbol TSLA
"""

import json
from .utils import to_float


def get_quote(api, symbol='AAPL'):
    """
    Get real-time quote with bid/ask/last prices.
    
    Args:
        api: TradeStation API instance
        symbol: Stock/futures symbol
    """
    quote = api.market_data.get_quote(symbol)
    
    print(f"Real-time Quote for {symbol}:")
    print(json.dumps(quote, indent=2))
    
    # Check if we got quotes in the response
    if 'Quotes' in quote and len(quote['Quotes']) > 0:
        q = quote['Quotes'][0]
        
        # Extract price information
        last = to_float(q.get('Last', 0))
        bid = to_float(q.get('Bid', 0))
        ask = to_float(q.get('Ask', 0))
        volume = to_float(q.get('Volume', 0))
        
        print(f"\nKey Prices:")
        print(f"  Last: ${last:.2f}")
        print(f"  Bid:  ${bid:.2f}")
        print(f"  Ask:  ${ask:.2f}")
        print(f"  Volume: {volume:,.0f}")
        print(f"  Spread: ${(ask - bid):.2f}")
    else:
        print("\n⚠️ No quote data returned")
        print("Check if the symbol is valid and market is open")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get real-time quote')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='AAPL', help='Stock/futures symbol')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    get_quote(api, args.symbol)


if __name__ == '__main__':
    main()

