"""Stream real-time quotes.

⚠️ This runs indefinitely until interrupted with Ctrl+C

CLI Usage:
    python -m tradestation.demo.stream_quotes
    python -m tradestation.demo.stream_quotes --symbols AAPL
    python -m tradestation.demo.stream_quotes --symbols AAPL,MSFT,GOOGL
    python -m tradestation.demo.stream_quotes --symbols TSLA,NVDA --environment sim
"""

import json


def stream_quotes(api, symbols='AAPL'):
    """
    Stream real-time bid/ask/last prices for symbols.
    
    This provides continuous price updates as the market moves.
    
    Args:
        api: TradeStation API instance
        symbols: Comma-separated symbol list (e.g., 'AAPL,MSFT,GOOGL')
    """
    print(f"Streaming quotes for: {symbols}")
    print("Press Ctrl+C to stop\n")
    
    for quote_update in api.market_data.stream_quotes(symbols):
        quote_data = json.loads(quote_update)
        
        symbol = quote_data.get('Symbol', 'N/A')
        last = quote_data.get('Last', 'N/A')
        bid = quote_data.get('Bid', 'N/A')
        ask = quote_data.get('Ask', 'N/A')
        volume = quote_data.get('Volume', 'N/A')
        
        print(f"{symbol}: Last=${last} Bid=${bid} Ask=${ask} Vol={volume}")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Stream real-time quotes')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbols', default='AAPL', 
                        help='Comma-separated symbols (e.g., AAPL,MSFT)')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    
    print("\n⚠️  Streaming data - runs indefinitely until Ctrl+C")
    print("⚠️  Best used in standalone scripts, not notebooks\n")
    
    stream_quotes(api, args.symbols)


if __name__ == '__main__':
    main()

