"""Stream time-based bars.

⚠️ This runs indefinitely until interrupted with Ctrl+C

CLI Usage:
    python -m tradestation.demo.stream_bars
    python -m tradestation.demo.stream_bars --symbol @ES
    python -m tradestation.demo.stream_bars --symbol @ES --interval 1 --unit Minute
    python -m tradestation.demo.stream_bars --symbol AAPL --interval 5 --unit Minute
"""

import json


def stream_bars(api, symbol='@ES', interval=1, unit='Minute'):
    """
    Stream real-time bar data (time-based).
    
    Args:
        api: TradeStation API instance
        symbol: Stock/futures symbol
        interval: Time interval per bar
        unit: Time unit ('Minute', 'Hour', 'Day', etc.)
    """
    print(f"Streaming {interval}-{unit} bars for: {symbol}")
    print("Press Ctrl+C to stop\n")
    
    for bar_data in api.market_data.stream_bars(
        symbol=symbol,
        interval=interval,
        unit=unit
    ):
        print(f"New bar: {bar_data}")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Stream time-based bars')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='@ES', help='Stock/futures symbol')
    parser.add_argument('--interval', type=int, default=1, 
                        help='Time interval per bar')
    parser.add_argument('--unit', default='Minute',
                        help='Time unit (Minute, Hour, Day, etc.)')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    
    print("\n⚠️  Streaming data - runs indefinitely until Ctrl+C")
    print("⚠️  Best used in standalone scripts, not notebooks\n")
    
    stream_bars(api, args.symbol, args.interval, args.unit)


if __name__ == '__main__':
    main()

