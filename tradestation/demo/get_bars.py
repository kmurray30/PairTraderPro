"""Get historical bar chart data (OHLC prices).

CLI Usage:
    python -m tradestation.demo.get_bars
    python -m tradestation.demo.get_bars --symbol ES --interval 5 --bars-back 10
    python -m tradestation.demo.get_bars --symbol AAPL --interval 1 --unit Hour
    python -m tradestation.demo.get_bars --symbol @ES --interval 15 --unit Minute --bars-back 20
"""

import json


def get_bars(api, symbol='ES', interval=5, unit='Minute', bars_back=10):
    """
    Get historical bar data for a symbol.
    
    Args:
        api: TradeStation API instance
        symbol: Stock/futures symbol
        interval: Number of time units per bar
        unit: Time unit ('Minute', 'Hour', 'Day', etc.)
        bars_back: Number of bars to retrieve
    """
    # Get bar data from API
    bars = api.market_data.get_bars(
        symbol=symbol,
        interval=interval,
        unit=unit,
        bars_back=bars_back
    )
    
    bars_list = bars.get('Bars', [])
    print(f"Retrieved {len(bars_list)} bars for {symbol}")
    
    if bars_list:
        print(f"\nFirst bar:")
        print(json.dumps(bars_list[0], indent=2))
        print(f"\nLast bar:")
        print(json.dumps(bars_list[-1], indent=2))
    else:
        print("No bars data returned")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Get historical bar data')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='ES', help='Stock/futures symbol')
    parser.add_argument('--interval', type=int, default=5, help='Time interval per bar')
    parser.add_argument('--unit', default='Minute', help='Time unit (Minute, Hour, Day, etc.)')
    parser.add_argument('--bars-back', type=int, default=10, help='Number of bars to retrieve')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    get_bars(api, args.symbol, args.interval, args.unit, args.bars_back)


if __name__ == '__main__':
    main()

