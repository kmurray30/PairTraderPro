"""Stream tick bars.

⚠️ This runs indefinitely until interrupted with Ctrl+C

CLI Usage:
    python -m tradestation.demo.stream_tick_bars
    python -m tradestation.demo.stream_tick_bars --symbol AAPL
    python -m tradestation.demo.stream_tick_bars --symbol AAPL --interval 100 --bars-back 5
    python -m tradestation.demo.stream_tick_bars --symbol MSFT --interval 50 --bars-back 10
"""

import json


def stream_tick_bars(api, symbol='AAPL', interval=100, bars_back=5):
    """
    Stream tick bars - bars based on trade COUNT rather than TIME.
    
    Each bar represents a fixed number of trades (e.g., 100 trades per bar).
    
    Args:
        api: TradeStation API instance
        symbol: Stock/futures symbol
        interval: Number of ticks per bar
        bars_back: Number of initial historical bars
    """
    print(f"Streaming {interval}-tick bars for: {symbol}")
    print("Press Ctrl+C to stop\n")
    
    for tick_bar in api.market_data.stream_tick_bars(
        symbol=symbol,
        interval=interval,
        bars_back=bars_back
    ):
        bar_data = json.loads(tick_bar)
        
        timestamp = bar_data.get('TimeStamp', 'N/A')
        open_price = bar_data.get('Open', 'N/A')
        high = bar_data.get('High', 'N/A')
        low = bar_data.get('Low', 'N/A')
        close = bar_data.get('Close', 'N/A')
        volume = bar_data.get('TotalVolume', 'N/A')
        
        print(f"{timestamp}: O={open_price} H={high} L={low} C={close} V={volume}")


def main():
    """CLI entry point."""
    import argparse
    from .setup_api import setup_api
    
    parser = argparse.ArgumentParser(description='Stream tick bars')
    parser.add_argument('--environment', default='sim', choices=['sim', 'prod'])
    parser.add_argument('--symbol', default='AAPL', help='Stock/futures symbol')
    parser.add_argument('--interval', type=int, default=100, 
                        help='Number of ticks per bar')
    parser.add_argument('--bars-back', type=int, default=5,
                        help='Number of initial historical bars')
    args = parser.parse_args()
    
    api, _ = setup_api(args.environment)
    
    print("\n⚠️  Streaming data - runs indefinitely until Ctrl+C")
    print("⚠️  Best used in standalone scripts, not notebooks")
    print("\nTick Bars vs Time Bars:")
    print("  Time Bars: Fixed time interval (e.g., 5 minutes)")
    print("  Tick Bars: Fixed trade count (e.g., 100 trades)")
    print("  Advantage: More uniform volatility across bars\n")
    
    stream_tick_bars(api, args.symbol, args.interval, args.bars_back)


if __name__ == '__main__':
    main()

