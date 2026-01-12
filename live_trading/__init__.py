"""
Live Pairs Trading Algorithm

This package implements a live pairs trading algorithm that mirrors the logic
from the historical simulation. It trades between two correlated stocks,
swapping positions when their price ratio deviates from its moving average.

Key Components:
    - state_machine: Algorithm state management and transitions
    - price_tracker: Quote polling and moving average calculation
    - order_executor: Order placement and fill verification
    - performance_tracker: Return calculation and logging
    - observability: Prometheus metrics and Loki logging
    - reconciliation: Position verification and recovery

Usage:
    python -m live_trading.live_pairs_trader
    
    Or import and use programmatically:
    
    from live_trading.live_pairs_trader import LivePairsTrader
    trader = LivePairsTrader()
    trader.run()

Configuration:
    All settings are in settings.yaml in this directory.
    Account credentials are in .env at the project root.

Safety:
    This implementation is HARDCODED to use the TradeStation simulation API.
    There is no option to run against production - that requires code changes.
"""

__version__ = "1.0.0"
__author__ = "PairTraderPro"

