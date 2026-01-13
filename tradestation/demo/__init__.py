"""
TradeStation API Demo Module

This module provides interactive demonstrations of the TradeStation API.
Each function can be called directly or run as a CLI script.

Example usage:
    from tradestation.demo import setup_api, get_quote
    
    api, account_id = setup_api('sim')
    get_quote(api, 'AAPL')

CLI usage:
    python -m tradestation.demo.get_quote --symbol AAPL
"""

from .utils import to_float
from .setup_api import setup_api
from .get_accounts import get_accounts
from .get_bars import get_bars
from .get_symbol_details import get_symbol_details
from .get_quote import get_quote
from .get_balances import get_balances
from .get_positions import get_positions
from .confirm_order import confirm_order
from .place_order import place_order
from .cancel_order import cancel_order
from .get_orders import get_orders
from .get_order_by_id import get_order_by_id
from .get_historical_orders import get_historical_orders
from .get_routes import get_routes
from .stream_quotes import stream_quotes
from .stream_tick_bars import stream_tick_bars
from .stream_bars import stream_bars
from .stream_positions_orders import stream_positions_orders

__all__ = [
    'to_float',
    'setup_api',
    'get_accounts',
    'get_bars',
    'get_symbol_details',
    'get_quote',
    'get_balances',
    'get_positions',
    'confirm_order',
    'place_order',
    'cancel_order',
    'get_orders',
    'get_order_by_id',
    'get_historical_orders',
    'get_routes',
    'stream_quotes',
    'stream_tick_bars',
    'stream_bars',
    'stream_positions_orders',
]

