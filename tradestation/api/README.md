# TradeStation API Library

A clean, modular Python library for interacting with the TradeStation API with built-in environment management for safe trading operations.

## Features

- **Simple environment selection** - Just pass 'sim' or 'prod' parameter
- **Safety-first defaults** - Always defaults to simulation mode
- **Modular design** - Clean separation of concerns (auth, market data, account, orders)
- **Type hints** - Full type annotations for better IDE support
- **Streaming support** - Built-in support for real-time data streams
- **Auto-token refresh** - Handles 20-minute access token expiration automatically

## Quick Start

### 1. Set up your environment file

```bash
# Copy the example file
cp ../../.env.example ../../.env

# Edit with your credentials
# Get credentials from TradeStation Developer Portal
```

### 2. Basic usage

```python
from tradestation.api import TradeStationAPI

# Use simulation mode (paper trading) - default and safe
api = TradeStationAPI('sim')  # or just TradeStationAPI()

# Get market data
bars = api.market_data.get_bars('@ES', interval=5, bars_back=10)

# Get account info
balances = api.account.get_balances(api.config.account_id)

# Confirm an order (doesn't place it)
confirmation = api.orders.confirm_order(
    account_id=api.config.account_id,
    symbol='ESZ24',
    quantity=1,
    action='BUY'
)
```

## Module Structure

```
tradestation/api/
├── __init__.py         # Main exports and TradeStationAPI class
├── config.py           # Environment configuration
├── auth.py             # OAuth authentication
├── client.py           # Base HTTP client
├── market_data.py      # Market data endpoints
├── account.py          # Account information endpoints
├── orders.py           # Order management endpoints
├── example.py          # Complete usage example
└── USAGE.md            # Detailed documentation
```

## API Modules

### Market Data (`api.market_data`)
- `get_bars()` - Get historical bar data
- `stream_bars()` - Stream real-time bars
- `get_symbol_details()` - Get symbol information
- `get_quote()` - Get current quotes
- `stream_quotes()` - Stream real-time quotes
- `stream_tick_bars()` - Stream tick-based bars

### Account (`api.account`)
- `get_accounts()` - List all accounts
- `get_balances()` - Get account balances
- `get_bod_balances()` - Get beginning-of-day balances
- `get_positions()` - Get current positions
- `stream_positions()` - Stream position updates

### Orders (`api.orders`)
- `confirm_order()` - Validate order (dry-run)
- `place_order()` - Place an order
- `cancel_order()` - Cancel an order
- `get_orders()` - Get current orders
- `get_historical_orders()` - Get order history
- `stream_orders()` - Stream order updates
- `get_routes()` - Get available routing options

## Environment Management

The library supports two environments:

- **`sim`** - Paper trading (simulation) - **DEFAULT**
- **`prod`** - Live trading ⚠️ REAL MONEY

Just pass the environment when creating the API:
```python
api = TradeStationAPI('sim')   # Paper trading
api = TradeStationAPI('prod')  # Live trading (REAL MONEY)
```

The same .env file is used for both - the environment parameter only determines which API URL is used.

## Safety Features

1. Always defaults to `sim` if no environment specified
2. Displays warning banner when using `prod` environment
3. Validates required credentials on initialization
4. Single .env file prevents confusion

## Documentation

See [USAGE.md](./USAGE.md) for complete documentation including:
- Initial OAuth setup
- Detailed API examples
- Streaming data usage
- Error handling
- Advanced usage patterns

## Running the Example

```bash
# Make sure you have .env.sim configured
python -m tradestation.api.example
```

## Requirements

- Python 3.7+
- `requests`
- `python-dotenv`

Install with:
```bash
pip install -r ../../requirements.txt
```

## Notes

- Access tokens expire after 20 minutes (handled automatically by the library)
- Refresh tokens last indefinitely (store securely in `.env.*` files)
- Never commit `.env.*` files to version control (they're in `.gitignore`)
- Use `.env.example` as a template for creating your environment files

