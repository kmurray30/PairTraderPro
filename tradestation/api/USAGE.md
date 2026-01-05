# TradeStation API Library - Usage Guide

This library provides a clean, modular interface to the TradeStation API with built-in environment management for safe trading operations.

## Environment Setup

### 1. Create Environment File

Copy `.env.example` to create your `.env` file:

```bash
cp .env.example .env
```

### 2. Fill in Your Credentials

Edit `.env` with your TradeStation credentials:

**Note**: The same `.env` file works for both simulation and production. You choose the environment when creating the API.

### 3. Get Your Credentials

1. **API Key & Secret**: Get from TradeStation Developer Portal
2. **Refresh Token**: Use the initial OAuth flow (see below)
3. **Account ID**: Your TradeStation account ID (e.g., `SIM2977785M` for simulation)

## Getting a Refresh Token (First Time Setup)

```python
from tradestation.api import TradeStationAPI

# Create API instance
api = TradeStationAPI('sim')

# Get authorization URL
auth_url = api.get_authorization_url()
print(f"Visit this URL in your browser:\n{auth_url}")

# After logging in, you'll be redirected to http://localhost:3000?code=YOUR_CODE
# Copy the 'code' parameter from the URL

# Exchange the code for tokens
code = "paste_your_code_here"
tokens = api.auth.exchange_code_for_tokens(code)

print(f"Refresh Token: {tokens['refresh_token']}")
print("Add this to your .env.sim file as REFRESH_TOKEN")
```

## Basic Usage

### Initialize the API

```python
from tradestation.api import TradeStationAPI

# Simulation (default and safe)
api = TradeStationAPI('sim')  # or just TradeStationAPI()

# Production (REAL MONEY)
api = TradeStationAPI('prod')
```

### Market Data

```python
# Get bar chart data
bars = api.market_data.get_bars(
    symbol='@ES',
    interval=5,
    unit='Minute',
    bars_back=10
)

# Get symbol details
symbol_info = api.market_data.get_symbol_details('@ES')

# Stream real-time bar data
for line in api.market_data.stream_bars('@ES', interval=5):
    print(line)
    # Press Ctrl+C to stop streaming
```

### Account Information

```python
# Get all accounts
accounts = api.account.get_accounts()
print(accounts)

# Get account balances
account_id = 'SIM2977785M'  # or from config: api.config.account_id
balances = api.account.get_balances(account_id)
print(balances)

# Get positions
positions = api.account.get_positions(account_id)
print(positions)

# Stream position updates
for line in api.account.stream_positions(account_id):
    print(line)
```

### Order Management

```python
account_id = api.config.account_id

# Confirm order (dry-run, doesn't place the order)
confirmation = api.orders.confirm_order(
    account_id=account_id,
    symbol='ESZ24',
    quantity=1,
    action='BUY',
    order_type='Market'
)
print(f"Estimated cost: {confirmation}")

# Place a market order
order_result = api.orders.place_order(
    account_id=account_id,
    symbol='ESZ24',
    quantity=1,
    action='BUY',
    order_type='Market'
)
print(f"Order ID: {order_result}")

# Place a limit order
limit_order = api.orders.place_order(
    account_id=account_id,
    symbol='AAPL',
    quantity=100,
    action='BUY',
    order_type='Limit',
    limit_price=150.00
)

# Get current orders
orders = api.orders.get_orders(account_id)
print(orders)

# Cancel an order
if orders['Orders']:
    order_id = orders['Orders'][0]['OrderID']
    cancel_result = api.orders.cancel_order(order_id)
    print(cancel_result)

# Stream order updates
for line in api.orders.stream_orders(account_id):
    print(line)
```

## Environment Selection

Just pass the environment parameter:

```python
from tradestation.api import TradeStationAPI

# Simulation (paper trading)
api = TradeStationAPI('sim')

# Production (live trading - REAL MONEY)
api = TradeStationAPI('prod')
```

The environment only determines which API URL is used:
- `'sim'`: https://sim-api.tradestation.com
- `'prod'`: https://api.tradestation.com

## Safety Features

1. **Defaults to 'sim'** - If no environment specified, uses simulation
2. **Production warning** - Big red warning when using 'prod' environment
3. **Explicit environment files** - Impossible to mix sim/prod credentials
4. **Validation** - Checks for missing credentials and invalid environments

## Complete Example

```python
import os
from tradestation.api import TradeStationAPI

# Use simulation environment
os.environ['ENV'] = 'sim'

# Initialize API
api = TradeStationAPI()

# Get account info
account_id = api.config.account_id
balances = api.account.get_balances(account_id)
print(f"Account balance: {balances}")

# Get market data
bars = api.market_data.get_bars('@ES', interval=5, bars_back=10)
print(f"Latest bar: {bars}")

# Confirm an order (doesn't place it)
confirmation = api.orders.confirm_order(
    account_id=account_id,
    symbol='ESZ24',
    quantity=1,
    action='BUY'
)
print(f"Order confirmation: {confirmation}")
```

## Switching to Production

⚠️ **WARNING**: Production mode uses REAL MONEY. Only use when ready.

```python
from tradestation.api import TradeStationAPI

# Switch to production (REAL MONEY)
api = TradeStationAPI('prod')

# You'll see a warning:
# ================================================================================
# ⚠️  WARNING: RUNNING IN PRODUCTION MODE - REAL MONEY AT RISK ⚠️
# ================================================================================
```

## Advanced: Direct Module Access

```python
from tradestation.api import TradeStationClient, MarketData, Account, Orders

# Create client
client = TradeStationClient()

# Use modules directly
market_data = MarketData(client)
bars = market_data.get_bars('@ES', interval=5, bars_back=10)
```

## Error Handling

```python
from tradestation.api import TradeStationAPI

try:
    api = TradeStationAPI('sim')
    balances = api.account.get_balances('INVALID_ACCOUNT')
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"API error: {e}")
```

## Notes

- Access tokens expire after 20 minutes (handled automatically)
- Refresh tokens last indefinitely (store securely in .env files)
- Simulation API URL: `https://sim-api.tradestation.com`
- Production API URL: `https://api.tradestation.com`

