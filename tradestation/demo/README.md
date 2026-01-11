# TradeStation API Demo Module

This module provides interactive demonstrations of the TradeStation API functionality.

## Features

- **Modular Design**: Each demo function is in its own file
- **Dual Usage**: Functions can be called from Python or run as CLI scripts
- **Clean Notebook**: The Jupyter notebook now provides a guided tour using these modules

## Structure

```
tradestation/demo/
├── __init__.py                    # Export all demo functions
├── utils.py                       # Shared utilities (to_float helper)
├── setup_api.py                   # Environment setup & API initialization
├── get_bars.py                    # Get historical bar data
├── get_symbol_details.py          # Get symbol information
├── get_quote.py                   # Get real-time quotes
├── get_balances.py                # Get account balances
├── get_positions.py               # Get current positions
├── confirm_order.py               # Confirm order (dry-run)
├── place_order.py                 # Place actual order
├── cancel_order.py                # Cancel order by ID
├── get_orders.py                  # Get current orders
├── get_order_by_id.py             # Get specific order details
├── get_historical_orders.py       # Get historical orders
├── get_routes.py                  # Get available routing options
├── stream_quotes.py               # Stream real-time quotes
├── stream_tick_bars.py            # Stream tick bars
├── stream_bars.py                 # Stream time-based bars
├── stream_positions_orders.py     # Stream positions and orders
└── api_usage.ipynb                # Interactive notebook
```

## Usage

### In Python

```python
from tradestation.demo import setup_api, get_quote, get_bars

# Initialize API
api, account_id = setup_api('sim')

# Get real-time quote
get_quote(api, 'AAPL')

# Get historical bars
get_bars(api, symbol='ES', interval=5, unit='Minute', bars_back=10)
```

### Command Line

Each module can be run as a standalone script:

```bash
# Market Data
python -m tradestation.demo.get_bars --symbol ES --interval 5 --bars-back 10
python -m tradestation.demo.get_symbol_details --symbol ES
python -m tradestation.demo.get_quote --symbol AAPL

# Account Information
python -m tradestation.demo.get_balances --environment sim
python -m tradestation.demo.get_positions --environment sim

# Order Management
python -m tradestation.demo.confirm_order --symbol AAPL --quantity 1 --action BUY
python -m tradestation.demo.get_orders --environment sim
python -m tradestation.demo.get_historical_orders --since 2024-01-01
python -m tradestation.demo.get_routes

# Streaming (runs indefinitely - press Ctrl+C to stop)
python -m tradestation.demo.stream_quotes --symbols AAPL,MSFT
python -m tradestation.demo.stream_bars --symbol @ES --interval 1
python -m tradestation.demo.stream_tick_bars --symbol AAPL --interval 100
```

### Jupyter Notebook

Open `api_usage.ipynb` in Jupyter for an interactive guided tour:

```bash
jupyter notebook tradestation/demo/api_usage.ipynb
```

The notebook demonstrates all functionality with example calls to the demo modules.

## Module Details

### Shared Utilities

- **`utils.py`**: Contains `to_float()` helper for safe type conversions
- **`setup_api.py`**: Initializes the API and returns `(api, account_id)` tuple

### Market Data Modules

- **`get_bars`**: Historical OHLC bar data with configurable intervals
- **`get_symbol_details`**: Symbol metadata and trading specifications
- **`get_quote`**: Real-time bid/ask/last prices with spread calculation

### Account Modules

- **`get_balances`**: Account value, cash balance, and buying power
- **`get_positions`**: Current positions with P&L calculations

### Order Management Modules

- **`confirm_order`**: Validate orders without placing them (dry-run)
- **`place_order`**: Execute actual trades ⚠️
- **`cancel_order`**: Cancel pending orders
- **`get_orders`**: List active orders
- **`get_order_by_id`**: Track specific order status
- **`get_historical_orders`**: Review past trades
- **`get_routes`**: Available order routing options

### Streaming Modules

**Note**: Streaming functions run indefinitely until interrupted (Ctrl+C).
Best used in standalone scripts rather than notebooks.

- **`stream_quotes`**: Real-time price updates
- **`stream_bars`**: Time-based OHLC bars
- **`stream_tick_bars`**: Volume-based bars (fixed trade count)
- **`stream_positions_orders`**: Real-time position and order updates

## CLI Arguments

Most modules support these common arguments:

- `--environment`: Trading environment (`sim` or `prod`)
- `--symbol`: Stock/futures symbol to trade
- `--quantity`: Number of shares/contracts
- `--action`: Order action (`BUY`, `SELL`, `BUYTOCOVER`, `SELLSHORT`)

Use `--help` with any module to see all available options:

```bash
python -m tradestation.demo.get_bars --help
```

## Safety Features

- Default environment is `sim` (paper trading)
- Order placement requires explicit parameters
- Confirmation before actual trades
- Clear warnings for live trading operations

## See Also

- [TradeStation API Documentation](../api/README.md)
- [API Usage Guide](../api/USAGE.md)
- [Example Scripts](../api/example.py)

