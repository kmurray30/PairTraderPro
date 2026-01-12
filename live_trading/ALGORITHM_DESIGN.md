# Pairs Trading Algorithm - Design Document

This document provides comprehensive documentation for the live pairs trading algorithm, including theory, implementation details, configuration, monitoring, and operational procedures.

## Table of Contents

1. [Algorithm Theory](#algorithm-theory)
2. [State Machine](#state-machine)
3. [Configuration Reference](#configuration-reference)
4. [Order Execution Flow](#order-execution-flow)
5. [Performance Tracking](#performance-tracking)
6. [Monitoring & Alerting](#monitoring--alerting)
7. [Grafana Dashboard Setup](#grafana-dashboard-setup)
8. [Operational Runbook](#operational-runbook)

---

## Algorithm Theory

### What is Pairs Trading?

Pairs trading is a **market-neutral** strategy that exploits temporary deviations in the price relationship between two correlated securities. Instead of betting on market direction, pairs trading bets on the relationship between two stocks returning to its historical norm.

### The Core Strategy

This algorithm trades between two correlated stocks (e.g., Visa and Mastercard):

1. **Calculate the price ratio**: `ratio = price_A / price_B`
2. **Track the moving average** of this ratio over a rolling window (e.g., 240 minutes)
3. **When the ratio deviates** significantly from its MA:
   - If ratio > MA + trigger: Stock A is "overvalued" relative to B → Sell A, Buy B
   - If ratio < MA - trigger: Stock B is "overvalued" relative to A → Sell B, Buy A

### Why This Works

The premise is that correlated stocks tend to move together. When they temporarily diverge, they often revert to their historical relationship. The algorithm captures profit from this mean reversion.

**Example:**
- Visa and Mastercard historically trade at a ratio of ~0.54 (V/MA)
- If Visa jumps up relative to Mastercard, pushing ratio to 0.56
- The algorithm sells Visa (overvalued) and buys Mastercard (undervalued)
- When the ratio reverts to 0.54, the algorithm profits from the convergence

### Mathematical Formulation

```
ratio_t = price_A_t / price_B_t

MA_t = mean(ratio_{t-240} ... ratio_{t-1})

deviation_t = (ratio_t / MA_t) - 1

trigger_condition:
  - If holding A and deviation > +0.4%: SWAP A → B
  - If holding B and deviation < -0.4%: SWAP B → A
```

### Market Impact Model (Slippage Estimation)

The algorithm estimates expected slippage using a market impact model:

```
market_impact = volatility × impact_coefficient × sqrt(shares / average_daily_volume)
```

This helps compare expected vs actual execution costs.

---

## State Machine

The algorithm operates as a finite state machine with the following states:

### State Diagram

```
                              ┌──────────────────┐
                              │   WARMING_UP     │
                              │  (Bootstrap MA)  │
                              └────────┬─────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
            ┌───────────┐      ┌───────────────┐   ┌─────────────┐
            │   CASH    │◄────►│HOLDING_WAITING│──►│   ERROR     │
            │(No position)     │ (Monitoring)  │   │  (Frozen)   │
            └─────┬─────┘      └───────┬───────┘   └─────────────┘
                  │                    │
                  ▼                    ▼
            ┌───────────┐      ┌───────────────────┐
            │PENDING_BUY│      │HOLDING_TRIGGERED  │
            │(Order out)│      │  (Trigger met)    │
            └─────┬─────┘      └────────┬──────────┘
                  │                     │
                  │                     ▼
                  │            ┌───────────────┐
                  │            │ PENDING_SELL  │
                  │            │  (Sell first) │
                  │            └───────┬───────┘
                  │                    │
                  └────────────────────┘
                           │
                           ▼
                  ┌───────────────────┐
                  │HOLDING_DAILY_LIMIT│
                  │(Max trades reached)│
                  └───────────────────┘
```

### State Descriptions

| State | Description | Valid Transitions |
|-------|-------------|-------------------|
| `WARMING_UP` | Fetching historical bars to bootstrap the moving average | → CASH, HOLDING_WAITING, ERROR |
| `CASH` | No position held, ready to buy undervalued stock | → PENDING_BUY, ERROR |
| `PENDING_BUY` | Buy order placed, waiting for fill confirmation | → HOLDING_WAITING, ERROR |
| `HOLDING_WAITING` | Position held, monitoring for trigger condition | → HOLDING_TRIGGERED, HOLDING_DAILY_LIMIT, ERROR |
| `HOLDING_TRIGGERED` | Trigger condition met, about to initiate swap | → PENDING_SELL, HOLDING_WAITING, ERROR |
| `PENDING_SELL` | Sell order placed, waiting for fill (buy follows) | → CASH, ERROR |
| `HOLDING_DAILY_LIMIT` | Position held, daily trade limit reached | → HOLDING_WAITING (on new day), ERROR |
| `ERROR` | Fatal error occurred, app is frozen | None (terminal state) |

### State Data

In addition to the state enum, the following data is tracked:

- `current_stock`: Which stock is held (`ticker_a`, `ticker_b`, or `none`)
- `pending_order_id`: Order ID when in PENDING states
- `trades_today`: Count of trades executed today
- `portfolio_value_at_trade_start`: For return calculation

---

## Configuration Reference

All configuration is in `settings.yaml`:

### Environment

```yaml
environment: "sim"   # "sim" for paper trading, "prod" for REAL MONEY
```

**WARNING:** Setting `environment: "prod"` will execute REAL trades with REAL money!

### Trading Pair

```yaml
ticker_a: "V"      # First stock (numerator of ratio)
ticker_b: "MA"     # Second stock (denominator of ratio)
```

**Important:** The ratio is always `ticker_a / ticker_b`. Choose ordering carefully.

### Algorithm Parameters

```yaml
trigger_percent: 0.4              # Min % deviation to trigger swap
moving_average_window_minutes: 240 # 4 hours of 1-minute bars
trades_per_day_limit: 1           # Max swaps per day (0 = unlimited)
```

### Hardcoded Safety Flag

In addition to `trades_per_day_limit`, there is a **hardcoded** safety flag in `live_pairs_trader.py`:

```python
ENFORCE_ONE_TRADE_PER_DAY = True
```

When `True` (default), this flag **overrides** `trades_per_day_limit` to ensure no more than 1 swap per day, regardless of configuration. This is a failsafe against misconfiguration.

To disable this safety check (not recommended), you must edit the code directly.

### Slippage Model

```yaml
slippage:
  volatility: 0.15                # Annual volatility (decimal)
  average_daily_volume: 6000000   # Avg daily volume in shares
  impact_coefficient: 0.0055      # Impact factor
  sec_fee_rate: 0.0000278         # SEC fee on sales
```

### Operational Settings

```yaml
poll_interval_seconds: 1              # Quote polling frequency
reconciliation_interval_seconds: 60   # Position verification frequency
retry_attempts: 3                     # API retry count
retry_backoff_seconds: 2              # Retry delay multiplier
swap_cutoff_minutes_before_close: 10  # No swaps after 3:50 PM ET
```

### Performance Tracking

```yaml
performance_timeframes:
  - "7d"    # 7-day rolling performance
  - "60d"   # 60-day rolling performance
```

### Environment Variables

In `.env` at project root:

```bash
# TradeStation Credentials
TRADESTATION_API_KEY=your_api_key
TRADESTATION_SECRET=your_secret
REFRESH_TOKEN=your_refresh_token

# Account IDs (auto-switch based on sim/prod)
ACCOUNT_ID_SIM=SIM12345
ACCOUNT_ID_PROD=PROD67890

# Grafana Cloud (for metrics/logs)
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-prod-us-central-0.grafana.net/otlp
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic base64_encoded_creds
```

---

## Order Execution Flow

### Swap Execution (Sell → Buy)

The algorithm executes swaps **sequentially**, not in parallel:

```
1. Trigger condition detected
2. Transition: HOLDING_WAITING → HOLDING_TRIGGERED
3. Calculate shares to sell (entire position)
4. Transition: HOLDING_TRIGGERED → PENDING_SELL
5. Place SELL market order
6. Poll order status until Filled
   - Log actual fill price
   - Calculate actual slippage
7. Transition: PENDING_SELL → CASH
8. Get updated buying power from API
9. Calculate shares to buy (floor(cash / price))
10. Transition: CASH → PENDING_BUY
11. Place BUY market order
12. Poll order status until Filled
    - Log actual fill price
    - Calculate actual slippage
13. Transition: PENDING_BUY → HOLDING_WAITING
14. Record trade in performance tracker
15. Emit metrics and log to CSV
16. Increment daily trade counter
```

### Why Sequential?

Selling before buying ensures:
- No margin requirements (using proceeds from sale)
- Simpler error handling
- Clear state at each step
- Easier reconciliation

### Order Status Polling

After placing an order, the algorithm polls `get_order(order_id)` until:
- `Filled`: Success, proceed to next step
- `Rejected`: CRITICAL error, halt algorithm
- Timeout: Error after ~60 seconds

### Shares Calculation

```python
shares_to_buy = floor(available_cash / current_price)
```

This rounds DOWN to whole shares, leaving small cash residual.

---

## Performance Tracking

### Compound Return Multipliers

Returns are tracked as **multipliers** that compound correctly regardless of deposits/withdrawals:

```python
# After each trade:
period_multiplier = portfolio_value_after / portfolio_value_before

# All-time return:
all_time_multiplier = product(all_period_multipliers)

# As percentage:
return_pct = (all_time_multiplier - 1) × 100
```

**Example:**
- Period 1: $10,000 → $10,500 = multiplier 1.05
- Period 2: $15,000 → $15,300 = multiplier 1.02 (after $5k deposit)
- All-time: 1.05 × 1.02 = 1.071 = **+7.1% return**

### Market Benchmark

The "market" benchmark is what you'd get holding 50/50 of both stocks:

```python
market_multiplier = (ticker_a_return + ticker_b_return) / 2
```

### Relative Performance

```python
relative_perf = algo_multiplier / market_multiplier
```

A value > 1.0 means outperforming the market.

### Tracked Metrics

| Metric | Description |
|--------|-------------|
| `portfolio_value` | Current total value in dollars |
| `return_multiplier_7d` | 7-day compound return |
| `return_multiplier_60d` | 60-day compound return |
| `return_multiplier_all_time` | All-time compound return |
| `relative_perf_*` | Algo vs market for each timeframe |
| `last_trade_return_pct` | Most recent trade's return |

### CSV Trade Log

Every trade is logged to `trades.csv` with columns:

```
trade_id, timestamp, action, symbol, quantity, expected_price, actual_price,
slippage_pct, portfolio_value_before, portfolio_value_after, period_return_pct,
compound_return_all_time, algo_return_7d, algo_return_60d, relative_perf_7d,
relative_perf_60d, relative_perf_all_time
```

### Deposits/Withdrawals Log

Manual tracking in `deposits_withdrawals.csv`:

```csv
timestamp,type,amount,notes
2026-01-15T09:00:00Z,deposit,10000,Initial funding
```

Use this to contextualize portfolio value changes not from trading.

---

## Monitoring & Alerting

### Prometheus Metrics

All metrics are prefixed with `pairs_trader_`:

| Metric | Type | Description |
|--------|------|-------------|
| `portfolio_value` | Gauge | Current $ value |
| `cash_available` | Gauge | Buying power |
| `return_multiplier_*` | Gauge | Compound returns |
| `relative_perf_*` | Gauge | Vs market |
| `state` | Gauge | Current state (0-7) |
| `trades_today` | Gauge | Daily trade count |
| `ratio` | Gauge | Current A/B ratio |
| `ratio_ma` | Gauge | Moving average |
| `ratio_deviation` | Gauge | % from MA |
| `slippage_expected` | Histogram | Estimated |
| `slippage_actual` | Histogram | Actual |

### Alert-Worthy Log Patterns

**SET UP ALERTS ON THESE PATTERNS** - they indicate the app has frozen:

| Pattern | Condition | Severity |
|---------|-----------|----------|
| `ORDER_REJECTED` | Order rejected by TradeStation | CRITICAL |
| `POSITION_MISMATCH` | Internal state doesn't match API | CRITICAL |
| `API_FAILURE_EXHAUSTED` | Retries exhausted | CRITICAL |
| `INSUFFICIENT_BUYING_POWER` | Can't afford 1 share | ERROR (recoverable) |
| `INVALID_STATE_TRANSITION` | State machine error | CRITICAL |
| `UNEXPECTED_POSITION` | Wrong stock held | CRITICAL |

### Loki Log Structure

All logs include these fields:
- `timestamp`: ISO 8601
- `level`: info/warning/error/critical
- `state`: Current algorithm state
- `ticker_a`, `ticker_b`: Configured pair
- `trade_id`: When applicable

Query examples:
```logql
{service_name="PairTraderPro"} |= "CRITICAL"
{service_name="PairTraderPro"} | json | level="error"
{service_name="PairTraderPro"} | json | alert_type="ORDER_REJECTED"
```

---

## Grafana Dashboard Setup

### Recommended Panels

#### 1. Portfolio Value (Time Series)
```promql
pairs_trader_portfolio_value{service_name="PairTraderPro"}
```

#### 2. Performance Chart (Multiple Lines)
```promql
# Algo return
pairs_trader_return_multiplier_all_time{service_name="PairTraderPro"} - 1

# Market return (you'll need to compute or store this)
# Or use relative performance
pairs_trader_relative_perf_all_time{service_name="PairTraderPro"} - 1
```

#### 3. Price Ratio with Trigger Bands
```promql
pairs_trader_ratio{service_name="PairTraderPro"}
pairs_trader_ratio_ma{service_name="PairTraderPro"}
# Calculate trigger bands in Grafana
```

#### 4. Slippage Comparison (Histogram)
```promql
histogram_quantile(0.5, pairs_trader_slippage_expected_bucket{service_name="PairTraderPro"})
histogram_quantile(0.5, pairs_trader_slippage_actual_bucket{service_name="PairTraderPro"})
```

#### 5. State Timeline (State Chart)
```promql
pairs_trader_state{service_name="PairTraderPro"}
```

Map values: 0=WARMING_UP, 1=CASH, 2=PENDING_BUY, 3=HOLDING_WAITING, 4=HOLDING_TRIGGERED, 5=PENDING_SELL, 6=HOLDING_DAILY_LIMIT, 7=ERROR

### Alert Rules

Create alerts in Grafana for:

1. **App Frozen** (CRITICAL logs):
```yaml
- alert: PairsTraderCritical
  expr: count_over_time({service_name="PairTraderPro"} |= "CRITICAL" [5m]) > 0
  for: 0m
  labels:
    severity: critical
  annotations:
    summary: "Pairs trader has frozen - manual intervention required"
```

2. **Error Rate**:
```yaml
- alert: PairsTraderErrors
  expr: count_over_time({service_name="PairTraderPro"} | json | level="error" [15m]) > 5
  for: 5m
  labels:
    severity: warning
```

3. **Stuck State**:
```yaml
- alert: PairsTraderStuck
  expr: changes(pairs_trader_state[1h]) == 0 and pairs_trader_state == 7
  for: 5m
  labels:
    severity: critical
```

---

## Operational Runbook

### Starting the Algorithm

```bash
cd /path/to/PairTraderPro
python -m live_trading.live_pairs_trader
```

The algorithm will:
1. Load configuration from `settings.yaml`
2. Connect to TradeStation simulation API
3. Bootstrap MA from 240 minutes of historical bars
4. Query current positions and recover state
5. Enter main trading loop

### Stopping the Algorithm

Press `Ctrl+C` for graceful shutdown. The algorithm will:
- Flush all metrics and logs
- Save current state
- Exit cleanly

**Note:** Stopping mid-trade is safe - on restart, the algorithm recovers state from the API.

### Recovering from Errors

When the algorithm enters ERROR state:

1. **Check logs** for the CRITICAL error:
```bash
grep "CRITICAL" /var/log/pairs_trader.log
```

2. **Identify the issue**:
   - `ORDER_REJECTED`: Check account balance, symbol validity
   - `POSITION_MISMATCH`: Log into TradeStation, verify positions
   - `API_FAILURE_EXHAUSTED`: Check network, TradeStation status

3. **Resolve the issue** (varies by error type)

4. **Restart the algorithm**:
```bash
python -m live_trading.live_pairs_trader
```

The algorithm will recover state from API on restart.

### Common Issues

#### "Insufficient buying power"
- Not enough cash to buy 1 share
- Wait for funds to be added, or deposit more
- Algorithm will automatically recover

#### "Order rejected"
- Check TradeStation for rejection reason
- Common causes: Market closed, invalid symbol, account restrictions

#### "Position mismatch"
- Internal state doesn't match API
- May occur after manual trading in TradeStation interface
- Restart algorithm to re-sync

### Changing Configuration

1. Stop the algorithm (`Ctrl+C`)
2. Edit `settings.yaml`
3. Restart the algorithm

**Live reload is not supported** - changes require restart.

### Monitoring Health

Check these regularly:
- Grafana dashboards for metrics
- Loki logs for errors
- `trades.csv` for trade history
- TradeStation interface for positions

### Backup and Recovery

The algorithm stores minimal state - everything can be recovered from:
- `settings.yaml` - Configuration
- TradeStation API - Positions and orders
- `trades.csv` - Historical performance

---

## Safety Guarantees

1. **Environment Toggle**: Controlled by `environment` in settings.yaml (default: "sim"). Production mode shows prominent warning.

2. **ENFORCE_ONE_TRADE_PER_DAY**: Hardcoded flag in code that prevents more than 1 swap per day, regardless of `trades_per_day_limit` setting. This is a failsafe against configuration errors.

3. **Sequential Orders**: Sell always completes before buy starts.

4. **Full Fill Wait**: Never proceeds with partial fills.

5. **Position Reconciliation**: Periodic verification against API.

6. **Recoverable vs Fatal Errors**: 
   - Insufficient funds: Wait and retry
   - Order rejection: Halt (CRITICAL)

7. **Swap Cutoff**: No new swaps after 3:50 PM ET.

8. **Daily Trade Limit**: Enforced per configuration AND by hardcoded safety flag.

---

## Glossary

| Term | Definition |
|------|------------|
| **Ratio** | Price of ticker_a divided by price of ticker_b |
| **MA** | Moving average of the ratio over configured window |
| **Trigger** | Minimum % deviation from MA to initiate swap |
| **Swap** | Selling one stock and buying the other |
| **Period** | Time between trades (for return calculation) |
| **Multiplier** | Return expressed as 1 + decimal_return (e.g., 1.05 = +5%) |
| **Slippage** | Difference between expected and actual execution price |

---

*Document version: 1.0.0*
*Last updated: January 2026*

