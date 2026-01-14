
# State Machine Flow Diagram

This document provides a visual flow diagram for the pairs trading algorithm's state machine, along with detailed documentation of each state, transitions, and the data tracked throughout the trading lifecycle.

## Visual State Diagram

```mermaid
stateDiagram-v2
    [*] --> WARMING_UP: Algorithm starts
    
    WARMING_UP --> CASH: MA bootstrapped, no position
    WARMING_UP --> HOLDING_WAITING: MA bootstrapped, has position (recovery)
    WARMING_UP --> ERROR: Bootstrap failed
    
    CASH --> PENDING_BUY: Initiate buy of undervalued stock
    CASH --> ERROR: API failure
    
    PENDING_BUY --> HOLDING_WAITING: Buy order filled
    PENDING_BUY --> ERROR: Order rejected
    
    HOLDING_WAITING --> HOLDING_TRIGGERED: Trigger condition met (ratio deviation > threshold)
    HOLDING_WAITING --> HOLDING_DAILY_LIMIT: Daily trade limit reached
    HOLDING_WAITING --> ERROR: Position mismatch / reconciliation failure
    
    HOLDING_TRIGGERED --> PENDING_SELL: Initiate swap (sell current position first)
    HOLDING_TRIGGERED --> HOLDING_WAITING: Abort swap (past cutoff time / conditions changed)
    HOLDING_TRIGGERED --> ERROR: API failure
    
    PENDING_SELL --> CASH: Sell order filled, now in cash
    PENDING_SELL --> ERROR: Order rejected
    
    HOLDING_DAILY_LIMIT --> HOLDING_WAITING: New trading day started (reset counter)
    HOLDING_DAILY_LIMIT --> ERROR: Position mismatch
    
    ERROR --> [*]: Terminal state - requires manual restart
    
    note right of WARMING_UP
        Fetching historical bars
        to bootstrap MA (240 min)
        Can recover existing position
    end note
    
    note right of HOLDING_WAITING
        Main monitoring state
        - Polls quotes every 1 sec
        - Tracks ratio vs MA
        - Checks trigger condition
        - Runs reconciliation
    end note
    
    note right of PENDING_SELL
        Sequential execution:
        1. Sell completes first
        2. Transition to CASH
        3. Then buy executes
        (Ensures no margin needed)
    end note
    
    note right of ERROR
        Frozen - no auto recovery
        Requires manual intervention
        Check logs for:
        - ORDER_REJECTED
        - POSITION_MISMATCH
        - API_FAILURE_EXHAUSTED
    end note
    
    note left of HOLDING_DAILY_LIMIT
        Hit trade limit for today
        Will resume monitoring
        on next trading day
        (Prevents overtrading)
    end note
```

## State Descriptions

### WARMING_UP (State 0)
**Purpose:** Bootstrap the moving average from historical data

**Entry Conditions:**
- Algorithm initialization
- First state on startup

**Activities:**
- Fetch 240 minutes of 1-minute historical bars
- Calculate initial moving average
- Query TradeStation for existing positions (recovery mode)

**Exit Conditions:**
- Success with no position → CASH
- Success with existing position → HOLDING_WAITING (recovery)
- Failure → ERROR

**State Data:**
- `current_stock`: Set based on API position query
- All counters reset

---

### CASH (State 1)
**Purpose:** Ready to buy undervalued stock, no position held

**Entry Conditions:**
- MA bootstrap complete with no position
- Sell order completed during swap
- Initial funding available

**Activities:**
- Monitor ratio vs MA
- Wait for buy opportunity (when algorithm determines undervalued stock)
- Calculate shares to buy based on available cash

**Exit Conditions:**
- Buy initiated → PENDING_BUY
- API failure → ERROR

**State Data:**
- `current_stock`: NONE
- `pending_order_id`: None

---

### PENDING_BUY (State 2)
**Purpose:** Buy order placed, waiting for fill confirmation

**Entry Conditions:**
- Buy order submitted to TradeStation
- Transitioning from CASH or after sell completes

**Activities:**
- Poll order status via API
- Wait for order status = "Filled"
- Log actual fill price and slippage

**Exit Conditions:**
- Order filled → HOLDING_WAITING
- Order rejected → ERROR (CRITICAL)
- Timeout (60 seconds) → ERROR

**State Data:**
- `current_stock`: Set to ticker being bought
- `pending_order_id`: Order ID from TradeStation

---

### HOLDING_WAITING (State 3)
**Purpose:** Main monitoring state - holding position, watching for trigger

**Entry Conditions:**
- Buy order filled
- MA bootstrapped with existing position (recovery)
- Daily limit reset to new day
- Swap aborted (conditions changed)

**Activities:**
- Poll quotes every 1 second
- Update ratio and MA continuously
- Check trigger condition: `abs(ratio / MA - 1) > trigger_percent`
- Run reconciliation every 60 seconds
- Monitor for daily limit

**Exit Conditions:**
- Trigger met → HOLDING_TRIGGERED
- Daily limit reached → HOLDING_DAILY_LIMIT
- Position mismatch → ERROR

**State Data:**
- `current_stock`: TICKER_A or TICKER_B
- `pending_order_id`: None
- `trades_today`: Tracked and incremented

---

### HOLDING_TRIGGERED (State 4)
**Purpose:** Trigger condition met, about to initiate swap

**Entry Conditions:**
- In HOLDING_WAITING
- Ratio deviation exceeds trigger_percent threshold
- Holding opposite stock to what's undervalued

**Activities:**
- Verify trigger condition still met
- Check swap cutoff time (not within 10 min of close)
- Check daily trade limit
- Prepare for swap execution

**Exit Conditions:**
- Proceed with swap → PENDING_SELL
- Abort (past cutoff or conditions changed) → HOLDING_WAITING
- API failure → ERROR

**State Data:**
- `current_stock`: Unchanged
- `portfolio_value_at_trade_start`: Captured for return calculation

---

### PENDING_SELL (State 5)
**Purpose:** Sell order placed, waiting for fill (buy follows)

**Entry Conditions:**
- Swap initiated from HOLDING_TRIGGERED
- Sell order submitted to TradeStation

**Activities:**
- Poll order status via API
- Wait for order status = "Filled"
- Log actual fill price and slippage
- Calculate actual proceeds

**Exit Conditions:**
- Order filled → CASH (then immediate buy follows)
- Order rejected → ERROR (CRITICAL)
- Timeout → ERROR

**State Data:**
- `current_stock`: About to become NONE
- `pending_order_id`: Sell order ID

**Critical Note:** After transitioning to CASH, the algorithm immediately initiates a buy of the other stock. This is the sequential sell→buy pattern that ensures no margin is needed.

---

### HOLDING_DAILY_LIMIT (State 6)
**Purpose:** Position held, daily trade limit reached

**Entry Conditions:**
- In HOLDING_WAITING
- `trades_today` >= `trades_per_day_limit` (from settings.yaml)
- `enforce_one_trade_per_day` check also enforced

**Activities:**
- Continue monitoring ratio and MA (logging only)
- Continue reconciliation
- Check for new trading day (date change)
- NO trading actions taken

**Exit Conditions:**
- New trading day detected → HOLDING_WAITING (counter reset)
- Position mismatch → ERROR

**State Data:**
- `current_stock`: Unchanged
- `trades_today`: At limit
- `last_trade_day`: Date of last trade

**Safety Note:** This state prevents overtrading. Both `trades_per_day_limit` and `enforce_one_trade_per_day` must allow the trade.

---

### ERROR (State 7)
**Purpose:** Fatal error occurred, algorithm frozen

**Entry Conditions:**
- Order rejected by TradeStation
- Position mismatch during reconciliation
- API retries exhausted
- Invalid state transition attempted
- Any CRITICAL error

**Activities:**
- Log CRITICAL error with full context
- Emit error metrics to Grafana
- Halt all trading activity
- Preserve state for debugging

**Exit Conditions:**
- **NONE** - Terminal state
- Requires manual restart of algorithm

**State Data:**
- All data preserved for debugging

**Recovery Procedure:**
1. Check logs for CRITICAL error pattern
2. Identify root cause (ORDER_REJECTED, POSITION_MISMATCH, etc.)
3. Manually resolve issue (fund account, fix positions, etc.)
4. Restart algorithm
5. Algorithm will recover state from TradeStation API

---

## State Data Tracked

The `StateData` class (lines 106-126 in `state_machine.py`) tracks the following across states:

| Field | Type | Description |
|-------|------|-------------|
| `current_stock` | StockHeld | Which stock is held: NONE, TICKER_A, or TICKER_B |
| `pending_order_id` | Optional[str] | Order ID when in PENDING states |
| `trades_today` | int | Count of trades executed today |
| `last_trade_day` | Optional[str] | Date of last trade (for daily reset) |
| `portfolio_value_at_trade_start` | float | Portfolio value when trade sequence began |

---

## Transition Triggers

### Automatic Transitions (Algorithm-Driven)

| From State | To State | Trigger |
|------------|----------|---------|
| WARMING_UP | CASH | MA bootstrap complete, no position |
| WARMING_UP | HOLDING_WAITING | MA bootstrap complete, has position |
| PENDING_BUY | HOLDING_WAITING | Order status = "Filled" |
| HOLDING_WAITING | HOLDING_TRIGGERED | `abs(ratio/MA - 1) > trigger_percent` |
| HOLDING_WAITING | HOLDING_DAILY_LIMIT | `trades_today >= limit` |
| HOLDING_TRIGGERED | PENDING_SELL | Swap execution approved |
| HOLDING_TRIGGERED | HOLDING_WAITING | Swap aborted (cutoff time) |
| PENDING_SELL | CASH | Order status = "Filled" |
| HOLDING_DAILY_LIMIT | HOLDING_WAITING | New trading day (date change) |

### Error Transitions (Exception-Driven)

Any state can transition to ERROR when:
- API call fails after retry exhaustion
- Order rejected by TradeStation
- Position mismatch detected in reconciliation
- Invalid state transition attempted
- Any exception marked CRITICAL

---

## Trading Flow Examples

### Successful Swap Flow

```
HOLDING_WAITING (holding V)
  ↓ (ratio too high, V overvalued)
HOLDING_TRIGGERED
  ↓ (initiate swap)
PENDING_SELL (selling V)
  ↓ (order filled)
CASH (proceeds available)
  ↓ (immediate buy)
PENDING_BUY (buying MA)
  ↓ (order filled)
HOLDING_WAITING (holding MA)
```

### Initial Buy Flow

```
WARMING_UP (bootstrapping MA)
  ↓ (MA ready, no position)
CASH (ready to trade)
  ↓ (algorithm determines undervalued stock)
PENDING_BUY (buying undervalued stock)
  ↓ (order filled)
HOLDING_WAITING (monitoring for triggers)
```

### Daily Limit Flow

```
HOLDING_WAITING (1 trade done today)
  ↓ (trigger met again)
HOLDING_TRIGGERED
  ↓ (daily limit check fails)
HOLDING_WAITING (abort - can't trade)
  ↓ (limit officially hit)
HOLDING_DAILY_LIMIT (frozen for today)
  ↓ (next day arrives)
HOLDING_WAITING (reset, ready to trade)
```

### Error Recovery Flow

```
HOLDING_WAITING
  ↓ (API failure after retries)
ERROR (frozen)
  ↓ (manual investigation)
[Fix issue in TradeStation]
  ↓ (restart algorithm)
WARMING_UP (bootstrap MA)
  ↓ (recover existing position)
HOLDING_WAITING (resumed)
```

---

## Code References

**State Machine Implementation:**
- `live_trading/state_machine.py` - Core state machine class
  - Lines 29-44: `TradingState` enum definitions
  - Lines 55-98: `VALID_TRANSITIONS` dictionary
  - Lines 128-336: `StateMachine` class

**State Usage in Main Algorithm:**
- `live_trading/live_pairs_trader.py` - Main trading loop
  - State transitions occur throughout the main loop
  - Trigger checking in monitoring logic
  - Order execution flows

**Configuration:**
- `live_trading/settings.yaml`
  - Line 55: `trades_per_day_limit`
  - Line 61: `enforce_one_trade_per_day`
  - Line 45: `trigger_percent`
  - Line 121: `swap_cutoff_minutes_before_close`

---

## Monitoring State Transitions

### Prometheus Metric

```promql
pairs_trader_state{service_name="PairTraderPro"}
```

**Values:** 0-7 corresponding to state enum values

**Visualization:** Use Grafana state timeline panel to see state changes over time

### Log Patterns

State transitions are logged with:
```
[INFO] State transition: HOLDING_WAITING → HOLDING_TRIGGERED (reason: Trigger condition met)
```

Filter in Loki:
```logql
{service_name="PairTraderPro"} |= "State transition"
```

---

## Safety Considerations

1. **Sequential Order Execution:** The PENDING_SELL → CASH → PENDING_BUY flow ensures sell completes before buy starts, eliminating margin requirements.

2. **Daily Limit Protection:** Two-layer check (`trades_per_day_limit` + `enforce_one_trade_per_day`) prevents runaway trading.

3. **Swap Cutoff:** No swaps initiated within 10 minutes of market close (configurable).

4. **Error State is Terminal:** Algorithm won't auto-recover from ERROR state, requiring manual intervention to prevent compounding issues.

5. **Reconciliation:** Periodic position checks ensure internal state matches TradeStation reality.

6. **Full Fill Requirement:** Algorithm never proceeds with partial fills, only "Filled" status.

---

*Generated from state_machine.py version as of January 2026*

