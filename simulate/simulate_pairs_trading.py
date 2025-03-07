
### Constants

# Trading parameters
trigger_percent = 0.5 # The difference that triggers a buy or sell (need to convert from percentage)
trigger = trigger_percent / 100
trades_per_day_limit = 10
moving_average_window = 240 # In minutes
trade_delay = 1 # In minutes
eval_time = "close" # Use either "open", "close", or "mid" for the price
trade_time = "close" # Use either "open", "close", or "mid" for the price
is_limit_ordering = False
initial_cash = 100000

skip_start = 0
duration = 0 # Duration in days to run the sim. If 0, no duration. Max for my file is 237 (won't error if higher though)
eval_freq = 60 # How often to evaluate performance, in days

# Slippage calculation - https://www.barchart.com/stocks/quotes/V
volatility = 0.15
adv = 6000000
impact_coefficient = 0.0055
sec_fee = 0.0000278 # Only applies to sales

### Start conditions
cash = initial_cash
visa_shares = 0
mastercard_shares = 0

### Imports
import pandas as pd
import math
from typing import Tuple
from enum import Enum
import sys
sys.path.insert(0, "..")
import Utilities

# Capture command-line arguments
args = sys.argv[1:]  # Exclude the script name

tickers = args # ["AMD", "TSM"]
if len(tickers) != 2:
    raise Exception("Must provide exactly 2 tickers. e.g. `python simulate_pairs_trading.py AMD TSM`")

# File paths
matching_files = Utilities.find_files_by_pattern(Utilities.get_path_from_project_root("simulate/histories"), f"{tickers[0]}_{tickers[1]}*.csv")
if len(matching_files) != 1:
    raise Exception(f"Found {len(matching_files)} files matching {tickers[0]}_{tickers[1]}*.csv. Expected 1.")
combined_file_path = matching_files[0]

### Global variables
visa_last_price = 0
mastercard_last_price = 0
total_gains = 0
total_losses = 0
visa_price_col_name_t0 = f"visa_{eval_time}"
mastercard_price_col_name_t0 = f"mastercard_{eval_time}"
visa_price_col_name_t1 = f"visa_{trade_time}"
mastercard_price_col_name_t1 = f"mastercard_{trade_time}"
performance_chart: pd.DataFrame = pd.DataFrame(columns=["day", "visa_shares", "visa_price", "mastercard_shares", "mastercard_price", "cash", "total_value"])

# Define the stock enum
class Stock(Enum):
    VISA = 1
    MASTERCARD = 2

### Functions

def add_daily_performance(day, visa_price, visa_shares, mastercard_price, mastercard_shares, cash):
    global performance_chart

    # Add the current day's performance to the performance chart
    total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
    performance_chart = performance_chart._append({
        "day": day,
        "visa_shares": visa_shares,
        "visa_price": visa_price,
        "mastercard_shares": mastercard_shares,
        "mastercard_price": mastercard_price,
        "cash": cash,
        "total_value": total_value
    }, ignore_index=True)

def log_daily_performance(day: int, freq: int = 1, pad_end: bool = True):
    # Log the performance both all time and from the last [freq] days
    # Compare the performance with the market

    # Make sure the day > freq
    if day < freq:
        raise Exception("Duration days must be greater than or equal to the frequency")

    # Get algo performance numbers
    current_value = performance_chart[performance_chart["day"] == day]["total_value"].values[0]
    init_value = performance_chart[performance_chart["day"] == 0]["total_value"].values[0]
    last_freq_value = performance_chart[performance_chart["day"] == day - freq]["total_value"].values[0]
    algo_growth_all_time = current_value / init_value
    algo_growth_last_freq = current_value / last_freq_value
    
    # Get market performance numbers
    current_stock_prices = performance_chart[performance_chart["day"] == day]["visa_price"].values[0] + performance_chart[performance_chart["day"] == day]["mastercard_price"].values[0]
    init_stock_prices = performance_chart[performance_chart["day"] == 0]["visa_price"].values[0] + performance_chart[performance_chart["day"] == 0]["mastercard_price"].values[0]
    last_freq_stock_prices = performance_chart[performance_chart["day"] == day - freq]["visa_price"].values[0] + performance_chart[performance_chart["day"] == day - freq]["mastercard_price"].values[0]
    market_growth_all_time = current_stock_prices / init_stock_prices
    market_growth_last_freq = current_stock_prices / last_freq_stock_prices

    algo_perf_all_time = algo_growth_all_time / market_growth_all_time
    algo_perf_last_freq = algo_growth_last_freq / market_growth_last_freq

    # Print a table of the values, with the columns: day, last {freq}d, all time; and rows: market, algo, performance
    print(f"+{'-'*10}+{'-'*10}+{'-'*10}+")
    print(f"|{'Day ' + str(day):<10}|{'Last ' + str(freq) + 'd':<10}|{'All time':<10}|")
    print(f"+{'-'*10}+{'-'*10}+{'-'*10}+")
    print(f"|{'Market':<10}|{str(round(market_growth_last_freq * 100, 1))+'%':<10}|{str(round(market_growth_all_time * 100, 1))+'%':<10}|")
    print(f"+{'-'*10}+{'-'*10}+{'-'*10}+")
    print(f"|{'Algo':<10}|{str(round(algo_growth_last_freq * 100, 1))+'%':<10}|{str(round(algo_growth_all_time * 100, 1))+'%':<10}|")
    print(f"+{'-'*10}+{'-'*10}+{'-'*10}+")
    print(f"|{'Perf':<10}|{str(round(algo_perf_last_freq * 100, 1))+'%':<10}|{str(round(algo_perf_all_time * 100, 1))+'%':<10}|")
    print(f"+{'-'*10}+{'-'*10}+{'-'*10}+")
    if pad_end:
        print("\n\n\n")

# Calculate the market impact of a trade
def calculate_market_impact(volume) -> float:
    return volatility * impact_coefficient * math.sqrt(volume / adv)

# Buy stock
# Returns a tuple with the remaining cash and number of shares bought
def buy_stock(cash, price, stock: Stock) -> Tuple[float, float]:
    # Calculate the number of shares to buy
    wanted_shares = cash / price

    # Calculate the market impact
    market_impact = calculate_market_impact(wanted_shares)

    # Adjust the volume to account for the market impact
    adjusted_shares = wanted_shares / (1 + market_impact)

    # Calculate the total cost of the trade
    total_cost = adjusted_shares * price * (1 + market_impact)

    # Calculate the remaining cash
    remaining_cash = cash - total_cost

    # Update the last price of the stock
    global visa_last_price
    global mastercard_last_price
    if stock == Stock.VISA:
        visa_last_price = price * (1 + market_impact)
    elif stock == Stock.MASTERCARD:
        mastercard_last_price = price * (1 + market_impact)

    return (remaining_cash, adjusted_shares)

# Sell stock
# Returns a tuple with the remaining cash and remaining shares
def sell_stock(shares, price, stock: Stock) -> Tuple[float, float]:
    # Calculate the market impact
    market_impact = calculate_market_impact(shares)

    # Calculate the total sale of the trade
    total_sale = shares * price * (1 - market_impact - sec_fee)

    # Update the total gains/losses for the stock
    global total_gains
    global total_losses
    if stock == Stock.VISA:
        sell_price = price * (1 - market_impact)
        if sell_price > visa_last_price:
            total_gains += (sell_price - visa_last_price) * shares
        elif sell_price < visa_last_price:
            total_losses += (sell_price - visa_last_price) * shares
    elif stock == Stock.MASTERCARD:
        sell_price = price * (1 - market_impact)
        if sell_price > mastercard_last_price:
            total_gains += (sell_price - mastercard_last_price) * shares
        elif sell_price < mastercard_last_price:
            total_losses += (sell_price - mastercard_last_price) * shares

    return (total_sale, 0)

### Setup

# Read csv files into dataframes, with columns open,high,low,close,volume,vwap,timestamp,transactions,otc
stocks_df = pd.read_csv(combined_file_path)

# Create a new column that captures the ratio between the two stocks
stocks_df['ratio'] = stocks_df[visa_price_col_name_t0] / stocks_df[mastercard_price_col_name_t0]

### Main

# Go through each row, comparing the timestamps (in format "YYYY-MM-DD HH:MM:SS"). If they don't match, print the row, then move on to the next row in whichever dataframe has the earlier timestamp
trade_swap_count = 0
attempted_trade_swap_count = 0
trades_left_today = trades_per_day_limit
previous_day = stocks_df['timestamp'][moving_average_window].split(" ")[0]
trade_limit_reached_counter = 0
days_passed = 0
days_processed = 0
init_run = True # Is this the first processed run of the algorithm?
print("Starting algorithm...\n")
for i in range(moving_average_window, len(stocks_df) - trade_delay):
    # Check if new day and reset trades left based on the timestamp
    current_day = stocks_df['timestamp'][i].split(" ")[0]
    if current_day != previous_day:
        previous_day = current_day
        trades_left_today = trades_per_day_limit
        days_passed += 1 # Add that the previous day has passed
        if days_passed > skip_start:
            days_processed += 1 # Add that the previous day was processed
            add_daily_performance(days_processed, visa_price, visa_shares, mastercard_price, mastercard_shares, cash)
        if trades_left_today == 0:
            print(f"Daily limit reset\n")
        if days_processed > 0 and days_processed % eval_freq == 0:
            log_daily_performance(days_processed, eval_freq)
    elif trades_per_day_limit != 0 and trades_left_today == 0:
        i += 1
        continue

    # Skip the first [skip_start] days
    if days_passed < skip_start:
        i += 1
        continue

    # Break the loop if the duration is reached
    if duration != 0 and days_processed >= duration:
        # visa_price = stocks_df[visa_price_col_name_t1][i]
        # mastercard_price = stocks_df[mastercard_price_col_name_t1][i]
        break

    # Variables
    visa_price = stocks_df[visa_price_col_name_t1][i + trade_delay]
    mastercard_price = stocks_df[mastercard_price_col_name_t1][i + trade_delay]
    time = stocks_df['timestamp'][i]

    # Initial condition - buy mastercard
    if init_run:
        (cash, mastercard_shares) = buy_stock(cash, mastercard_price, Stock.MASTERCARD)
        total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
        # print(f"Bought mastercard at time {time}")
        # print(f"[Visa: {visa_shares}, Mastercard: {mastercard_shares}, Value: {total_value}]\n")
        mastercard_bought_price = mastercard_price
        init_visa_price = visa_price
        init_mastercard_price = mastercard_price
        add_daily_performance(days_processed, visa_price, visa_shares, mastercard_price, mastercard_shares, cash) # Add the first day's performance
        init_run = False

    # Calculate the moving average of the ratio for the last [moving_average_window] minutes
    ratio_moving_average = stocks_df['ratio'][i - moving_average_window:i].mean()
    
    ratio_diff_from_moving_average = stocks_df['ratio'][i] / ratio_moving_average
    diff = ratio_diff_from_moving_average - 1

    # If holding visa and the ratio is above the moving average, sell visa and buy mastercard
    limit_visa_sale_price = stocks_df[visa_price_col_name_t0][i] # Take price at eval time with no delay
    limit_mastercard_sale_price = stocks_df[mastercard_price_col_name_t0][i] # Take price at eval time with no delay
    if visa_shares > mastercard_shares and diff > trigger:
        attempted_trade_swap_count += 1
        if not is_limit_ordering or visa_price >= limit_visa_sale_price:
            (cash, visa_shares) = sell_stock(visa_shares, visa_price, Stock.VISA)
            (cash, mastercard_shares) = buy_stock(cash, mastercard_price, Stock.MASTERCARD)
            total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
            # print(f"Sold visa, bought mastercard at diff {diff} - time {time}")
            # print(f"[Visa: {visa_shares}, Mastercard: {mastercard_shares}, Value: {total_value}]\n")
            trade_swap_count += 1
            trades_left_today -= 1
            if trades_per_day_limit != 0 and trades_left_today == 0:
                trade_limit_reached_counter += 1
                # print(f"Reached daily trade limit, skipping the rest of the day")
    # If holding mastercard and the ratio is below the moving average, sell mastercard and buy visa
    elif mastercard_shares > visa_shares and diff < -trigger:
        attempted_trade_swap_count += 1
        if not is_limit_ordering or mastercard_price >= limit_mastercard_sale_price:
            (cash, mastercard_shares) = sell_stock(mastercard_shares, mastercard_price, Stock.MASTERCARD)
            (cash, visa_shares) = buy_stock(cash, visa_price, Stock.VISA)
            total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
            # print(f"Sold mastercard, bought visa at diff {diff} - time {time}")
            # print(f"[Visa: {visa_shares}, Mastercard: {mastercard_shares}, Value: {total_value}]\n")
            trade_swap_count += 1
            trades_left_today -= 1
            if trades_per_day_limit != 0 and trades_left_today == 0:
                trade_limit_reached_counter += 1
                # print(f"Reached daily trade limit, skipping the rest of the day")
# Count the last day
days_passed += 1
if init_run == False:
    days_processed += 1
    add_daily_performance(days_processed, visa_price, visa_shares, mastercard_price, mastercard_shares, cash)
    log_daily_performance(days_processed, eval_freq, False)

print(f"Done simulating pairs trading! Triggered {trade_swap_count} total time out of {attempted_trade_swap_count} attempts.")
print(f"Reached daily trade limit {trade_limit_reached_counter} times.")
print(f"Total gains: {total_gains}, total losses: {total_losses}")
print(f"Days passed: {days_passed}")