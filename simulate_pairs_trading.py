import Utilities
import pandas as pd
import math
from typing import Tuple
from enum import Enum

### Constants

# Define the stock enum
class Stock(Enum):
    VISA = 1
    MASTERCARD = 2

# Trading parameters
trigger_percent = 0.5 # The difference that triggers a buy or sell (need to convert from percentage)
trigger = trigger_percent / 100
trades_per_day_limit = 10
moving_average_window = 240 # In minutes
trade_delay = 1 # In minutes
eval_time = "close" # Use either "open", "close", or "mid" for the price
trade_time = "close" # Use either "open", "close", or "mid" for the price
limit_ordering = False
initial_cash = 100000

skip_start = 0
duration = 0 # Duration in days to run the sim. If 0, no duration. Max for my file is 237 (won't error if higher though)
eval_freq = 1 # How often to evaluate performance, in days

# Slippage calculation - https://www.barchart.com/stocks/quotes/V
volatility = 0.15
adv = 6000000
impact_coefficient = 0.0055
sec_fee = 0.0000278 # Only applies to sales

### Start conditions
cash = initial_cash
visa_shares = 0
mastercard_shares = 0

# File paths
combined_file_path = Utilities.get_path_from_project_root("histories/visa_mastercard_clean_2024-01-01_to_2024-12-31.csv")

### Global variables
visa_last_price = 0
mastercard_last_price = 0
total_gains = 0
total_losses = 0
visa_price_col_name_t0 = f"visa_{eval_time}"
mastercard_price_col_name_t0 = f"mastercard_{eval_time}"
visa_price_col_name_t1 = f"visa_{trade_time}"
mastercard_price_col_name_t1 = f"mastercard_{trade_time}"

### Functions

def evaluate_performance(visa_price, mastercard_price, init_visa_price, init_mastercard_price) -> Tuple[float, float, float]:
    global visa_shares
    global mastercard_shares
    global cash

    # Calculate how well the stocks did on their own, compared to how my algo performed
    # Average the growth of each stock
    visa_growth = visa_price / init_visa_price
    mastercard_growth = mastercard_price / init_mastercard_price
    average_growth = (visa_growth + mastercard_growth) / 2

    # Algorithm growth
    algo_growth = (cash + visa_shares * visa_price + mastercard_shares * mastercard_price) / initial_cash

    # Normalized algorithm growth
    normalized_algo_growth = algo_growth / average_growth
    return (algo_growth, average_growth, normalized_algo_growth)

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
i = moving_average_window # Start at the moving average window to calculate the moving average
print("Starting algorithm...\n")
trigger_count = 0
attempted_trigger_count = 0
trades_left_today = trades_per_day_limit
last_day = stocks_df['timestamp'][i].split(" ")[0]
daily_limit_reached_counter = 0
days_passed = 0
init_run = True
for i in range(moving_average_window, len(stocks_df) - trade_delay):
    if duration != 0 and days_passed >= duration + skip_start:
        visa_price = stocks_df[visa_price_col_name_t1][i]
        mastercard_price = stocks_df[mastercard_price_col_name_t1][i]
        break
    # Check if new day and reset trades left based on the timestamp
    current_day = stocks_df['timestamp'][i].split(" ")[0]
    if current_day != last_day:
        last_day = current_day
        trades_left_today = trades_per_day_limit
        days_passed += 1
        if trades_left_today == 0:
            print(f"Daily limit reset\n")
        if (days_passed - skip_start) % eval_freq == 0 and days_passed > skip_start:
            (algo_growth, average_growth, normalized_algo_growth) = evaluate_performance(visa_price, mastercard_price, init_visa_price, init_mastercard_price)
            print(f"Day {days_passed - skip_start} eval:\n\t{round(average_growth * 100, 1)}% market growth\n\t{round(algo_growth * 100, 1)}% algo growth value\n\t{round(normalized_algo_growth * 100, 1)}% performance\n")
    elif trades_per_day_limit != 0 and trades_left_today == 0:
        i += 1
        continue

    if days_passed < skip_start:
        i += 1
        continue

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
        init_run = False

    # Calculate the moving average of the ratio for the last [moving_average_window] minutes
    ratio_moving_average = stocks_df['ratio'][i - moving_average_window:i].mean()
    
    ratio_diff_from_moving_average = stocks_df['ratio'][i] / ratio_moving_average
    diff = ratio_diff_from_moving_average - 1

    # If holding visa and the ratio is above the moving average, sell visa and buy mastercard
    limit_visa_sale_price = stocks_df[visa_price_col_name_t0][i] # Take price at eval time with no delay
    limit_mastercard_sale_price = stocks_df[mastercard_price_col_name_t0][i] # Take price at eval time with no delay
    if visa_shares > mastercard_shares and diff > trigger:
        attempted_trigger_count += 1
        if not limit_ordering or visa_price >= limit_visa_sale_price:
            (cash, visa_shares) = sell_stock(visa_shares, visa_price, Stock.VISA)
            (cash, mastercard_shares) = buy_stock(cash, mastercard_price, Stock.MASTERCARD)
            total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
            # print(f"Sold visa, bought mastercard at diff {diff} - time {time}")
            # print(f"[Visa: {visa_shares}, Mastercard: {mastercard_shares}, Value: {total_value}]\n")
            trigger_count += 1
            trades_left_today -= 1
            if trades_per_day_limit != 0 and trades_left_today == 0:
                daily_limit_reached_counter += 1
                print(f"Reached daily trade limit, skipping the rest of the day")
    # If holding mastercard and the ratio is below the moving average, sell mastercard and buy visa
    elif mastercard_shares > visa_shares and diff < -trigger:
        attempted_trigger_count += 1
        if not limit_ordering or mastercard_price >= limit_mastercard_sale_price:
            (cash, mastercard_shares) = sell_stock(mastercard_shares, mastercard_price, Stock.MASTERCARD)
            (cash, visa_shares) = buy_stock(cash, visa_price, Stock.VISA)
            total_value = cash + visa_shares * visa_price + mastercard_shares * mastercard_price
            # print(f"Sold mastercard, bought visa at diff {diff} - time {time}")
            # print(f"[Visa: {visa_shares}, Mastercard: {mastercard_shares}, Value: {total_value}]\n")
            trigger_count += 1
            trades_left_today -= 1
            if trades_per_day_limit != 0 and trades_left_today == 0:
                daily_limit_reached_counter += 1
                print(f"Reached daily trade limit, skipping the rest of the day")

(algo_growth, average_growth, normalized_algo_growth) = evaluate_performance(visa_price, mastercard_price, init_visa_price, init_mastercard_price)
print(f"Day {days_passed - skip_start} eval:\n\t{round(average_growth * 100, 1)}% market growth\n\t{round(algo_growth * 100, 1)}% algo growth value\n\t{round(normalized_algo_growth * 100, 1)}% performance\n")

print(f"Done simulating pairs trading! Triggered {trigger_count} total time out of {attempted_trigger_count} attempts.")
print(f"Reached daily trade limit {daily_limit_reached_counter} times.")
print(f"Total gains: {total_gains}, total losses: {total_losses}")
print(f"Days passed: {days_passed}")