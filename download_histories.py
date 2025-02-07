import pandas as pd
import os
from polygon import RESTClient
import time
import Utilities

market_open = "09:30:00"
market_close = "04:00:00"

# Notes:
# - The polygon API returns data for one day later for some reason
# - The polygon API returns data for the entire day, not just market hours, and outside market hours the data is not every minute. Decided to trim to market hours
# - The polygon API has a rate limit of 5 requests per minute

# TODO:
# - Daylight savings messes up the market open and close times
# - It adds the headers again for every day
# - Make sure all the dates of the two align afterwards in the excel doc (seems like the do so far)

# TODO Analysis:
# - Every time there's a flip (column="TRUE") compare how much I lost/gained owning that stock vs how much I would have lost with the other stock. This will answer a lot of questions

def trim_to_market_hours(priceData, start_date_str):
    # Calculate the market open and close times
    market_open_time = pd.Timestamp(start_date_str + " " + market_open)
    market_open_time = market_open_time.tz_localize('America/New_York').tz_convert('UTC').tz_localize(None)
    market_close_time = pd.Timestamp(start_date_str + " " + market_close)
    market_close_time = market_close_time.tz_localize('America/New_York').tz_convert('UTC').tz_localize(None)
    
    try:
        # Get the index of the row that has the market open time
        try:
            market_open_index = priceData[priceData['timestamp'] == market_open_time].index[0]
        except IndexError:
            raise Exception(f"Exception: Market open time {market_open_time} not found in data for day {start_date_str}")

        # Get the index of the row that has the market close time
        try:
            market_close_index = priceData[priceData['timestamp'] == market_close_time].index[0] + 1
        except IndexError:
            raise Exception(f"Exception: Market close time {market_close_time} not found in data for day {start_date_str}")
    except Exception as e:
        print(e)
        return priceData
    
    # Trim the data to only include rows between the market open and close times
    market_hours_data = priceData[market_open_index:market_close_index]

    return market_hours_data

Utilities.init_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY")
polygon_client = RESTClient(api_key=polygon_api_key)

max_retries = 5

ticker = "MA"
interval = "1_minute"
# days: int = 366
init_start_date = "2023-01-01"
final_end_date = "2023-12-31"
days_at_a_time: int = 1 # Does not work for more than 1 day at a time for now

# Calculate the number of days between the start and end date
days = (pd.Timestamp(final_end_date) - pd.Timestamp(init_start_date)).days + 1

if days % days_at_a_time != 0:
    raise Exception("days must be a multiple of days_at_a_time")

final_end_date = (pd.Timestamp(init_start_date) + pd.Timedelta(days=days-1)).strftime("%Y-%m-%d")

multiplier = interval.split("_")[0]
timespan = interval.split("_")[1]

file_path = Utilities.get_path_from_project_root(f"histories/{ticker}_{init_start_date}_to_{final_end_date}.csv")

rate_limit = 5 # Requests per minute
for i in range(0, int(days / days_at_a_time)):
    start_date = pd.Timestamp(init_start_date) + pd.Timedelta(days = i * days_at_a_time)
    # end_date = (start_date + pd.Timedelta(days=days_at_a_time))

    # If the day is a weekend, skip it
    if start_date.dayofweek >= 5:
        print(f"Skipping weekend day {start_date.strftime('%Y-%m-%d')}")
        continue

    # TODO If the day is a stock market holiday, skip it

    start_date_str = start_date.strftime("%Y-%m-%d")
    # end_date_str = end_date.strftime("%Y-%m-%d")
    print(f"Fetching data from date {start_date_str}")
    # Retry n times if the request fails
    for j in range(0, max_retries):
        try:
            result = polygon_client.list_aggs(ticker=ticker, multiplier=multiplier, timespan=timespan, from_=start_date_str, to=start_date_str, limit=5000, sort="asc")
            priceData = pd.DataFrame(result)
            break
        except Exception as e:
            print(f"Exception: {e}")
            print("Sleeping for 1 minute and retrying")
            time.sleep(60)
            rate_limit = 5
    rate_limit -= 1

    if len(priceData) == 0:
        print(f"No data fetched for day {start_date_str}")
        continue
    else:
        print(f"Fetched {len(priceData)} rows")

    # Convert the timestamp column to a datetime string
    priceData['timestamp'] = pd.to_datetime(priceData['timestamp'], unit='ms')

    market_hours_data = trim_to_market_hours(priceData, start_date_str)
    print(f"Trimmed to {len(market_hours_data)} market hour rows")

    # Append the data to a running CSV file
    if not os.path.isfile(file_path):
        market_hours_data.to_csv(file_path, index=False)
    else:
        market_hours_data.to_csv(file_path, mode='a', index=False, header=False)

    if rate_limit == 0:
        # Sleep for 1 minute to avoid rate limiting
        print("Sleeping for 1 minute to avoid rate limiting")
        time.sleep(60)
        rate_limit = 5

print("Done!")
print(f"Data saved to {file_path}")