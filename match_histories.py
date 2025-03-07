import Utilities
import pandas as pd
import sys

# Capture command-line arguments
args = sys.argv[1:]  # Exclude the script name

tickers = args # ["AMD", "TSM"]
if len(tickers) != 2:
    raise Exception("Must provide exactly 2 tickers. e.g. `python download_histories.py AMD TSM`")

# Throw error if more than one file starting with the ticker is found
matching_files = Utilities.find_files_by_pattern(Utilities.get_path_from_project_root("histories/downloaded"), f"{tickers[0]}*.csv")
if len(matching_files) != 1:
    raise Exception(f"Found {len(matching_files)} files matching {tickers[0]}*.csv. Expected 1.")
file_path_visa = matching_files[0]

matching_files = Utilities.find_files_by_pattern(Utilities.get_path_from_project_root("histories/downloaded"), f"{tickers[1]}*.csv")
if len(matching_files) != 1:
    raise Exception(f"Found {len(matching_files)} files matching {tickers[1]}*.csv. Expected 1.")
file_path_mastercard = matching_files[0]

# Get the date range string from the file name.
date_range = file_path_visa.split("_")[1:]
date_range = "_".join(date_range)
date_range = date_range.split(".")[0]

new_file_path = Utilities.get_path_from_project_root(f"histories/{tickers[0]}_{tickers[1]}_clean_{date_range}.csv")

market_open = "09:30:00"
market_close = "16:00:00"

# Read csv files into dataframes, with columns open,high,low,close,volume,vwap,timestamp,transactions,otc
data_visa = pd.read_csv(file_path_visa)
data_mastercard = pd.read_csv(file_path_mastercard)

# Create a new dataframe, that adds the time, visa price, and mastercard price, but only for the rows where the timestamps match
new_df = pd.DataFrame()
# Define the columns without populating them
new_df['timestamp'] = []
new_df['visa_open'] = []
new_df['visa_close'] = []
new_df['visa_mid'] = []
new_df['mastercard_open'] = []
new_df['mastercard_close'] = []
new_df['mastercard_mid'] = []

def is_outside_market_hours(timestamp):
    date = timestamp.split(" ")[0]
    market_open_time = pd.Timestamp(date + " " + market_open).tz_localize('America/New_York').tz_convert('UTC').tz_localize(None)
    market_close_time = pd.Timestamp(date + " " + market_close).tz_localize('America/New_York').tz_convert('UTC').tz_localize(None)
    current_time = pd.Timestamp(timestamp)

    # print("")
    # print(f"Market open time: {market_open_time}")
    # print(f"Market close time: {market_close_time}")
    # print(f"Current time: {current_time}")
    
    return current_time < market_open_time or current_time >= market_close_time

# Go through each row, comparing the timestamps (in format "YYYY-MM-DD HH:MM:SS"). If they don't match, print the row, then move on to the next row in whichever dataframe has the earlier timestamp
i_v = 0
i_m = 0
mismatches = 0
previous_day = data_visa.iloc[0]['timestamp'].split(" ")[0]
i = 0
while i_v < len(data_visa) and i_m < len(data_mastercard):
    i += 1
    # if i == 2500:
    #     exit()
    time_v = data_visa.iloc[i_v]['timestamp']
    time_m = data_mastercard.iloc[i_m]['timestamp']

    if time_v.split(" ")[0] != previous_day:
        print(f"Processed day {previous_day}. Length of table: {len(new_df)}")
        previous_day = time_v.split(" ")[0]
    if time_v < time_m:
        if is_outside_market_hours(time_v):
            # print(f"Time {time_v} in visa outside market hours")
            i_v += 1
            continue
        print(f"Time {time_v} missing from mastercard")
        i_v += 1
        mismatches += 1
    elif time_m < time_v:
        if is_outside_market_hours(time_m):
            # print(f"Time {time_m} in mastercard outside market hours")
            i_m += 1
            continue
        print(f"Time {time_m} missing from visa")
        i_m += 1
        mismatches += 1
    else:
        if is_outside_market_hours(time_v):
            # print(f"Time {time_v} in both outside market hours")
            i_v += 1
            i_m += 1
            continue
        visa_open = data_visa['open'][i_v]
        visa_close = data_visa['close'][i_v]
        visa_mid = (data_visa['high'][i_v] + data_visa['low'][i_v]) / 2
        mastercard_open = data_mastercard['open'][i_m]
        mastercard_close = data_mastercard['close'][i_m]
        mastercard_mid = (data_mastercard['high'][i_m] + data_mastercard['low'][i_m]) / 2
        new_df = new_df._append(
            {
                'timestamp': time_v, 
                'visa_open': visa_open, 
                'visa_close': visa_close, 
                'visa_mid': visa_mid,
                'mastercard_open': mastercard_open, 
                'mastercard_close': mastercard_close,
                'mastercard_mid': mastercard_mid
            }, ignore_index=True)
        i_v += 1
        i_m += 1

print(f"Processed day {previous_day}")
print("")
print(f"Done comparing files! Found {mismatches} mismatches.")
print(f"Writing to file {new_file_path}")
new_df.to_csv(new_file_path, index=False)

