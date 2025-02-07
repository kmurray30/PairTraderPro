import Utilities
import pandas as pd

file_path_visa = Utilities.get_path_from_project_root("histories/V_2024-01-01_to_2024-12-31.csv")
file_path_mastercard = Utilities.get_path_from_project_root("histories/MA_2024-01-01_to_2024-12-31.csv")
new_file_path = Utilities.get_path_from_project_root("histories/visa_mastercard_clean_2024-01-01_to_2024-12-31.csv")

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

# Go through each row, comparing the timestamps (in format "YYYY-MM-DD HH:MM:SS"). If they don't match, print the row, then move on to the next row in whichever dataframe has the earlier timestamp
i_v = 0
i_m = 0
mismatches = 0
while i_v < len(data_visa) and i_m < len(data_mastercard):
    time_v = data_visa.iloc[i_v]['timestamp']
    time_m = data_mastercard.iloc[i_m]['timestamp']
    if time_v < time_m:
        print(f"Time {time_v} missing from mastercard")
        i_v += 1
        mismatches += 1
    elif time_v > time_m:
        print(f"Time {time_m} missing from visa")
        i_m += 1
        mismatches += 1
    else:
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

print(f"Done comparing files! Found {mismatches} mismatches.")
print("Writing to file {new_file_path}")
new_df.to_csv(new_file_path, index=False)