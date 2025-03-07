import subprocess
import Utilities
import pandas as pd
import sys

# Capture command-line arguments
args = sys.argv[1:]  # Exclude the script name
if len(args) != 2:
    raise Exception("Must provide exactly 2 tickers. e.g. `python run_all.py AMD TSM`")

# Define the script to call and its arguments
download_histories_path = Utilities.get_path_from_project_root('download_histories.py')
start_date = (pd.Timestamp.now() - pd.Timedelta(days=365*5)).strftime("%Y-%m-%d")
end_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
args = args[0:2] + [start_date, end_date]

# Call the script
result = subprocess.run(['python', download_histories_path] + args, capture_output=True, text=True)

# # Print the output
# print(result.stdout)
# print(result.stderr)