import pandas as pd

start_date_str = "2024-01-01"
market_open = "09:30:00"

# Find the row that has the 14:30 timestamp
market_open_time = pd.Timestamp(start_date_str + " " + market_open)

# Convert the market open time to UTC, accounting for daylight savings, and remove the timezone
market_open_time = market_open_time.tz_localize('America/New_York').tz_convert('UTC').tz_localize(None)

print(market_open_time)