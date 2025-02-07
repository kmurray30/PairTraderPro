import Utilities
from polygon import RESTClient
import os
import pandas as pd

Utilities.init_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY")
polygon_client = RESTClient(api_key=polygon_api_key)

ticker = "V"
interval = "1_hour"
multiplier = interval.split("_")[0]
timespan = interval.split("_")[1]
start_date_str = "2024-03-09"

for j in range(0, 10):
    result = None
    print(f"Fetch {j}")
    try:
        result = polygon_client.list_aggs(ticker=ticker, multiplier=multiplier, timespan=timespan, from_=start_date_str, to=start_date_str, limit=5000, sort="asc")
        priceData = pd.DataFrame(result)
    except Exception as e:
        print(f"Exception: {e}")
    print("Done fetching")

    print(priceData)