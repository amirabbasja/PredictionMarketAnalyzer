from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from src.utils.math_utils import *
from dotenv import load_dotenv
import copy
import requests
from datetime import datetime
from huggingface_hub import list_repo_files, hf_hub_download, login
import pprint
import time
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

import os

# Clear terminal
os.system('cls' if os.name=='nt' else 'clear')

# Get all market names
print("Getting all markets...")

allMarkets = pd.DataFrame(queryParquetFolder("./src/data/polymarket/markets_with_price", "SELECT * FROM data LIMIT 100"))
print(allMarkets.dtypes)
exit()


allMarkets = pd.DataFrame(queryParquetFile("src/data/polymarket/markets_with_price", "SELECT slug, outcome_0_ID, outcome_1_ID, conditionID, startDate, endDate, FROM data"))
print(allMarkets.columns)
print(allMarkets.iloc[0])
# Process the dataframe
allMarkets["startDate"] = pd.to_datetime(allMarkets["startDate"])
allMarkets["endDate"] = pd.to_datetime(allMarkets["endDate"])

sys.stdout.write('\033[F\033[K')
sys.stdout.flush()
print(f"All available market count: {len(allMarkets):,}")
print("Lowest start date:", allMarkets.startDate.min())
print("Highest end date:", allMarkets.endDate.max())


# Get trades
for idx, market in allMarkets.iterrows():
    slug = market.slug
    outcome_0_id = market.outcome_0_ID
    outcome_1_id = market.outcome_1_ID
    endDate = market.endDate
    startDate = market.startDate
    conditionID = market.conditionID
    
    # Get trades for the market
    fullMarketData = queryParquetFolder("./src/data/polymarket/trades", f"SELECT * FROM data WHERE slug = '{slug}'").iloc[0]
    
    if not fullMarketData["has_price_history"]:
        continue
    
    outcome_0_price = fullMarketData["outcome_0_history_price"]
    outcome_0_price_ts = fullMarketData["outcome_0_history_price_ts"]
    outcome_1_price = fullMarketData["outcome_1_history_price"]
    outcome_1_price_ts = fullMarketData["outcome_1_history_price_ts"]
    hasPriceHostory = fullMarketData["has_price_history"]
    
    
        
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), gridspec_kw={'height_ratios': [4, 1]}, sharex=True)
    ax1.plot(outcome_1_trades["block_timestamp"], outcome_1_trades["price"], label="Outcome 1 Price")
    ax2.plot(outcome_1_trades["block_timestamp"], drawDown(outcome_1_trades["price"], "absolute"), label="Outcome 1 Max Drawdown")
    print("Max Drawdown:", maxDrawDown(outcome_1_trades["price"], "absolute"))
    print("Fraction of Time Spent Above 0.5:", calculateVolatility(outcome_1_trades["price"]))
    print("Monotonicity:", calculateMonotonicity(outcome_1_trades["price"]))
    plt.savefig("saveFig.png")