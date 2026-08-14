from tqdm import tqdm
import pandas as pd

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
allMarkets = pd.DataFrame(queryParquetFile("src/data/polymarket/markets_with_price", "SELECT slug, outcome_0_ID, outcome_1_ID, conditionID, startDate, endDate FROM data"))
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
for idx, market in tqdm(allMarkets.iterrows(), total=len(allMarkets), desc="Processing markets"):
    slug = market.slug
    outcome_0_id = market.outcome_0_ID
    outcome_1_id = market.outcome_1_ID
    endDate = market.endDate
    startDate = market.startDate
    conditionID = market.conditionID
    
    # Get trades for the market
    fullMarketData = pd.DataFrame(queryParquetFolder("src/data/polymarket/markets_with_price", f"SELECT * FROM data WHERE slug = '{slug}'"))
    
    if fullMarketData.empty or not fullMarketData.shape[0] == 0:
        continue
    
    fullMarketData = fullMarketData.iloc[0]
    
    if not fullMarketData["has_price_history"]:
        continue
    outcome_0_price = fullMarketData["outcome_0_history_price"]
    outcome_0_price_ts = fullMarketData["outcome_0_history_price_ts"]
    outcome_1_price = fullMarketData["outcome_1_history_price"]
    outcome_1_price_ts = fullMarketData["outcome_1_history_price_ts"]
    hasPriceHostory = fullMarketData["has_price_history"]
    
    # Calculate necessary metrics - outcome 0
    fractionOfTimeSpent_above_80_outcome_0   = fractionOfTimeSpent(outcome_0_price, outcome_0_price_ts, 0.80)[0]
    fractionOfTimeSpent_above_90_outcome_0   = fractionOfTimeSpent(outcome_0_price, outcome_0_price_ts, 0.90)[0]
    fractionOfTimeSpent_above_95_outcome_0   = fractionOfTimeSpent(outcome_0_price, outcome_0_price_ts, 0.95)[0]
    fractionOfTimeSpent_above_97_5_outcome_0 = fractionOfTimeSpent(outcome_0_price, outcome_0_price_ts, 0.975)[0]
    priceAtRemainingTime_80_outcome_0   = next(i for i, v in enumerate(outcome_0_price_ts) if v > (outcome_0_price_ts[-1] - outcome_0_price_ts[0]) * 0.80 + outcome_0_price_ts[0])
    priceAtRemainingTime_90_outcome_0   = next(i for i, v in enumerate(outcome_0_price_ts) if v > (outcome_0_price_ts[-1] - outcome_0_price_ts[0]) * 0.90 + outcome_0_price_ts[0])
    priceAtRemainingTime_95_outcome_0   = next(i for i, v in enumerate(outcome_0_price_ts) if v > (outcome_0_price_ts[-1] - outcome_0_price_ts[0]) * 0.95 + outcome_0_price_ts[0])
    priceAtRemainingTime_97_5_outcome_0 = next(i for i, v in enumerate(outcome_0_price_ts) if v > (outcome_0_price_ts[-1] - outcome_0_price_ts[0]) * 0.975 + outcome_0_price_ts[0])
    timeToReachThreshold_80_outcome_0   = timeToReachThreshold(outcome_0_price, outcome_0_price_ts, 0.80)
    timeToReachThreshold_90_outcome_0   = timeToReachThreshold(outcome_0_price, outcome_0_price_ts, 0.90)
    timeToReachThreshold_95_outcome_0   = timeToReachThreshold(outcome_0_price, outcome_0_price_ts, 0.95)
    timeToReachThreshold_97_5_outcome_0 = timeToReachThreshold(outcome_0_price, outcome_0_price_ts, 0.975)
    calculateMonotonicity_outcome_0 = calculateMonotonicity(outcome_0_price)
    areaAroundThreshold_80_outcome_0   = areaAroundThreshold(outcome_0_price, outcome_0_price_ts, 0.80)
    areaAroundThreshold_80_outcome_0   = areaAroundThreshold(outcome_0_price, outcome_0_price_ts, 0.80)
    areaAroundThreshold_90_outcome_0   = areaAroundThreshold(outcome_0_price, outcome_0_price_ts, 0.90)
    areaAroundThreshold_95_outcome_0   = areaAroundThreshold(outcome_0_price, outcome_0_price_ts, 0.95)
    areaAroundThreshold_97_5_outcome_0 = areaAroundThreshold(outcome_0_price, outcome_0_price_ts, 0.975)
    drawDown_outcome_0 = drawDown(outcome_0_price, "relative")
    
    # Calculate necessary metrics - outcome 1
    fractionOfTimeSpent_above_80_outcome_1   = fractionOfTimeSpent(outcome_1_price, outcome_1_price_ts, 0.80)[0]
    fractionOfTimeSpent_above_90_outcome_1   = fractionOfTimeSpent(outcome_1_price, outcome_1_price_ts, 0.90)[0]
    fractionOfTimeSpent_above_95_outcome_1   = fractionOfTimeSpent(outcome_1_price, outcome_1_price_ts, 0.95)[0]
    fractionOfTimeSpent_above_97_5_outcome_1 = fractionOfTimeSpent(outcome_1_price, outcome_1_price_ts, 0.975)[0]
    priceAtRemainingTime_80_outcome_1   = next(i for i, v in enumerate(outcome_1_price_ts) if v > (outcome_1_price_ts[-1] - outcome_1_price_ts[0]) * 0.80 + outcome_1_price_ts[0])
    priceAtRemainingTime_90_outcome_1   = next(i for i, v in enumerate(outcome_1_price_ts) if v > (outcome_1_price_ts[-1] - outcome_1_price_ts[0]) * 0.90 + outcome_1_price_ts[0])
    priceAtRemainingTime_95_outcome_1   = next(i for i, v in enumerate(outcome_1_price_ts) if v > (outcome_1_price_ts[-1] - outcome_1_price_ts[0]) * 0.95 + outcome_1_price_ts[0])
    priceAtRemainingTime_97_5_outcome_1 = next(i for i, v in enumerate(outcome_1_price_ts) if v > (outcome_1_price_ts[-1] - outcome_1_price_ts[0]) * 0.975 + outcome_1_price_ts[0])
    timeToReachThreshold_80_outcome_1   = timeToReachThreshold(outcome_1_price, outcome_1_price_ts, 0.80)
    timeToReachThreshold_90_outcome_1   = timeToReachThreshold(outcome_1_price, outcome_1_price_ts, 0.90)
    timeToReachThreshold_95_outcome_1   = timeToReachThreshold(outcome_1_price, outcome_1_price_ts, 0.95)
    timeToReachThreshold_97_5_outcome_1 = timeToReachThreshold(outcome_1_price, outcome_1_price_ts, 0.975)
    calculateMonotonicity_outcome_1 = calculateMonotonicity(outcome_1_price)
    areaAroundThreshold_80_outcome_1   = areaAroundThreshold(outcome_1_price, outcome_1_price_ts, 0.80)
    areaAroundThreshold_80_outcome_1   = areaAroundThreshold(outcome_1_price, outcome_1_price_ts, 0.80)
    areaAroundThreshold_90_outcome_1   = areaAroundThreshold(outcome_1_price, outcome_1_price_ts, 0.90)
    areaAroundThreshold_95_outcome_1   = areaAroundThreshold(outcome_1_price, outcome_1_price_ts, 0.95)
    areaAroundThreshold_97_5_outcome_1 = areaAroundThreshold(outcome_1_price, outcome_1_price_ts, 0.975)
    drawDown_outcome_1 = drawDown(outcome_1_price, "relative")
    
    
    
    
    # fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), gridspec_kw={'height_ratios': [4, 1]}, sharex=True)
    # ax1.plot(outcome_1_trades["block_timestamp"], outcome_1_trades["price"], label="Outcome 1 Price")
    # ax2.plot(outcome_1_trades["block_timestamp"], drawDown(outcome_1_trades["price"], "absolute"), label="Outcome 1 Max Drawdown")
    # print("Max Drawdown:", maxDrawDown(outcome_1_trades["price"], "absolute"))
    # print("Fraction of Time Spent Above 0.5:", calculateVolatility(outcome_1_trades["price"]))
    # print("Monotonicity:", calculateMonotonicity(outcome_1_trades["price"]))
    # plt.savefig("saveFig.png")