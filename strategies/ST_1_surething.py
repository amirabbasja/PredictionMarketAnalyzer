"""
SureThing Strategy

Pass necessary flags to run the script. The script can be run in two modes:
1. Backtest Mode: This mode computes metrics for all markets in the Polymarket dataset
   that have price history. It reads the data from the local Parquet files and saves
   the computed metrics to a new Parquet file. To run in backtest mode, use the following command:
   ```python
   python ST_1_surething.py --backtest
   ```

2. Run Mode: This mode fetches all currently active markets from Polymarket using the API.
   Run with --v or --verbose to see progress information. To run in run mode, use the following command:
   ```python
   python ST_1_surething.py --run
   ```
"""

import os, time, sys, pprint, json
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.utils.utils import queryParquetFile, appendToParquet
from src.utils.math_utils import calculateMonotonicity
from src.handlers.polymarket import PolymarketHandler

from datetime import datetime, timedelta, timezone

args = sys.argv

THRESHOLDS = [0.80, 0.90, 0.95, 0.975]


def _timestampsAsFloat(timestamps) -> np.ndarray:
    """Convert a sorted timestamp grid to the numeric scale used by metrics."""
    timestampArray = np.asarray(timestamps)
    if np.issubdtype(timestampArray.dtype, np.number):
        return timestampArray.astype(np.float64, copy=False)

    datetimeArray = pd.to_datetime(
        timestampArray, format="ISO8601", utc=True
    ).to_numpy(dtype="datetime64[ns]")
    return ((datetimeArray - datetimeArray[0]) / np.timedelta64(1, "s")).astype(
        np.float64,
        copy=False,
    )


def _prepareOutcomeHistory(prices, timestamps):
    """Validate and sort one history, converting its timestamps only once."""
    priceArray = np.asarray(prices, dtype=np.float64)
    timestampArray = np.asarray(timestamps)
    if len(priceArray) != len(timestampArray):
        raise ValueError("Each outcome price history must match its timestamp history")

    if np.issubdtype(timestampArray.dtype, np.number):
        order = np.argsort(timestampArray, kind="stable")
    else:
        timestampArray = pd.to_datetime(
            timestampArray, format="ISO8601", utc=True
        ).to_numpy(dtype="datetime64[ns]")
        order = np.argsort(timestampArray, kind="stable")

    sortedTimestamps = timestampArray[order]
    return priceArray[order], _timestampsAsFloat(sortedTimestamps)


def _outcomeMetricsFromSorted(price, priceTs) -> dict:
    """Calculate every threshold metric from one prepared, sorted history."""
    price = np.asarray(price, dtype=np.float64)
    priceTs = np.asarray(priceTs, dtype=np.float64)
    out = {}

    totalTime = priceTs[-1] - priceTs[0]
    durations = np.diff(priceTs)

    # Point weights make dot(weights, values) equivalent to trapezoidal
    # integration while letting us reuse the timestamp work at each threshold.
    integrationWeights = np.zeros(len(price), dtype=np.float64)
    if len(price) > 1:
        integrationWeights[0] = durations[0] / 2.0
        integrationWeights[-1] = durations[-1] / 2.0
        if len(price) > 2:
            integrationWeights[1:-1] = (durations[:-1] + durations[1:]) / 2.0
    totalArea = np.dot(integrationWeights, price)

    for th in THRESHOLDS:
        if totalTime > 0:
            intervalPrices = price[:-1]
            above = durations[intervalPrices > th].sum() / totalTime
            below = durations[intervalPrices < th].sum() / totalTime
            equal = durations[intervalPrices == th].sum() / totalTime
        else:
            above = below = equal = 0.0

        target = priceTs[0] + totalTime * th
        remainingIndex = min(int(np.searchsorted(priceTs, target, side="right")),len(priceTs) - 1,)

        hitIndices = np.flatnonzero(price >= th)
        if len(hitIndices) == 0:
            timeToReach = -1
        elif totalTime <= 0:
            timeToReach = 0.0 if priceTs[hitIndices[0]] == priceTs[0] else -1
        else:
            timeToReach = (priceTs[hitIndices[0]] - priceTs[0]) / totalTime

        aboveMask = price >= th
        areaAbove = np.dot(integrationWeights, np.where(aboveMask, price, 0.0))
        areaBelow = np.dot(integrationWeights, np.where(~aboveMask, price, 0.0))

        out[f"fractionAbove_{th}"] = above
        out[f"priceAtRemaining_{th}"] = remainingIndex
        out[f"timeToReach_{th}"] = timeToReach
        out[f"area_{th}"] = {
            "totalTime": totalTime,
            "totalArea": totalArea,
            "areaAbove": areaAbove,
            "areaBelow": areaBelow,
            "relativeAreaAbove": areaAbove / totalTime if totalTime > 0 else 0.0,
            "relativeAreaBelow": areaBelow / totalTime if totalTime > 0 else 0.0,
        }

    out["monotonicity"] = calculateMonotonicity(price)
    return out


def _outcomeMetrics(price, priceTs) -> dict:
    sortedPrices, sortedTimestamps = _prepareOutcomeHistory(price, priceTs)
    return _outcomeMetricsFromSorted(sortedPrices, sortedTimestamps)


def iterMarketBatches(dataDir: str):
    """
    Yields one DataFrame at a time, one per `polymarket_markets_with_prices_pt_*`
    part-file, instead of loading the whole folder into memory in one go.

    This keeps memory bounded to a single part-file's worth of data — the
    same size cap (MAX_FILE_SIZE_MB) that was already enforced when these
    files were written by `addPricesToMarketData` — while still reading
    every byte on disk exactly once for the whole run (one scan per file,
    never one scan per market).
    """
    files = sorted(
        f for f in os.listdir(dataDir)
        if f.startswith("polymarket_markets_with_prices_pt_") and f.endswith(".parquet")
    )
    for fileName in files:
        path = os.path.join(dataDir, fileName)
        df = pd.DataFrame(queryParquetFile(path, f"SELECT {MARKETS_WITH_PRICE_COLUMNS} FROM data"))
        yield fileName, df


def _isEmpty(x) -> bool:
    """
    Safe emptiness check for values that may be None, a Python list, or a
    numpy array (parquet list columns come back as numpy arrays via
    duckdb/pandas, and `not someArray` raises ValueError once it has more
    than one element — numpy can't tell if you mean .any() or .all()).
    """
    if x is None:
        return True
    return len(x) == 0


def _symmetrizeOutcomeHistories(
    outcome0Prices,
    outcome0Timestamps,
    outcome1Prices,
    outcome1Timestamps,
):
    """Build complementary binary-outcome prices on one timestamp grid."""
    outcome0Prices = np.asarray(outcome0Prices, dtype=np.float64)
    outcome1Prices = np.asarray(outcome1Prices, dtype=np.float64)
    outcome0Timestamps = np.asarray(outcome0Timestamps)
    outcome1Timestamps = np.asarray(outcome1Timestamps)

    if len(outcome0Prices) != len(outcome0Timestamps):
        raise ValueError("Each outcome price history must match its timestamp history")
    if len(outcome1Prices) != len(outcome1Timestamps):
        raise ValueError("Each outcome price history must match its timestamp history")

    allTimestamps = np.concatenate((outcome0Timestamps, outcome1Timestamps))
    impliedOutcome0Prices = np.concatenate(
        (outcome0Prices, 1.0 - outcome1Prices)
    )

    symmetricTimestamps, timestampGroups = np.unique(
        allTimestamps,
        return_inverse=True,
    )
    priceSums = np.bincount(timestampGroups, weights=impliedOutcome0Prices)
    priceCounts = np.bincount(timestampGroups)
    symmetricOutcome0Prices = priceSums / priceCounts
    symmetricOutcome1Prices = 1.0 - symmetricOutcome0Prices

    return (
        symmetricOutcome0Prices,
        symmetricTimestamps,
        symmetricOutcome1Prices,
        symmetricTimestamps,
    )


def processMarket(row: dict, makeOutcomePricesSymmetric: bool = True):
    """
    Pure, in-memory, single-market computation. No disk I/O here, which is
    what makes this safe (and worthwhile) to fan out across worker
    threads.

    Polymarket binary outcomes are complementary, so their prices should sum
    to 1 at any timestamp. The stored histories are built from trades in each
    outcome token independently, however, which gives the two outcomes
    different timestamp grids and can make their recorded prices appear
    non-complementary.

    When ``makeOutcomePricesSymmetric`` is True (the default), every observed
    trade is converted to a complete binary-market observation: an outcome-0
    trade at price ``p`` becomes ``(p, 1 - p)``, while an outcome-1 trade at
    price ``p`` becomes ``(1 - p, p)``. Both histories then use the sorted
    union of their timestamps and sum to exactly 1 at every position. If
    multiple trades have the same timestamp, their implied outcome-0 prices
    are averaged before the complementary outcome-1 price is calculated.
    Pass False to compute the metrics from the original independent trade
    histories instead.

    Args:
        row (dict): A market record containing both outcomes' price and
            timestamp histories.
        makeOutcomePricesSymmetric (bool): Whether to reconstruct both price
            histories as complementary observations on a shared timestamp
            grid. Defaults to True.
    """
    if not row.get("has_price_history"):
        return None
    if _isEmpty(row["outcome_0_history_price"]) or _isEmpty(row["outcome_1_history_price"]):
        return None

    outcome0Prices = row["outcome_0_history_price"]
    outcome0Timestamps = row["outcome_0_history_price_ts"]
    outcome1Prices = row["outcome_1_history_price"]
    outcome1Timestamps = row["outcome_1_history_price_ts"]

    if makeOutcomePricesSymmetric:
        (
            outcome0Prices,
            outcome0Timestamps,
            outcome1Prices,
            outcome1Timestamps,
        ) = _symmetrizeOutcomeHistories(
            outcome0Prices,
            outcome0Timestamps,
            outcome1Prices,
            outcome1Timestamps,
        )

        # np.unique already returned a sorted shared grid, so prepare it once
        # and reuse it for both complementary outcomes.
        metricTimestamps = _timestampsAsFloat(outcome0Timestamps)
        outcome0Metrics = _outcomeMetricsFromSorted(
            outcome0Prices,
            metricTimestamps,
        )
        outcome1Metrics = _outcomeMetricsFromSorted(
            outcome1Prices,
            metricTimestamps,
        )
    else:
        outcome0Metrics = _outcomeMetrics(outcome0Prices, outcome0Timestamps)
        outcome1Metrics = _outcomeMetrics(outcome1Prices, outcome1Timestamps)

    return {
        "slug": row["slug"],
        "conditionID": row["conditionID"],
        "outcome_0": outcome0Metrics,
        "outcome_1": outcome1Metrics,
    }


MARKETS_WITH_PRICE_COLUMNS = """
    slug, conditionID, startDate, endDate,
    outcome_0_ID, outcome_1_ID,
    outcome_0_history_price, outcome_0_history_price_ts,
    outcome_1_history_price, outcome_1_history_price_ts,
    has_price_history
"""

# Print what is happening to user
verbose = "--verbose" in args or "-v" in args


if "--backtest" in args:
    os.system('cls' if os.name == 'nt' else 'clear')
    dataDir = "src/data/polymarket/markets_with_price"
    outPath = "src/data/strategyFiles/surething_metrics.parquet"
    os.makedirs(os.path.dirname(outPath), exist_ok=True)

    totalMarkets = 0
    totalResults = 0

    # Threads, not processes: the math here (fractionOfTimeSpent,
    # timeToReachThreshold, areaAroundThreshold, drawDown,
    # calculateMonotonicity) is pandas/numpy under the hood and releases
    # the GIL for most of the actual work, so threads give real parallelism
    # here without paying to pickle each market's price-history lists
    # across process boundaries — and without worker stdout being piped
    # back through the parent, which is its own source of stalls.
    startTime = time.time()
    with ThreadPoolExecutor() as executor:
        for fileName, batchDf in iterMarketBatches(dataDir):

            # Kill the process after 3.5 hours of activity - to abide by the HPC's rules for free plans
            if 12600 < time.time() - startTime:
                exit()

            if batchDf.empty:
                continue

            batchDf["startDate"] = pd.to_datetime(
                batchDf["startDate"], format="ISO8601", utc=True
            )
            batchDf["endDate"] = pd.to_datetime(
                batchDf["endDate"], format="ISO8601", utc=True
            )
            records = batchDf.to_dict("records")
            totalMarkets += len(records)

            # Only this file's records are in flight at once — the next
            # file isn't read until this batch's results are flushed and
            # discarded below, which is what actually bounds memory.
            batchResults = []
            futures = {executor.submit(processMarket, r): r["slug"] for r in records}
            for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {fileName}"):
                res = fut.result()
                if res is not None:
                    batchResults.append(res)

            if batchResults:
                batchOut = pd.json_normalize(batchResults)
                if not os.path.exists(outPath):
                    batchOut.to_parquet(outPath, index=False)
                else:
                    appendToParquet(outPath, batchOut.to_dict("records"), safe=True)
                totalResults += len(batchResults)

            # Explicitly drop references before moving to the next file.
            del batchDf, records, batchResults

            print(f"[{fileName}] running total: {totalResults:,} markets with metrics / {totalMarkets:,} scanned")

    print(f"Done. Computed metrics for {totalResults:,} / {totalMarkets:,} markets. Saved to {outPath}")
elif "--run" in args:
    # Handler
    polymarketHandler = PolymarketHandler(
        polymarketAPIkey = os.getenv("polymarketAPI_key"),
        web3APIkey = os.getenv("alchemyAPI_key"),
        provider = "alchemy"
    )
    
    # Get a list of all online markets in polymarket
    activeMarkets = polymarketHandler.getAllActiveMarkets(minLiquidity=500_000, verbose = verbose)
    
    # Analyze the markets and filter them
    filteredDf = activeMarkets[["id", "conditionId", "questionID", "slug", "endDate", "startDate",
                                "outcomePrices", "volume", "makerBaseFee", "takerBaseFee", "spread",
                                "outcomes"]].copy()
    filteredDf["startDate_ts"] = pd.to_datetime(filteredDf["startDate"], format="ISO8601", utc=True).astype(int) / 10**9
    filteredDf["endDate_ts"] = pd.to_datetime(filteredDf["endDate"], format="ISO8601", utc=True).astype(int) / 10**9
    filteredDf["timeToEnd"] = filteredDf["endDate_ts"] - time.time()
    filteredDf["duration"] = filteredDf["endDate_ts"] - filteredDf["startDate_ts"]
    filteredDf["remainingTimeFraction"] = filteredDf["timeToEnd"] / filteredDf["duration"]
    filteredDf["eventSlug"] = activeMarkets["events"].apply(lambda items: ",".join(d["slug"] for d in items) if isinstance(items, list) else "")
    for col in ["outcomes", "outcomePrices"]:
        filteredDf[col] = filteredDf[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    filteredDf = filteredDf.explode(["outcomes", "outcomePrices"], ignore_index=True)
    filteredDf.rename(columns={"outcomes": "outcome", "outcomePrices": "outcomePrice"}, inplace=True)
    filteredDf["link"] = "http://polymarket.com/market/" + filteredDf["slug"]
    filteredDf.sort_values(by = "outcomePrice", ascending = False, inplace = True)
    filteredDf.to_csv("src/data/strategyFiles/polymarketActiveMarkets.csv", index=False)
    
    # Filter for near certain markets (SureThing strategy)
    
    print(f"Found {len(activeMarkets)} active markets in Polymarket.")
else:
    print("This script is not meant to be run directly. Please pass an acceptable flag.")
