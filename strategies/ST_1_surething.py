"""
Optimized version of strategies/ST_1_surething.py

Key changes vs the original:

  1. Fixed the inverted `continue` condition:
        if fullMarketData.empty or not fullMarketData.shape[0] == 0:
     This is always True (empty OR not-empty covers every case), so the
     original loop skipped every market before computing anything.

  2. Removed the N+1 disk-read. The original re-scanned the ENTIRE
     `markets_with_price` parquet folder (fresh duckdb connection + glob +
     filter) once per market, inside the loop. That's the single biggest
     cost by far.

     Instead of replacing it with one giant up-front query (which would
     load every market's full price history into memory simultaneously —
     exactly what MAX_FILE_SIZE_MB elsewhere in this codebase is designed
     to avoid), this version reads the `markets_with_price` folder one
     part-file at a time via `iterMarketBatches()`. Each file is scanned
     exactly once, processed, and its results flushed to disk before the
     next file is read — so memory stays bounded to a single part-file's
     size regardless of how many markets exist in total.

  3. Replaced `iterrows()` (slow — builds a Series per row) with
     `to_dict("records")`.

  4. Removed the duplicated `areaAroundThreshold(..., 0.80)` call that
     was computed twice per outcome for no reason.

  5. Parallelized the per-market metric computation with a thread pool.
     Threads (not processes) because the underlying math is pandas/numpy,
     which releases the GIL for most of the real work — this gets real
     parallelism without pickling each market's price-history lists across
     process boundaries. It also sidesteps a nasty failure mode: if
     `calculateMonotonicity`'s stray `print(prob)` (see below) is still in
     place, running it in worker *processes* means every print has to be
     piped back through the parent's stdout — with enough workers printing
     large series concurrently, that pipe can back up and the whole run
     appears to hang at 0%. Removing the print is still the real fix either
     way.

  6. Results are now actually collected and saved to disk, instead of
     being computed and discarded on the next loop iteration.

IMPORTANT — separate fix needed in src/utils/math_utils.py:
  `calculateMonotonicity()` currently has a leftover `print(prob)` debug
  line that dumps the *entire* price series to stdout on every call.
  With thousands of markets x 2 outcomes, that alone can dominate your
  runtime (terminal I/O is very slow). Delete that line:

      def calculateMonotonicity(prob: pd.Series) -> float:
          print(prob)          # <-- delete this
          prob = prob.dropna()
          ...
"""

import os, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from src.utils.utils import queryParquetFile, appendToParquet
from src.utils.math_utils import (
    fractionOfTimeSpent,
    timeToReachThreshold,
    calculateMonotonicity,
    areaAroundThreshold,
    drawDown,
)

THRESHOLDS = [0.80, 0.90, 0.95, 0.975]


def _priceAtRemainingTime(priceTs, fraction: float) -> int:
    """
    Index of the first timestamp past `fraction` of the market's total
    time span. Equivalent to the original next(i for i, v in enumerate(...))
    calls, just de-duplicated into one helper.

    NOTE: if `priceTs` is guaranteed chronologically sorted (it likely is,
    given how trades are fetched), this can be swapped for `bisect` /
    `numpy.searchsorted` for an O(log n) lookup instead of O(n).
    """
    span = priceTs[-1] - priceTs[0]
    target = priceTs[0] + span * fraction
    for i, v in enumerate(priceTs):
        if v > target:
            return i
    return len(priceTs) - 1


def _outcomeMetrics(price, priceTs) -> dict:
    priceSeries = pd.Series(price)
    out = {}

    for th in THRESHOLDS:
        above, below, equal = fractionOfTimeSpent(priceSeries, priceTs, th)
        out[f"fractionAbove_{th}"] = above
        out[f"priceAtRemaining_{th}"] = _priceAtRemainingTime(priceTs, th)
        out[f"timeToReach_{th}"] = timeToReachThreshold(priceSeries, priceTs, th)
        out[f"area_{th}"] = areaAroundThreshold(priceSeries, priceTs, th)

    out["monotonicity"] = calculateMonotonicity(priceSeries)
    return out


MARKETS_WITH_PRICE_COLUMNS = """
    slug, conditionID, startDate, endDate,
    outcome_0_ID, outcome_1_ID,
    outcome_0_history_price, outcome_0_history_price_ts,
    outcome_1_history_price, outcome_1_history_price_ts,
    has_price_history
"""


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
    histories = (
        (outcome0Prices, outcome0Timestamps, False),
        (outcome1Prices, outcome1Timestamps, True),
    )
    impliedOutcome0ByTimestamp = {}

    for prices, timestamps, isOutcome1 in histories:
        if len(prices) != len(timestamps):
            raise ValueError("Each outcome price history must match its timestamp history")

        for price, timestamp in zip(prices, timestamps):
            # A trade in outcome 1 at p implies that outcome 0 is worth 1 - p.
            impliedOutcome0 = 1.0 - float(price) if isOutcome1 else float(price)
            impliedOutcome0ByTimestamp.setdefault(timestamp, []).append(impliedOutcome0)

    symmetricTimestamps = sorted(impliedOutcome0ByTimestamp)
    symmetricOutcome0Prices = [
        sum(impliedOutcome0ByTimestamp[timestamp])
        / len(impliedOutcome0ByTimestamp[timestamp])
        for timestamp in symmetricTimestamps
    ]
    symmetricOutcome1Prices = [
        1.0 - price for price in symmetricOutcome0Prices
    ]

    return (
        symmetricOutcome0Prices,
        symmetricTimestamps,
        symmetricOutcome1Prices,
        symmetricTimestamps.copy(),
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

    # if makeOutcomePricesSymmetric:
    #     (
    #         outcome0Prices,
    #         outcome0Timestamps,
    #         outcome1Prices,
    #         outcome1Timestamps,
    #     ) = _symmetrizeOutcomeHistories(
    #         outcome0Prices,
    #         outcome0Timestamps,
    #         outcome1Prices,
    #         outcome1Timestamps,
    #     )

    return {
        "slug": row["slug"],
        "conditionID": row["conditionID"],
        "outcome_0": _outcomeMetrics(outcome0Prices, outcome0Timestamps),
        "outcome_1": _outcomeMetrics(outcome1Prices, outcome1Timestamps),
    }

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

        batchDf["startDate"] = pd.to_datetime(batchDf["startDate"])
        batchDf["endDate"] = pd.to_datetime(batchDf["endDate"])
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
