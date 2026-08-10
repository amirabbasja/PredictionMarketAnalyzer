# Utility functions for mathematical operations
import pandas as pd
import numpy as np

def maxDrawDown(probSeries: pd.Series, type: str) -> float:
    """
    Calculate the maximum drawdown of a probability series.

    Args:
        probSeries (pd.Series): A pandas Series representing the probability values over time.
        type (str): The type of drawdown to calculate. Options are 'absolute' or 'relative'.

    Returns:
        float: The maximum drawdown value.
    """
    runningMax = probSeries.cummax()
    if type == 'absolute':
        drawdown = runningMax - probSeries
    elif type == 'relative':
        drawdown = (probSeries - runningMax) / runningMax
    else:
        raise ValueError("Invalid type. Choose 'absolute' or 'relative'.")
    
    maxDrawdown = abs(drawdown).max()
    return maxDrawdown

def drawDown(probSeries: pd.Series, type: str) -> pd.Series:
    """
    Calculate the drawdown series of a probability series.

    Args:
        probSeries (pd.Series): A pandas Series representing the probability values over time.

    Returns:
        pd.Series: The drawdown values over time (as positive values).
    """
    runningMax = probSeries.cummax()
    if type == 'absolute':
        drawdown = runningMax - probSeries
    elif type == 'relative':
        drawdown = (probSeries - runningMax) / runningMax
    else:
        raise ValueError("Invalid type. Choose 'absolute' or 'relative'.")
    return drawdown.abs()

def fractionOfTimeSpent(prices, datetimes, threshold):
    """
    Compute the fraction of total time that price spent above and below a threshold.
    
    Assumes the price observed at each timestamp holds until the next timestamp.
    Equal-to-threshold periods are ignored (contribute to neither fraction).
    Datetimes are converted via pd.to_datetime.
    
    Args:
        prices (list or pd.Series): A list or Series of price values.
        datetimes (list or pd.Series): A list or Series of datetime values corresponding to the prices.
        threshold (float): The threshold value to compare against.
    
    Returns:
        tuple: Fractions of time spent above, below, and equal to the threshold.
    """
    work = pd.DataFrame({
        'ts': pd.to_datetime(datetimes),
        'price': prices
    }).sort_values('ts').reset_index(drop=True)
    
    work['duration'] = work['ts'].diff().shift(-1)
    work = work.dropna(subset=['duration'])
    
    totalSeconds = work['duration'].sum().total_seconds()
    if totalSeconds <= 0:
        return 0.0, 0.0
    
    aboveSeconds = work.loc[work['price'] >  threshold, 'duration'].sum().total_seconds()
    belowSeconds = work.loc[work['price'] <  threshold, 'duration'].sum().total_seconds()
    equalSeconds = work.loc[work['price'] == threshold, 'duration'].sum().total_seconds()
    
    return aboveSeconds / totalSeconds, belowSeconds / totalSeconds, equalSeconds / totalSeconds

def timeToReachThreshold(prices, datetimes, threshold):
    """
    Relative time (fraction of total span) until price first reaches the threshold.
    "Reach" means price >= threshold. Returns -1 if never reached.
    
    Assumes price at each timestamp holds until the next. Datetimes converted via pd.to_datetime.
    
    Args:
        prices (list or pd.Series): A list or Series of price values.
        datetimes (list or pd.Series): A list or Series of datetime values corresponding to the prices.
        threshold (float): The threshold value to compare against.
    """
    work = pd.DataFrame({
        'ts': pd.to_datetime(datetimes),
        'price': prices
    }).sort_values('ts').reset_index(drop=True)

    if len(work) == 0:
        return -1

    start = work['ts'].iloc[0]
    end = work['ts'].iloc[-1]
    totalSeconds = (end - start).total_seconds()

    hits = work.loc[work['price'] >= threshold]
    if hits.empty:
        return -1

    hitTime = hits['ts'].iloc[0]
    if totalSeconds <= 0:
        return 0.0 if hitTime == start else -1

    return (hitTime - start).total_seconds() / totalSeconds

def calculateVolatility(prob: pd.Series) -> float:
    """
    Computes the volatility of a probability trajectory as the standard
    deviation of consecutive probability changes.
    Lower values indicate a more stable market with fewer fluctuations,
    while higher values indicate more frequent or larger changes in the
    market's implied probability.

    Typical values:
        < 0.005 : Extremely stable (strong candidate for a near-certain market)
    0.005-0.02 : Low volatility
     0.02-0.05 : Moderate volatility
         >0.05 : High volatility
    
    Args:
        prob (pd.Series): A pandas Series representing the probability values over time.
    """
    prob = prob.dropna()

    if len(prob) < 2:
        return 0.0

    probabilityChanges = prob.diff().dropna()

    return probabilityChanges.std()

def calculateMonotonicity(prob: pd.Series) -> float:
    """
    Computes the monotonicity of a probability trajectory.

    The score is the fraction of non-zero changes that are positive.
    Values range from 0 to 1:
        1.0 -> strictly increasing
        0.5 -> equal upward/downward movements
        0.0 -> strictly decreasing
    
    Args:
        prob (pd.Series): A pandas Series representing the probability values over time.
    """
    prob = prob.dropna()

    if len(prob) < 2:
        return 1.0

    changes = prob.diff().dropna()

    nonZeroChanges = changes[changes != 0]

    if len(nonZeroChanges) == 0:
        return 1.0

    positiveChanges = (nonZeroChanges > 0).sum()

    return positiveChanges / len(nonZeroChanges)

def areaAroundThreshold(P: pd.Series, threshold: float = 0.9) -> tuple[float, float]:
    """
    Compute the normalized area under a probability (or confidence) curve
    P(t) and check whether it exceeds a given threshold.

    This corresponds to:
        A = ∫ P(t) dt                  (area under the curve)
        normalizedArea = A / T         (area normalized by total time span)
        isAboveThreshold = (A / T) > threshold

    A high normalizedArea (e.g. > 0.90) means P(t) spends almost the
    entire time span near 1 (i.e. "near certainty"), rather than
    fluctuating or staying low for large portions of time.

    Args:
        P (pd.Series): A pandas Series representing P(t)
        threshold (float):
            The normalized-area threshold used to determine whether the
            system has effectively spent almost its whole lifetime near
            certainty.

    Returns (tuple):
        totalTime (float): T = t[-1] - t[0], the total elapsed time span.
        totalArea (float): A = ∫ P(t) dt over the full time span (trapezoidal 
            rule).
        areaAbove (float): Absolute area contributed by time points where 
            P(t) >= threshold.
        areaBelow (float): Absolute area contributed by time points where 
            P(t) < threshold.
        relativeAreaAbove (float): areaAbove / totalTime — fraction of the 
            average curve height coming from the "above threshold" region.
        relativeAreaBelow (float): areaBelow / totalTime — fraction of the 
            average curve height coming from the "below threshold" region.
    """
    # Extract time (t) from the index and P(t) from the values
    t = P.index.to_numpy(dtype=float)
    pValues = P.to_numpy(dtype=float)

    # T = total elapsed time span
    totalTime = t[-1] - t[0]

    # A = ∫ P(t) dt over the full time span
    totalArea = np.trapz(pValues, t)

    # Boolean mask: True where P(t) is at/above the threshold
    aboveMask = pValues >= threshold

    # Zero out values outside each region, then integrate over the full grid
    pAboveOnly = np.where(aboveMask, pValues, 0.0)
    pBelowOnly = np.where(~aboveMask, pValues, 0.0)

    areaAbove = np.trapz(pAboveOnly, t)
    areaBelow = np.trapz(pBelowOnly, t)

    # Normalize each region's area by the total time span
    relativeAreaAbove = areaAbove / totalTime
    relativeAreaBelow = areaBelow / totalTime

    return {
        "totalTime": totalTime,
        "totalArea": totalArea,
        "areaAbove": areaAbove,
        "areaBelow": areaBelow,
        "relativeAreaAbove": relativeAreaAbove,
        "relativeAreaBelow": relativeAreaBelow,
    }