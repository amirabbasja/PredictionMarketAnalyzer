from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
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

if __name__ == "__main__":
    # Get all market names
    allMarkets = pd.DataFrame(queryParquetFile("src/data/polymarket_HistoricalMarkets.parquet", "SELECT * FROM data"))
    
else:
    print("This file should be ran directly")