from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv
from pathlib import Path
import os, pprint, sys, json, duckdb
import pandas as pd
from datetime import datetime
import asyncio

load_dotenv()

handler = PolymarketHandler(
    os.getenv("polymarket_api_key", None),
    os.getenv("alchemyAPI_key", None),
    provider = "alchemy"
)

if   sys.argv[1] == "--getAllEvents":
    result = handler.getAllEvents(
        saveFile = f"src/data/liveEvents_{int(datetime.now().timestamp())}.parquet", 
        getMarkets = False,
        reqOptions = {
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getLiveMarkets":
    getPriceData = False
    if(len(sys.argv) > 2 and sys.argv[2] == "--price"):
        getPriceData = True
        
    result = handler.getAllMarkets(
        saveFile = f"src/data/polymarket_liveMarkets_{int(datetime.now().timestamp())}.parquet", 
        getMarkets = True,
        getPriceData = getPriceData,
        reqOptions = {
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getHistoricalMarkets":
    getPriceData = False
    if len(sys.argv) > 2 and sys.argv[2] == "--price":
        getPriceData = True

    lastCursor = None
    filePath = None
    lineCount = 0
    if "--continue" in sys.argv:
        idx = sys.argv.index("--continue")
        
        # Check existence of file
        if len(sys.argv) < idx + 2:
            print("Please provide a file path to continue from.")
            exit()
        
        filePath = sys.argv[idx+1]
        dirName = os.path.dirname(filePath)
        saveKey = os.path.basename(filePath).split(".")[0] 
        
        if os.path.isfile(os.path.join(dirName, ".progress")):
            try:
                with open(os.path.join(dirName, ".progress"), 'r', encoding='utf-8') as f:
                    progressData = json.load(f)
                    if saveKey in progressData:
                        lastCursor = progressData[saveKey].get("next_cursor")
                        lineCount = progressData[saveKey].get("eventCount", 0)
                        print(f"Resuming from cursor: {lastCursor}, line count: {lineCount}")
                    else:
                        print("No progress found for the provided file. Starting fresh.")
            except FileNotFoundError:
                print("Progress file not found.")
        else:
            print("Progress file not found. Cannot continue.")
            exit()
    
    result = handler.getAllMarkets(
        saveFile = f"src/data/polymarket/historical_markets/polymarket_HistoricalMarkets.parquet" if not "--continue" in sys.argv else filePath, 
        getMarkets = True,
        getPriceData = getPriceData,
        checkpoint = (lastCursor, lineCount),
        reqOptions = {
            "closed": True,
            "order": "volumeNum",
            "ascending": False
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getPriceHistory":
    if len(sys.argv) < 3:
        print("Please provide a market ID.")
        exit()
        
    if len(sys.argv) < 5:
        print("Please provide a token ID for both outcomes.")
        exit()
    
    marketId = sys.argv[2]
    outcomeId1 = sys.argv[3]
    outcomeId2 = sys.argv[4]
    
    result = handler.getPriceHistory_sync(
        marketID = marketId,
        outcomeIDs = (outcomeId1, outcomeId2),
    )
    print(result)
elif sys.argv[1] == "--toCsv":
    if len(sys.argv) < 3:
        print("Please provide a file or folder path.")
        exit()
    
    filePath = sys.argv[2].replace("\\", "/")
    _path = Path(filePath)
    
    if not _path.exists():
        print(f"Path not found: {filePath}")
        exit()
    
    # Determine output filename
    if _path.is_file():
        fileName = Path(_path.stem).stem if "jsonl.gz" in filePath else _path.stem
    else:
        # For folders, use folder name
        fileName = _path.name
    
    print(f"Processing: {filePath}")
    
    all_data = []
    
    if _path.is_file():
        # === Single file ===
        print(f"File size: {humanReadableFileSize(filePath)}")
        
        if filePath.endswith(".jsonl.gz") or filePath.endswith(".jsonl"):
            data = readJSONL(filePath)
        elif filePath.endswith(".parquet"):
            data = readParquet(filePath)
        else:
            print("Unsupported file format.")
            exit()
        
        all_data.extend(data)
        print(f"Items read: {len(data)}")
        
    elif _path.is_dir():
        # === Folder with multiple Parquet files ===
        parquet_files = list(_path.rglob("*.parquet"))
        
        if not parquet_files:
            print("No .parquet files found in the folder.")
            exit()
        
        print(f"Found {len(parquet_files)} Parquet files.")
        
        for i, pfile in enumerate(parquet_files, 1):
            print(f"[{i}/{len(parquet_files)}] Reading: {pfile.name}")
            try:
                data = readParquet(str(pfile))
                all_data.extend(data)
                print(f"   → {len(data)} items")
            except Exception as e:
                print(f"   → Error reading {pfile.name}: {e}")
    
    else:
        print("Path is neither a file nor a directory.")
        exit()
    
    if not all_data:
        print("No data found.")
        exit()
    
    print(f"\nTotal items collected: {len(all_data)}")
    
    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(all_data)
    
    output_path = f"src/data/{fileName}.csv"
    df.to_csv(output_path, index=False)   # index=False is usually better for data exports
    
    print(f"Successfully saved to: {output_path}")
    print(f"CSV shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
elif sys.argv[1] == "--getAllTrades":
    _saveDirectory = "./src/data/polymarketData"
    
    if not os.getenv("graphQLAPI_key", None):
        print("please provide a Graph QL api key")
        exit()
    
    if not os.path.isdir(_saveDirectory):
        print("No prior data were found for polymarket trades, starting from scratch...")
        # TODO: Get polymarket v1 trades from hugging face
        print("Not implemented yet")
        exit()
    else:
        blockNumber = None
        
        # Get the checkpoint to start getting data from
        maxBlockNumber = queryParquetFolder(_saveDirectory, "SELECT MAX(block_number) FROM data AS max_value")
        if maxBlockNumber.iloc[0,0] == -1:
            # No block_number avilable, get the latest block number from timestamp
            maxTimestamp = queryParquetFolder(_saveDirectory, "SELECT MAX(block_timestamp) FROM data AS max_value").iloc[0,0]
            
            if maxTimestamp == -1:
                raise ValueError("Cannot pinpoint where to start getting trades. No latest block number or timestamp avilable")
            else: 
                print("Getting the latest downloaded block number...")
                blockNumber = getBlockNumberFromTS(handler.w3["polygon"], maxTimestamp)
        else:
            blockNumber = maxBlockNumber.iloc[0,0]
        
        print(f"Starting to fetch from block number {blockNumber} to the latest block")
        if blockNumber is not None:
            # # Start Fetching trades from this block until now
            # handler.getAllTrades_Graph(_saveDirectory, os.getenv("graphQLAPI_key"), None, "latest")
            handler.getAllTrades_RPC(
                _saveDirectory,
                fromBlock = blockNumber,
                toBlock = "latest",
                blockBatchSize = 400,
                maxFileSize_GB = 0.1,
                saveBlockRange = 6_000
            )
# elif sys.argv[1] == "--getMarketPrices":
#     # Gets prices for markets and saves them
#     # Saves the data to ./src/data/polymarket/markets_with_price
#     _SaveDir = "./src/data/polymarket/markets_with_price"
#     if len(sys.argv) < 3 or (("--tradesLocation" not in sys.argv) and ("--markets" not in sys.argv)):
#         print("Please pass --tradesLocation and --markets flags, specifying")
#         print("1) Directory containing all trades (Its files should have polymarket_trades_pt_xxx.parquet format)")
#         print("2) The parquet files containing all markets that you want to get their utcomes' prices")
#         exit()
    
#     tradesDir = sys.argv[sys.argv.index("--tradesLocation") + 1]
#     marketsFileLoc = sys.argv[sys.argv.index("--markets") + 1]
    
#     if not os.path.exists(marketsFileLoc):
#         print("Markets file not found:", marketsFileLoc)
#         exit()
  
#     if not os.path.isdir(tradesDir):
#         print("Trades directory found:", tradesDir)
#         exit()
        
#     # Make the directory to save the data
#     if not os.path.isdir(_SaveDir):
#         os.makedirs(_SaveDir, exist_ok=True)
    
#     print("Loading all markets...")
#     allMarkets = pd.DataFrame(queryParquetFile(marketsFileLoc, "SELECT * FROM data"))
    
#     # Get previously acquired market prices
#     processedData = pd.DataFrame(queryParquetFile(marketsFileLoc, "SELECT marketID FROM data")).marketID.to_list()
    
    
else: 
    print("Invalid command.")