from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import readJSONL, humanReadableFileSize, Errors
from dotenv import load_dotenv
from pathlib import Path
import os, pprint, sys, json
import pandas as pd
from datetime import datetime
import asyncio

load_dotenv()

handler = PolymarketHandler(
    os.getenv("polymarket_api_key", None),
    os.getenv("drpcAPI_key", None),
    provider = "drpc"
)

if sys.argv[1] == "--getAllEvents":
    result = handler.getAllEvents(
        saveFile = f"src/data/liveEvents_{int(datetime.now().timestamp())}.jsonl.gz", 
        getMarkets = False,
        reqOptions = {
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getAllMarkets":
    getPriceData = False
    if(len(sys.argv) > 2 and sys.argv[2] == "--price"):
        getPriceData = True
        
    result = handler.getAllMarkets(
        saveFile = f"src/data/liveMarkets_{int(datetime.now().timestamp())}.jsonl.gz", 
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
        saveFile = f"src/data/historicalMarkets_{int(datetime.now().timestamp())}.jsonl.gz" if not "--continue" in sys.argv else filePath, 
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
        print("Please provide a file path to read.")
        exit()
    
    filePath = sys.argv[2].replace("\\", "/")  # Handle Windows-style paths
    _path = Path(filePath)
    if not _path.is_file():
        print(f"File not found: {filePath}")
        exit()
    
    fileName = ""
    if "jsonl.gz" in filePath:
        fileName = Path(_path.stem).stem
    else:
        fileName = _path.stem
    
    print(f"File size: {humanReadableFileSize(filePath)}")
    
    data = readJSONL(filePath)
    print(f"Total events read: {len(data)}")
    
    df = pd.DataFrame(data)
    df.to_csv(f"src/data/{fileName}.csv", index=True)
elif sys.argv[1] == "--getAllTrades":
    handler.getAllTrades(
        saveLocation = f"src/data/polymarketTrades", 
        toBlock = "latest",
        fromBlock = 0,
        blockBatchSize = 1000,
        parallelRequests = 10
    )
else:
    print("Invalid command.")