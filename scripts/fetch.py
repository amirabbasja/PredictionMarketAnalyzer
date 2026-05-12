from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import readJSONL, humanReadableFileSize, streamJsonlGz
from dotenv import load_dotenv
from pathlib import Path
import os, pprint, sys
import pandas as pd
from datetime import datetime

load_dotenv()

handler = PolymarketHandler(os.getenv("polymarket_api_key"))

if sys.argv[1] == "--getAllEvents":
    result = handler.getAllEvents(
        saveFile = f"src/data/events_{int(datetime.now().timestamp())}.jsonl.gz", 
        getMarkets = False,
        reqOptions = {
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getAllMarkets":
    result = handler.getAllMarkets(
        saveFile = f"src/data/markets_{int(datetime.now().timestamp())}.jsonl.gz", 
        reqOptions = {
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
elif sys.argv[1] == "--getHistoricMarkets":
    result = handler.getAllMarkets(
        saveFile = f"src/data/markets_{int(datetime.now().timestamp())}.jsonl.gz", 
        reqOptions = {
            "closed": True,
            "order": "id"
            # "liquidity_min": 10_000,
            # "liquidity_max": 100_000_000,
        },
    )
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
else:
    print("Invalid command.")