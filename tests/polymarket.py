# Polymarket Test
from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv

import os

# Load environment variables
load_dotenv()

# Make a PolymarketHandler instance
polymarketHandler = PolymarketHandler(
    polymarketAPIkey = os.getenv("polymarketAPI_key"),
    web3APIkey = os.getenv("drpcAPI_key"),
    provider = "drpc"
)

print("Connected:", polymarketHandler.w3["polygon"].is_connected()) 

try:
    # Get logs
    logs = polymarketHandler.w3["polygon"].eth.get_logs(
        {
            "address": polymarketHandler.contract_CFT_exchange_v2.address,
            "topics": ["0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c"],
            "fromBlock": str(hex(87707647-1000)),
            "toBlock": str(hex(87707647)),
        }
    )
    
    print(f"Fetched {len(logs)} logs.")
    
    # Print logs
    for log in logs[:5]:  # Print the first 5 logs
        print(log)
        exit()
except Exception as e:
    print("Error fetching logs:", e)