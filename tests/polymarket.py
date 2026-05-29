# Polymarket Test
from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv

import os

# Load environment variables
load_dotenv()

# Make a PolymarketHandler instance
polymarket_handler = PolymarketHandler(
    polymarketAPIkey = os.getenv("polymarketAPI_key"),
    web3APIkey = os.getenv("alchemyAPI_key")
)

print(polymarket_handler.w3.is_connected())