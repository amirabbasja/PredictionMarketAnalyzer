from pprint import pprint
from urllib.parse import urljoin
from src.utils.utils import *
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
import time, json, pprint, gzip
import pickle, os, datetime, asyncio
from tqdm import tqdm
from web3 import Web3
from eth_abi import decode
import concurrent.futures
import gc
from collections import defaultdict
from functools import wraps
import time
import pyarrow as pa
import pyarrow.parquet as pq

class PolymarketHandler:
    def __init__(self, polymarketAPIkey: Union[str, None] = None, web3APIkey: Union[str, None] = None, provider: str = None):
        """
        Initializes the PolymarketHandler with the provided API keys.
        Some methods use polymarket API, however, some need a web3 provider to get the actual data from blockchain
        
        Documentation: https://docs.polymarket.com/api-reference/rate-limits
        
        Args:
            polymarketAPIkey (str): The API key for authenticating with the Polymarket API.
            web3APIkey (str): The API key for authenticating with the Web3 API providers (e.g. alchemy, infura, etc.)
        """
        # Get necessary parameters
        self.polymarketAPIkey = polymarketAPIkey
        self.web3APIkey = web3APIkey
        
        if not self.polymarketAPIkey:
            print("Warning: No Polymarket API key provided.")
        
        if not self.web3APIkey:
            self.w3 = None
            print("Warning: No Web3 API key provided.")
        else:
            _endpoint = ""
            if provider.lower() == "alchemy":
                _endpoint = f"https://polygon-mainnet.g.alchemy.com/v2/{self.web3APIkey}"
            elif provider.lower() == "dwellir":
                _endpoint = f"https://api-polygon-mainnet-full.n.dwellir.com/{self.web3APIkey}"
            elif provider.lower() == "drpc":
                _endpoint = f"https://lb.drpc.live/polygon/{self.web3APIkey}"
            else:
                raise ValueError("Invalid provider specified. Please choose either 'alchemy', 'dwellir' or 'drpc'.")
            
            self.w3 = {
                "polygon": Web3(Web3.HTTPProvider(_endpoint))
            }

        # Base URLs
        # Gamma - Markets, events, tags, series, comments, sports, search, and public profiles
        self.baseURL_Gamma = "https://gamma-api.polymarket.com"
        
        # Data - User positions, trades, activity, holder data, open interest, leaderboards, and builder analytics.
        self.baseURL_Data = "https://data-api.polymarket.com"
        
        # Data - Orderbook data, pricing, midpoints, spreads, and price history. Also handles order placement,
        # cancellation, and other trading operations. Trading endpoints require authentication.
        self.baseURL_CLOB = "https://clob.polymarket.com"
        
        # Polymarket contract addresses
        self.exchange_CFT_v1 = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        self.exchange_NegRiskCFT_v1 = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
        self.polymarket_v2_Creation_Block = 84902353 # Blocknumber for which v2 started working
        self.exchange_CFT_v2 = "0xE111180000d2663C0091e4f400237545B87B996B"
        self.exchange_NegRiskCFT_v2 = "0xe2222d279d744050d28e00520010520000310F59"
        
        # Polymarket logs
        self.exchange_CFT_v1_OrderFilled_topic0 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
        self.exchange_CFT_v2_OrderFilled_topic0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
        self.exchange_NEGRISK_v1_OrderFilled_topic0 = self.exchange_CFT_v1_OrderFilled_topic0
        self.exchange_NEGRISK_v2_OrderFilled_topic0 = self.exchange_CFT_v2_OrderFilled_topic0
        
        # Polymarket contracts
        self.contract_CFT_exchange_v2 = self.w3["polygon"].eth.contract(address = self.exchange_CFT_v2, abi = loadABI("polygon", "polymarket_exchange_CFT_v2"))
        self.contract_Neg_Risk_CFT_exchange_v2 = self.w3["polygon"].eth.contract(address = self.exchange_NegRiskCFT_v2, abi = loadABI("polygon", "polymarket_exchange_neg_risk_CFT"))
        
        # Necessary data for decoding polymarket logs
        self.CTF_V1_DATA_TYPES     = ["uint256", "uint256", "uint256", "uint256", "uint256"]
        self.CTF_V1_DATA_NAMES     = ["makerAssetId", "takerAssetId", "maker_amount_filled", "taker_amount_filled", "fee"]
        self.CTF_V2_DATA_TYPES     = ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"]
        self.CTF_V2_DATA_NAMES     = ["side", "tokenId", "maker_amount_filled", "taker_amount_filled", "fee", "builder", "metadata"]
        self.NEGRISK_V1_DATA_TYPES = ["uint256", "uint256", "uint256", "uint256", "uint256"]
        self.NEGRISK_V1_DATA_NAMES = ["makerAssetId", "takerAssetId", "maker_amount_filled", "taker_amount_filled", "fee"]
        self.NEGRISK_V2_DATA_TYPES = ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"]
        self.NEGRISK_V2_DATA_NAMES = ["side", "tokenId", "maker_amount_filled", "taker_amount_filled", "fee", "builder", "metadata"]
        
        # Necessary schemas for saving data in parquet/sql
        self.eventsSchema = pa.schema([
            ("slug", pa.string()),
            ("marketID", pa.string()),
            ("bestAsk", pa.float64()),
            ("bestBid", pa.float64()),
            ("yesPrice", pa.float64()),
            ("noPrice", pa.float64()),
            ("yesReturn", pa.float64()),
            ("noReturn", pa.float64()),
            ("spread", pa.float64()),
            ("active", pa.bool_()),
            ("liquidity", pa.float64()),
            ("description", pa.string()),
            ("createdAt", pa.string()),
            ("startDate", pa.string()),
            ("endDate", pa.string()),
            ("status", pa.string()),
            ("type", pa.string()),
            ("orderMinSize", pa.float64()),
        ])
        self.marketsSchema = pa.schema([
            ("slug", pa.string()),
            ("marketID", pa.string()),
            ("bestAsk", pa.float64()),
            ("bestBid", pa.float64()),
            ("outcome_0", pa.string()),
            ("outcome_0_ID", pa.string()),
            ("outcome_0_price", pa.float64()),
            ("outcome_0_return", pa.float64()),
            ("outcome_1", pa.string()),
            ("outcome_1_ID", pa.string()),
            ("outcome_1_price", pa.float64()),
            ("outcome_1_return", pa.float64()),
            ("spread", pa.float64()),
            ("volumeNum", pa.float64()),
            ("volume1yr", pa.float64()),
            ("volumeAmm", pa.float64()),
            ("volumeClob", pa.float64()),
            ("takerBaseFee", pa.float64()),
            ("makerBaseFee", pa.float64()),
            ("active", pa.bool_()),
            ("archived", pa.bool_()),
            ("closed", pa.bool_()),
            ("liquidity", pa.float64()),
            ("liquidityNum", pa.float64()),
            ("description", pa.string()),
            ("createdAt", pa.string()),
            ("startDate", pa.string()),
            ("endDate", pa.string()),
            ("daysTillExpiry", pa.float64()),
            ("hoursTillExpiry", pa.float64()),
            ("question", pa.string()),
            ("questionID", pa.string()),
            ("conditionID", pa.string()),
            ("orderMinSize", pa.float64()),
        ])
        self.marketsSchemaWithPrice = pa.schema([
            ("slug", pa.string()),
            ("marketID", pa.string()),
            ("bestAsk", pa.float64()),
            ("bestBid", pa.float64()),
            ("outcome_0", pa.string()),
            ("outcome_0_ID", pa.string()),
            ("outcome_0_price", pa.float64()),
            ("outcome_0_return", pa.float64()),
            ("outcome_1", pa.string()),
            ("outcome_1_ID", pa.string()),
            ("outcome_1_price", pa.float64()),
            ("outcome_1_return", pa.float64()),
            ("spread", pa.float64()),
            ("volumeNum", pa.float64()),
            ("volume1yr", pa.float64()),
            ("volumeAmm", pa.float64()),
            ("volumeClob", pa.float64()),
            ("takerBaseFee", pa.float64()),
            ("makerBaseFee", pa.float64()),
            ("active", pa.bool_()),
            ("archived", pa.bool_()),
            ("closed", pa.bool_()),
            ("liquidity", pa.float64()),
            ("liquidityNum", pa.float64()),
            ("description", pa.string()),
            ("createdAt", pa.string()),
            ("startDate", pa.string()),
            ("endDate", pa.string()),
            ("daysTillExpiry", pa.float64()),
            ("hoursTillExpiry", pa.float64()),
            ("question", pa.string()),
            ("questionID", pa.string()),
            ("conditionID", pa.string()),
            ("orderMinSize", pa.float64()),
            ("outcome_0_history_price", pa.list_(pa.float64())),
            ("outcome_0_history_price_ts", pa.list_(pa.int64())),
            ("outcome_1_history_price", pa.list_(pa.float64())),
            ("outcome_1_history_price_ts", pa.list_(pa.int64())),
            ("has_price_history", pa.bool_()),
        ])
        self.tradesSchema = pa.schema([
            ("block_number", pa.int64()),
            ("block_timestamp", pa.int64()),
            ("tx_hash", pa.string()),
            ("platform_name", pa.string()),
            ("platform_version", pa.string()),
            ("taker", pa.string()),
            ("maker", pa.string()),
            ("token_id", pa.string()),
            ("slug", pa.string()),
            ("condition_id", pa.string()),
            ("price", pa.float64()),
            ("taker_side", pa.string()),
            ("amount", pa.float64()),
            ("amount_asset", pa.string()),
            ("fee", pa.float64()),
            ("fee_asset", pa.string()),
            ("extra_data", pa.string()),
        ])
    def __requireWeb3APIkey(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.web3APIkey:
                raise ValueError("This method requires a Web3 API key. Please provide one during initialization.")
            return func(self, *args, **kwargs)
        return wrapper

    def _CTF_V1_OrderFilled_fastDecode(self, log):
        """
        Decodes CTF Exchange V1 OrderFilled events.
        Direction (buy/sell) is inferred: if makerAssetId == 0 the maker is selling
        USDC to buy the outcome token (BUY side); otherwise SELL side.
        """
        raw = bytes(log["data"])
        decoded_data = decode(self.CTF_V1_DATA_TYPES, raw)
        result = dict(zip(self.CTF_V1_DATA_NAMES, decoded_data))

        result["orderHash"] = log["topics"][1]
        result["maker"]     = "0x" + log["topics"][2].hex()[-40:]
        result["taker"]     = "0x" + log["topics"][3].hex()[-40:]

        # Derive tokenId and side from the asset pair.
        # Convention: assetId == 0 means USDC (the collateral leg).
        makerSide = None
        takerSide = None
        if result["makerAssetId"] == 0:
            token_id = str(result["takerAssetId"])
            makerSide = 0  # BUY  – maker pays USDC, receives outcome token
            takerSide = 1
        else:
            token_id = str(result["makerAssetId"])
            makerSide = 1  # SELL – maker pays outcome token, receives USDC
            takerSide = 0

        return {
            "token_id":            token_id,
            "maker_side":          makerSide,
            "taker_side":          takerSide,
            "maker_amount_filled": result["maker_amount_filled"],
            "taker_amount_filled": result["taker_amount_filled"],
            "order_hash":          str(result["orderHash"].hex()).upper(),
            "taker":               Web3.to_checksum_address(result["taker"]),
            "maker":               Web3.to_checksum_address(result["maker"]),
            "fee":                 result["fee"],
            "platform_name":       "polymarket",
            "platform_version":    "ctf_v1"
        }
    
    def _CTF_V2_OrderFilled_fastDecode(self, log):
        """
        For decoding the CTF exchange (V2) events more quickly.
        """
        # Decode non-indexed fields from data
        # log["data"] is already bytes in web3.py — no need for .hex() / fromhex()
        raw = bytes(log["data"])
        decoded_data = decode(self.CTF_V2_DATA_TYPES, raw)
        result = dict(zip(self.CTF_V2_DATA_NAMES, decoded_data))

        # Decode indexed fields from topics[1], topics[2], topics[3]
        # topics[0] is the event signature hash
        result["orderHash"] = log["topics"][1]           # already bytes32
        result["maker"]     = "0x" + log["topics"][2].hex()[-40:]  # last 20 bytes → address
        result["taker"]     = "0x" + log["topics"][3].hex()[-40:]  # last 20 bytes → address
        
        returnDict = {
            "token_id":            str(result["tokenId"]),
            "maker_side":          result["side"],
            "taker_side":          1 if result["side"] == 0 else 0,
            "maker_amount_filled": result["maker_amount_filled"],
            "taker_amount_filled": result["taker_amount_filled"],
            "order_hash":          str(result["orderHash"].hex()).upper(),
            "taker":               Web3.to_checksum_address(result["taker"]),
            "maker":               Web3.to_checksum_address(result["maker"]),
            "fee":                 result["fee"],
            "platform_name":       "polymarket",
            "platform_version":    "ctf_v2"
        }

        return returnDict

    def _NEGRISK_V1_OrderFilled_fastDecode(self, log):
        """
        Decodes NegRisk CTF Exchange V1 OrderFilled events.
        Same layout as CTF V1; direction inferred from the zero-asset leg.
        """
        raw = bytes(log["data"])
        decoded_data = decode(self.NEGRISK_V1_DATA_TYPES, raw)
        result = dict(zip(self.NEGRISK_V1_DATA_NAMES, decoded_data))

        result["orderHash"] = log["topics"][1]
        result["maker"]     = "0x" + log["topics"][2].hex()[-40:]
        result["taker"]     = "0x" + log["topics"][3].hex()[-40:]

        if result["makerAssetId"] == 0:
            token_id = str(result["takerAssetId"])
            side = 0  # BUY
        else:
            token_id = str(result["makerAssetId"])
            side = 1  # SELL

        return {
            "token_id":            token_id,
            "maker_side":          side,
            "taker_side":          1 if side == 0 else 0,
            "maker_amount_filled": result["maker_amount_filled"],
            "taker_amount_filled": result["taker_amount_filled"],
            "order_hash":          str(result["orderHash"].hex()).upper(),
            "taker":               Web3.to_checksum_address(result["taker"]),
            "maker":               Web3.to_checksum_address(result["maker"]),
            "fee":                 result["fee"],
            "platform_name":       "polymarket",
            "platform_version":    "negrisk_v1"
        }

    def _NEGRISK_V2_OrderFilled_fastDecode(self, log):
        """
        Decodes NegRisk CTF Exchange V2 OrderFilled events.
        Identical wire format to CTF V2; kept separate for contract-source clarity.
        """
        raw = bytes(log["data"])
        decoded_data = decode(self.NEGRISK_V2_DATA_TYPES, raw)
        result = dict(zip(self.NEGRISK_V2_DATA_NAMES, decoded_data))

        result["orderHash"] = log["topics"][1]
        result["maker"]     = "0x" + log["topics"][2].hex()[-40:]
        result["taker"]     = "0x" + log["topics"][3].hex()[-40:]

        return {
            "token_id":            str(result["tokenId"]),
            "maker_side":          result["side"],
            "taker_side":          1 if result["side"] == 0 else 0,
            "maker_amount_filled": result["maker_amount_filled"],
            "taker_amount_filled": result["taker_amount_filled"],
            "order_hash":          str(result["orderHash"].hex()).upper(),
            "taker":               Web3.to_checksum_address(result["taker"]),
            "maker":               Web3.to_checksum_address(result["maker"]),
            "fee":                 result["fee"],
            "platform_name":       "polymarket",
            "platform_version":    "negrisk_v2"
        }

    async def getPriceHistory_async(self, marketID: str, outcomeIDs: tuple[str, str], **kwargs):
        """
        Fetches the historical price for a market. Makes simultaneous requests for both outcome tokens to speed up the process.

        Args:
            marketID (str): The unique market ID for which to fetch the price history.
            outcomeIDs (tuple[str, str]): The unique outcome IDs for which to fetch the price history.
        
        Keyword Args:
            startTs (int): The starting timestamp (in milliseconds) for the price history. Default is None, which means it will fetch from the earliest available data.
            endTs (int): The ending timestamp (in milliseconds) for the price history. Default is None, which means it will fetch until the latest available data.
            interval (str): The interval for the price history data. Avilable options are "max", "all", "1m", "1w", "1d", "6h", "1h"
        """
        
        _params = {
            **kwargs
        }
        
        try:
            # Get responses in parallel for both outcomes
            results = await asyncio.gather(
                sendRequest_Async(
                    url = urljoin(self.baseURL_CLOB, "/prices-history"),
                    method = "GET",
                    params = {
                        "market": outcomeIDs[0],
                        **_params
                    }
                ),
                sendRequest_Async(
                    url = urljoin(self.baseURL_CLOB, "/prices-history"),
                    method = "GET",
                    params = {
                        "market": outcomeIDs[1],
                        **_params
                    }
                )
            )
            
            jsonResponse_0 = await results[0].json()
            jsonResponse_1 = await results[1].json()
            
            if "error" in jsonResponse_0 or "error" in jsonResponse_1:
                if "Max retries exceeded" in jsonResponse_0["error"] or "Max retries exceeded" in jsonResponse_1["error"]:
                    return {
                        "error": True,
                        "code": Errors.RATE_LIMITED,
                        "msg": jsonResponse_0["error"]
                    }
                
                return {
                    "error": True,
                    "code": Errors.REQUEST_ERROR,
                    "msg": jsonResponse_0["error"] if "error" in jsonResponse_0 else jsonResponse_1["error"]
                }
            else:
                return {
                    "marketID": marketID,
                    "outcome_0": (jsonResponse_0).get("history", []),
                    "outcome_1": (jsonResponse_1).get("history", [])
                }
        except Exception as e:
            return {
                "error": True,
                "code": Errors.UNKNOWN_ERROR,
                "msg": f"{e}"
            }

    def getPriceHistory_sync(self, marketID: str, outcomeIDs: tuple[str, str], **kwargs):
        """
        Fetches the historical price for a market. Makes synchronous requests for both outcome, effectively doubling the fetch time.

        Args:
            marketID (str): The unique market ID for which to fetch the price history.
            outcomeIDs (tuple[str, str]): The unique outcome IDs for which to fetch the price history.
        
        Keyword Args:
            startTs (int): The starting timestamp (in milliseconds) for the price history. Default is None, which means it will fetch from the earliest available data.
            endTs (int): The ending timestamp (in milliseconds) for the price history. Default is None, which means it will fetch until the latest available data.
            interval (str): The interval for the price history data. Avilable options are "max", "all", "1m", "1w", "1d", "6h", "1h"
        """
        
        _params = {
            **kwargs
        }
        
        try:
            # Get responses in parallel for both outcomes
            results = []
            results.append(sendRequest_Sync(
                url = urljoin(self.baseURL_CLOB, "/prices-history"),
                method = "GET",
                params = {
                    "market": outcomeIDs[0],
                    **_params
                }
            ))
            results.append(sendRequest_Sync(
                url = urljoin(self.baseURL_CLOB, "/prices-history"),
                method = "GET",
                params = {
                    "market": outcomeIDs[1],
                    **_params
                }
            ))
            
            jsonResponse_0 = results[0].json()
            jsonResponse_1 = results[1].json()
            
            if "error" in jsonResponse_0 or "error" in jsonResponse_1:
                if "Max retries exceeded" in jsonResponse_0["error"] or "Max retries exceeded" in jsonResponse_1["error"]:
                    return {
                        "error": True,
                        "code": Errors.RATE_LIMITED,
                        "msg": jsonResponse_0["error"]
                    }
                
                return {
                    "error": True,
                    "code": Errors.REQUEST_ERROR,
                    "msg": jsonResponse_0["error"] if "error" in jsonResponse_0 else jsonResponse_1["error"]
                }
            else:
                return {
                    "marketID": marketID,
                    "outcome_0": (jsonResponse_0).get("history", []),
                    "outcome_1": (jsonResponse_1).get("history", [])
                }
        except Exception as e:
            return {
                "error": True,
                "code": Errors.UNKNOWN_ERROR,
                "msg": f"{e}"
            }

    def getAllEvents(self, active: bool = True, archived: bool = False, closed : bool = False, getMarkets:bool = True, **kwargs):
        """
        Fetches all events from the Polymarket API. By default saves the data to a 
        parquet/JSONL file in the src/data/ directory. It is not able to hold all market data 
        due to memory constraints.
        """
        
        # Function to process the raw data
        def _processData(data: Dict[str, Any]) -> Dict[str, Any]:
            # For processing and normalizing the raw data from the API.
            returnData = {}
            
            returnData = {}
            returnData["markets"] = []
            returnData["tags"] = []
            
            # Event-level data
            returnData["active"] = data.get("active", None)
            returnData["archived"] = data.get("archived", None)
            returnData["closed"] = data.get("closed", None)
            returnData["createdAt"] = data.get("createdAt", None)
            returnData["startDate"] = data.get("startDate", None)
            returnData["endDate"] = data.get("endDate", None)
            returnData["liquidity"] = data.get("liquidity", None)
            returnData["description"] = data.get("description", "").replace("\n", " ").replace("\r", " ") if data.get("description", None) is not None else None # Deleted due to storage issues
            returnData["slug"] = data.get("slug", None)
            returnData["createdAt"] = data.get("createdAt", None)
            returnData["eventID"] = data.get("id", None)
            
            # Market-level data
            returnData["marketsCount"] = len(data.get("markets", [])) if "markets" in data and isinstance(data["markets"], list) else 0
            if "markets" in data and getMarkets:
                if isinstance(data["markets"], list):
                    for market in data["markets"]:
                        spread = float(market.get("spread")) * 100 if market.get("spread", None) is not None else None
                        
                        # We take that each market can either be yes or no (As of coding date)
                        outcomes = json.loads(market.get("outcomes", None)) if market.get("outcomes", None) is not None else None
                        outcomePrices = json.loads(market.get("outcomePrices", None)) if market.get("outcomePrices", None) is not None else None

                        if isinstance(outcomes, list) and len(outcomes) == 2:
                            yesIDX = outcomes.index("Yes") if "Yes" in outcomes else None
                            noIDX = outcomes.index("No") if "No" in outcomes else None
                            
                            if yesIDX is not None and noIDX is not None and outcomePrices is not None:
                                market["yesPrice"] = float(outcomePrices[yesIDX])
                                market["yesReturn"] = round((1- market["yesPrice"]) / market["yesPrice"] * 100, 2) if market["yesPrice"] is not None and market["yesPrice"] != 0 else None
                                market["noPrice"] = float(outcomePrices[noIDX])
                                market["noReturn"] = round((1 - market["noPrice"]) / market["noPrice"] * 100, 2) if market["noPrice"] is not None and market["noPrice"] != 0 else None
                            else:
                                market["yesPrice"] = None
                                market["noPrice"] = None
                                market["yesReturn"] = None
                                market["noReturn"] = None
                        
                        marketData = {
                            "slug": market.get("slug", None),
                            "marketID" : market.get("id", None),
                            "bestAsk": market.get("bestAsk", None),
                            "bestBid": market.get("bestBid", None),
                            "yesPrice": market.get("yesPrice", None),
                            "noPrice": market.get("noPrice", None),
                            "yesReturn": market.get("yesReturn", None),             # In percentage
                            "noReturn": market.get("noReturn", None),               # In percentage
                            "spread": spread,                                       # In percentage
                            "active": market.get("active", None),
                            "liquidity": float(market.get("liquidity")) * 100 if market.get("liquidity", None) is not None else None,
                            "description": market.get("description", "").replace("\n", " ").replace("\r", " ") if market.get("description", None) is not None else None, # Deleted due to storage issues
                            "createdAt": market.get("createdAt", None),
                            "startDate": market.get("startDate", None),
                            "endDate": market.get("endDate", None),
                            "status": market.get("status", None),
                            "type": market.get("type", None),
                            "orderMinSize": market.get("orderMinSize", None),
                        }
                        returnData["markets"].append(marketData)
            
            # Tag-level data
            if "tags" in data:
                if isinstance(data["tags"], list):
                    for tag in data["tags"]:
                        returnData["tags"].append(tag["label"])
            
            return returnData

        if "saveFile" not in kwargs:
            raise ValueError("For now, you must specify saveFile so that the data is saved to disk instead of held in memory. This is because the amount of data is too large to hold in memory.")
        
        
        if "saveFile" in kwargs and kwargs["saveFile"].endswith(".jsonl") or  kwargs["saveFile"].endswith(".jsonl.gz"):
            if not os.path.isfile(kwargs["saveFile"]):
                makeEmptyJSONLFile(kwargs["saveFile"], compressed = kwargs["saveFile"].endswith(".gz"))
        elif "saveFile" in kwargs and kwargs["saveFile"].endswith(".parquet"):
            if not os.path.isfile(kwargs["saveFile"]):
                schema = self.eventsSchema
                makeEmptyParquetFile(kwargs["saveFile"], schema)
        else:
            raise ValueError("Unacceptable save file format")
        
        _params = {
            "limit": 500,
            "active": active,
            "archived": archived,
            "closed": closed,
            **kwargs["reqOptions"],
            "order": "liquidity",
        }

        # Get the first event
        res = sendRequest_Sync(
            url = urljoin(self.baseURL_Gamma, "/events/keyset"),
            method = "GET",
            params = _params
        )
        
        try:
            jsonResponse = res.json()
            print(f"Total events fetched: {len(jsonResponse.get('events', []))}")
        except Exception as e:
            print(f"Error parsing JSON response: {e}")
            jsonResponse = {}
        
        # Continue getting the rest with cursor pagination
        allEventCounter = 0
        counter = 0
        while res.status_code == 200 and "next_cursor" in jsonResponse and "next_cursor" in jsonResponse:
            print(f"Fetching next page of events... (Page {counter + 2}) | Total events fetched so far: {allEventCounter}")
            counter += 1
            
            _params["after_cursor"] = jsonResponse["next_cursor"]
            res = sendRequest_Sync(
                url = urljoin(self.baseURL_Gamma, "/events/keyset"),
                method = "GET",
                params = _params
            )
            
            try:
                jsonResponse = res.json()
                allEventCounter += len(jsonResponse.get("events", []))

                if not isinstance(jsonResponse, dict):
                    print(f"Unexpected JSON type: {type(jsonResponse)}")
                    break
                
                processedList = []
                for event in jsonResponse.get("events", []):
                    processedData = _processData(event)
                    processedList.append(processedData)
                
                if "saveFile" in kwargs and kwargs["saveFile"].endswith(".jsonl"):
                    # We do this so the script opens the file only once and appends to it, instead of 
                    # opening and closing the file for each event which would be very inefficient.
                    appendToJSONL(kwargs["saveFile"], processedList)
                elif "saveFile" in kwargs and kwargs["saveFile"].endswith(".parquet"):
                    # Reads the parquet file, appends to it then saves it. Its not memory efficient.
                    appendToParquet(kwargs["saveFile"], processedList, schema)
                
                print(f"Liquidity: {jsonResponse.get('events', [])[0].get('liquidity', None)} -> {jsonResponse.get('events', [])[-1].get('liquidity', None)}")
                print(f"endDate: {jsonResponse.get('events', [])[0].get('endDate', None)} -> {jsonResponse.get('events', [])[-1].get('endDate', None)}")
                    
                if jsonResponse.get("next_cursor", None) is None:
                    break
            except Exception as e:
                raise e

        return None

    def getMarket(self, *, id: int | None = None, slug: str | None = None, tokenID: str | None = None, getPriceData: bool = False, **kwargs):
        """
        Fetches a market's data using either its unique market ID, slug, or token ID. 
        If multiple identifiers are provided, it prioritizes them in the order of ID, 
        slug, and then token ID.
        """
        # Function to process the raw data
        def _processData(data: Dict[str, Any]) -> Dict[str, Any]:
            # For processing and normalizing the raw data from the API.
            returnData = {}
            
            # Processing
            _createdAt = datetime.datetime.strptime(data.get("createdAt", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S")  if data.get("createdAt", None) is not None else None
            _startDate = datetime.datetime.strptime(data.get("startDate", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("startDate", None) is not None else None
            _endDate = datetime.datetime.strptime(data.get("endDate", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("endDate", None) is not None else None
            _now = datetime.datetime.now()
            _diffDays = (_endDate - _now).days if _endDate is not None else None
            _diffHours = (_endDate - _now).total_seconds() / 3600 if _endDate is not None else None
            
            # Event-level data
            returnData["active"] = data.get("active", None)
            returnData["marketID"] = data.get("id", "")
            returnData["archived"] = data.get("archived", None)
            returnData["closed"] = data.get("closed", None)
            returnData["createdAt"] = _createdAt.isoformat() if _createdAt is not None else None
            returnData["startDate"] = _startDate.isoformat() if _startDate is not None else None
            returnData["endDate"] = _endDate.isoformat() if _endDate is not None else None
            returnData["daysTillExpiry"] = _diffDays
            returnData["hoursTillExpiry"] = _diffHours
            returnData["liquidity"] = data.get("liquidity", None)
            returnData["liquidityNum"] = data.get("liquidityNum", None)
            returnData["volumeNum"] = data.get("volume", None)
            returnData["volume1yr"] = data.get("volume1yr", None)
            returnData["volumeAmm"] = data.get("volumeAmm", None)
            returnData["volumeClob"] = data.get("volumeClob", None)
            returnData["description"] = data.get("description", "").replace('\n', ' ').replace('\r', ' ') if data.get("description", None) is not None else None # Deleted due to storage issues
            returnData["slug"] = data.get("slug", None)
            returnData["spread"] = data.get("spread", None)
            returnData["takerBaseFee"] = data.get("takerBaseFee", None)
            returnData["makerBaseFee"] = data.get("makerBaseFee", None)
            
            # Market-level data
            returnData["eventsCount"] = len(data.get("events", [])) if "events" in data and isinstance(data["events"], list) else 0
            
            # Disregard the Events for now
            returnData["events"] = [{"slug": event["slug"]} for event in data.get("events")] if data.get("events", None) is not None else None
            
            # We take that each market can either be yes or no (As of coding date)
            outcomes = json.loads(data.get("outcomes")) if data.get("outcomes", None) is not None else None
            outcomePrices = json.loads(data.get("outcomePrices")) if data.get("outcomePrices", None) is not None else ""
            tokenIDs = json.loads(data.get("clobTokenIds", ["",""])) if data.get("clobTokenIds", None) is not None else ["",""]

            if isinstance(outcomes, list) and len(outcomes) == 2 and outcomePrices != "" and outcomePrices is not None:
                # Outcome 0 - first outcome
                returnData["outcome_0_price"] = float(outcomePrices[0])
                returnData["outcome_0_return"] = round((1- returnData["outcome_0_price"]) / returnData["outcome_0_price"] * 100, 2) if returnData["outcome_0_price"] is not None and returnData["outcome_0_price"] != 0 else None
                returnData["outcome_0_ID"] = str(tokenIDs[0]) if len(tokenIDs) > 0 else ""
                
                # Outcome 1 - second outcome
                returnData["outcome_1_price"] = float(outcomePrices[1])
                returnData["outcome_1_return"] = round((1 - returnData["outcome_1_price"]) / returnData["outcome_1_price"] * 100, 2) if returnData["outcome_1_price"] is not None and returnData["outcome_1_price"] != 0 else None
                returnData["outcome_1_ID"] = str(tokenIDs[1]) if len(tokenIDs) > 1 else ""
            
            if getPriceData and returnData.get("outcome_0_ID", None) is not None and returnData.get("outcome_1_ID", None) is not None:
                priceHistory = self.getPriceHistory_sync(returnData["marketID"], (returnData["outcome_0_ID"], returnData["outcome_1_ID"]), interval="all")
                returnData["priceHistory"] = priceHistory
            
            return returnData
    
        endPoint = ""
        if id is not None:
            endPoint = f"/markets/{id}"
            kwargs["id"] = id
        elif slug is not None:
            endPoint = f"/markets/slug/{slug}"
            kwargs["slug"] = slug
        elif tokenID is not None:
            endPoint = f"/markets-by-token/{tokenID}"
            kwargs["toke_id"] = tokenID
        else:
            raise ValueError("Invalid identifier provided. Please provide a valid id, slug, or tokenID.")
        
        _params = {**kwargs}
        
        res = sendRequest_Sync(
            url = urljoin(self.baseURL_Gamma, endPoint),
            method = "GET",
            params = _params
        )
        return _processData(res.json())

    def getLastCursorFromFile(self, filePath: str, n: int = 1000) -> tuple[str, int]:
        """
        Reads a .jsonl or .jsonl.gz file and returns the last "next_cursor" value found in the file. 
        This is useful for resuming data fetching from where it left off in case of interruptions.

        Args:
            filePath (str): Path to the .jsonl or .jsonl.gz file from which to extract the last cursor value.
            n (int): The number of lines from the end of the file to read for finding the last cursor.
                This is an optimization to avoid reading the entire file if it's large. Default is 1000 lines.

        Returns:
            tuple[str, int]: A tuple containing the last "next_cursor" value found in the file and the line count in the file.
            If the file is empty or no cursor is found, returns an empty string and 0.
        """
        # TODO: Reading the entire file might be sub-optimal for large files. Consider optimizing by reading only 
        # the last few lines of the file using a two-pass approach.
        print("Reading file...")
        if filePath.endswith(".jsonl.gz") or filePath.endswith(".jsonl"):
            lines = getLastNLines(filePath, n)
        else:
            raise ValueError("File must be a .jsonl or .jsonl.gz file.")
        
        print("Getting entry counts...")
        lineCount = countLines(filePath)
        print(f"Total entries in file: {lineCount}")
        
        for line in reversed(lines):
            data = json.loads(line)
            if "next_cursor" in data:
                return (data["next_cursor"], lineCount)
        
        return ("", 0)  # Return empty string if no cursor is found
    
    def getAllMarkets(
        self, 
        active: Optional[bool] = True, 
        archived: Optional[bool] = False, 
        closed: Optional[bool] = False, 
        getEvents: Optional[bool] = False, 
        getPriceData: Optional[bool] = False, 
        checkpoint: Optional[tuple[str | None, int]] = None,
        **kwargs
    ):
        """
        Fetches all events from the Polymarket API. By default saves the data to a 
        JSONL file in the src/data/ directory. It is not able to hold all market data
        due to memory constraints. 
        
        When getPricesData is set to True, it also fetches the historical price data 
        for each market. Separate API calls are made for each outcome in each market, 
        so it can significantly increase the time taken to fetch all data. Use with 
        caution.
        
        Args:
            getPriceData (bool): Whether to fetch the price data for each market. Each 
                market is consisted of two outcome tokens, each one has a price.
            checkpoint (str, int):  A tuple consisted of the cursor value and the line 
                count. The cursor value to start fetching data from. This is used for 
                pagination. If not provided, it will start from the beginning. This is 
                useful for resuming data fetching from where it left off in case of 
                interruptions.
        """
        
        # Function to process the raw data
        def _processData(data: Dict[str, Any]) -> Dict[str, Any]:
            # For processing and normalizing the raw data from the API.
            returnData = {}
            
            # Processing
            _createdAt = datetime.datetime.strptime(data.get("createdAt", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S")  if data.get("createdAt", None) is not None else None
            _startDate = datetime.datetime.strptime(data.get("startDate", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("startDate", None) is not None else None
            _endDate = datetime.datetime.strptime(data.get("endDate", "").split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("endDate", None) is not None else None
            _now = datetime.datetime.now()
            _diffDays = (_endDate - _now).days if _endDate is not None else None
            _diffHours = (_endDate - _now).total_seconds() / 3600 if _endDate is not None else None
            
            # Event-level data
            returnData["active"] = data.get("active", None)
            returnData["marketID"] = data.get("id", "NO_ID")
            returnData["archived"] = data.get("archived", None)
            returnData["closed"] = data.get("closed", None)
            returnData["createdAt"] = _createdAt.isoformat() if _createdAt is not None else None
            returnData["startDate"] = _startDate.isoformat() if _startDate is not None else None
            returnData["endDate"] = _endDate.isoformat() if _endDate is not None else None
            returnData["daysTillExpiry"] =  _diffDays
            returnData["hoursTillExpiry"] = _diffHours
            returnData["description"] = data.get("description", "NO_DESCRIPTION").replace('\n', ' ').replace('\r', ' ') if data.get("description", None) is not None else "NO_DESCRIPTION" # Deleted due to storage issues
            returnData["question"] = data.get("question", "NO_QUESTION")
            returnData["questionID"] = data.get("questionID", "NO_QUESTION_ID")
            returnData["conditionID"] = data.get("conditionId", "NO_CONDITION_ID")
            returnData["slug"] = data.get("slug", "NO_SLUG")
            returnData["spread"] = float(data.get("spread", -1))
            returnData["takerBaseFee"] = float(data.get("takerBaseFee", -1))
            returnData["makerBaseFee"] = float(data.get("makerBaseFee", -1))
            returnData["liquidity"] = float(data.get("liquidity", -1))
            returnData["liquidityNum"] = float(data.get("liquidityNum", -1))
            returnData["volumeNum"] = float(data.get("volume", -1))
            returnData["volume1yr"] = float(data.get("volume1yr", -1))
            returnData["volumeAmm"] = float(data.get("volumeAmm", -1))
            returnData["volumeClob"] = float(data.get("volumeClob", -1))
            returnData["bestBid"] = float(data.get("bestBid", -1))
            returnData["bestAsk"] = float(data.get("bestAsk", -1))
            returnData["orderMinSize"] = float(data.get("orderMinSize", -1))
            
            # Market-level data
            returnData["eventsCount"] = len(data.get("events", [])) if "events" in data and isinstance(data["events"], list) else 0
            
            # Disregard the Events for now
            returnData["events"] = None
            
            # We take that each market can either be yes or no (As of coding date)
            outcomes = json.loads(data.get("outcomes")) if data.get("outcomes", None) is not None else None
            outcomePrices = json.loads(data.get("outcomePrices")) if data.get("outcomePrices", None) is not None else ""
            tokenIDs = json.loads(data.get("clobTokenIds", ["",""])) if data.get("clobTokenIds", None) is not None else ["",""]
            
            if isinstance(outcomes, list) and len(outcomes) == 2 and outcomePrices != "" and outcomePrices is not None:
                # Outcome 0 - first outcome
                returnData["outcome_0_price"] = float(outcomePrices[0])
                returnData["outcome_0_return"] = round((1- returnData["outcome_0_price"]) / returnData["outcome_0_price"] * 100, 2) if returnData["outcome_0_price"] is not None and returnData["outcome_0_price"] != 0 else None
                returnData["outcome_0_ID"] = str(tokenIDs[0]) if len(tokenIDs) > 0 else ""
                
                # Outcome 1 - second outcome
                returnData["outcome_1_price"] = float(outcomePrices[1])
                returnData["outcome_1_return"] = round((1 - returnData["outcome_1_price"]) / returnData["outcome_1_price"] * 100, 2) if returnData["outcome_1_price"] is not None and returnData["outcome_1_price"] != 0 else None
                returnData["outcome_1_ID"] = str(tokenIDs[1]) if len(tokenIDs) > 1 else ""
                
                # Outcome descriptions
                returnData["outcome_0"] = outcomes[0] if outcomes else "NO_NAME"
                returnData["outcome_1"] = outcomes[1] if outcomes else "NO_NAME"
            
            if getPriceData and returnData.get("outcome_0_ID", None) is not None and returnData.get("outcome_1_ID", None) is not None:
                priceHistory = self.getPriceHistory_sync(returnData["marketID"], (returnData["outcome_0_ID"], returnData["outcome_1_ID"]), interval="all")
                returnData["priceHistory"] = priceHistory
            
            return returnData

        if "saveFile" not in kwargs:
            raise ValueError("For now, you must specify saveFile so that the data is saved to disk instead of held in memory. This is because the amount of data is too large to hold in memory.")
        
        if "saveFile" in kwargs and (kwargs["saveFile"].endswith(".jsonl") or kwargs["saveFile"].endswith(".jsonl.gz")):
            if not os.path.isfile(kwargs["saveFile"]):
                makeEmptyJSONLFile(kwargs["saveFile"], compressed = kwargs["saveFile"].endswith(".gz"))
        elif "saveFile" in kwargs and kwargs["saveFile"].endswith(".parquet"):
            if not os.path.isfile(kwargs["saveFile"]):
                parquetSchema = self.marketsSchema
                makeEmptyParquetFile(kwargs["saveFile"], parquetSchema)
            else:
                parquetSchema = pq.read_schema(kwargs["saveFile"])
        else:
            raise ValueError("Unacceptable save file format")
    
        _params = {
            "limit": 500,
            "active": active,
            "archived": archived,
            "closed": closed,
            **kwargs["reqOptions"],
        }
        
        # Pickup where left off, if nextCursor is provided
        if checkpoint is not None:
            _params["after_cursor"] = checkpoint[0]
            print(f"Resuming data fetching from cursor: {checkpoint[0]} | Line count in file: {checkpoint[1]}")
        
        # Get the first event
        res = sendRequest_Sync(
            url = urljoin(self.baseURL_Gamma, "/markets/keyset"),
            method = "GET",
            params = _params
        )
        
        try:
            jsonResponse = res.json()
            print(f"Total markets fetched: {len(jsonResponse.get('markets', []))}")
        except Exception as e:
            print(f"Error parsing JSON response: {e}")
            jsonResponse = {}
        
        # Continue getting the rest with cursor pagination
        allEventCounter = 0 if checkpoint is None else checkpoint[1]
        counter = 0 
        processedList = []
        while res.status_code == 200 and "next_cursor" in jsonResponse and "next_cursor" in jsonResponse:
            print(f"Fetching next page of markets... (Page {counter + 2}) | Total markets fetched so far: {allEventCounter}")
            counter += 1
            
            _params["after_cursor"] = jsonResponse["next_cursor"]
            res = sendRequest_Sync(
                url = urljoin(self.baseURL_Gamma, "/markets/keyset"),
                method = "GET",
                params = _params
            )
            
            try:
                jsonResponse = res.json()
                allEventCounter += len(jsonResponse.get("markets", []))

                if not isinstance(jsonResponse, dict):
                    print(f"Unexpected JSON type: {type(jsonResponse)}")
                    break
                
                for event in tqdm(jsonResponse.get("markets", []), desc="Processing markets"):
                    processedData = _processData(event)
                    
                    # For consistency between all rows
                    processedData["next_cursor"] = "" 
                    
                    processedList.append(processedData)
                
                if "jsonl" in kwargs["saveFile"] or "jsonl.gz" in kwargs["saveFile"] :
                    # We do this so the script opens the file only once and appends to it, instead of 
                    # opening and closing the file for each event which would be very inefficient.
                    appendToJSONL(kwargs["saveFile"], processedList)
                    processedList = []
                
                    # Save the progress in a file
                    saveProgress(kwargs.get("saveFile"), {
                        "next_cursor": jsonResponse.get("next_cursor"),
                        "eventCount": allEventCounter,
                        "timestamp": int(datetime.datetime.now().timestamp())
                    })
                elif "parquet" in kwargs["saveFile"]:
                    # Saving parquet files is memory-intensive, therefore we lower the frequency of
                    # writing into hard disk
                    if 2_000 <= len(processedList):
                        # Appends to a parquet database. Its important to know that its not memory efficient 
                        # and needs loading the entire database before adding to it.
                        print("Writing new data to file...")
                        appendToParquet(kwargs["saveFile"], processedList, parquetSchema, True)
                        processedList = []
                        
                        # Save the progress in a file
                        saveProgress(kwargs.get("saveFile"), {
                            "next_cursor": jsonResponse.get("next_cursor"),
                            "eventCount": allEventCounter,
                            "timestamp": int(datetime.datetime.now().timestamp())
                        })
                
                print(f"Acquired {len(processedList)} data | Total: {allEventCounter}")
                    
                if jsonResponse.get("next_cursor", None) is None:
                    # Save before exiting
                    if "parquet" in kwargs["saveFile"] and len(processedList) != 0:
                        appendToParquet(kwargs["saveFile"], processedList, parquetSchema, True)
                    
                    print("No data for next page was sent from the exchange. Assuming the end of fetch.")
                    break
                else:
                    # # Add the next cursor to the last item of the batch so that if process ran into 
                    # # errors, we could pickup where we left off
                    # processedList[-1]["next_cursor"] = jsonResponse["next_cursor"]
                    pass
            except Exception as e:
                raise e

        return None
    
    def getBatchPriceHistory(self, marketID: list[str], outcomeIDs: list[str], **kwargs):
        """
        Fetches the historical price for a batch of markets. Makes asynchronous requests for both outcomes of each market, significantly reducing the fetch time compared to synchronous requests.

        Args:
            marketID (str): The unique market ID for which to fetch the price history. For each there should be a corresponding outcome ID passed.
            outcomeIDs (list[str]): A list of unique outcome IDs for which to fetch the price history.

        Keyword Args:
            startTs (int): The starting timestamp (in milliseconds) for the price history. Default is None, which means it will fetch from the earliest available data.
            endTs (int): The ending timestamp (in milliseconds) for the price history. Default is None, which means it will fetch until the latest available data.
            interval (str): The interval for the price history data. Avilable options are "max", "all", "1m", "1w", "1d", "6h", "1h"
        """
        
        _params = {
            **kwargs
        }
        
        try:   
            batchResult = sendRequest_Sync(
                url = urljoin(self.baseURL_CLOB, "/batch-prices-history"),
                method = "POST",
                payload = {
                    "markets": outcomeIDs,
                    **_params
                }
            )  
            jsonResult = batchResult.json()
            
            # Handle errors
            if "error" in jsonResult:
                if "Max retries exceeded" in jsonResult["error"]:
                    return {
                        "error": True,
                        "code": Errors.RATE_LIMITED,
                        "msg": jsonResult["error"]
                    }
                
                # General error
                return {
                    "error": True,
                    "code": Errors.REQUEST_ERROR,
                    "msg": jsonResult["error"]
                }
                
            # Process and return the data
            return jsonResult
        except Exception as e:
            return {
                "error": True,
                "code": Errors.UNKNOWN_ERROR,
                "msg": f"{e}"
            }

    def getAllTrades_Graph(
        self, 
        saveLocation: str, 
        apiKey: str, 
        fromBlock: Union[int, None], 
        toBlock: Union[int, None, str], 
        maxFileSiz: float = 0.8,
        blockBatch: float = 1000
    ):
        """
        Using a graphQL API, gets all trades for polymarket v2 and saves them into a parquet database.
        
        
        """
        # Link: https://thegraph.com/explorer/subgraphs/B9mm21DKCex8ka4g8cteQU4NQqtviwmcTjQAYLbzQ1eR?view=Query&chain=arbitrum-one
        _polymarketV2_subgraphID = "B9mm21DKCex8ka4g8cteQU4NQqtviwmcTjQAYLbzQ1eR"
        
        if toBlock == "latest":
            toBlock = self.w3["polygon"].eth.block_number
        
        if fromBlock is None:
            fromBlock = self.polymarket_v2_Creation_Block
        
        
        # Make the directory if not there
        if not os.path.exists(saveLocation):
            print("Making a new directory for saving the trade data since it does not exist.")
            os.makedirs(saveLocation)
        
        # Get the file to append trades into
        idx = 1
        for file in os.listdir(saveLocation):
            if "polymarket_trades_pt_" in file:
                _idx = int(file.replace("polymarket_trades_pt_","").replace(".parquet", ""))
                if idx < _idx:
                    idx = _idx
        
        
        
        print(idx)

    @__requireWeb3APIkey
    def getAllTrades_RPC(
            self, 
            saveLocation: str, 
            fromBlock: Union[int, None], 
            toBlock: Union[int, None, str],
            blockBatchSize: int = 1000,
            parallelRequests: int = 1,
            saveBlockRange: Union[int, None, str] = None, 
            decodeLogs: bool = True,
            maxFileSize_GB: float = 0.8,
            stopAfter: Union[int, None] = None
        ):
        """
        Using a web3 RPC, gets all trades for polymarket v1 and v2 and saves them into a parquet database.
        Support parallel requests to the RPC for increased speed.
        
        The trades are saved in a parquet file, located at 'saveLocation' directory. the file names have
        the following pattern: polymarket_trades_pt_<xxxx>.parquet
        
        Each file's size is limited by 'maxFileSiz' argument. This choice was made to meet different RAM 
        constraints of different machines
        
        Args:
            saveLocation (str): The directory to save the parquet databases

            stopAfter (int): Stop getting trades after some time (in seconds) 
        """
        
        def _fetchLogs(args: tuple):
            """
            Fetches logs for a polymarket contract

            Args:
                args (tuple): First two indexes indicate start and end of the block range, and the third is for exchangeType
                    blockRange (int, int): The block range to get the data from    
                    exchangeType (str): The market type (Acceptable values: ctf_v1, ctf_v2, negrisk_v1, negrisk_v2)
            """
            startTime = time.time()
            fromBlock, toBlock, exchangeType = args
            fromBlock = int(fromBlock)
            toBlock = int(toBlock)
            
            if exchangeType not in ["ctf_v1", "ctf_v2", "negrisk_v1", "negrisk_v2"]:
                raise ValueError(f"Invalid market type: {exchangeType}. Acceptable values are: ctf_v1, ctf_v2, negrisk_v1, negrisk_v2")
            
            contractAddress = None
            topic0 = None
            if exchangeType == "ctf_v1":
                contractAddress = self.exchange_CFT_v1
                topic0 = self.exchange_CFT_v1_OrderFilled_topic0
            elif exchangeType == "ctf_v2":
                contractAddress = self.exchange_CFT_v2
                topic0 = self.exchange_CFT_v2_OrderFilled_topic0
            elif exchangeType == "negrisk_v1":
                contractAddress = self.exchange_NegRiskCFT_v1
                topic0 = self.exchange_CFT_v1_OrderFilled_topic0
            elif exchangeType == "negrisk_v2":
                contractAddress = self.exchange_NegRiskCFT_v2
                topic0 = self.exchange_CFT_v2_OrderFilled_topic0
            else: 
                raise ValueError(f"Invalid market type: {exchangeType}. Acceptable values are: ctf_v1, ctf_v2, negrisk_v1, negrisk_v2")
            
            try:
                maxRetries = 5
                _delay = 2  # seconds

                for attempt in range(maxRetries):
                    try:
                        # Get logs for polymarket CTF v2 exchange
                        logs = self.w3["polygon"].eth.get_logs({
                            "address": contractAddress,
                            "fromBlock": fromBlock,
                            "toBlock": toBlock,
                            "topics": [topic0]
                        })
                        return logs, exchangeType

                    except Exception as e:
                        is_last_attempt = attempt == maxRetries - 1
                        is_connection_error = any(keyword in str(e).lower() for keyword in [
                            "connection", "network", "timeout", "reset", "eof",
                            "broken pipe", "remote end closed", "unreachable"
                        ])

                        if is_connection_error and not is_last_attempt:
                            delay = _delay * (2 ** attempt)  # Exponential backoff
                            print(f"Connection error on attempt {attempt + 1}: {e}")
                            print(f"Retrying in {delay}s...")
                            time.sleep(delay)
                        else:
                            # Non-connection error, or out of retries
                            print(f"Error fetching range {fromBlock}-{toBlock}: {e}")
                            return [], exchangeType

            except Exception as e:
                print(f"Unexpected error fetching range {fromBlock}-{toBlock}: {e}")
                return [], exchangeType

        def _saveDataToParquet(saveDir: str, idx: int, data) -> None:
            _fileName = os.path.join(saveDir, f"polymarket_trades_pt_{idx:03d}.parquet")
            if not os.path.exists(_fileName):
                # Make an empty parquet file and append to it
                makeEmptyParquetFile(_fileName, self.tradesSchema)
                
            appendToParquet(_fileName, data, self.tradesSchema, True)
                
        # Make the directory if not there
        if not os.path.exists(saveLocation):
            print("Making a new directory for saving the trade data since it does not exist.")
            os.makedirs(saveLocation)
        
        # Get the file to append trades into
        saveFileIDX = 1
        for file in os.listdir(saveLocation):
            if "polymarket_trades_pt_" in file:
                _idx = int(file.replace("polymarket_trades_pt_","").replace(".parquet", ""))
                if saveFileIDX < _idx:
                    saveFileIDX = _idx
        
        if toBlock == "latest":
            toBlock = self.w3["polygon"].eth.block_number
        
        
        # Get the logs
        allLogs = []
        
        retries = 0
        batchStart = fromBlock
        startTime = time.time()
        acquiredBlocks = 0
        while batchStart <= toBlock:
            # Check the save file size - If the file size is higehr than maxFileSize_GB, aim to save in a new file
            if os.path.exists(os.path.join(saveLocation, f"polymarket_trades_pt_{saveFileIDX:03d}.parquet")):
                if maxFileSize_GB < getSizeInGB(os.path.join(saveLocation, f"polymarket_trades_pt_{saveFileIDX:03d}.parquet")):
                    saveFileIDX += 1
            
            if 10 < retries:
                raise Exception("Max retries reached. Aborting")
            
            batchEnd = min(batchStart + blockBatchSize * parallelRequests - 1, toBlock)
            try:
                print(f"Fetching logs from block {batchStart:,} to {batchEnd:,}. Remaining blocks: {toBlock - batchEnd:,} (Fetched {(batchEnd - fromBlock)/(toBlock - fromBlock) * 100:.2f}%)")
                
                if os.getenv("telegramBotToken", None) and os.getenv("chatID"):
                    sendTelegramMessage(
                        os.getenv("telegramBotToken"),
                        os.getenv("chatID"),
                        f"Fetching logs from block {batchStart:,} to {batchEnd:,}. Remaining blocks: {toBlock - batchEnd:,} (Fetched {(batchEnd - fromBlock)/(toBlock - fromBlock) * 100:.2f}%)"
                    )   

                # Make parallel requests for 4 types of polymarket contracts and get their "OrderFilled" events
                logs = {"ctf_v1": [], "ctf_v2": [], "negrisk_v1": [], "negrisk_v2": []}
                _batchStartTime = time.time()
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    _args = [
                        (*(batchStart, batchEnd), "ctf_v1"),
                        (*(batchStart, batchEnd), "ctf_v2"),
                        (*(batchStart, batchEnd), "negrisk_v1"),
                        (*(batchStart, batchEnd), "negrisk_v2"),
                    ]
                    results = executor.map(_fetchLogs, _args)
                    
                    # Aggregate the results into a single list
                    for (_logs, version) in results:
                        logs[version].extend(_logs)

                acquiredBlocks += batchEnd - batchStart
                print(f"Logs count in batch: ctf_v1:{len(logs['ctf_v1'])} | negrisk_v1:{len(logs['negrisk_v1'])} | ctf_v2:{len(logs['ctf_v2'])} | negrisk_v2:{len(logs['negrisk_v2'])}")
                print(f"Fetched {batchEnd - fromBlock + 1} blocks so far\n")
                
                for version in list(logs.keys()):
                    for log in tqdm(logs[version], desc=f"Decoding OrderFilled logs for {version}", leave = False):
                        # Decode the log
                        decoded = {}
                        if decodeLogs:
                            if   version == "ctf_v1":
                                decoded = self._CTF_V1_OrderFilled_fastDecode(log)
                            elif version == "ctf_v2":
                                decoded = self._CTF_V2_OrderFilled_fastDecode(log)
                            elif version == "negrisk_v1":
                                decoded = self._NEGRISK_V1_OrderFilled_fastDecode(log)
                            elif version == "negrisk_v2":
                                decoded = self._NEGRISK_V1_OrderFilled_fastDecode(log)
                        else:
                            if   version == "ctf_v1":
                                topic0 = self.exchange_CFT_v1_OrderFilled_topic0
                            elif version == "ctf_v2":
                                topic0 = self.exchange_CFT_v2_OrderFilled_topic0
                            elif version == "negrisk_v1":
                                topic0 = self.exchange_CFT_v1_OrderFilled_topic0
                            elif version == "negrisk_v2":
                                topic0 = self.exchange_CFT_v2_OrderFilled_topic0
                                
                            decoded = {
                                "log": log,
                                "method": "OrderFilled",
                                "topic0": topic0,
                                "contract": version
                            }
                        
                        # Add block data to the transaction
                        decoded["tx_hash"] = f"0x{log["transactionHash"].hex()}"
                        decoded["block_timestamp"] = int(int(log["blockTimestamp"].replace("0x", ""), 16))
                        decoded["block_number"] = int(log["blockNumber"])
                        
                        # Do the necessary calculations
                        decoded["amount_asset"] = "USDC"
                        decoded["fee"] =  decoded["fee"] / 1e6
                        decoded["fee_asset"] = "USDC"
                        decoded["taker_side"] = "BUY" if decoded["taker_side"] == 0 else "SELL"
                        decoded["maker_side"] = "SELL" if decoded["taker_side"] == 0 else "BUY"
                        
                        amount = None
                        price = None
                        if decoded["taker_side"] == "BUY":
                            # Taker -> buy | Maker -> sell
                            if decoded["maker_amount_filled"] > 0:
                                price = decoded["taker_amount_filled"] / decoded["maker_amount_filled"]
                            else:
                                price = 0
                            
                            amount = decoded["taker_amount_filled"] / 1e6
                        else:
                            # Taker -> sell | Maker -> buy
                            if decoded["taker_amount_filled"] > 0:
                                price = decoded["maker_amount_filled"] / decoded["taker_amount_filled"]
                            else:
                                price = 0
                                
                            amount = decoded["maker_amount_filled"] / 1e6
                            
                        decoded["amount"] = amount
                        decoded["price"] = price
                        decoded["extra_data"] = json.dumps({
                            "negative_risk": True if version == "negrisk_v1" or version == "negrisk_v2" else False,
                            "order_hash": decoded["order_hash"],
                            "maker_amount_filled": decoded["maker_amount_filled"],
                            "taker_amount_filled": decoded["taker_amount_filled"]
                        })
                        
                        if 1 < decoded["price"]:
                            print("unacceptable price value. Price should be between 0 and 1. Check the decoding function for errors.") # DEBUG: DELETE
                            pprint.pprint(decoded)                                                                                      # DEBUG: DELETE
                            exit()                                                                                                      # DEBUG: DELETE
                            raise ValueError("unacceptable price value. Price should be between 0 and 1. Check the decoding function for errors.")
                        allLogs.append(decoded)
                print(f"Gathered {len(allLogs):,} logs so far | size: {getObjectSize(allLogs)} | Cumulative time: {time.time() - startTime:.2f}s | Batch time: {time.time() - _batchStartTime:.2f}s")

                # Save the dataframe as parquet
                if saveBlockRange is not None:
                    if saveBlockRange <= acquiredBlocks:
                        _saveDataToParquet(saveLocation, saveFileIDX, allLogs)

                        # Clean up
                        del allLogs
                        gc.collect()
                        allLogs = []
                        acquiredBlocks = 0
                    else:
                        # Do nothing, keep gathering logs and decoding them
                        pass
                else:
                    _saveDataToParquet(saveLocation, saveFileIDX, allLogs)

                    # Clean up
                    del allLogs
                    gc.collect()
                    allLogs = []
                    acquiredBlocks = 0
                
                batchStart += blockBatchSize * parallelRequests
                
                # Reset the retries after a successful fetch
                retries = 0 

                # Stop after required timespan
                if stopAfter and stopAfter < time.time() - startTime:
                    return
            except Exception as e:
                print(f"Failed to fetch blocks from {batchStart} to {batchEnd}. Error: {e}")
                
                time.sleep(1)
                retries += 1


    def addPricesToMarketData(self, marketDataPath: str, tradesDataPath: str, saveLocation: str, stopAfter: int = None, marketBatchSize: int = 200) -> None:
        """
        Adds the price data to the market data. Adds two columns to the market,
        outcome_0_history_price and outcome_1_history_price. The price data is fetched from
        the trades data. The trades data is expected to be a parquet file containing
        the following columns: marketID, price, outcome, timestamp. The market
        data is expected to be a parquet file with the following columns: marketID,
        outcome_0_ID, outcome_1_ID. The function will match the marketID and
        outcome_IDs to get the latest price for each outcome and add them to
        the market data.
    
        Args:
            marketDataPath (str): The path to the market data parquet file.
            tradesDataPath (str): The path to the trades data parquet file.
            saveLocation (str): The path to save the new market data parquet file with prices added
            stopAfter (int, optional): The number of seconds after which to stop processing. Defaults to None.
            marketBatchSize (int, optional): The number of markets to process at a time within
                a single file-group. Keeps memory usage bounded by only loading trades for a
                small batch of markets' token_ids at once, rather than the whole group. Defaults to 200.
        """
        MAX_FILE_SIZE_MB = 2000  # In MB
        _toMB = lambda x: x / 1024 / 1024
    
        # First, get the min/max timestamp of each parquet file so we could avoid searching the entire directory (Its faster)
        print("Mapping the timestamps to files")
        files = sorted(
            os.listdir(tradesDataPath),
            key=lambda x: int(x.replace("polymarket_trades_pt_", "").replace(".parquet", ""))
        )
        filesDateRange = {}
        for file in tqdm(files, total=len(files)):
            minTS = queryParquetFile(os.path.join(tradesDataPath, file), "SELECT MIN(block_timestamp) FROM data").iloc[0, 0]
            maxTS = queryParquetFile(os.path.join(tradesDataPath, file), "SELECT MAX(block_timestamp) FROM data").iloc[0, 0]
            filesDateRange[file] = [minTS, maxTS]
    
        # Get a set of all markets
        allMarkets = pd.DataFrame(queryParquetFile(marketDataPath, "SELECT * FROM data"))
        allMarketIDs = set(allMarkets["marketID"].tolist())
    
        # Get a record of all previously processed markets
        acquiredMarketIDs = pd.DataFrame(queryParquetFolder(saveLocation, "SELECT marketID FROM data"))
        dfToSave, processedMarkets, saveFileIdx = None, None, None
        if acquiredMarketIDs is None or acquiredMarketIDs.empty:
            processedMarkets = set()
            dfToSave = pd.DataFrame(columns=allMarkets.columns.tolist() + ["outcome_0_history_price", "outcome_0_history_price_ts", "outcome_1_history_price", "outcome_1_history_price_ts", "has_price_history"])
            saveFileIdx = 1
        else:
            processedMarkets = set(acquiredMarketIDs["marketID"].tolist())
    
            latestPart = sorted(
                [f for f in os.listdir(saveLocation) if f.startswith("polymarket_markets_with_prices_pt_") and f.endswith(".parquet")],
                key=lambda x: int(x.replace("polymarket_markets_with_prices_pt_", "").replace(".parquet", ""))
            )[-1]
    
            dfToSave = pd.DataFrame(queryParquetFile(os.path.join(saveLocation, latestPart), "SELECT * FROM data"))
            if dfToSave is not None:
                if MAX_FILE_SIZE_MB < _toMB(dfToSave.memory_usage(deep=True).sum()):
                    dfToSave = pd.DataFrame(columns=allMarkets.columns.tolist() + ["outcome_0_history_price", "outcome_0_history_price_ts", "outcome_1_history_price", "outcome_1_history_price_ts", "has_price_history"])
                    saveFileIdx = int(latestPart.replace("polymarket_markets_with_prices_pt_", "").replace(".parquet", "")) + 1
                else:
                    saveFileIdx = int(latestPart.replace("polymarket_markets_with_prices_pt_", "").replace(".parquet", ""))
            else:
                dfToSave = pd.DataFrame(columns=allMarkets.columns.tolist() + ["outcome_0_history_price", "outcome_0_history_price_ts", "outcome_1_history_price", "outcome_1_history_price_ts", "has_price_history"])
    
        remainingMarkets = allMarketIDs - processedMarkets
        RemainingMarketsDF = allMarkets[allMarkets["marketID"].isin(remainingMarkets)]
        print("Processed markets count:", len(processedMarkets))
        print("Remaining markets count:", len(remainingMarkets))
    
        startTime = time.time()
        rows = dfToSave.to_dict("records")
    
        runningSize = getObjectSizeInBytes(rows) if rows else 0
    
    
        # Precompute market start/end timestamps and file groups without hitting disk.
        # This grouping is cached to disk since it's a pure function of allMarkets +
        # filesDateRange and can be expensive to recompute for large market sets.
        groupsCacheFile = os.path.join(saveLocation, "_market_groups_cache.pkl")
    
        if os.path.exists(groupsCacheFile):
            print(f"Found cached market groups at {groupsCacheFile}, loading instead of recomputing")
            with open(groupsCacheFile, "rb") as f:
                cache = pickle.load(f)
            marketMeta = cache["marketMeta"]
            groups = cache["groups"]
        else:
            marketMeta = {}  # marketID -> dict of market row fields
            groups = defaultdict(list)  # frozenset(filesToSearch) -> list of marketIDs
    
            for market in tqdm(RemainingMarketsDF.itertuples(index=False), total=len(RemainingMarketsDF), desc="Grouping markets"):
                marketID = market.marketID
                if marketID in processedMarkets:
                    continue
    
                marketStartTS = datetime.datetime.fromisoformat(market.startDate).timestamp() if market.startDate is not None else 0
                marketEndTS = datetime.datetime.fromisoformat(market.endDate).timestamp() if market.endDate is not None else 9999999999
    
                filesToSearch = set()
                for file, (beginTS, endTS) in filesDateRange.items():
                    if beginTS <= marketStartTS <= endTS or beginTS <= marketEndTS <= endTS:
                        filesToSearch.add(file)
    
                marketMeta[marketID] = market._asdict()
                groups[frozenset(filesToSearch)].append(marketID)
    
            groups = dict(groups)
            os.makedirs(saveLocation, exist_ok=True)
            with open(groupsCacheFile, "wb") as f:
                pickle.dump({"marketMeta": marketMeta, "groups": groups}, f)
            print(f"Cached market groups to {groupsCacheFile}")
    
        filteredGroups = {}
        skippedAlreadyProcessed = 0
        for fileset, marketIDs in groups.items():
            remainingIDs = [m for m in marketIDs if m not in processedMarkets]
            skippedAlreadyProcessed += len(marketIDs) - len(remainingIDs)
            if remainingIDs:
                filteredGroups[fileset] = remainingIDs
        groups = filteredGroups
    
        if skippedAlreadyProcessed:
            print(f"Skipping {skippedAlreadyProcessed} markets already processed in a previous run")
    
        totalRemainingInGroups = sum(len(v) for v in groups.values())
    
        pBar = tqdm(total=totalRemainingInGroups, desc="Processing markets")
        stopped = False
        
        n = len(groups)
        for i, (filesToSearch, marketIDs) in enumerate(groups.items()):
            isLast = (i == n - 1)

            # No trades files match this group at all -> every market here has no price history
            if len(filesToSearch) == 0:
                for marketID in marketIDs:
                    newRow = dict(marketMeta[marketID])
                    newRow["outcome_0_history_price"] = []
                    newRow["outcome_0_history_price_ts"] = []
                    newRow["outcome_1_history_price"] = []
                    newRow["outcome_1_history_price_ts"] = []
                    newRow["has_price_history"] = False
    
                    rows.append(newRow)
                    runningSize += getObjectSizeInBytes(newRow)
                    pBar.update(1)
                continue
    
            for batchStart in range(0, len(marketIDs), marketBatchSize):
                batchMarketIDs = marketIDs[batchStart: batchStart + marketBatchSize]
    
                # Collect every outcome token_id needed by markets in this batch
                neededTokenIDs = set()
                for marketID in batchMarketIDs:
                    m = marketMeta[marketID]
                    neededTokenIDs.add(m["outcome_0_ID"])
                    neededTokenIDs.add(m["outcome_1_ID"])
    
                # Single query per batch, for all token_ids in the batch, across the group's files
                tokenIDList = "', '".join(str(t) for t in neededTokenIDs)
                groupTrades = pd.DataFrame(queryParquetFile(
                    [os.path.join(tradesDataPath, f) for f in filesToSearch],
                    f"SELECT * FROM data WHERE token_id IN ('{tokenIDList}')"
                ))
    
                # Build an O(1) lookup: token_id -> DataFrame of its trades
                if groupTrades.shape[0] == 0:
                    tradesByToken = {}
                else:
                    tradesByToken = {tok: df for tok, df in groupTrades.groupby("token_id")}
    
                # Free the raw trades DataFrame as soon as we've split it up
                del groupTrades
    
                for marketID in batchMarketIDs:
                    market = marketMeta[marketID]
                    newRow = dict(market)
    
                    outcome_0_trades = tradesByToken.get(market["outcome_0_ID"])
                    outcome_1_trades = tradesByToken.get(market["outcome_1_ID"])
    
                    if outcome_0_trades is None or outcome_0_trades.shape[0] == 0:
                        newRow["outcome_0_history_price"] = []
                        newRow["outcome_0_history_price_ts"] = []
                        newRow["has_price_history"] = False
                    else:
                        newRow["outcome_0_history_price"] = outcome_0_trades["price"].tolist()
                        newRow["outcome_0_history_price_ts"] = outcome_0_trades["block_timestamp"].tolist()
                        newRow["has_price_history"] = True
    
                    if outcome_1_trades is None or outcome_1_trades.shape[0] == 0:
                        newRow["outcome_1_history_price"] = []
                        newRow["outcome_1_history_price_ts"] = []
                        newRow["has_price_history"] = newRow.get("has_price_history", False)
                    else:
                        newRow["outcome_1_history_price"] = outcome_1_trades["price"].tolist()
                        newRow["outcome_1_history_price_ts"] = outcome_1_trades["block_timestamp"].tolist()
                        newRow["has_price_history"] = True
    
                    rows.append(newRow)
                    runningSize += getObjectSizeInBytes(newRow)
    
                    # Flush to disk once the accumulated size crosses the threshold
                    if MAX_FILE_SIZE_MB < _toMB(runningSize) or isLast:
                        dfToSave = pd.DataFrame(rows)
    
                        outFile = os.path.join(saveLocation, f"polymarket_markets_with_prices_pt_{saveFileIdx:03d}.parquet")
                        if os.path.exists(outFile):
                            appendToParquet(outFile, dfToSave.to_dict("records"), self.marketsSchemaWithPrice, True)
                        else:
                            makeEmptyParquetFile(outFile, self.marketsSchemaWithPrice)
                            appendToParquet(outFile, dfToSave.to_dict("records"), self.marketsSchemaWithPrice, True)
    
                        saveFileIdx += 1
                        rows = []
                        runningSize = 0
                        del dfToSave
    
                    pBar.set_postfix(mem=f"{_toMB(runningSize):.4f} MB")
                    pBar.update(1)
    
                    if len(rows) % 100 == 0:
                        if os.getenv("telegramBotToken", None) and os.getenv("chatID"):
                            sendTelegramMessage(
                                os.getenv("telegramBotToken"),
                                os.getenv("chatID"),
                                f"Rows acquired: {len(rows)} | Dataframe size: {_toMB(runningSize)}"
                            )
    
                    if stopAfter and stopAfter < time.time() - startTime:
                        print(f"Stopping after {stopAfter} seconds as requested.")
                        stopped = True
                        break
    
                # Free per-batch lookup dict before moving to the next batch
                del tradesByToken
    
                if stopped:
                    break
    
            if stopped:
                break
    
        pBar.close()
        exit()