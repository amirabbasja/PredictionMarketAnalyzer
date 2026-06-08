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
from functools import wraps
import time

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
        self.w3 = None
        
        if not self.polymarketAPIkey:
            print("Warning: No Polymarket API key provided.")
        
        if not self.web3APIkey:
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
        self.exchange_CFT_v2 = "0xE111180000d2663C0091e4f400237545B87B996B"
        self.exchange_NegRiskCFT_v2 = "0xe2222d279d744050d28e00520010520000310F59"
        self.exchange_CFT_v1 = "0" # TODO
        self.exchange_NegRiskCFT_v1 = "0" # TODO
        
        # Polymarket logs
        self.exchange_CFT_v2_OrderFilled_topic0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
        
        # Polymarket contracts
        self.contract_CFT_exchange_v2 = self.w3["polygon"].eth.contract(address = self.exchange_CFT_v2, abi = loadABI("polygon", "polymarket_exchange_CFT_v2"))
        self.contract_Neg_Risk_CFT_exchange_v2 = self.w3["polygon"].eth.contract(address = self.exchange_NegRiskCFT_v2, abi = loadABI("polygon", "polymarket_exchange_neg_risk_CFT"))

    def __requireWeb3APIkey(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.web3APIkey:
                raise ValueError("This method requires a Web3 API key. Please provide one during initialization.")
            return func(self, *args, **kwargs)
        return wrapper

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
        JSONL file in the src/data/ directory. It is not able to hold all market data 
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
        
        if "saveFile" in kwargs and not kwargs["saveFile"].endswith(".jsonl"):
            if not os.path.isfile(kwargs["saveFile"]):
                makeEmptyJSONLFile(kwargs["saveFile"], compressed = kwargs["saveFile"].endswith(".gz"))
        
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
            print(f"Fetching next page of markets... (Page {counter + 2}) | Total events fetched so far: {allEventCounter}")
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
                
                # We do this so the script opens the file only once and appends to it, instead of 
                # opening and closing the file for each event which would be very inefficient.
                appendToJSONL(kwargs["saveFile"], processedList)
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
            returnData["marketID"] = data.get("id", "")
            returnData["archived"] = data.get("archived", None)
            returnData["closed"] = data.get("closed", None)
            returnData["createdAt"] = _createdAt.isoformat() if _createdAt is not None else None
            returnData["startDate"] = _startDate.isoformat() if _startDate is not None else None
            returnData["endDate"] = _endDate.isoformat() if _endDate is not None else None
            returnData["daysTillExpiry"] =  _diffDays
            returnData["hoursTillExpiry"] = _diffHours
            returnData["description"] = data.get("description", "").replace('\n', ' ').replace('\r', ' ') if data.get("description", None) is not None else None # Deleted due to storage issues
            returnData["slug"] = data.get("slug", None)
            returnData["spread"] = data.get("spread", None)
            returnData["takerBaseFee"] = data.get("takerBaseFee", None)
            returnData["makerBaseFee"] = data.get("makerBaseFee", None)
            returnData["liquidity"] = data.get("liquidity", None)
            returnData["liquidityNum"] = data.get("liquidityNum", None)
            returnData["volumeNum"] = data.get("volume", None)
            returnData["volume1yr"] = data.get("volume1yr", None)
            returnData["volumeAmm"] = data.get("volumeAmm", None)
            returnData["volumeClob"] = data.get("volumeClob", None)
            
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
            
            if getPriceData and returnData.get("outcome_0_ID", None) is not None and returnData.get("outcome_1_ID", None) is not None:
                priceHistory = self.getPriceHistory_sync(returnData["marketID"], (returnData["outcome_0_ID"], returnData["outcome_1_ID"]), interval="all")
                returnData["priceHistory"] = priceHistory
            
            return returnData

        if "saveFile" not in kwargs:
            raise ValueError("For now, you must specify saveFile so that the data is saved to disk instead of held in memory. This is because the amount of data is too large to hold in memory.")
        
        if "saveFile" in kwargs and not kwargs["saveFile"].endswith(".jsonl"):
            if not os.path.isfile(kwargs["saveFile"]):
                makeEmptyJSONLFile(kwargs["saveFile"], compressed = kwargs["saveFile"].endswith(".gz"))
            
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
                
                # Save the progress in a file
                saveProgress(kwargs.get("saveFile"), {
                    "next_cursor": jsonResponse.get("next_cursor"),
                    "eventCount": allEventCounter,
                    "timestamp": int(datetime.datetime.now().timestamp())
                })

                if not isinstance(jsonResponse, dict):
                    print(f"Unexpected JSON type: {type(jsonResponse)}")
                    break
                
                processedList = []
                for event in tqdm(jsonResponse.get("markets", []), desc="Processing markets"):
                    processedData = _processData(event)
                    
                    # For consistency between all rows
                    processedData["next_cursor"] = "" 
                    
                    processedList.append(processedData)
                
                
                # We do this so the script opens the file only once and appends to it, instead of 
                # opening and closing the file for each event which would be very inefficient.
                appendToJSONL(kwargs["saveFile"], processedList)

                print(f"   {_params['order']}: {jsonResponse.get('markets', [])[-1].get(_params['order'], None)} -> {jsonResponse.get('markets', [])[0].get(_params['order'], None)} | ")
                    
                if jsonResponse.get("next_cursor", None) is None:
                    break
                else:
                    # Add the next cursor to the last item of the batch so that if process ran into 
                    # errors, we could pickup where we left off
                    processedList[-1]["next_cursor"] = jsonResponse["next_cursor"]
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

    @__requireWeb3APIkey
    def getAllTrades(
            self, 
            saveLocation: str, 
            fromBlock: Union[int, None], 
            toBlock: Union[int, None, str],
            blockBatchSize: int = 1000,
            parallelbatches: int = 1
        ):
        """
        Using a web3 RPC, gets all trades for polymarket v1 and v2 and saves them into a parquet database.
        
        saveLocation (str): The directory to save the parquet databases
        """
        
        CTF_V2_DATA_TYPES = ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"]
        CTF_V2_DATA_NAMES = ["side", "tokenId", "makerAmountFilled", "takerAmountFilled", "fee", "builder", "metadata"]

        def _CTF_V2_fastDecode(log):
            """
            For decoding the CTF exchange (V2) events more quickly.
            """
            # Decode non-indexed fields from data
            # log["data"] is already bytes in web3.py — no need for .hex() / fromhex()
            raw = bytes(log["data"])
            decoded_data = decode(CTF_V2_DATA_TYPES, raw)
            result = dict(zip(CTF_V2_DATA_NAMES, decoded_data))

            # Decode indexed fields from topics[1], topics[2], topics[3]
            # topics[0] is the event signature hash
            result["orderHash"] = log["topics"][1]           # already bytes32
            result["maker"]     = "0x" + log["topics"][2].hex()[-40:]  # last 20 bytes → address
            result["taker"]     = "0x" + log["topics"][3].hex()[-40:]  # last 20 bytes → address
            
            returnDict = {
                "tokenId": str(result["tokenId"]),
                "side": result["side"],
                "makerAmountFilled": result["makerAmountFilled"],
                "takerAmountFilled": result["takerAmountFilled"],
                "orderHash": result["orderHash"].hex(),
                "taker": result["taker"],
                "maker": result["maker"],
            }

            return returnDict
        
        def fetchLogs(blockRange):
            fromBlock, toBlock = blockRange
            try:
                logs = self.w3["polygon"].eth.get_logs({
                    "address": self.exchange_CFT_v2,
                    "fromBlock": fromBlock,
                    "toBlock": toBlock,
                    "topics": [self.exchange_CFT_v2_OrderFilled_topic0]
                })
                print("Got", len(logs), "logs")
                return logs
            except Exception as e:
                print(f"Error fetching range {fromBlock}-{toBlock}: {e}")
                return []

        # Make the directory if not there
        if not os.path.exists(saveLocation):
            print("Making a new directory for saving the trade data since it does not exist.")
            os.makedirs(saveLocation)

        # TODO: Check for existing trades
        
        if toBlock == "latest":
            toBlock = self.w3["polygon"].eth.block_number
        
        # Get the logs
        allLogs = []
        
        _retries = 0
        currentTo = toBlock
        currentFrom = max(fromBlock, currentTo - blockBatchSize * ) if fromBlock is not None else 0
        while fromBlock <= currentTo:
            if 10 < _retries:
                raise Exception("Max retries reached. Aborting")
            
            try:
                print(f"Fetching logs from block {currentFrom} to {currentTo}. Remaining blocks: {currentTo - fromBlock} ({(currentTo - fromBlock)/(toBlock - fromBlock) * 100:.2f}%)")

                # logs = self.w3["polygon"].eth.get_logs({
                #     "address": self.exchange_CFT_v2,
                #     "fromBlock": currentFrom,
                #     "toBlock": currentTo,
                #     "topics": [self.exchange_CFT_v2_OrderFilled_topic0]
                # })
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    blockRanges = []
                    for i in range(currentFrom, currentTo + 1, blockBatchSize):
                        start = i
                        end = min(i + blockBatchSize - 1, currentTo)
                        blockRanges.append((start, end))
                    results = executor.map(fetchLogs, )
                    
                    # 4. Aggregate the results into a single list
                    for logs in results:
                        logs.extend(logs)
                
                print("Decoding logs...")
                for log in tqdm(logs, desc="Decoding OrderFilled logs"):
                    # Decode the log
                    decoded = _CTF_V2_fastDecode(log)
                    
                    # Add block data to the transaction
                    decoded["blockTimestamp"] = str(int(log["blockTimestamp"].replace("0x", ""), 16))
                    decoded["blockNumber"] = str(log["blockNumber"])
                    
                    allLogs.append(decoded)
                
                # Save the dataframe as parquet
                tmpDF = pd.DataFrame(allLogs)
                tmpDF.to_parquet(os.path.join(saveLocation, f"trades_{currentFrom}_{currentTo}.parquet"), index=False)
                print("df size", tmpDF.memory_usage(deep=True).sum() / (1024**2), "MB")
                exit()
                
                currentTo -= blockBatchSize
                currentFrom = max(fromBlock, currentTo - blockBatchSize) if fromBlock is not None else 0
                
                # Reset the retries after a successful fetch
                _retries = 0 
            except Exception as e:
                print(f"Failed to fetch blocks from {currentFrom} to {currentTo}. Error: {e}")
                
                time.sleep(1)
                _retries += 1

