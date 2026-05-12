from pprint import pprint
from urllib.parse import urljoin
from src.utils.utils import sendRequest_Sync, sendRequest_Async, makeEmptyJSONLFile, appendToJSONL, readJSONL
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
import time, json, pprint, gzip
from datetime import datetime
import pickle, os, datetime

class PolymarketHandler:
    def __init__(self, apiKey: str):
        """
        Initializes the PolymarketHandler with the provided API key.
        
        Documentation: https://docs.polymarket.com/api-reference/rate-limits
        
        Args:
            apiKey (str): The API key for authenticating with the Polymarket API.
        """
        
        self.apiKey = apiKey
        
        # Base URLs
        # Gamma - Markets, events, tags, series, comments, sports, search, and public profiles
        self.baseURL_Gamma = "https://gamma-api.polymarket.com"
        
        # Data - User positions, trades, activity, holder data, open interest, leaderboards, and builder analytics.
        self.baseURL_Data = "https://data-api.polymarket.com"
        
        # Data - Orderbook data, pricing, midpoints, spreads, and price history. Also handles order placement,
        # cancellation, and other trading operations. Trading endpoints require authentication.
        self.baseURL_CLOB = "https://clob.polymarket.com"

    def getAllEvents(self, active: bool = True, archived: bool = False, closed : bool = False, getMarkets:bool = True, **kwargs):
        """
        Fetches all events from the Polymarket API. By default saves the data to a 
        JSONL file in the src/data/ directory. It is not able to hold all market data 
        due to memory constraints.
        """
        
        # Function to process the raw data
        def _processData(data: Dict[str, Any]) -> Dict[str, Any]:
            # pprint.pprint(data)
            # exit()
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
            returnData["description"] = data.get("description", None) # Deleted due to storage issues
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
                            "description": market.get("description", None), # Deleted due to storage issues
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

    def getAllMarkets(self, active: bool = True, archived: bool = False, closed : bool = False, getEvents:bool = False, **kwargs):
        """
        Fetches all events from the Polymarket API. By default saves the data to a 
        JSONL file in the src/data/ directory. It is not able to hold all market data 
        due to memory constraints.
        """
        
        # Function to process the raw data
        def _processData(data: Dict[str, Any]) -> Dict[str, Any]:
            # For processing and normalizing the raw data from the API.
            returnData = {}
            
            # Processing
            _createdAt = datetime.datetime.strptime(data.get("createdAt", None).split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S")  if data.get("createdAt", None) is not None else None
            _startDate = datetime.datetime.strptime(data.get("startDate", None).split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("startDate", None) is not None else None
            _endDate = datetime.datetime.strptime(data.get("endDate", None).split('.')[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S") if data.get("endDate", None) is not None else None
            _diff = (_endDate - _createdAt).days if _endDate is not None and _createdAt is not None else None
            
            # Event-level data
            returnData["active"] = data.get("active", None)
            returnData["marketID"] = data.get("id", None)
            returnData["archived"] = data.get("archived", None)
            returnData["closed"] = data.get("closed", None)
            returnData["createdAt"] = _createdAt.isoformat() if _createdAt is not None else None
            returnData["startDate"] = _startDate.isoformat() if _startDate is not None else None
            returnData["endDate"] = _endDate.isoformat() if _endDate is not None else None
            returnData["daysTillExpiry"] =  _diff
            returnData["liquidity"] = data.get("liquidity", None)
            returnData["description"] = data.get("description", None) # Deleted due to storage issues
            returnData["slug"] = data.get("slug", None)
            returnData["createdAt"] = data.get("createdAt", None)
            returnData["spread"] = data.get("spread", None)
            
            # Market-level data
            returnData["eventsCount"] = len(data.get("events", [])) if "events" in data and isinstance(data["events"], list) else 0
            
            # Disregard the Events for now
            returnData["events"] = None
            
            # We take that each market can either be yes or no (As of coding date)
            outcomes = json.loads(data.get("outcomes", None)) if data.get("outcomes", None) is not None else None
            outcomePrices = json.loads(data.get("outcomePrices", None)) if data.get("outcomePrices", None) is not None else None

            if isinstance(outcomes, list) and len(outcomes) == 2:
                returnData["outcome_0_price"] = float(outcomePrices[0])
                returnData["outcome_0_return"] = round((1- returnData["outcome_0_price"]) / returnData["outcome_0_price"] * 100, 2) if returnData["outcome_0_price"] is not None and returnData["outcome_0_price"] != 0 else None
                returnData["outcome_1_price"] = float(outcomePrices[1])
                returnData["outcome_1_return"] = round((1 - returnData["outcome_1_price"]) / returnData["outcome_1_price"] * 100, 2) if returnData["outcome_1_price"] is not None and returnData["outcome_1_price"] != 0 else None
            
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
        allEventCounter = 0
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

                if not isinstance(jsonResponse, dict):
                    print(f"Unexpected JSON type: {type(jsonResponse)}")
                    break
                
                processedList = []
                for event in jsonResponse.get("markets", []):
                    processedData = _processData(event)
                    processedList.append(processedData)
                
                # We do this so the script opens the file only once and appends to it, instead of 
                # opening and closing the file for each event which would be very inefficient.
                appendToJSONL(kwargs["saveFile"], processedList)
                print(f"Liquidity: {jsonResponse.get('markets', [])[0].get('id', None)} -> {jsonResponse.get('markets', [])[-1].get('id', None)}")
                    
                if jsonResponse.get("next_cursor", None) is None:
                    break
            except Exception as e:
                raise e
                print(f"JSON parsing failed: {repr(e)}")
                print(jsonResponse["markets"][0])
                print(f"Status code: {res.status_code}")
                print(f"Headers: {res.headers}")
                print(f"Raw response text: {res.text[:1000]}")
                break

        return None