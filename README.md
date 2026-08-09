# Prediction markets suite

A repository for interacting with various prediction markets.

## Necessary commands

Use *polymarket.py* to gather data and interact with gathered data in polymarket. Following commands are acceptable:

* Get all live event' data: 
* Get all live markets' data: `python -m scripts.polymarket --getAllMarkets`
* Get all live markets' data with price their data: `python -m scripts.polymarket --getAllMarkets --price`
* Convert the compressed `jsonl.gz` file into a csv file (Expect a 6~10 times increase in file size!): ``

The directory `./scripts` contains necessary commands for each market. Each market's commands reside inside a separate file.

* Polymarket
    * Get all live events:  
    **Command**: `python -m scripts.polymarket --getAllEvents`  
    **Description**: Gets all live events in polymarket and saves them into a parquet file loacted at `./src/data/polymarket/historical_markets` in a file named `polymarket_liveEvents_<Timestamp>.parquet` (JSONL or JSONL.GZ files are acceptable but parquet is much suggested).. To change the save file location, alter the main script file.

    * Get all live markets:  
    **Command**: `python -m scripts.polymarket --getLiveMarkets [--price]`  
    **Description**: Gets all live markets in polymarket and saves them into a parquet file loacted at `./src/data/polymarket/historical_markets` in a file named `polymarket_liveMarkets_<Timestamp>.parquet` (JSONL or JSONL.GZ files are acceptable but parquet is much suggested). To change the save file location, alter the main script file. Pass a *--price* flag so each market's price data is retreived as well. Please note that this method uses polymarket's API for getting the data and might make the data acquizition very slow; furthermore, polymarket's API does not provide the compelte price data for most markets.

    * Get historical markets:  
    **Command**: `python -m scripts.polymarket --getHistoricalMarkets [--price] [--continue <file_location>]`  
    **Description**: Gets all historical markets in polymarket and saves them into a parquet file loacted at `./src/data/polymarket/historical_markets` in a file named `polymarket_HistoricalMarkets.parquet`. To change the save file location, alter the main script file. Pass a *--price* flag so each market's price data is retreived as well. Please note that this method uses polymarket's API for getting the data and might make the data acquizition very slow; furthermore, polymarket's API does not provide the compelte price data for most markets. Pass a *--continue* followed by an existing parquet file location to pick up where it letf off (JSONL or JSONL.GZ files are acceptable but parquet is much suggested).

    * Get price history:  
    **Command**: `python -m scripts.polymarket --getPriceHistory <market_ID> <outcome_0_token_ID> <outcome_0_token_ID>`  
    **Description**: Gets price history for a market by fetching the price of its outcomes. Becareful that this method makes synchronous requests for both outcome, effectively doubling the fetch time. Prints a price history dictionary on the console screen.

    * Get all arades:  
    **Command**: `python -m scripts.polymarket --getAllTrades [--stopAfter <Seconds>]`  
    **Description**: Gets all polymarket trades using an RPC and saves them in `./src/data/polymarket/trades`. Alchemy RPC is preferred. Polymarket has v1 and v2. We have used `TimeSeventeen/Polymarket-v1` huggingface repository to get V1 trades. Use parts 3, 4 and, 5 in `./tests/polyamrket.py` to get the data and alter them to be uniform with the v2 data. After that, you can use this command to get the rest of the v2 trades. Pass a *--stopAfter* flag to get stop the downloading process after a designated timespan. You can further customize the downloading process by changing constants such as `toBlock`, `blockBatchSize`, `maxFileSize_GB` and, `saveBlockRange` in the `scripts.polymarket` file (*--getAllTrades* part)

    * Add prices to markets:  
    **Command**: `python -m scripts.polymarket --addPricesToMarketData --saveLocation <save_location> --marketData <market_data_file> --tradesLocation <directory_for_trades>[--stopAfter <Seconds>]`  
    **Description**: Having a file containg all markets, and having all trades for polymarket, adds `outcome_0_history_price`, `outcome_0_history_price_ts`, `outcome_1_history_price`, `outcome_1_history_price_ts`, `has_price_history` columns to each row and saves it in a files of designated size. It is important to note that this method has been chosen to accelerate the backtesting process (because we need to check the ENTIRE trades for getting price data of a single market). The new file(s) will be saved in the passed `save_location`
    
    * Change files to CSV:  
    **Command**: `python -m scripts.polymarket --toCsv <File_Location>`  
    **Description**: Converts a `.jsonl.gz` or `.parquet` file to `CSV` format.

## TODO

* The polymarket v1 trades seem incompelte. There are some markets that despite their high volume, no trades regarding them are present in the database. Fix it.
* When plotting the probability of a market against time, add a "mirror" option (maybe change the name), accounting for the change in price of the opposing token when a trade happens in one token (A trad efor YES token, changes its price, which consequently changes NO's price as well)