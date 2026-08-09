# Polymarket Test
from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv
import copy
import requests
from datetime import datetime
from huggingface_hub import list_repo_files, hf_hub_download, login
import pprint
import duckdb
from tqdm import tqdm

import os

# Load environment variables
load_dotenv()

# Make a PolymarketHandler instance
polymarketHandler = PolymarketHandler(
    polymarketAPIkey = os.getenv("polymarketAPI_key"),
    web3APIkey = os.getenv("alchemyAPI_key"),
    provider = "alchemy"
)

# dbConfig = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "polymarket",
#     "user": "postgres",
#     "password": "1234"
# }

# # 1. Getting the details of a pstgresql database table, that is being filled by GOldsky
# try:
#     def executeQuery(cursor, sql, params):
#         cursor.execute(sql, params)
#         _rows = cursor.fetchall()
#         return _rows

#     print("Syncing polymarket trades from Goldsky...")
    
#     conn = psycopg2.connect(**dbConfig)
#     cur = conn.cursor()
    
#     rows = executeQuery(cur, "SELECT pg_size_pretty(pg_database_size(current_database()))", None)
#     dbSize = rows[0][0]
    
#     rows = executeQuery(cur, "SELECT COUNT(*) FROM public.polymarket_order_filled", None)
#     rowCount = rows[0][0]
    
#     print("Total database size:", dbSize)
#     print("Total number of records:", f"{rowCount:,}")
    
#     # Get minimum and max block 
#     rows = executeQuery(cur, "SELECT MIN(block_number), MAX(block_number) FROM public.polymarket_order_filled", None)
#     minBlock, maxBlock = rows[0]
#     minBlockTimestamp = executeQuery(cur, f"SELECT block_timestamp FROM public.polymarket_order_filled WHERE block_number = {minBlock} LIMIT 1;", None)[0][0]
#     maxBlockTimestamp = executeQuery(cur, f"SELECT block_timestamp FROM public.polymarket_order_filled WHERE block_number = {maxBlock} LIMIT 1;", None)[0][0]
#     print("Block range:", f"{minBlock:,} -> {maxBlock:,} ({maxBlock - minBlock:,} blocks) ")
#     print("Start date: ", datetime.fromtimestamp(minBlockTimestamp))
#     print("Latest date: ", datetime.fromtimestamp(maxBlockTimestamp))
#     print("Latest block number:", f"{polymarketHandler.w3["polygon"].eth.block_number:,} ({polymarketHandler.w3["polygon"].eth.block_number - maxBlock:,} blocks to sync)")
    
#     cur.close()
# except Exception as e:
#     print("Error ", e)



# # 2. Getting a subgraph's data
# APIGraph = "a03be5e9f766c0de3ca0441519f1d9ca"
# graphBaseURL = "https://gateway.thegraph.com/api"
# subgraphID = "EZCTgSzLPuBSqQcuR3ifeiKHKBnpjHSNbYpty8Mnjm9D"

# query = """
# {
#     orderFilleds(first: 5) {
#     id
#     orderHash
#     blockTimestamp
#     blockNumber
#     maker
#     taker
#     }
# }
# """
# result = sendRequest_Sync(
#     "https://gateway.thegraph.com/api/subgraphs/id/EZCTgSzLPuBSqQcuR3ifeiKHKBnpjHSNbYpty8Mnjm9D",
#     "POST",
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization":  f"Bearer {APIGraph}"
#     },
#     payload = {"query": query}
# )
# print(result.json())




# # 3. Hugging face
# login(token="")
# files = list_repo_files("TimeSeventeen/Polymarket-v1", repo_type="dataset")
# for file in files:
#     if not "daily_aligned" in file: continue
#     print(f"Downloading {file} ...")
#     file_path = hf_hub_download(
#         repo_id="TimeSeventeen/Polymarket-v1",
#         filename=file,  # Replace with the exact file name or path
#         repo_type="dataset",
#         local_dir="./src/data/polymarketTrades"
#     )
# # pprint.pprint(files)
# print("Downloaded to: ", file_path)



### 4. Chunking a parquet folder into files of specific size
# import argparse
# import os
 
# import pyarrow as pa
# import pyarrow.dataset as ds
# import pyarrow.parquet as pq
 
# def batch_parquet_files(input_dir, output_dir, target_gb=1.0, rows_per_chunk=200_000):
#     target_bytes = int(target_gb * 1024**3)
#     os.makedirs(output_dir, exist_ok=True)
 
#     dataset = ds.dataset(input_dir, format="parquet")
#     schema = dataset.schema
 
#     batch_num = 1
#     out_path = os.path.join(output_dir, f"polymarket_trades_pt_{batch_num:03d}.parquet")
#     writer = pq.ParquetWriter(out_path, schema)
#     print(f"Writing {out_path} ...")
 
#     total_rows = 0
 
#     # Stream the combined dataset in row-batches so we never load
#     # everything into memory at once.
#     for record_batch in dataset.to_batches(batch_size=rows_per_chunk):
#         table = pa.Table.from_batches([record_batch], schema=schema)
#         writer.write_table(table)
#         total_rows += table.num_rows
 
#         current_size = os.path.getsize(out_path)
#         if current_size >= target_bytes:
#             writer.close()
#             print(f"  -> closed ({current_size / 1024**3:.2f} GB)")
#             batch_num += 1
#             out_path = os.path.join(output_dir, f"polymarket_trades_pt_{batch_num:03d}.parquet")
#             writer = pq.ParquetWriter(out_path, schema)
#             print(f"Writing {out_path} ...")
 
#     writer.close()
#     final_size = os.path.getsize(out_path)
#     print(f"  -> closed ({final_size / 1024**3:.2f} GB)")
#     print(f"\nDone. Wrote {batch_num} file(s), {total_rows:,} total rows, to {output_dir}")
 
 
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description=__doc__)
 
#     batch_parquet_files(
#         input_dir="./src/data/polymarket/trades/daily_aligned",
#         output_dir="./src/data/polymarket/trades",
#         target_gb=1,
#         rows_per_chunk=100_000,
#     )



# # 5. Script for changing the polymarket v1 parquet files structure
# for file in os.listdir("src/data/polymarketData"):
#     if file.endswith(".parquet"):
#         print("processing file:", file)
#         query = """
#             SELECT
#                 -- list every remaining column here, in the order you want
#                 block_number, block_timestamp, tx_hash, platform_name, platform_version, taker, maker, token_id, slug, condition_id, price, taker_side, amount, amount_asset, fee, fee_asset, extra_data
#             FROM (
#                 SELECT * EXCLUDE (
#                             outcome_seq, category, category_refined, outcome_label,
#                             winning_outcome_label, resolution_status, taker_base_fee,
#                             maker_base_fee, opens_at, close_at, resolved_at,
#                             market_slug, p_event, D
#                         )
#                         RENAME (asset_id AS token_id, taker_direction AS taker_side, usdc_amount AS amount, fee_usdc AS fee),
#                     -1::BIGINT    AS block_number,
#                     NULL::VARCHAR AS tx_hash,
#                     NULL::VARCHAR AS platform_name,
#                     NULL::VARCHAR AS platform_version,
#                     NULL::VARCHAR AS slug,
#                     NULL::VARCHAR AS amount_asset,
#                     NULL::VARCHAR AS fee_asset,
#                     NULL::VARCHAR AS extra_data,
#                 FROM data
#             )
#         """
#         df = changeParquetFileInplace(os.path.join("src/data/polymarketData", file), query)


# # 6. Fix a mistake in downloaded files
# import numpy as np
# import json
# files = sorted(os.listdir("./src/data/polymarket/trades"), key = lambda x: int(x.replace("polymarket_trades_pt_", "").replace(".parquet", "")))
# for file in files:
#     if "polymarket_trades" not in file:
#         continue

#     _idx = int(file.replace("polymarket_trades_pt_", "").replace(".parquet", ""))
#     if _idx != 130:
#         continue

#     print("Reading file", _idx)
#     _df = pd.DataFrame(queryParquetFile(
#         f"./src/data/polymarket/trades/polymarket_trades_pt_{_idx:03d}.parquet",
#         "SELECT * FROM data"
#     ))
#     print("Fixing file", _idx)
    

#     # Parse JSON column once, vectorized
#     parsed = _df["extra_data"].apply(json.loads)
#     extra = pd.json_normalize(parsed)

#     # Only rows where extra_data does NOT already have a maker_side get fixed
#     needs_fix = ~parsed.apply(lambda d: "maker_side" in d)

#     u = _df[_df["tx_hash"] == "0x8020a592be86aa047f0dffcb01d8945ffea3cc376f883e06ae27cf8b41b8bf83"]
#     print(u.iloc[0])
#     print("______")
#     print(u.iloc[1])
    
    
    
#     maker_amt = extra["maker_amount_filled"].astype(float)
#     taker_amt = extra["taker_amount_filled"].astype(float)

#     is_buy = _df["taker_side"] == "BUY"

#     # New taker_side: flip BUY<->SELL (only where needs_fix)
#     new_taker_side = np.where(is_buy, "SELL", "BUY")

#     # New price: maker/taker if was BUY, else taker/maker
#     new_price = np.where(is_buy, maker_amt / taker_amt, taker_amt / maker_amt)

#     # New amount: maker_amt/1e6 if was BUY, else taker_amt/1e6
#     new_amount = np.where(is_buy, maker_amt, taker_amt) / 1e6

#     # New maker_side for extra_data
#     new_maker_side = np.where(is_buy, "BUY", "SELL")

#     # Apply changes only where needs_fix is True; otherwise keep originals
#     _df["taker_side"] = np.where(needs_fix, new_taker_side, _df["taker_side"])
#     _df["price"] = np.where(needs_fix, new_price, _df["price"])
#     _df["amount"] = np.where(needs_fix, new_amount, _df["amount"])

#     extra["maker_side"] = np.where(needs_fix, new_maker_side, extra.get("maker_side", pd.Series([None] * len(extra))))
#     new_extra_data = extra.to_dict(orient="records")
#     new_extra_data = [json.dumps(d) for d in new_extra_data]

#     # Only overwrite extra_data for rows that needed fixing; keep original JSON otherwise
#     _df["extra_data"] = np.where(needs_fix, new_extra_data, _df["extra_data"])
          
         

#     u = _df[_df["tx_hash"] == "0x8020a592be86aa047f0dffcb01d8945ffea3cc376f883e06ae27cf8b41b8bf83"]
#     print(u.iloc[0])
#     print("______")
#     print(u.iloc[1])
#     exit() 
          
                
# # Test the integrity of data in the parquet files
# directory = "./src/data/polymarket/trades"
# files = sorted(
#     os.listdir(directory),
#     key = lambda x: int(x.replace("polymarket_trades_pt_", "").replace(".parquet", ""))
# )
# idx = 0
# for file in files:
#     idx = file.replace("polymarket_trades_pt_", "").replace(".parquet", "")
#     startBlock = pd.DataFrame(queryParquetFile(f"src/data/polymarket/trades/polymarket_trades_pt_{idx}.parquet", "SELECT MIN(block_number) AS largest_value FROM data")).iloc[0,0]
#     endBlock = pd.DataFrame(queryParquetFile(f"src/data/polymarket/trades/polymarket_trades_pt_{idx}.parquet", "SELECT MAX(block_number) AS largest_value FROM data")).iloc[0,0]
    
#     if startBlock == -1:
#         startBlock = getBlockNumberFromTS(polymarketHandler.w3["polygon"], pd.DataFrame(queryParquetFile(f"src/data/polymarket/trades/polymarket_trades_pt_{idx}.parquet", "SELECT MIN(block_timestamp) AS largest_value FROM data")).iloc[0,0])
    
#     if endBlock == -1:
#         endBlock = getBlockNumberFromTS(polymarketHandler.w3["polygon"], pd.DataFrame(queryParquetFile(f"src/data/polymarket/trades/polymarket_trades_pt_{idx}.parquet", "SELECT MAX(block_timestamp) AS largest_value FROM data")).iloc[0,0])
    
#     print(f"File: {file}, Start Block: {startBlock:,}, End Block: {endBlock:,}, Total Blocks: {endBlock - startBlock:,}")




# # Get the number of markets that have volumes but we have no trades for them
# allMarkets = pd.DataFrame(queryParquetFile(
#     "src/data/polymarket/historical_markets/polymarket_HistoricalMarkets.parquet",
#     "SELECT * FROM data"
# ))

# # Single scan of trades data instead of 2 queries per market
# traded_token_ids = queryParquetFolder(
#     "./src/data/polymarket/trades",
#     "SELECT DISTINCT token_id FROM data"
# )
# traded_token_ids_set = set(traded_token_ids["token_id"])

# # Only markets with volume matter
# markets_with_volume = allMarkets[allMarkets["volumeNum"] > 0].copy()

# markets_with_volume["outcome_0_has_trades"] = markets_with_volume["outcome_0_ID"].isin(traded_token_ids_set)
# markets_with_volume["outcome_1_has_trades"] = markets_with_volume["outcome_1_ID"].isin(traded_token_ids_set)

# no_trade_mask = ~markets_with_volume["outcome_0_has_trades"] | ~markets_with_volume["outcome_1_has_trades"]
# df_noTrdeSlugs = markets_with_volume.loc[no_trade_mask, ["slug"]].reset_index(drop=True)

# missing_0 = markets_with_volume.loc[~markets_with_volume["outcome_0_has_trades"], "outcome_0_ID"].rename("token_id")
# missing_1 = markets_with_volume.loc[~markets_with_volume["outcome_1_has_trades"], "outcome_1_ID"].rename("token_id")
# df_noTradeTokenIDs = pd.concat([missing_0, missing_1], ignore_index=True).to_frame(name="token_id")

# df_noTrdeSlugs.to_parquet("src/data/strategyFiles/noTradeMarkets.parquet", index=False)
# df_noTradeTokenIDs.to_parquet("src/data/strategyFiles/noTradeTokenIDs.parquet", index=False)

# print(f"Found {len(df_noTrdeSlugs)} markets with volume but no trades ({{:.2f}} % of total markets with volume)".format(len(df_noTrdeSlugs) / len(markets_with_volume) * 100))
# print(f"Found {len(df_noTradeTokenIDs)} token IDs with no trades")




# Align all trades by day
# Read files in trades directory and get the min and max timestamp of each file
files = sorted(
    os.listdir("./src/data/polymarket/trades"),
    key = lambda x: int(x.replace("polymarket_trades_pt_", "").replace(".parquet", ""))
)

# Get a record of date range in each parquet file
filesDateRange = {}
prevMaxTS = 0
for file in files:
    minTS = queryParquetFile(os.path.join("./src/data/polymarket/trades", file), "SELECT MIN(block_timestamp) FROM data").iloc[0,0]
    maxTS = queryParquetFile(os.path.join("./src/data/polymarket/trades", file), "SELECT MAX(block_timestamp) FROM data").iloc[0,0]
    
    filesDateRange[file] = [minTS, maxTS]

print(filesDateRange)
