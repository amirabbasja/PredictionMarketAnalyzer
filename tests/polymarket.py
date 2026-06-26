# Polymarket Test
from src.handlers.polymarket import PolymarketHandler
from src.utils.utils import *
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
import copy
import requests
from datetime import datetime
from huggingface_hub import list_repo_files, hf_hub_download, login
import pprint
import duckdb

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

# # Getting the details of a pstgresql database table, that is being filled by GOldsky
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



# # Getting a subgraph's data
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

# # Hugging face
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

# df = duckdb.query("SELECT * FROM 'src/data/polymarketTrades/OrderFilled/2026_04.parquet' LIMIT 10").df()
# print(df.iloc[0])
# df = duckdb.query("SELECT * FROM 'src/data/polymarketTrades/daily_aligned/2026-03-20.parquet' LIMIT 10").df()
# print(df.iloc[0])





### Chunking a parquet folder into files of specific size
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
#         input_dir="./src/data/polymarketData/daily_aligned",
#         output_dir="./src/data/polymarketData",
#         target_gb=1,
#         rows_per_chunk=100_000,
#     )



# # Script for changing the polymarket v1 parquet files structure
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



result = getFromSubgraph(
    "a03be5e9f766c0de3ca0441519f1d9ca",
    "",
    """
    {
        orderFilledEvents(first: 1000,block: {number: 84997444}) {
            id
            transactionHash
            timestamp
            orderHash
            blockNumber
            takerAssetId
            makerAssetId
        }
    }
    """
)
pprint.pprint(result["data"]["orderFilledEvents"][0])
pprint.pprint(len(result["data"]["orderFilledEvents"]))
txHashes_ql = [x.transactionHash for x in result["data"]["orderFilledEvents"]]

with open("src/ABIs/polygon/polymarket_exchange_CTF_v1.abi", "r") as f:
    abi = json.load(f)
contract_v1_CTF = polymarketHandler.w3["polygon"].eth.contract(address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", abi=abi)

result_2 = polymarketHandler.w3["polygon"].eth.get_logs({
    "address": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "fromBlock": 84997444,
    "toBlock": 84997444,
    "topics": ["0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"]
})
txHashes_rpc = [x.transactionHash for x in result_2]
print(len(result_2))
pprint.pprint(dict(contract_v1_CTF.events.OrderFilled().process_log(result_2[0])), indent = 4)