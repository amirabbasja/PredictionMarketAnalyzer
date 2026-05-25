# Prediction markets suite

A repository for interacting with various prediction markets.

## Necessary commands

Use *polymarket.py* to gather data and interact with gathered data in polymarket. Following commands are acceptable:

* Get all live event' data: `python -m scripts.polymarket --getAllEvents`
* Get all live markets' data: `python -m scripts.polymarket --getAllMarkets`
* Get all live markets' data with price their data: `python -m scripts.polymarket --getAllMarkets --price`
* Get closed markets' data: `python -m scripts.polymarket --getHistoricalMarkets`
* Get closed markets' data with price their data: `python -m scripts.polymarket --getHistoricalMarkets --price`
* Get closed markets' data, starting from a previously saved file: `python -m scripts.polymarket --getHistoricalMarkets --continue <jsonl or jsonl.gz fileLoc>`
* Convert the compressed `jsonl.gz` file into a csv file (Expect a 6~10 times increase in file size!): `python -m scripts.polymarket --toCsv <File_Location>`
