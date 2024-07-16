# SolAgg - a Solana Blockchain Data Aggregator

A mini Data aggregator software that collects and processes data from the Solana blockchain. The goal is to create a system capable of retrieving transaction and account data for the ongoing epoch.

![SolAgg Preview](https://github.com/mjzk/SolAgg/blob/main/docs/solagg_1_opt.gif)

[or watch the video](https://youtu.be/brjqA6-Nv2I)
## Features

1. **Data Retrieval:** capable of retrieving transaction and account data from the Solana blockchain on devnet or testnet. Utilise Solana's API or SDK to interact with the blockchain and fetch relevant data.

2. **Data Processing:** process the retrieved data efficiently. This includes parsing transaction records, extracting relevant information such as sender, receiver, amount, timestamp, etc., and organising data into a structured format for further analysis and queries.

3. **Data History:** Aggregating data from the current epoch and onwards. Exclude historical data to focus on recent transactions and account changes. Ensure the data aggregator provides real-time updates by continuously monitoring the blockchain for new transactions and account changes.

4. **Data Storage:** Current in-memory structure that offers scalability, reliability, and fast query capabilities.

5. **API Integration:** RESTful API layer to expose the aggregated data to external systems and applications. The API support various queries to retrieve transaction history, account details, and other relevant information.


## Architecture and Advantages


* Work stealing based multithreaded

    + Multithreaded data ingestion from mainnet and/or devnet transactions in realtime, and verified for rate-limited Solana RPC nodes, ~15x performance boost for common single threading data collectors.


## Notes on Design and Implementation
1. using solana RPC client
2. using Rust typed objects and serde
3. using solana RPC websocket for continuously monitoring
4. using Arrow for in-memory storage and SQL query
5. using tokio and type-safe wrapper for unified API layer

## Usage of API endpoints

* query the current number of total transactions in curruent epoch
```bash
curl -sS http://127.0.0.1:3666/transactions/count 
```

* query the transaction by id
```bash
curl -sS 'http://127.0.0.1:3666/transactions?id=fGLvYwnzu8wNbzKmFBJuwNZhcVXuoh4ynpcQEBsRoKX14CoYDtAZd9SCYayaR63X36Sv2sTiXW8yvhmYgH8Ux7A'
```

* query the transactions by day
```bash
curl -sS 'http://127.0.0.1:3666/transactions?day=16/07/2024'
```

* Arbitrary SQL query
```bash
curl -X POST -d 'select count(1) as count from transactions where fee=5000' -sS http://127.0.0.1:3666/sql
```
```bash
curl -X POST -d 'select block_time,fee from transactions where fee>5000 limit 2' -sS http://127.0.0.1:3666/sql
```

## Build and Run

(NOTE: assumed you have installed the Rust nightly toolchain in your dev env.)

1. start the solagg in release mode in the repo root for the perf eval
```bash
cargo run --release -- start
```
2. start the solagg with debug logging
```bash
RUST_LOG=solagg=debug cargo run --release -- start
```

3. start the solagg with trace logging(WARN: too many stdou)
```bash
RUST_LOG=solagg=trace cargo run --release -- start
```

## Test

## Benchmark

## Outlook