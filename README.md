# SolAgg - a Solana Blockchain Data Aggregator

A mini Data aggregator software that collects and processes data from the Solana blockchain. The goal is to create a system capable of retrieving transaction and account data for the ongoing epoch.

![SolAgg Preview](https://github.com/mjzk/SolAgg/blob/main/docs/solagg_1_opt.gif)

### Key Features

1. **Data Retrieval:** capable of retrieving transaction and account data from the Solana blockchain on devnet or testnet. Utilise Solana's API or SDK to interact with the blockchain and fetch relevant data.

2. **Data Processing:** process the retrieved data efficiently. This includes parsing transaction records, extracting relevant information such as sender, receiver, amount, timestamp, etc., and organising data into a structured format for further analysis and queries.

3. **Data History:** Aggregating data from the current epoch and onwards. Exclude historical data to focus on recent transactions and account changes. Ensure the data aggregator provides real-time updates by continuously monitoring the blockchain for new transactions and account changes.

4. **Data Storage:** Current in-memory structure that offers scalability, reliability, and fast query capabilities.

5. **API Integration:** RESTful API layer to expose the aggregated data to external systems and applications. The API support various queries to retrieve transaction history, account details, and other relevant information.

### Architecture


### Notes on Design and Implementation
1. using solana RPC client
2. using Rust typed objects and serde
3. using solana RPC websocket for continuously monitoring
4. using Arrow for in-memory storage and SQL query
5. using tokio and type-safe wrapper for unified API layer

### Usage of API endpoints

### Build

### Run

### Test

### Benchmark

### Outlook