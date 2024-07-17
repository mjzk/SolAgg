use crate::parse::{parse_transaction, Transaction};
use datafusion::arrow::{array::RecordBatch, json::ReaderBuilder};
use eyre::Ok;
use log::trace;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::{commitment_config::CommitmentConfig, epoch_info::EpochInfo};
use solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;

// pub const SOL_RPC_URL_HELIUS: &str =
//     "https://devnet.helius-rpc.com/?api-key=12ce24fe-c92e-42c0-8f29-b9fdb4757c49";
pub const SOL_RPC_URL: &str =
    "https://rpc.ankr.com/solana_devnet/000d1e909b4d8ecee5fa611ba9a540f5009d827f130612c2de094fad80f4d0e3";

pub struct SolFetcher {
    rpc_client: RpcClient,
}

#[derive(Debug)]
pub(crate) struct CurrentEpoch(EpochInfo);

impl CurrentEpoch {
    // pub(crate) fn current_epoch(&self) -> u64 {
    //     self.0.epoch
    // }

    pub(crate) fn start_slot(&self) -> u64 {
        self.0.absolute_slot - self.0.slot_index
    }

    pub(crate) fn current_slot(&self) -> u64 {
        self.0.absolute_slot
    }

    pub(crate) fn start_slot_next_epoch(&self) -> u64 {
        self.start_slot() + self.0.slots_in_epoch
    }
}

impl SolFetcher {
    pub fn new(rpc_url: &str) -> Self {
        let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::finalized());
        Self { rpc_client }
    }

    pub(crate) fn get_current_epoch(&self) -> eyre::Result<CurrentEpoch> {
        let epoch_info = self.rpc_client.get_epoch_info()?;
        Ok(CurrentEpoch(epoch_info))
    }

    #[inline(always)]
    pub(crate) async fn fetch_sol_block(&self, slot: u64) -> eyre::Result<UiConfirmedBlock> {
        const MAX_RETRIES: u32 = 5;
        const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(50);

        let mut retry_delay = INITIAL_RETRY_DELAY;

        for attempt in 0..MAX_RETRIES {
            let result = self.rpc_client.get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Binary),
                    transaction_details: None,
                    rewards: None,
                    commitment: None,
                    max_supported_transaction_version: Some(0),
                },
            );

            match result {
                std::result::Result::Ok(block) => return Ok(block),
                Err(e) if attempt < MAX_RETRIES - 1 => {
                    trace!(
                        "Error fetching block (attempt {}): {:?}. Retrying...",
                        attempt + 1,
                        e
                    );
                    sleep(retry_delay).await;
                    retry_delay *= 2; // Exponential backoff
                }
                Err(e) => eyre::bail!("Failed to fetch block: {}", e),
            }
        }

        Err(eyre::eyre!(
            "Failed to fetch block after {} attempts",
            MAX_RETRIES
        ))
    }

    #[inline(always)]
    pub(crate) fn fetch_sol_block_sync(&self, slot: u64) -> eyre::Result<UiConfirmedBlock> {
        let block = self.rpc_client.get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Binary),
                transaction_details: None,
                rewards: None,
                commitment: None,
                max_supported_transaction_version: Some(0),
            },
        )?;
        Ok(block)
    }

    #[inline(always)]
    pub(crate) async fn fetch_transactions(&self, slot: u64) -> eyre::Result<Vec<Transaction>> {
        let block = self.fetch_sol_block(slot).await?;
        Ok(block.transactions.map_or(vec![], |txs| {
            txs.iter()
                .filter_map(|tx| parse_transaction(tx, slot, block.block_time))
                .collect()
        }))
    }

    pub(crate) fn fetch_transactions_sync(&self, slot: u64) -> eyre::Result<Vec<Transaction>> {
        let block = self.fetch_sol_block_sync(slot)?;
        Ok(block.transactions.map_or(vec![], |txs| {
            txs.iter()
                .filter_map(|tx| parse_transaction(tx, slot, block.block_time))
                .collect()
        }))
    }

    pub async fn fetch_transactions_as_batch(&self, slot: u64) -> eyre::Result<RecordBatch> {
        let txs = self.fetch_transactions(slot).await?;
        let schema = Arc::new(Transaction::get_arrow_scheme());
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
        decoder.serialize(&txs)?;

        let batch = decoder.flush()?.unwrap_or(RecordBatch::new_empty(schema));
        Ok(batch)
    }

    ///NOTE sync version fetch series does not have retry mechanism
    ///     and great for historical data fetching
    pub fn fetch_transactions_as_batch_sync(&self, slot: u64) -> eyre::Result<RecordBatch> {
        let txs = self.fetch_transactions_sync(slot)?;
        let schema = Arc::new(Transaction::get_arrow_scheme());
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
        decoder.serialize(&txs)?;

        let batch = decoder.flush()?.unwrap_or(RecordBatch::new_empty(schema));
        Ok(batch)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_get_current_epoch() -> eyre::Result<()> {
        let sol_fetcher = SolFetcher::new(SOL_RPC_URL);

        let current_epoch = sol_fetcher.get_current_epoch()?;

        println!("current_epoch: {:#?}", current_epoch);

        assert!(current_epoch.0.epoch > 720);
        assert!(current_epoch.0.absolute_slot > 311516666);
        Ok(())
    }

    #[test]
    fn test_fetch_transactions_as_batch() -> eyre::Result<()> {
        let sol_fetcher = SolFetcher::new(SOL_RPC_URL);
        let slot = 311_516_666;
        let batch = sol_fetcher.fetch_transactions_as_batch_sync(slot)?;
        // println!("batch: {:#?}", batch);
        assert_eq!(batch.num_rows(), 15);
        Ok(())
    }
}
