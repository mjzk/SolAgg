use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use solana_transaction_status::EncodedTransactionWithStatusMeta;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Transaction {
    signature: String,
    slot: u64,
    err: Option<String>,
    block_time: Option<i64>, //The estimated production time, as Unix timestamp (seconds since the Unix epoch). It's null if not available
    fee: u64,
    sender: String,
    receiver: String,
    amount: i64,
}

impl Transaction {
    pub(crate) fn get_arrow_scheme() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::Utf8, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("err", DataType::Utf8, true),
            Field::new(
                "block_time",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new("fee", DataType::UInt64, false),
            Field::new("sender", DataType::Utf8, false),
            Field::new("receiver", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ])
    }
}

pub(crate) fn parse_transaction(
    tx: &EncodedTransactionWithStatusMeta,
    slot: u64,
    block_time: Option<i64>,
) -> Option<Transaction> {
    let meta = tx.meta.as_ref()?;
    // println!("---transaction: {:#?}", tx.transaction);
    let transaction = tx.transaction.decode()?;

    let signature = transaction.signatures.get(0)?.to_string();
    let message = transaction.message;

    let sender = message.static_account_keys().get(0)?.to_string();
    let receiver = message.static_account_keys().get(1)?.to_string();

    let amount = *meta.pre_balances.get(0)? as i64 - *meta.post_balances.get(0)? as i64;

    Some(Transaction {
        signature,
        slot: slot,
        err: meta.err.as_ref().map(|e| e.to_string()),
        block_time: block_time,
        fee: meta.fee,
        sender,
        receiver,
        amount,
    })
}

#[allow(unused)]
//NOTE parts from solana_sdk::account::Account
#[derive(Debug, Serialize, Deserialize)]
struct Account {
    pubkey: String,
    lamports: u64,
    owner: String,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    data_len: usize,
}

#[cfg(test)]
mod unit_tests {
    use crate::fetch::{SolFetcher, SOL_RPC_URL};

    use super::*;

    #[test]
    fn test_parse_transaction() -> eyre::Result<()> {
        let sol_fetcher = SolFetcher::new(SOL_RPC_URL);
        let slot = 311_516_666;
        let block = sol_fetcher.fetch_sol_block_sync(slot)?;

        // println!("Parsed {} transactions:", transactions.len());
        // for tx in &transactions {
        //     println!("{:#?}", tx);
        // }

        let transactions: Vec<Transaction> = block
            .transactions
            .unwrap() //FIXME
            .iter()
            .filter_map(|tx| parse_transaction(tx, slot, block.block_time))
            .collect();

        assert_eq!(transactions.len(), 15);
        for tx in &transactions {
            assert_eq!(tx.slot, slot);
        }
        //FIXME this may be hold true always
        assert_eq!(transactions[0].signature, "CZkAA4a27zv58tYMsT7NXsvsUsakzouJXuAmbzWuV23XMYtHVQ9SbZvFXaSNLgJJTqtNQWoebYYtYHrZMUKmdPQ");
        Ok(())
    }
}
