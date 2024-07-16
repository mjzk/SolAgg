use crate::fetch::{SolFetcher, SOL_RPC_URL};
use datafusion::{arrow::array::RecordBatch, prelude::SessionContext};
use log::trace;
use rayon::prelude::*;
use std::time::Instant;

const RPS_LIMIT: usize = 25;
const MOCKED_EPOCH_INIT_LEN: u64 = 25; //NOTE: 30 rps for ankr

//TODO abstract the trait for different store implementations
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct TransactionStore {
    pub(crate) tx_batches: Vec<RecordBatch>,
    pub(crate) current_epoch: u64,
    pub(crate) current_slot: u64,
    mocked: bool,
}

impl TransactionStore {
    pub(crate) fn new(mocked: bool) -> eyre::Result<Self> {
        let sol_fetcher = SolFetcher::new(SOL_RPC_URL);
        let cur_epoch = sol_fetcher.get_current_epoch()?;
        let current_epoch = cur_epoch.current_epoch();
        let current_slot = cur_epoch.current_slot();
        let start_slot = if mocked {
            current_slot - MOCKED_EPOCH_INIT_LEN
        } else {
            cur_epoch.start_slot()
        };

        let range: Vec<u64> = (start_slot..=current_slot).collect();
        let mut tx_batches = Vec::with_capacity(range.len());
        let windows = range.chunks(RPS_LIMIT);
        trace!("slot windows: {:#?}", windows);
        let mut timer = Instant::now();
        for window in windows {
            let bs: eyre::Result<Vec<RecordBatch>> = window
                .par_iter()
                .map(|slot| sol_fetcher.fetch_transactions_as_batch_sync(*slot))
                .collect();
            for b in bs? {
                tx_batches.push(b);
            }
            //NOTE mini rate limiting
            let elapsed = timer.elapsed().as_millis() as u64;
            trace!("elapsed time for window: {:#?}", elapsed);
            if elapsed < 1000 {
                let sleep_time = 1000 - elapsed;
                trace!("rate limit sleep(ms): {:#?}", sleep_time);
                std::thread::sleep(std::time::Duration::from_millis(sleep_time));
            }
            timer = Instant::now();
        }
        Ok(Self {
            tx_batches,
            current_epoch,
            current_slot,
            mocked,
        })
    }

    pub(crate) fn append_batch(&mut self, batch: RecordBatch) {
        self.tx_batches.push(batch);
    }

    pub(crate) async fn query(
        &self,
        sql: &str,
        table_name: &str,
    ) -> eyre::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let df = ctx.read_batches(self.tx_batches.clone())?;
        ctx.register_table(table_name, df.into_view())?;
        let result = ctx.sql(sql).await?;
        Ok(result.collect().await?)
    }

    pub(crate) async fn query_to_json(&self, sql: &str, table_name: &str) -> eyre::Result<String> {
        let batches = self.query(sql, table_name).await?;
        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.finish().unwrap();
        let json_data = writer.into_inner();
        let json_string = String::from_utf8(json_data)?;
        Ok(json_string)
    }

    #[allow(unused)]
    fn size(&self) -> usize {
        self.tx_batches.iter().map(|b| b.num_rows()).sum()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_transaction_store_new() -> eyre::Result<()> {
        // env_logger::Builder::new()
        //     .target(env_logger::Target::Stdout)
        //     .filter_module("solagg::store", log::LevelFilter::Trace)
        //     .init();
        let mocked = true;
        let tx_store = TransactionStore::new(mocked)?;
        println!("tx_store.size:\n{:#?}", tx_store.size());
        assert!(tx_store.size() > 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_transaction_store_query() -> eyre::Result<()> {
        env_logger::Builder::new()
            .target(env_logger::Target::Stdout)
            .filter_module("solagg::store", log::LevelFilter::Trace)
            .init();
        let mocked = true;
        let tx_store = TransactionStore::new(mocked)?;
        println!("tx_store.size:\n{:#?}", tx_store.size());
        assert!(tx_store.size() > 0);
        let table_name = "transactions";
        let sql = "SELECT count(1) FROM transactions";
        let batches = tx_store.query(sql, table_name).await?;
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(tx_store.size(), value as usize);
        //check json output
        let json = tx_store.query_to_json(sql, table_name).await?;
        // println!("json:\n{:#?}", json);
        assert!(json.contains("count(Int64(1))"));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_transaction_store_query_timestamp() -> eyre::Result<()> {
        // env_logger::Builder::new()
        //     .target(env_logger::Target::Stdout)
        //     .filter_module("solagg::store", log::LevelFilter::Trace)
        //     .init();
        let mocked = true;
        let tx_store = TransactionStore::new(mocked)?;
        println!("tx_store.size:\n{:#?}", tx_store.size());
        assert!(tx_store.size() > 0);
        let table_name = "transactions";
        //FIXME should check when day crosses
        let today = Utc::now().date_naive().format("%Y-%m-%d").to_string();
        println!("today: {}", today);
        let sql = format!(
            "SELECT * FROM transactions WHERE cast(block_time as DATE) = '{}'",
            today
        );
        let batches = tx_store.query(&sql, table_name).await?;
        // println!("batches: {:#?}", batches);
        assert_eq!(tx_store.size(), batches[0].num_rows());
        //check json output
        let json = tx_store.query_to_json(&sql, table_name).await?;
        // println!("json:\n{:#?}", json);
        assert!(json.contains("block_time"));
        Ok(())
    }
}
