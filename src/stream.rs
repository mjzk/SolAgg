use datafusion::arrow::{array::RecordBatch, json::ReaderBuilder};
use futures_util::{SinkExt, StreamExt};
use log::trace;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    RwLock,
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{
    fetch::{SolFetcher, SOL_RPC_URL},
    parse::Transaction,
    store::TransactionStore,
};

const SOL_RPC_WS: &str =
    "wss://devnet.helius-rpc.com/?api-key=12ce24fe-c92e-42c0-8f29-b9fdb4757c49";

pub struct SolAggStreamer {
    pub(crate) tx_store: TransactionStore,
}

pub type TheadSafeStreamer = Arc<RwLock<SolAggStreamer>>;

impl SolAggStreamer {
    pub fn new_arc(mocked: bool) -> eyre::Result<TheadSafeStreamer> {
        Ok(Arc::new(RwLock::new(Self {
            tx_store: TransactionStore::new(mocked)?,
        })))
    }
}

pub async fn start_streamer(streamer: TheadSafeStreamer) -> eyre::Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    let fut_watch_sol = tokio::spawn(async move { watch_sol_rpc_ws(tx).await });
    let fut_proc_sol = tokio::spawn(async move { process_sol_notifications(streamer, rx).await });
    let res = tokio::try_join!(fut_watch_sol, fut_proc_sol);
    match res {
        Ok((res0, res1)) => {
            log::trace!("streamer normal exit; res0 = {:?}, res1 = {:?}", res0, res1);
        }
        Err(err) => {
            log::error!("streamer failed; error = {}", err);
        }
    }
    Ok(())
}

// pub(crate) async fn query(
//     streamer: TheadSafeStreamer,
//     sql: &str,
//     table_name: &str,
// ) -> {
//     Ok(streamer
//         .read()
//         .await
//         .tx_store
//         .query(sql, table_name)
//         .await?)
// }

pub(crate) async fn query_to_json(
    streamer: TheadSafeStreamer,
    sql: &str,
    table_name: &str,
) -> eyre::Result<String> {
    Ok(streamer
        .read()
        .await
        .tx_store
        .query_to_json(sql, table_name)
        .await?)
}

async fn process_sol_notifications(
    streamer: TheadSafeStreamer,
    mut rx: UnboundedReceiver<u64>,
) -> eyre::Result<()> {
    let sol_fetcher = SolFetcher::new(SOL_RPC_URL);
    while let Some(slot) = rx.recv().await {
        trace!("--- Received slot number: {}", slot);
        let batch = sol_fetcher.fetch_transactions_as_batch(slot).await?;
        //NOTE minizie the lock scope
        let tx_store = &mut streamer.write().await.tx_store;
        //NOTE: corner case#1
        //      may have init slot gap in a very small possibility, just check once
        let cur_slot = tx_store.current_slot;
        if tx_store.init_slot == cur_slot {
            if slot > (cur_slot + 1) {
                for slot in (cur_slot + 1)..slot {
                    log::debug!(
                        "--- init slot gap backfill, init_slot: {}, insert slot: {}",
                        cur_slot,
                        slot
                    );
                    let batch = sol_fetcher.fetch_transactions_as_batch(slot).await?;
                    tx_store.append_batch(batch);
                }
            }
        }
        tx_store.current_slot = slot;

        tx_store.append_batch(batch);
    }
    trace!("??? process_sol_notifications exited.");
    Ok(())
}

async fn watch_sol_rpc_ws(tx: UnboundedSender<u64>) -> eyre::Result<()> {
    let (ws_stream, _) = connect_async(SOL_RPC_WS).await?;
    let (mut write, mut read) = ws_stream.split();

    let block_sub_msg = json!({
      "jsonrpc": "2.0",
      "id": "1",
      "method": "slotSubscribe",
    });
    trace!("send block_sub_msg");
    write.send(Message::Text(block_sub_msg.to_string())).await?;

    trace!("Subscribed to transaction and account change notifications.");
    trace!("Watching for changes...");
    // Handle incoming messages
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                if let Some(method) = json.get("method") {
                    match method.as_str().unwrap() {
                        "slotNotification" => {
                            trace!("New slot: {}", json);
                            if let Some(slot) = json["params"]["result"]["root"].as_u64() {
                                if let Err(e) = tx.send(slot) {
                                    eyre::bail!(
                                        "Failed to send slot number, receiver dropped: {}",
                                        e
                                    );
                                }
                            } else {
                                eyre::bail!("No slot in notification: {}", json);
                            }
                        }
                        "accountNotification" => {
                            trace!("Account changed: {}", json["params"]["result"]["value"]);
                        }
                        _ => trace!("Received other notification: {}", text),
                    }
                } else {
                    trace!("Received some message?: {}", text);
                }
            }
            Ok(Message::Close(..)) => {
                println!("WebSocket closed");
                break;
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
            _ => {}
        }
    }
    trace!("??? watch_sol_rpc_ws exited.");
    Ok(())
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[ignore = "manual integ test"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sol_streaming_loop() -> eyre::Result<()> {
        env_logger::Builder::new()
            .target(env_logger::Target::Stdout)
            .filter_module("solagg", log::LevelFilter::Trace)
            .init();

        let streamer = SolAggStreamer::new_arc(true)?;
        let s = streamer.clone();
        tokio::spawn(async move { start_streamer(s).await });
        let table_name = "transactions";
        let sql = "SELECT count(1) FROM transactions";
        let json = query_to_json(streamer, sql, table_name).await?;
        assert!(json.contains("count(Int64(1))"));
        Ok(())
    }
}
