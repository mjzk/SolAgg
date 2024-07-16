/*
API desgin:
1. transactions:
    - GET /transactions?id=:id
    - GET /transactions?day=:id
    - GET /transactions?hour_start=11&hour_end=12&fee_start=100&fee_end=200
    - GET /transactions/count
    - POST /sql with body {sql: "SELECT * FROM transactions WHERE fee = 500"}

2. accounts:
    TODO: implement this
*/
use axum::{
    body::Bytes,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use hyper::StatusCode;
use log::{debug, info};
use serde::Deserialize;
use serde_json::json;

use crate::{
    datetime::normalize_date,
    stream::{query_to_json, TheadSafeStreamer},
};

#[derive(Deserialize)]
struct TransactionQuery {
    id: Option<String>,
    day: Option<String>,
    // hour_start: Option<u8>,
    // hour_end: Option<u8>,
    // fee_start: Option<u64>,
    // fee_end: Option<u64>,
}

async fn transactions(
    State(streamer): State<TheadSafeStreamer>,
    Query(params): Query<TransactionQuery>,
) -> Result<String, ApiError> {
    if let Some(id) = params.id {
        let sql = format!("SELECT * FROM transactions WHERE signature = '{}'", id);
        let json = query_to_json(streamer, &sql, "transactions").await?;
        debug!("Query json: {}", json);
        Ok(json)
    } else if let Some(day) = params.day {
        let table_name = "transactions";
        let dt = normalize_date(&day)?;
        let sql = format!(
            "SELECT * FROM transactions WHERE cast(block_time as DATE) = '{}'",
            dt
        );
        let json = query_to_json(streamer, &sql, table_name).await?;
        debug!("Query json: {}", json);
        Ok(json)
    } else {
        // Handle GET /transactions/hour_start=11&hour_end=12&fee_start=100&fee_end=200
        todo!("1")
    }
}

struct ApiError(eyre::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal error: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<eyre::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

async fn transactions_count(State(streamer): State<TheadSafeStreamer>) -> Result<String, ApiError> {
    let sql = "SELECT count(1) as count FROM transactions";
    let json = query_to_json(streamer, sql, "transactions").await?;
    debug!("Query json: {}", json);
    Ok(json)
}

//TODO checking needed, now for demo
async fn transactions_sql(
    State(streamer): State<TheadSafeStreamer>,
    body: Bytes,
) -> Result<String, ApiError> {
    let sql = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => return Ok(json!({ "error": "Invalid UTF-8" }).to_string()),
    };

    let json = query_to_json(streamer, &sql, "transactions").await?;
    debug!("Query json: {}", json);
    Ok(json)
}

pub async fn api_server(stream: TheadSafeStreamer) -> eyre::Result<()> {
    let app = Router::new()
        .route("/transactions", get(transactions))
        .route("/transactions/count", get(transactions_count))
        .route("/sql", post(transactions_sql))
        .with_state(stream);

    let addr = "127.0.0.1:3666";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("SolAgg API server is starting at http://{} ...", addr);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
