use criterion::{criterion_group, criterion_main, Criterion};
use solagg::fetch::{SolFetcher, SOL_RPC_URL};

/**
 * Benchmark output:
 */
fn bench_query_txs(c: &mut Criterion) {
    // let sol_fetcher = SolFetcher::new(SOL_RPC_URL);
    // // let sol_fetcher = SolFetcher::new(SOL_RPC_URL_HELIUS);
    // let slot = 311_516_666;
    // //warm up
    // let batch = sol_fetcher.fetch_transactions_as_batch(slot).unwrap();
    // assert_eq!(batch.num_rows(), 15);
    // //benchmark
    // c.bench_function("fetch_txs", |b| {
    //     b.iter(|| {
    //         let batch = sol_fetcher.fetch_transactions_as_batch(slot).unwrap();
    //         assert_eq!(batch.num_rows(), 15);
    //     })
    // });
}

fn configure_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}
criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_query_txs
}

criterion_main!(benches);
