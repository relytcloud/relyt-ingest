//! Several tables, several partitions each: one client + one writer per
//! (table, partition) pair, all concurrent. Also shows the two stream
//! modes side by side — table A upserts on its primary key, table B has no
//! primary key and runs insert-only (duplicate rows are kept).
//!
//! A stuck table never blocks the others: every writer owns its notify
//! queue, so one table's failing load only stalls that writer.
//!
//! Run against a real stack with the same env the e2e suite uses.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use relyt_ingest::{Client, ClientConfig, StreamMode};

/// One (table, partition) consume loop. `mode` is per TABLE: every writer of
/// one table must use the same stream mode.
async fn run_stream(
    table: &str,
    topic: &str,
    partition: i64,
    mode: StreamMode,
) -> relyt_ingest::Result<()> {
    // Relyt-managed staging (the default): the client asks the server for
    // the bucket and its credentials at connect time, so nothing secret is
    // needed here. For your own bucket, use
    // ClientConfig::with_customer_staging instead.
    let mut cfg =
        ClientConfig::new(std::env::var("RELYT_E2E_DSN").expect("env RELYT_E2E_DSN required"));
    cfg.stream_mode = mode;
    cfg.cluster_id = std::env::var("RELYT_CLUSTER_ID").ok();

    let client = Client::connect(cfg).await?;
    let (writer, plan) = client
        .open_table(table, &format!("{topic}-p{partition}"))
        .await?;
    let mut offset = plan.kafka_resume_offset.unwrap_or(0);

    let schema = writer.schema();
    for _ in 0..3 {
        let start = offset;
        let end = start + 499;
        let ids =
            Int64Array::from_iter_values((start..=end).map(|o| partition * 1_000_000 + o % 5_000));
        let names = StringArray::from_iter_values((start..=end).map(|o| format!("{topic}-{o}")));
        let batch_data = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])
            .expect("batch matches table schema");
        writer.append(batch_data, start, end).await?;
        offset = end + 1;
    }
    writer.flush().await?;
    println!(
        "{table} {topic}-p{partition}: staged through {:?}",
        writer.staged_offset().await
    );
    Ok(())
}

#[tokio::main]
async fn main() -> relyt_ingest::Result<()> {
    // Tables (created beforehand):
    //   CREATE TABLE public.example_orders (id bigint PRIMARY KEY, name text)
    //     DISTRIBUTED BY (id);              -- upsert: PK required
    //   CREATE TABLE public.example_clicks (id bigint, name text)
    //     DISTRIBUTED BY (id);              -- insert-only: no PK needed
    let (a0, a1, b0, b1) = tokio::join!(
        run_stream("public.example_orders", "orders", 0, StreamMode::Upsert),
        run_stream("public.example_orders", "orders", 1, StreamMode::Upsert),
        run_stream("public.example_clicks", "clicks", 0, StreamMode::InsertOnly),
        run_stream("public.example_clicks", "clicks", 1, StreamMode::InsertOnly),
    );
    a0?;
    a1?;
    b0?;
    b1?;
    Ok(())
}
