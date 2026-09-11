//! One table, N Kafka partitions: one client + one writer PER PARTITION,
//! running concurrently (in production these are typically separate
//! processes; tokio tasks are semantically equivalent — every writer has its
//! own control connection, notify queue and lease identity).
//!
//! Key rule: writer_id must be unique per partition and stable across
//! restarts — include the topic name so two topics feeding one table can
//! never collide (e.g. "orders-p0", "orders-p1", ...).
//!
//! Run against a real stack with the same env the e2e suite uses.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use relyt_ingest::{Client, ClientConfig, StreamMode};

/// The consume loop of ONE partition. In production this wraps an rdkafka
/// StreamConsumer for that partition; here rows are synthesized.
async fn run_partition(partition: i64) -> relyt_ingest::Result<()> {
    // Relyt-managed staging (the default): the client asks the server for
    // the bucket and its credentials at connect time, so nothing secret is
    // needed here. For your own bucket, use
    // ClientConfig::with_customer_staging instead.
    let mut cfg =
        ClientConfig::new(std::env::var("RELYT_E2E_DSN").expect("env RELYT_E2E_DSN required"));
    cfg.stream_mode = StreamMode::Upsert;
    cfg.cluster_id = std::env::var("RELYT_CLUSTER_ID").ok();

    let client = Client::connect(cfg).await?;
    // The table must exist with a PRIMARY KEY (upsert mode), e.g.:
    //   CREATE TABLE public.example_orders (id bigint PRIMARY KEY, name text)
    //   DISTRIBUTED BY (id);
    let (writer, plan) = client
        .open_table("public.example_orders", &format!("orders-p{partition}"))
        .await?;

    // Seek THIS partition's consumer to its own resume point.
    let mut offset = plan.kafka_resume_offset.unwrap_or(0);
    println!("partition {partition}: resuming at offset {offset}");

    let schema = writer.schema();
    for _ in 0..5 {
        // Kafka guarantees a key lives on one partition; synthesize ids in a
        // per-partition range to model that.
        let start = offset;
        let end = start + 999;
        let ids =
            Int64Array::from_iter_values((start..=end).map(|o| partition * 1_000_000 + o % 10_000));
        let names = StringArray::from_iter_values((start..=end).map(|o| format!("row-{o}")));
        let batch_data = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])
            .expect("batch matches table schema");

        writer.append(batch_data, start, end).await?;
        offset = end + 1;
        // No flush here: files are cut by rotate_size_bytes /
        // rotate_interval_max. Commit consumer offsets against
        // writer.staged_offset() from a periodic committer instead.
    }

    // Drain the tail before shutdown so the last rows are durable, THEN
    // commit the final consumer offset.
    writer.flush().await?;
    println!(
        "partition {partition}: staged through {:?}",
        writer.staged_offset().await
    );
    Ok(())
}

#[tokio::main]
async fn main() -> relyt_ingest::Result<()> {
    let (a, b, c) = tokio::join!(run_partition(0), run_partition(1), run_partition(2));
    a?;
    b?;
    c?;
    Ok(())
}
