//! Skeleton of the intended production shape: one writer per Kafka
//! topic-partition, offsets committed only behind `staged_offset()`.
//!
//! The Kafka consumer is stubbed out (this repo does not depend on a Kafka
//! client); replace `FakePartition` with rdkafka's `StreamConsumer` and the
//! commit call with `consumer.commit()`.
//!
//! Run against a real stack with the same env the e2e suite uses:
//! ```text
//! RELYT_E2E_DSN=... RELYT_E2E_OSS_ENDPOINT=... (etc.) \
//!     cargo run --example kafka_partition_writer
//! ```

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use relyt_ingest::{Client, ClientConfig, StreamMode};

/// Stand-in for a Kafka partition: yields (batch, start_offset, end_offset).
struct FakePartition {
    next_offset: i64,
}

impl FakePartition {
    fn poll(&mut self, schema: &SchemaRef, rows: i64) -> (RecordBatch, i64, i64) {
        let start = self.next_offset;
        let end = start + rows - 1;
        self.next_offset = end + 1;
        let ids = Int64Array::from_iter_values(start..=end);
        let names = StringArray::from_iter_values((start..=end).map(|o| format!("msg-{o}")));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])
            .expect("batch matches table schema");
        (batch, start, end)
    }
}

#[tokio::main]
async fn main() -> relyt_ingest::Result<()> {
    let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("env {k} required"));

    // Relyt-managed staging (the default): the client asks the server for the
    // bucket and its credentials at connect time, so nothing secret is needed
    // here. To use your own bucket instead, build a StagingConfig and call
    // ClientConfig::with_customer_staging.
    let mut cfg = ClientConfig::new(env("RELYT_E2E_DSN"));
    cfg.stream_mode = StreamMode::Upsert;
    // Local demo clusters usually have no relyt.instanceid configured.
    cfg.cluster_id = std::env::var("RELYT_CLUSTER_ID").ok();

    let client = Client::connect(cfg).await?;
    // The target table must exist, e.g.:
    //   CREATE TABLE public.example_events (id bigint PRIMARY KEY, name text)
    //   DISTRIBUTED BY (id);
    let (writer, plan) = client.open_table("public.example_events", "p0").await?;

    // Seek the consumer: resume where the SDK left off, else offset 0.
    let mut partition = FakePartition {
        next_offset: plan.kafka_resume_offset.unwrap_or(0),
    };
    println!("resuming at offset {}", partition.next_offset);

    let schema = writer.schema();
    let mut committed = partition.next_offset - 1;
    for _ in 0..3 {
        let (batch, start, end) = partition.poll(&schema, 1000);
        writer.append(batch, start, end).await?;

        // Commit gate: only offsets that are durably staged may be committed.
        writer.flush().await?;
        let staged = writer.staged_offset().await.expect("flushed at least once");
        assert!(staged >= end, "flush returned before the batch was durable");
        committed = staged;
        println!("staged through offset {staged}, committing consumer offset");
        // consumer.commit(committed + 1) would go here.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Graceful shutdown: drain + release the lease so a successor (e.g. the
    // next rolling-upgrade pod) starts instantly. Wire this to SIGTERM in a
    // real deployment.
    let last = writer.close().await?;
    println!("done; last committed offset {committed}, closed at {last:?}");
    Ok(())
}
