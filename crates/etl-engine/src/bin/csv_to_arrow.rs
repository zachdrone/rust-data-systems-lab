use arrow_array::{Int64Array, RecordBatch};
use arrow_csv::ReaderBuilder;
use arrow_json::writer::LineDelimitedWriter;
use arrow_ops::transforms::{add_normalized_col, normalize_i64};
use arrow_schema::{DataType, Field, Schema};
use std::fs::File;
use std::sync::Arc;
use tokio::sync::mpsc;

async fn read_csv(
    filepath: &str,
    schema: Arc<arrow_schema::Schema>,
    tx: mpsc::Sender<RecordBatch>,
) {
    let file = File::open(filepath).unwrap();

    let mut csv = ReaderBuilder::new(schema)
        .with_header(true)
        .build(file)
        .unwrap();
    while let Some(batch) = csv.next() {
        tx.send(batch.unwrap()).await.unwrap();
    }

    drop(tx);
}

async fn normalize(tx: mpsc::Sender<RecordBatch>, mut rx: mpsc::Receiver<RecordBatch>) {
    while let Some(batch) = rx.recv().await {
        let idx = batch.schema().index_of("heart_rate_bpm").unwrap();
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let normed_col = normalize_i64(&col);
        let normed_batch = add_normalized_col(&batch, normed_col, "normed_heart_rate_bpm");
        tx.send(normed_batch).await.unwrap();
    }
}

async fn write_output(mut rx: mpsc::Receiver<RecordBatch>) {
    let file = File::create("normed_output.json").unwrap();
    while let Some(batch) = rx.recv().await {
        let mut writer = LineDelimitedWriter::new(&file);
        writer.write_batches(&[&batch]).unwrap();
    }
}

#[tokio::main]
async fn main() {
    let (raw_tx, raw_rx) = mpsc::channel(32);
    let (norm_tx, norm_rx) = mpsc::channel(32);

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("heart_rate_bpm", DataType::Int64, false),
        Field::new("distance_miles", DataType::Float32, false),
        Field::new("pace_min_per_mile", DataType::Float32, false),
    ]));

    tokio::join!(
        async move {
            read_csv("running_data.csv", schema.clone(), raw_tx.clone()).await;
            drop(raw_tx);
        },
        async move {
            normalize(norm_tx.clone(), raw_rx).await;
            drop(norm_tx);
        },
        write_output(norm_rx),
    );
}
