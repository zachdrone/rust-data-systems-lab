use arrow_array::{Int64Array, RecordBatch};
use arrow_csv::ReaderBuilder;
use arrow_json::writer::LineDelimitedWriter;
use arrow_ops::transforms::{add_normalized_col, normalize_i64};
use arrow_schema::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, path::Path};
use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;

async fn read_csv(filepath: String, schema: Arc<Schema>, tx: mpsc::Sender<RecordBatch>) {
    tokio::task::spawn_blocking(move || {
        let file = File::open(filepath).unwrap();

        let csv = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .unwrap();

        for batch in csv {
            tx.blocking_send(batch.unwrap()).unwrap();
        }
    })
    .await
    .unwrap();
}

async fn read_csv_store(filepath: String, schema: Arc<Schema>, tx: mpsc::Sender<RecordBatch>) {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix("./").unwrap());

    let location = Path::from(filepath);

    let bytes = store.get(&location).await.unwrap().bytes().await.unwrap();
    let cursor = Cursor::new(bytes);

    tokio::task::spawn_blocking(move || {
        let csv = ReaderBuilder::new(schema)
            .with_header(true)
            .build(cursor)
            .unwrap();

        for batch in csv {
            tx.blocking_send(batch.unwrap()).unwrap();
        }
    })
    .await
    .unwrap();
}

async fn normalize(tx: mpsc::Sender<RecordBatch>, mut rx: mpsc::Receiver<RecordBatch>) {
    while let Some(batch) = rx.recv().await {
        let tx = tx.clone();

        tokio::task::spawn_blocking(move || {
            let idx = batch.schema().index_of("heart_rate_bpm").unwrap();
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let normed_col = normalize_i64(&col);
            let normed_batch = add_normalized_col(&batch, normed_col, "normed_heart_rate_bpm");
            tx.blocking_send(normed_batch).unwrap();
        });
    }
}

async fn write_output(mut rx: mpsc::Receiver<RecordBatch>) {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix("./").unwrap());
    while let Some(batch) = rx.recv().await {
        let mut buffer = Vec::new();
        {
            let mut writer = LineDelimitedWriter::new(&mut buffer);
            writer.write_batches(&[&batch]).unwrap();
        }

        let path = Path::from("normed_output.json");
        store.put(&path, buffer.into()).await.unwrap();
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

    let reader = tokio::spawn(read_csv_store(
        "running_data.csv".to_owned(),
        schema.clone(),
        raw_tx,
    ));
    let normalizer = tokio::spawn(normalize(norm_tx, raw_rx));
    let writer = tokio::spawn(write_output(norm_rx));

    let _ = tokio::join!(reader, normalizer, writer);
}
