use futures::future::join_all;
use std::string::String;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

async fn stream_file_bytes(tx: mpsc::Sender<Vec<u8>>, filepath: &str) {
    let mut file = File::open(filepath).await.unwrap();
    const CHUNK: usize = 16 * 1024;
    let mut buf = vec![0u8; CHUNK];

    loop {
        let n = file.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }

        let chunk = buf[..n].to_vec();

        let _ = tx.send(chunk).await;
    }
}

async fn bytes_to_lines(mut rx: mpsc::Receiver<Vec<u8>>, tx: mpsc::Sender<String>) {
    let mut pending = String::new();

    while let Some(chunk) = rx.recv().await {
        let s = String::from_utf8(chunk).unwrap();
        pending.push_str(&s);

        while let Some(pos) = pending.find('\n') {
            let line = pending[..pos].to_string();
            tx.send(line).await.unwrap();

            pending.drain(..=pos);
        }
    }
    if !pending.is_empty() {
        tx.send(pending).await.unwrap();
    }
}

async fn transform_csv_lines(mut rx: mpsc::Receiver<String>, tx: mpsc::Sender<String>) {
    const FIRST_NAME_INDEX: usize = 2;
    const LAST_NAME_INDEX: usize = 3;
    let mut is_header = true;

    while let Some(line) = rx.recv().await {
        if is_header {
            tx.send(line).await.unwrap();
            is_header = false;
            continue;
        }

        let mut cols: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        cols[FIRST_NAME_INDEX] = cols[FIRST_NAME_INDEX].to_uppercase();
        cols[LAST_NAME_INDEX] = cols[LAST_NAME_INDEX].to_uppercase();

        let new_line = cols.join(",");
        tx.send(new_line).await.unwrap();
    }
}

async fn write_csv_lines(mut rx: mpsc::Receiver<String>, output: &str) {
    let mut file = File::create(output).await.unwrap();

    while let Some(line) = rx.recv().await {
        file.write_all(line.as_bytes()).await.unwrap();
        file.write_all(b"\n").await.unwrap();
    }
}

pub async fn pipeline() {
    let (tx1, rx1) = mpsc::channel(1024);
    let (tx2, rx2) = mpsc::channel(1024);
    let (tx3, rx3) = mpsc::channel(1024);

    let mut handles = Vec::new();

    handles.push(tokio::spawn(stream_file_bytes(
        tx1.clone(),
        "examples/customers-10000.csv",
    )));

    drop(tx1);

    handles.push(tokio::spawn(bytes_to_lines(rx1, tx2.clone())));

    drop(tx2);

    handles.push(tokio::spawn(transform_csv_lines(rx2, tx3.clone())));

    drop(tx3);

    handles.push(tokio::spawn(write_csv_lines(
        rx3,
        "transformed-customers-10000.csv",
    )));

    join_all(handles).await;
}
