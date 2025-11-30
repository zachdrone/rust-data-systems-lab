use futures::future::join_all;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

async fn send_num(tx: mpsc::Sender<u32>, start: u32, end: u32) {
    for num in start..=end {
        if tx.send(num).await.is_err() {
            println!("transform_num: downstream dropped, stopping producer");
            break;
        }
    }
}

async fn transform_num(mut rx: mpsc::Receiver<u32>, tx: mpsc::Sender<u64>) {
    while let Some(message) = rx.recv().await {
        let u64message = message as u64;
        if tx.send(u64message * u64message).await.is_err() {
            println!("transform_num: downstream dropped, stopping producer");
            break;
        }
    }
}

async fn sink(mut rx: mpsc::Receiver<u64>) {
    while let Some(message) = rx.recv().await {
        println!("GOT = {message}");
    }
}

pub async fn pipeline() {
    let total = 10_000_000;
    let workers = 8;
    let chunk_size = total / workers;

    let (tx, rx) = mpsc::channel(1024);
    let (tx2, rx2) = mpsc::channel(1024);

    // producers
    let mut handles = Vec::new();

    for worker_id in 0..workers {
        let start = worker_id * chunk_size;
        let end = (worker_id + 1) * chunk_size;
        let handle = tokio::spawn(send_num(tx.clone(), start, end));
        handles.push(handle);
    }

    drop(tx);

    // transformer
    let t = tokio::spawn(transform_num(rx, tx2.clone()));
    handles.push(t);

    drop(tx2);

    // sink
    let s = tokio::spawn(sink(rx2));
    handles.push(s);

    join_all(handles).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
