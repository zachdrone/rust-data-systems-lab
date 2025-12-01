use pipeline_core::chunked_file_pipeline;

use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};

#[tokio::main]
async fn main() {
    chunked_file_pipeline::pipeline().await;
}
