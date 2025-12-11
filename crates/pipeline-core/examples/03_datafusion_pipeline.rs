use pipeline_core::datafusion_pipeline::pipeline;

#[tokio::main]
async fn main() {
    pipeline().await.unwrap();
}
