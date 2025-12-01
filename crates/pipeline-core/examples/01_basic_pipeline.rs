use pipeline_core::basic_pipeline;

#[tokio::main]
async fn main() {
    basic_pipeline::pipeline().await
}
