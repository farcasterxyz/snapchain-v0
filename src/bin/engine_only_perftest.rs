use snapchain::perf::engine_only_perftest;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    engine_only_perftest::run().await
}
