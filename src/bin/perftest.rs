use snapchain::perf::perftest;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    perftest::run().await
}
