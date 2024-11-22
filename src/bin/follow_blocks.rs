use clap::Parser;
use hex;
use snapchain::utils::cli::follow_blocks;
use tokio::sync::mpsc;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    addr: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (block_tx, mut block_rx) = mpsc::channel(1000);
    tokio::spawn(async move {
        follow_blocks(args.addr, block_tx).await.unwrap();
    });

    while let Some(block) = block_rx.recv().await {
        let block_header = block.header.as_ref().ok_or("Block header missing").unwrap();

        let block_number = block_header.height.as_ref().unwrap().block_number;

        println!("Block {}", block_number);

        for chunk in &block.shard_chunks {
            for tx in &chunk.transactions {
                for msg in &tx.user_messages {
                    let hash = hex::encode(&msg.hash);
                    println!(" - {}", hash);
                }
            }
        }
    }
}
