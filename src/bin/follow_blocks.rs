use hex;
use snapchain::proto::{rpc, snapchain::Block};
use std::error::Error;
use tokio::sync::mpsc;
use tokio::time;

const FETCH_SIZE: u64 = 100;

pub async fn follow_blocks(
    addr: String,
    block_tx: mpsc::Sender<Block>,
) -> Result<(), Box<dyn Error>> {
    let mut client = rpc::snapchain_service_client::SnapchainServiceClient::connect(addr).await?;

    let mut i = 1;

    loop {
        let msg = rpc::BlocksRequest {
            shard_id: 0,
            start_block_number: i,
            stop_block_number: Some(i + FETCH_SIZE),
        };

        let request = tonic::Request::new(msg);
        let response = client.get_blocks(request).await?;

        let inner = response.into_inner();
        if inner.blocks.is_empty() {
            time::sleep(time::Duration::from_millis(10)).await;
            continue;
        }

        for block in &inner.blocks {
            block_tx.send(block.clone()).await.unwrap();
            i += 1;
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = "http://127.0.0.1:3383".to_string();
    let (block_tx, mut block_rx) = mpsc::channel(1000);
    tokio::spawn(async move {
        follow_blocks(addr, block_tx).await.unwrap();
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
