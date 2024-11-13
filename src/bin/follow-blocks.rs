use hex;
use snapchain::proto::rpc;
use std::error::Error;
use tokio::time;

const FETCH_SIZE: u64 = 100;

async fn follow_blocks(addr: String) -> Result<(), Box<dyn Error>> {
    let mut client = rpc::snapchain_service_client::SnapchainServiceClient::connect(addr).await?;
    let mut i = 1;

    loop {
        let msg = rpc::BlocksRequest {
            shard_id: 1,
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
            let block_header = block.header.as_ref().ok_or("Block header missing")?;

            let block_number = block_header
                .height
                .as_ref()
                .ok_or("Block height missing")?
                .block_number;

            if block_number != i {
                return Err("Unexpected block number found".into());
            }

            println!("Block {}", block_number);

            for chunk in &block.shard_chunks {
                for tx in &chunk.transactions {
                    for msg in &tx.user_messages {
                        let hash = hex::encode(&msg.hash);
                        println!(" - {}", hash);
                    }
                }
            }

            i += 1;
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = "http://127.0.0.1:50061".to_string();
    follow_blocks(addr).await.unwrap();
}
