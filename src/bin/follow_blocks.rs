use clap::Parser;
use hex;
use snapchain::proto::{message_data, Message, MessageType};
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
            let height = chunk.header.as_ref().unwrap().height.unwrap();
            let shard_id = height.shard_index;
            let shard_height = height.block_number;

            for tx in &chunk.transactions {
                for msg in &tx.user_messages {
                    show_msg(shard_id, shard_height, msg);
                }
            }
        }
    }
}

fn show_msg(shard_id: u32, _shard_height: u64, msg: &Message) {
    let hash = hex::encode(&msg.hash);

    let data = match msg.data.as_ref() {
        Some(data) => data,
        None => {
            println!("- {} ** {} **", hash, "no data");
            return;
        }
    };

    let mt = match MessageType::try_from(data.r#type) {
        Ok(mt) => mt,
        Err(_) => {
            println!("- {} ** {} **", hash, "invalid message type");
            return;
        }
    };

    match mt {
        MessageType::CastAdd => {
            if let Some(message_data::Body::CastAddBody(body)) = data.body.as_ref() {
                println!(" - {} {} {} {}", hash, shard_id, msg.fid(), body.text);
            } else {
                panic!("Body is not CastAddBody");
            }
        }

        _ => println!(" - {}", hash),
    }
}
