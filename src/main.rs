use parking_lot::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use std::net::SocketAddr;
use sha2::{Sha256, Digest};
use tonic::{transport::Server, Request, Response, Status};
use clap::Parser;

pub mod snapchain {
    tonic::include_proto!("snapchain");
}

use snapchain::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use snapchain::{
    Block, ShardChunk, AccountStateTransition, 
    SubmitTransactionRequest, SubmitTransactionResponse,
    SubmitShardRequest, SubmitShardResponse,
    SubmitBlockRequest, SubmitBlockResponse,
    RegisterValidatorRequest, RegisterValidatorResponse,
};

#[derive(Debug, Clone)]
struct SnapchainState {
    blocks: Vec<Block>,
}

#[derive(Debug)]
struct SnapchainApp {
    id: u32,
    state: SnapchainState,
    mempool: Vec<AccountStateTransition>,
    validators: HashMap<u32, String>,
}

impl SnapchainApp {
    fn new(id: u32) -> Self {
        SnapchainApp {
            id,
            state: SnapchainState {
                blocks: vec![],
            },
            mempool: vec![],
            validators: HashMap::new(),
        }
    }

    fn add_transaction(&mut self, tx: AccountStateTransition) {
        self.mempool.push(tx);
    }

    fn create_block(&mut self) -> Block {
        let height = self.state.blocks.len() as u64 + 1;
        let state_transitions: Vec<AccountStateTransition> = self.mempool.drain(..).collect();
        let previous_hash = if let Some(last_block) = self.state.blocks.last() {
            last_block.previous_hash.clone()
        } else {
            "0".repeat(64)
        };
        let merkle_root = calculate_merkle_root(&state_transitions);

        Block {
            height,
            leader_schedule: vec![], // TODO: Implement leader schedule
            shard_chunks: vec![ShardChunk {
                height,
                state_transitions,
                previous_hash: previous_hash.clone(),
                merkle_root: merkle_root.clone(),
                signature: "".to_string(), // TODO: Implement signature
            }],
            previous_hash,
            merkle_root,
        }
    }

    fn apply_block(&mut self, block: Block) {
        self.state.blocks.push(block);
    }

    fn register_validator(&mut self, id: u32, address: String) -> bool {
        if !self.validators.contains_key(&id) {
            self.validators.insert(id, address);
            println!("Registered validator with ID: {}", id);
            true
        } else {
            false
        }
    }
}

fn calculate_merkle_root(transactions: &[AccountStateTransition]) -> String {
    if transactions.is_empty() {
        return "0".repeat(64);
    }
    let mut hashes: Vec<String> = transactions
        .iter()
        .map(|tx| {
            let mut hasher = Sha256::new();
            hasher.update(format!("{}{}{}", tx.fid, tx.merkle_root, tx.data));
            format!("{:x}", hasher.finalize())
        })
        .collect();

    while hashes.len() > 1 {
        let mut new_hashes = Vec::new();
        for chunk in hashes.chunks(2) {
            let mut hasher = Sha256::new();
            hasher.update(chunk[0].as_bytes());
            if chunk.len() > 1 {
                hasher.update(chunk[1].as_bytes());
            }
            new_hashes.push(format!("{:x}", hasher.finalize()));
        }
        hashes = new_hashes;
    }

    hashes[0].clone()
}

#[derive(Debug)]
pub struct SnapchainServiceImpl {
    app: Arc<Mutex<SnapchainApp>>,
    id: u32,
}

impl SnapchainServiceImpl {
    fn new(id: u32) -> Self {
        SnapchainServiceImpl {
            app: Arc::new(Mutex::new(SnapchainApp::new(id))),
            id,
        }
    }
}

#[tonic::async_trait]
impl SnapchainService for SnapchainServiceImpl {
    async fn submit_transaction(
        &self,
        request: Request<SubmitTransactionRequest>,
    ) -> Result<Response<SubmitTransactionResponse>, Status> {
        let tx = request.into_inner().transaction.unwrap();
        
        let mut app = self.app.lock();
        app.add_transaction(tx);

        Ok(Response::new(SubmitTransactionResponse {
            success: true,
            message: "Transaction added to mempool".to_string(),
        }))
    }

    async fn submit_shard(
        &self,
        request: Request<SubmitShardRequest>,
    ) -> Result<Response<SubmitShardResponse>, Status> {
        let shard = request.into_inner().shard.unwrap();
        
        // Here you would implement the logic to handle the submitted shard
        // For now, we'll just print the shard details
        println!("Received shard: {:?}", shard);

        Ok(Response::new(SubmitShardResponse {
            success: true,
            message: "Shard received".to_string(),
        }))
    }

    async fn submit_block(
        &self,
        request: Request<SubmitBlockRequest>,
    ) -> Result<Response<SubmitBlockResponse>, Status> {
        let block = request.into_inner().block.unwrap();
        
        let mut app = self.app.lock();
        app.apply_block(block);

        Ok(Response::new(SubmitBlockResponse {
            success: true,
            message: "Block applied to the chain".to_string(),
        }))
    }

    async fn register_validator(
        &self,
        request: Request<RegisterValidatorRequest>,
    ) -> Result<Response<RegisterValidatorResponse>, Status> {
        let req = request.into_inner();
        
        if self.id != 0 {
            return Ok(Response::new(RegisterValidatorResponse {
                success: false,
                message: "Only the primary validator (ID 0) can accept registrations".to_string(),
            }));
        }

        let mut app = self.app.lock();
        let success = app.register_validator(req.id, req.address);

        if success {
            Ok(Response::new(RegisterValidatorResponse {
                success: true,
                message: format!("Validator {} registered successfully", req.id),
            }))
        } else {
            Ok(Response::new(RegisterValidatorResponse {
                success: false,
                message: format!("Validator {} already registered", req.id),
            }))
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    id: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let base_port = 50050;
    let port = base_port + args.id;
    let addr: SocketAddr = format!("[::1]:{}", port).parse()?;
    let snapchain_service = SnapchainServiceImpl::new(args.id);

    println!("SnapchainService (ID: {}) listening on {}", args.id, addr);

    if args.id > 0 {
        // Register with the primary validator (ID 0)
        let primary_addr = format!("http://[::1]:{}", base_port);
        let mut client = snapchain::snapchain_service_client::SnapchainServiceClient::connect(primary_addr).await?;

        let request = tonic::Request::new(RegisterValidatorRequest {
            id: args.id,
            address: addr.to_string(),
        });

        match client.register_validator(request).await {
            Ok(response) => {
                println!("Registration response: {:?}", response);
            }
            Err(e) => {
                eprintln!("Failed to register with primary validator: {:?}", e);
            }
        }
    }

    Server::builder()
        .add_service(SnapchainServiceServer::new(snapchain_service))
        .serve(addr)
        .await?;

    Ok(())
}