pub mod rpc {
    tonic::include_proto!("rpc");
}

pub mod message {
    tonic::include_proto!("message");
}

mod username_proof {
    tonic::include_proto!("username_proof");
}

use std::error::Error;
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};
use tonic::Code::Unimplemented;
use tracing::{info};
use hex::ToHex;
use tokio::sync::mpsc::Sender;
use rpc::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use message::{Message};


pub struct MySnapchainService {
    messages_tx: Sender<Message>
}

impl MySnapchainService {
    pub fn new(messages_tx: Sender<Message>) -> Self {
        Self { messages_tx }
    }
}

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(&self, request: Request<Message>) -> Result<Response<Message>, Status> {
        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received a message");

        let response = Response::new(request.into_inner());
        Ok(response)
    }
}
