use std::error::Error;
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};
use tonic::Code::Unimplemented;
use tracing::{info};
use hex::ToHex;


pub mod rpc {
    tonic::include_proto!("rpc");
}

pub mod message {
    tonic::include_proto!("message");
}

pub mod username_proof {
    tonic::include_proto!("username_proof");
}

use rpc::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use message::{Message};

#[derive(Default)]
pub struct MySnapchainService;

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(&self, request: Request<Message>) -> Result<Response<Message>, Status> {
        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received a message");
        Err(Status::new(Unimplemented, "not implemented"))
    }
}
