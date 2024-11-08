use crate::proto::message::Message;
use crate::proto::rpc::snapchain_service_server::{SnapchainService, SnapchainServiceServer};
use hex::ToHex;
use std::error::Error;
use std::net::SocketAddr;
use tonic::Code::Unimplemented;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

#[derive(Default)]
pub struct MySnapchainService;

#[tonic::async_trait]
impl SnapchainService for MySnapchainService {
    async fn submit_message(&self, request: Request<Message>) -> Result<Response<Message>, Status> {
        let hash = request.get_ref().hash.encode_hex::<String>();
        info!(hash, "Received a message");

        let response = Response::new(request.into_inner());
        Ok(response)
    }
}
