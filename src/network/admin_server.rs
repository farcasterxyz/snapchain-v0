use crate::proto::admin_rpc;
use crate::proto::admin_rpc::admin_service_server::AdminService;
use std::process;
use tonic::{Request, Response, Status};
use tracing::warn;

pub struct MyAdminService {}

impl MyAdminService {}

#[tonic::async_trait]
impl AdminService for MyAdminService {
    async fn terminate(
        &self,
        request: Request<admin_rpc::TerminateRequest>,
    ) -> Result<Response<admin_rpc::TerminateResponse>, Status> {
        let destroy_database = request.get_ref().destroy_database;
        tokio::spawn(async move {
            warn!(destroy_database, "Terminate scheduled");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            process::exit(0);
        });

        let response = Response::new(admin_rpc::TerminateResponse {});
        Ok(response)
    }
}
