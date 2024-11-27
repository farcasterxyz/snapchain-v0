use crate::proto::admin_rpc;
use crate::proto::admin_rpc::admin_service_server::AdminService;
use std::process;
use tonic::{Request, Response, Status};
use tracing::warn;

pub struct MyAdminService {
    db_dir: String,
}

impl MyAdminService {
    pub fn new(db_path: String) -> Self {
        Self { db_dir: db_path }
    }

    pub(crate) fn destroy_database(&self) -> Result<(), Status> {
        std::fs::remove_dir_all(&self.db_dir)?;
        std::fs::create_dir_all(&self.db_dir)?;
        warn!("Cleared db at {}", self.db_dir);
        Ok(())
    }
}

#[tonic::async_trait]
impl AdminService for MyAdminService {
    async fn terminate(
        &self,
        request: Request<admin_rpc::TerminateRequest>,
    ) -> Result<Response<admin_rpc::TerminateResponse>, Status> {
        let destroy_database = request.get_ref().destroy_database;

        if destroy_database {
            if let Err(e) = self.destroy_database() {
                warn!("Failed to destroy database: {}", e);
                return Err(Status::internal(format!(
                    "failed to destroy database: {}",
                    e
                )));
            }
        }

        tokio::spawn(async move {
            warn!(destroy_database, "Terminate scheduled");

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            process::exit(0);
        });

        let response = Response::new(admin_rpc::TerminateResponse {});
        Ok(response)
    }
}
