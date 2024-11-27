use crate::proto::admin_rpc;
use crate::proto::admin_rpc::admin_service_server::AdminService;
use rocksdb;
use std::{io, path, process};
use thiserror::Error;
use tonic::{Request, Response, Status};
use tracing::warn;

pub struct MyAdminService {
    db_dir: String,
    admin_db_dir: String,
    db: Option<rocksdb::TransactionDB>,
}

#[derive(Debug, Error)]
pub enum AdminServiceError {
    #[error(transparent)]
    RocksDBError(#[from] rocksdb::Error),

    #[error(transparent)]
    IoError(#[from] io::Error),
}

const DB_DESTROY_KEY: &[u8] = b"__destroy_all_databases_on_start__";

impl MyAdminService {
    pub fn new(db_dir: &str) -> Self {
        let admin_db_dir = path::Path::new(db_dir)
            .join("admin")
            .to_string_lossy()
            .into_owned();
        Self {
            db_dir: db_dir.to_string(),
            admin_db_dir,
            db: None,
        }
    }

    pub fn maybe_destroy_databases(&mut self) -> Result<(), AdminServiceError> {
        let db = rocksdb::TransactionDB::open_default(&self.admin_db_dir)?;
        if db.get(DB_DESTROY_KEY)?.is_some() {
            db.delete(DB_DESTROY_KEY)?; // we're about to remove but do this anyway
            drop(db);
            warn!(db_dir = &self.db_dir, "destroying all databases");
            std::fs::remove_dir_all(&self.db_dir)?;
            let db = rocksdb::TransactionDB::open_default(&self.admin_db_dir)?;
            self.db.replace(db);
        } else {
            self.db.replace(db);
        }

        Ok(())
    }

    fn schedule_destruction(&self) -> Result<(), Status> {
        if let Some(ref db) = self.db {
            db.put(DB_DESTROY_KEY, &[]).map_err(|err| {
                Status::internal(format!(
                    "failed to schedule destruction of databases: {}",
                    err,
                ))
            })
        } else {
            Err(Status::internal("admin database is not open"))
        }
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
            if let Err(err) = self.schedule_destruction() {
                const TEXT: &str = "failed to schedule database destruction";
                warn!(err = err.to_string(), TEXT);
                return Err(Status::internal(format!("{}: {}", TEXT, err)));
            }
        }

        tokio::spawn(async move {
            warn!(destroy_database, "terminate scheduled");

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            process::exit(0);
        });

        let response = Response::new(admin_rpc::TerminateResponse {});
        Ok(response)
    }
}
