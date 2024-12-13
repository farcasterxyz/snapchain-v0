use crate::mempool::routing::MessageRouter;
use crate::proto::admin_service_server::AdminService;
use crate::proto::ValidatorMessage;
use crate::proto::{self, OnChainEvent};
use crate::storage::store::engine::{MempoolMessage, Senders};
use rocksdb;
use std::collections::HashMap;
use std::{io, path, process};
use thiserror::Error;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

pub struct DbManager {
    db_dir: String,
    admin_db_dir: String,
    db: Option<rocksdb::TransactionDB>,
}

impl DbManager {
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

pub struct MyAdminService {
    db_manager: DbManager,
    num_shards: u32,
    message_router: Box<dyn MessageRouter>,
    pub shard_senders: HashMap<u32, Senders>,
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
    pub fn new(
        db_manager: DbManager,
        shard_senders: HashMap<u32, Senders>,
        num_shards: u32,
        message_router: Box<dyn MessageRouter>,
    ) -> Self {
        Self {
            db_manager,
            shard_senders,
            num_shards,
            message_router,
        }
    }
}

#[tonic::async_trait]
impl AdminService for MyAdminService {
    async fn terminate(
        &self,
        request: Request<proto::TerminateRequest>,
    ) -> Result<Response<proto::TerminateResponse>, Status> {
        let destroy_database = request.get_ref().destroy_database;

        if destroy_database {
            if let Err(err) = self.db_manager.schedule_destruction() {
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

        let response = Response::new(proto::TerminateResponse {});
        Ok(response)
    }

    async fn submit_on_chain_event(
        &self,
        request: Request<OnChainEvent>,
    ) -> Result<Response<OnChainEvent>, Status> {
        info!("Received call to [submit_on_chain_event] RPC");

        let onchain_event = request.into_inner();

        let fid = onchain_event.fid;
        if fid == 0 {
            return Err(Status::invalid_argument(
                "no fid or invalid fid".to_string(),
            ));
        }

        let dst_shard = self.message_router.route_message(fid, self.num_shards);

        let sender = match self.shard_senders.get(&dst_shard) {
            Some(sender) => sender,
            None => {
                return Err(Status::invalid_argument(
                    "no shard sender for fid".to_string(),
                ))
            }
        };

        let result = sender
            .messages_tx
            .send(MempoolMessage::ValidatorMessage(ValidatorMessage {
                on_chain_event: Some(onchain_event.clone()),
                fname_transfer: None,
            }))
            .await;

        match result {
            Ok(()) => {
                let response = Response::new(onchain_event);
                Ok(response)
            }
            Err(err) => Err(Status::from_error(Box::new(err))),
        }
    }
}
