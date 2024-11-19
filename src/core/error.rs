use crate::storage::db;
use std::error::Error;
use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
pub struct HubError {
    pub code: String,
    pub message: String,
}

impl HubError {
    pub fn validation_failure(error_message: &str) -> HubError {
        HubError {
            code: "bad_request.validation_failure".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn invalid_parameter(error_message: &str) -> HubError {
        HubError {
            code: "bad_request.invalid_param".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn internal_db_error(error_message: &str) -> HubError {
        HubError {
            code: "db.internal_error".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn not_found(error_message: &str) -> HubError {
        HubError {
            code: "not_found".to_string(),
            message: error_message.to_string(),
        }
    }

    pub fn invalid_internal_state(error_message: &str) -> HubError {
        HubError {
            code: "invalid_internal_state".to_string(),
            message: error_message.to_string(),
        }
    }
}

impl Error for HubError {}

impl Display for HubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.code, self.message)
    }
}

/** Convert RocksDB errors  */
impl From<rocksdb::Error> for HubError {
    fn from(e: rocksdb::Error) -> HubError {
        HubError {
            code: "db.internal_error".to_string(),
            message: e.to_string(),
        }
    }
}

// Convert RocksDB errors
impl From<db::RocksdbError> for HubError {
    fn from(e: db::RocksdbError) -> HubError {
        HubError {
            code: "db.internal_error".to_string(),
            message: e.to_string(),
        }
    }
}

/** Convert io::Result error type to HubError */
impl From<std::io::Error> for HubError {
    fn from(e: std::io::Error) -> HubError {
        HubError {
            code: "bad_request.io_error".to_string(),
            message: e.to_string(),
        }
    }
}

impl From<prost::DecodeError> for HubError {
    fn from(e: prost::DecodeError) -> HubError {
        HubError {
            code: "bad_request.decode_error".to_string(),
            message: e.to_string(),
        }
    }
}
