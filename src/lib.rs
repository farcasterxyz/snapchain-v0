pub mod cfg;
pub mod connectors;
pub mod consensus;
pub mod core;
pub mod network;
pub mod node;

mod tests;

pub mod proto {
    pub mod snapchain {
        tonic::include_proto!("snapchain");
    }

    pub mod rpc {
        tonic::include_proto!("rpc");
    }

    pub mod message {
        tonic::include_proto!("message");
    }

    pub mod username_proof {
        tonic::include_proto!("username_proof");
    }
}
