pub mod cfg;
pub mod connectors;
pub mod consensus;
pub mod core;
pub mod network;
pub mod node;
pub mod storage;
pub mod utils;

mod tests;

pub mod proto {
    pub mod snapchain {
        tonic::include_proto!("snapchain");
    }

    pub mod rpc {
        tonic::include_proto!("rpc");
    }

    pub mod msg {
        tonic::include_proto!("msg");
    }

    pub mod hub_event {
        tonic::include_proto!("hub_event");
    }

    pub mod username_proof {
        tonic::include_proto!("username_proof");
    }

    pub mod sync_trie {
        tonic::include_proto!("sync_trie");
    }

    pub mod onchain_event {
        tonic::include_proto!("onchain_event");
    }
}
