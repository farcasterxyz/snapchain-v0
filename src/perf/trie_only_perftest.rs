use crate::perf::generate;
use crate::perf::generate::{new_generator, GeneratorTypes};
use crate::storage::db;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::Context;
use crate::utils::statsd_wrapper;
use clap::Parser;
use std::collections::VecDeque;
use std::error::Error;
use std::net;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    branching_factor: u32,

    #[arg(long, default_value_t = 150)]
    messages_per_block: u32,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let turbohash = 1;

    let host = ("127.0.0.1", cadence::DEFAULT_PORT);
    let socket = net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = cadence::UdpMetricSink::from(host, socket)?;
    let statsd_client = cadence::StatsdClient::builder("trietest", sink).build();
    let statsd_client = statsd_wrapper::StatsdClientWrapper::new(statsd_client, true);

    let gen_type = GeneratorTypes::MultiUser;
    let mut gen = new_generator(gen_type, 1);

    let dir = TempDir::new_in(".")?; // TODO: config, etc.
    let db_path = dir.path().join("a.db");
    let db_path = db_path.to_str().unwrap();

    // TODO: branching factor metric
    let mut t = merkle_trie::MerkleTrie::new(args.branching_factor)?;
    let db = &db::RocksDB::new(db_path);
    db.open()?;
    t.initialize(db)?;

    let mut seq = 0u64;
    let mut message_queue: VecDeque<generate::NextMessage> = VecDeque::new();

    let start_time = Instant::now();
    let mut last_tick = Instant::now();

    let sdc = statsd_client.clone();
    let count_callback = move |read_count: (u64, u64)| {
        sdc.count_with_shard(1, "engine.trie.db_get_count.total", read_count.0);
        sdc.count_with_shard(1, "engine.trie.mem_get_count.total", read_count.1);
    };

    let mut ctx = Context::with_callback(count_callback.clone());
    loop {
        if last_tick.elapsed() >= Duration::from_secs(1) {
            let elapsed_secs = start_time.elapsed().as_secs();
            last_tick = Instant::now();

            let items = t.items()?;
            println!("Seconds since start: {}, items={}", elapsed_secs, items);
            statsd_client.gauge_with_shard(1, "engine.trie.num_items", items as u64);
            statsd_client.gauge_with_shard(
                1,
                "engine.trie.branching_factor",
                args.branching_factor as u64,
            );
            statsd_client.gauge_with_shard(1, "messages_per_block", args.branching_factor as u64);
            statsd_client.gauge_with_shard(1, "turbohash", turbohash as u64);

            ctx = Context::with_callback(count_callback.clone());
        }

        while message_queue.len() < args.messages_per_block as usize {
            seq += 1;
            let msgs = gen.next(seq);
            message_queue.extend(msgs);
        }

        let mut txn_batch = db::RocksDbTransactionBatch::new();

        loop {
            let msg = message_queue.pop_front();
            if msg.is_none() {
                break;
            }
            let msg = msg.unwrap();
            match msg {
                generate::NextMessage::Message(message) => {
                    t.insert(&ctx, db, &mut txn_batch, vec![message.hash.clone()])?;
                }
                generate::NextMessage::OnChainEvent(_) => {
                    // Ignore for now
                }
            }
        }

        db.commit(txn_batch)?;
        t.reload(db)?;
    }
}
