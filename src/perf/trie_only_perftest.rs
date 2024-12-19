use crate::perf::generate;
use crate::perf::generate::{new_generator, GeneratorTypes};
use crate::storage::db;
use crate::storage::trie::merkle_trie;
use crate::storage::trie::merkle_trie::Context;
use crate::utils::statsd_wrapper;
use clap::Parser;
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{net, thread};
use tempfile::TempDir;

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    branching_factor: u32,

    #[arg(long, default_value_t = 150)]
    messages_per_block: u32,

    #[arg(long, default_value_t = String::from("."))]
    db_dir: String,

    #[arg(long, default_value_t = 1)]
    shard_count: u32,

    #[arg(long, default_value_t = 1_000_000)]
    users_per_shard: u32,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let turbohash = 1;

    let host = ("127.0.0.1", cadence::DEFAULT_PORT);
    let socket = net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = cadence::UdpMetricSink::from(host, socket)?;
    let statsd_client = cadence::StatsdClient::builder("trietest", sink).build();
    let statsd_client = Arc::new(statsd_wrapper::StatsdClientWrapper::new(
        statsd_client,
        true,
    ));

    let dir = TempDir::new_in(&args.db_dir)?;
    let base_path = dir.into_path();

    let mut handles = Vec::new();

    for shard_id in 0..args.shard_count {
        let shard_statsd = statsd_client.clone();
        let shard_args = args.clone();
        let shard_path = base_path.join(format!("shard_{}", shard_id));
        std::fs::create_dir_all(&shard_path)?;

        let handle = thread::spawn(move || {
            if let Err(e) = run_shard(shard_id, &shard_args, shard_statsd, &shard_path) {
                eprintln!("Error in shard {}: {:?}", shard_id, e);
            }
        });
        handles.push(handle);
    }

    {
        // common metrics thread
        let handle = thread::spawn(move || loop {
            statsd_client.gauge("branching_factor", args.branching_factor as u64);
            statsd_client.gauge("messages_per_block", args.messages_per_block as u64);
            statsd_client.gauge("turbohash", turbohash);
            statsd_client.gauge("users_per_shard", args.users_per_shard as u64);
            statsd_client.gauge("shard_count", args.shard_count as u64);

            thread::sleep(Duration::from_secs(1));
        });
        handles.push(handle);
    }

    // Join all shard threads
    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}

fn run_shard(
    shard_id: u32,
    args: &Args,
    statsd_client: Arc<statsd_wrapper::StatsdClientWrapper>,
    shard_path: &std::path::Path,
) -> Result<(), Box<dyn Error>> {
    let db_path = shard_path.join("db");
    let db_path_str = db_path.to_str().unwrap();

    let db = db::RocksDB::new(db_path_str);
    db.open()?;

    let mut t = merkle_trie::MerkleTrie::new(args.branching_factor)?;
    t.initialize(&db)?;

    let gen_type = GeneratorTypes::MultiUser;
    let mut gen = new_generator(
        gen_type,
        shard_id,
        generate::Config {
            users_per_shard: args.users_per_shard,
        },
    );

    let mut seq = 0u64;
    let mut message_queue: VecDeque<generate::NextMessage> = VecDeque::new();

    let start_time = Instant::now();
    let mut last_tick = Instant::now();

    let sdc = statsd_client.clone();
    let count_callback = move |read_count: (u64, u64)| {
        sdc.count_with_shard(shard_id, "engine.trie.db_get_count.total", read_count.0);
        sdc.count_with_shard(shard_id, "engine.trie.mem_get_count.total", read_count.1);
    };

    let mut ctx = Context::with_callback(count_callback.clone());

    loop {
        if last_tick.elapsed() >= Duration::from_secs(1) {
            let elapsed_secs = start_time.elapsed().as_secs();
            last_tick = Instant::now();

            let items = t.items()?;
            println!(
                "Shard {}: Seconds since start: {}, items={}",
                shard_id, elapsed_secs, items
            );
            statsd_client.gauge_with_shard(shard_id, "engine.trie.num_items", items as u64);
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
                    t.insert(&ctx, &db, &mut txn_batch, vec![message.hash.clone()])?;
                }
                generate::NextMessage::OnChainEvent(_) => {
                    // Ignore for now
                }
            }
        }

        db.commit(txn_batch)?;
        t.reload(&db)?;
    }
}
