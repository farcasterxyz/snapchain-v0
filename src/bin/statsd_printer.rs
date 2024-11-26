use clap::Parser;
use std::collections::HashSet;
use std::net::UdpSocket;

/// A simple StatsD UDP listener and parser
#[derive(Parser)]
struct Args {
    /// Don't print metric values
    #[arg(long)]
    no_values: bool,

    /// Print each metric name and type only once
    #[arg(long)]
    unique: bool,
}

fn parse_and_print_metric(
    metric: &str,
    name_only: bool,
    unique: bool,
    seen_metrics: &mut HashSet<(String, String)>,
) {
    let parts: Vec<&str> = metric.split([':', '|']).collect();
    if parts.len() >= 3 {
        let name = parts[0].to_string();
        let metric_type = parts[2].to_string();

        if unique {
            if seen_metrics.contains(&(name.clone(), metric_type.clone())) {
                return;
            }
            seen_metrics.insert((name.clone(), metric_type.clone()));
        }

        let value = parts[1];

        if name_only {
            println!("{:<2} {}", metric_type, name);
        } else {
            println!("{:<2} {:>10} {}", metric_type, value, name);
        }
    }
}

fn main() {
    let args = Args::parse();
    let socket = UdpSocket::bind("0.0.0.0:8125").unwrap();
    let mut buf = vec![0u8; 65536];
    let mut seen_metrics = HashSet::new();

    loop {
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        let data = &buf[..size];
        let message = String::from_utf8_lossy(data);

        for line in message.lines() {
            if !line.trim().is_empty() {
                parse_and_print_metric(line, args.no_values, args.unique, &mut seen_metrics);
            }
        }
    }
}
