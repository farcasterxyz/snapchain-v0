use snapchain::perf::trie_only_perftest;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    trie_only_perftest::run()
}
