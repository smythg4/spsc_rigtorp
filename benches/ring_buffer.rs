use spsc_rigtorp;
use std::thread;

const RING_BUFFER_CAPACITY: usize = 100_000;
const NUM_ITEMS: usize = 100_000_000;

fn bench_cached_version() {
    let (tx, rx) = spsc_rigtorp::channel(RING_BUFFER_CAPACITY);

    let read_jh = thread::spawn(move || {
        let mut reads = 0;
        while reads < NUM_ITEMS {
            match rx.recv() {
                None => continue,
                Some(_) => reads += 1,
            }
        }
        reads
    });
    let start = std::time::Instant::now();
    let mut writes = 0;
    while writes < NUM_ITEMS {
        match tx.send(writes) {
            None => writes += 1,
            Some(_) => continue,
        }
    }

    let r = read_jh.join().expect("Failed to join read join handle");
    let run_time = start.elapsed();
    let w = writes;
    assert_eq!(r, w, "Reads didn't equal writes!");

    let ops_rate = NUM_ITEMS as u128 * 1_000_000_000 / run_time.as_nanos();
    println!(
        "With Caching:\t\t{} ops / second (total time = {:?})",
        ops_rate, run_time
    );
}

fn bench_no_cache_version() {
    let (tx, rx) = spsc_rigtorp::channel(RING_BUFFER_CAPACITY);

    let read_jh = thread::spawn(move || {
        let mut reads = 0;
        while reads < NUM_ITEMS {
            match rx.recv_no_cache() {
                None => continue,
                Some(_) => reads += 1,
            }
        }
        reads
    });
    let start = std::time::Instant::now();

    let mut writes = 0;
    while writes < NUM_ITEMS {
        match tx.send_no_cache(writes) {
            None => writes += 1,
            Some(_) => continue,
        }
    }

    let r = read_jh.join().expect("Failed to join read join handle");
    let run_time = start.elapsed();
    let w = writes;
    assert_eq!(r, w, "Reads didn't equal writes!");

    let ops_rate = NUM_ITEMS as u128 * 1_000_000_000 / run_time.as_nanos();
    println!(
        "Without Caching:\t{} ops / second (total time = {:?})",
        ops_rate, run_time
    );
}

fn bench_blocking() {
    let (tx, rx) = spsc_rigtorp::channel(RING_BUFFER_CAPACITY);

    let read_jh = thread::spawn(move || {
        let mut reads = 0;
        while reads < NUM_ITEMS {
            let _ = rx.recv_blocking();
            reads += 1;
        }
        reads
    });
    let start = std::time::Instant::now();

    let mut writes = 0;
    while writes < NUM_ITEMS {
        let _ = tx.send_blocking(writes);
        writes += 1;
    }

    let r = read_jh.join().expect("Failed to join read join handle");
    let run_time = start.elapsed();
    let w = writes;
    assert_eq!(r, w, "Reads didn't equal writes!");

    let ops_rate = NUM_ITEMS as u128 * 1_000_000_000 / run_time.as_nanos();
    println!(
        "Blocking With Caching:\t{} ops / second (total time = {:?})",
        ops_rate, run_time
    );
}

fn main() {
    bench_no_cache_version();
    bench_cached_version();
    bench_blocking();
}
