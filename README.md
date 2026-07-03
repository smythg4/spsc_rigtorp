# spsc-queue

## Overview
A learning project exploring concurrent data structures in Rust. I followed Erik Rigtorp's [_Optimizing a ring buffer for throughput_](https://rigtorp.se/ringbuffer/) adapting the C++ code into Rust.

## Design
It's a classic ring buffer implementation for a Single Producer, Single Consumer queue (SPSC): heap allocated `Vec` with two `AtomicUsize` indices for read index and write index.

### Producer/Consumer Handles
I took it one step further to introduce `Producer<T>` and `Consumer<T>`. Each of these simply holds an `Arc<RingBuffer>` pointing to the same data structure. We do this so Rust's type system enforces the fact that we can have only one Producer and one Consumer at a time. Failure to do so will break the `RingBuffer`. I believe this is now a compile time guarantee.

### Cache-Aligned Indices
Rigtorp uses `alignas(64)` in his struct definitions in C++. I created a type `CacheAligned<T>` to accomplish the same goal. I implemented `Deref`, `DerefMut`, and `From<T>` for `CacheAligned<T>` for easier ergonomics, so it's relatively transparent.

### Cached Index Optimization
Following Rigtorp's lead, I added cached `write_idx` and `read_idx` values for the opposing thread to improve performance by reducing `Acquire` atomic reads.

Each time we do an `Acquire` load, we save the value in the cached value slot. When checking if the queue is empty or full, we first compare against the cached value, if that indicates full or empty, we then load the real value from the atomic and confirm it. If it's confirmed, we return an appropriate `None` in the event of popping off an empty queue (or block for that variety of the call) or `Some(val)` when pushing onto a full queue (or block for that variety of the call).

## Blocking Variant
I added a `Mutex` and `Condvar` to `RingBuffer` to be used for `send_blocking` and `recv_blocking` calls. These methods will block the thread in the event of an empty queue on `pop` or full queue on `push`.

This allows the `Consumer<T>` or `Producer<T>` thread to go to sleep until the condition changes. Let's think through the possibility for a deadlock: `pop` is called and the queue is empty, `pop` acquires the `Mutex` (the producer thread would have no reason to be holding it), hands it off to the `Condvar` with a call to `wait`, whenever we `push` we call `notify_one` on the `Condvar`. The pattern would be symmetrical from the perspective of the reader: `push` is called and the queue is full, `push` acquires the `Mutex` (the consumer thread would have no reason to be holding it), hands it off to the `Condvar` with a call to `wait`, whenever we `pop` we call `notify_one` on the `Condvar`.

All `Mutex` acquisitions (almost) immediately release, so I don't think it will deadlock.

Once a thread is woken up, it has to start its `pop` or `push` operation over again otherwise it'll read memory with outdated information (ask me how I know). We get around that by calling `return self.push_back_blocking(value);` or `return self.pop_front_blocking(value);` as soon as the lock is reacquired.

### Full vs Empty — The Wasted Slot
Like Rigtorp, I don't enforce power of 2 `capacity` and require at least one item to be unused in the queue. This distinguishes an empty queue from a full one. You'll notice in `push_back` that we check if `next_write_idx` equals `read_idx` (a gap of 1)
```
if next_write_idx == unsafe { *self.read_idx_cached.get() } {
    ...
}
```
which is our indication of a full queue. In `pop_front` we check if `read_idx` equals `write_idx` (a gap of 0)
```
if read_idx == unsafe { *self.write_idx_cached.get() } {
    ...
}
```
which is our indication of an empty queue.

### Memory Ordering
Because `pop_front` is only ever called by the `Consumer<T>` aka the reader thread, we can load `read_idx` using `Relaxed` memory ordering. Same logic applies to `push_back`, the `Producer<T>` aka writer thread, and the load of `write_idx`.

We need to use `Acquire` ordering when we do the check of our `read_idx` or `write_idx` against the other thread's index. This is paired with a `Release` ordering that's used for our index incrementing and subsequent `store` operation.

## Usage
Create a channel with a defined capacity (in this case 4) using
```
// this channel only has 3 effective slots due to the wasted slot mentioned above
// `channel` will panic if you give it a capacity of 1 or 0
let (tx, rx) = spsc_rigtorp::channel(4);
```

Push something onto the queue using
```
// tx.send(val) will return an Option<T>, None means success and Some(val) gets you your value back on failure
let foo = tx.send(val);
```

Pop something off the queue using
```
// rx.recv() will return None if the queue is empty and Some(val) if there's something to grab.
let val = rx.recv();
```

## Performance
This still needs deeper exploration with criterion and flamegraph, but preliminary benchmarks for throughput are displayed below. I'm using a 2020 Apple M1 with 8 cores (4 Performance and 4 Efficiency).

I'd be curious to analyze lock contention on the blocking version. I believe it should be relatively light as the lock is acquired and almost immediately released in all cases.

I need to dig into the implementation of `std::sync::mpsc` to figure out why that blew me (and Rigtorp) out of the water!

* Without Caching Optimization:                  19,504,436 ops / second
* With Caching Optimization:                     20,068,849 ops / second
* Blocking Version With Caching Optimization:    19,611,095 ops / second
* `std::sync::mpsc::sync_channel()`:             **136,514,974 ops / second**
* `std::sync::mpsc::sync_channel()` non-blocking **141,972,115 ops / second**

Rigtorp reports benchmarks for his C++ implementation on an AMD Ryzen 9 3900X 12-Core Processor, placing the two threads on different chiplets / core complexes (CCX) to be:
* Without Caching Optimization: 5,513,850 ops / second
* With Caching Optimization:    112,287,037 ops / second

The Rust version dramatically out performs (~4x faster) when we look at the version without caching optimization throughput, but falls far short (~5.5x slower) compared to C++ with the optimization implemented. Perhaps it's an ARM/x86 difference? Perhaps the (minimal) overhead that comes with `UnsafeCell` for my cached items? There's always more to learn!

The blocking variant seems more or less inline with the other versions.

## Safety
`RingBuffer<T>` implements `Send` and `Sync` both assuming that `T` is `Send`. The compiler can't auto derive these marker traits because `UnsafeCell` is inherently `!Sync`.

We can safely assert this as we're limited to two threads (one for a `Producer<T>` and one for a `Consumer<T>`). The `Producer<T>` thread has exclusive access to `write_idx` and the `Consumer<T>` thread has exclusive access to `read_idx`.

Synchronization is required in the cases of an empty or full queue, which is discussed in the Memory Ordering section of this README.

`UnsafeCell` and `MaybeUninit` uses are documented with `SAFETY` comments in the source code.

## Limitations
* Doesn't batch pushes and pops, which could reduce the number of times we have to update the read and write indices.
* Capacity of 1 will cause a panic due to the wasted slot issue. This could be remedied by enforcing power of two sizes and a bitmask.
* No `async` support. That would be a fun exercise to port this to the `async` environment and deal with `Waker`
* Capacity is bounded at initialization. No growth or resizing allowed. We could allow resize calls if we find a performant way to block reads and writes while we allocate a new `Vec` and copy values over.

## References
* Erik Rigtorp [_Optimizing a ring buffer for throughput_](https://rigtorp.se/ringbuffer/)
* Brian Troutwine _Hands On Concurrency With Rust_ (a little dated, but good fundamentals)
* Mara Bos _Rust Atomics and Locks_