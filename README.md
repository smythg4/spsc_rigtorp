# spsc-queue

## Overview
A learning project exploring concurrent data structures in Rust. I followed Erik Rigtorp's [_Optimizing a ring buffer for throughput_](https://rigtorp.se/ringbuffer/) adapting the C++ code into Rust.

## Design
It's a classic ring buffer implementation for a Single Producer, Single Consumer queue (SPSC): heap allocated `Vec` with two `AtomicUsize` indices for read index and write index.

### Producer/Consumer Handles
I took it one step further to introduce `Producer<T>` and `Consumer<T>` types to let Rust's type system enforce the fact that we can have only one Producer and one Consumer at a time. Failure to do so will break the RingBuffer.

### Cache-Aligned Indices
Rigtorp uses `alignas(64)` in his struct definitions in C++. I created a type `CacheAligned<T>` to accomplish the same goal. I implemented `Deref`, `DerefMut`, and `From<T>` for `CacheAligned<T>` for easier ergonomics.

### Cached Index Optimization
Following his lead, I added the cached values to improve performance by reducing `Acquire` atomic reads. Each time we do an `Acquire` load, we save the value in the cached value slot. When checking if the queue is empty or full, we first compare against the cached value, if that indicates full or empty, we then load the real value and confirm it. If it's confirmed, we return an appropriate `None` in the event of popping off an empty queue or `Some(val)` when pushing onto a full queue.

### Full vs Empty — The Wasted Slot
Like Rigtorp, I don't enforce power of 2 `capacity` and require at least one item is unused to distinguish an empty queue from a full one. You'll notice in `push_back` that we check 
```
if next_write_idx == unsafe { *self.read_idx_cached.get() } {
    ...
}
```
This is our indication of a full queue. In `pop_front` we check
```
if read_idx == unsafe { *self.write_idx_cached.get() } {
    ...
}
```
Which is our indication of an empty queue.

### Memory Ordering
Because `pop_front` is only ever called by the `Consumer<T>` aka the reader thread, we can load `read_idx` using `Relaxed` memory ordering. Same logic applies to `push_back`, the `Producer<T>` aka writer thread and the load of `write_idx`.

We need to use `Acquire` ordering when we do the check of our `read_idx` or `write_idx` against the other thread's index. This is paired with a `Release` ordering that's used for our index incrementing and subsequent `store` operation.

## Usage
Create a channel with a defined capacity (in this case 4) using
```
let (tx, rx) = spsc_rigtorp::channel(4);
```

Push something onto the queue using
```
// tx.send(val) will return an Option<T>, None means success,
// Some(val) gets you your value back
let foo = tx.send(val);
```

Pop something off the queue using
```
// rx.recv() will return None if the queue is empty and Some(val) if there's something
// to grab.
let val = rx.recv();
```

## Performance
This still needs deeper exploration with criterion and flamegraph, but preliminary benchmarks reveal the following about throughput. I'm using a 2020 Apple M1 with 8 cores (4 Performance and 4 Efficiency).

* Without Caching Optimization: 19,504,436 ops / second
* With Caching Optimization:    20,068,849 ops / second

Rigtorp reports benchmarks for his C++ implementation on his AMD Ryzen 9 3900X 12-Core Processor placing the two threads on different chiplets / core complexes (CCX) to be:
* Without Caching Optimization: 5,513,850 ops / second
* With Caching Optimization:    112,287,037 ops / second

The Rust version dramatically out performs (~4x faster) when we look at the version without caching optimization throughput, but falls far short (~5.5x slower) compared to C++ with the optimization implemented. Perhaps it's an ARM/x86 difference? Perhaps the (minimal) overhead that comes with `UnsafeCell` for my cached items? There's always more to learn!

## Safety
`RingBuffer<T>` implements `Send` and `Sync` both assuming that `T` is `Send`. The compiler can't auto derive these marker traits because `UnsafeCell` is inherently `!Sync`. This is ok because we're limited to two threads (a `Producer<T>` and a `Consumer<T>`). `Producer<T>` has exclusive access to `write_idx` and `Consumer<T>` has exclusive access to `read_idx`. Synchronization is required in the cases of an empty or full queue, which is discussed in the Memory Ordering section of this README.

`UnsafeCell` and `MaybeUninit` uses are documented with `SAFETY` comments in the source code.

## Limitations
* Doesn't batch pushes and pops, which could reduce the number of times we have to update the read and write indices.
* Capacity of 1 will cause a panic due to the wasted slot issue. This could be remedied by enforcing power of two sizes and a bitmask.
* No `async` support. That would be a fun exercise to port this to the `async` environment and deal with `Waker`
* Capacity is bounded at initialization. No growth or resizing allowed at this juncture.

## References
* Erik Rigtorp [_Optimizing a ring buffer for throughput_](https://rigtorp.se/ringbuffer/)
* Brian Troutwine _Hands On Concurrency With Rust_ (a little dated, but good fundamentals)
* Mara Bos _Rust Atomics and Locks_