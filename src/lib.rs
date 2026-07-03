use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

// Rigtorp's C++ definition of `ringbuffer`
// struct ringbuffer {
//   std::vector<int> data_;
//   alignas(64) std::atomic<size_t> readIdx_{0};
//   alignas(64) std::atomic<size_t> writeIdx_{0};

//   ringbuffer(size_t capacity) : data_(capacity, 0) {}
// }

/// Returns a tuple of `Producer<T>` and `Consumer<T>`, accepts internal `RingBuffer`
/// capacity as an argument.
/// # Panics will `panic!` if `capacity` == 1 due to required dummy slot to distinguish
/// empty from full (see README)
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    RingBuffer::new_channels(capacity)
}

/// A wrapper around a `Arc<RingBuffer<T>>`, call `.send(val)` to push a value on to
/// the underlying `RingBuffer`
pub struct Producer<T> {
    buffer: Arc<RingBuffer<T>>,
}

impl<T> Producer<T> {
    /// Public API for accessing `RingBuffer`s internal `push_back()`
    pub fn send(&self, value: T) -> Option<T> {
        self.buffer.push_back(value)
    }

    /// Public API for accessing `RingBuffer`s internal `push_back_no_cache()`
    /// For benchmarking purposes only
    pub fn send_no_cache(&self, value: T) -> Option<T> {
        self.buffer.push_back_no_cache(value)
    }

    /// Public API for accessing `RingBuffer`s internal `push_back_blocking()`
    /// This version will block if the queue is full when you try to send
    pub fn send_blocking(&self, value: T) {
        self.buffer.push_back_blocking(value)
    }

    /// Convenience method to check if the queue is full before calling `.send(val)`
    /// Has side-effect of updating the internal cached index values
    pub fn is_full(&self) -> bool {
        self.buffer.is_full()
    }
}

/// A wrapper around a `Arc<RingBuffer<T>>`, call `.recv()` to pop a value off
/// the underlying `RingBuffer`
pub struct Consumer<T> {
    buffer: Arc<RingBuffer<T>>,
}

impl<T> Consumer<T> {
    /// Public API for accessing `RingBuffer`s internal `pop_front()`
    pub fn recv(&self) -> Option<T> {
        self.buffer.pop_front()
    }

    /// Public API for accessing `RingBuffer`s internal `pop_front_no_cache()`
    /// For benchmarking purposes only
    pub fn recv_no_cache(&self) -> Option<T> {
        self.buffer.pop_front_no_cache()
    }

    /// Public API for accessing `RingBuffer`s internal `pop_front_blocking()`
    /// This version will block if the queue is empty when you try to recv
    pub fn recv_blocking(&self) -> T {
        self.buffer.pop_front_blocking()
    }

    /// Convenience method to check if the queue is empty before calling `.recv()`
    /// Has side-effect of updating the internal cached index values
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

struct RingBuffer<T> {
    data: Vec<UnsafeCell<MaybeUninit<T>>>, // `MaybeUninit<T>` is similar to `Option<T>` without safety checks or overhead
    read_idx: CacheAligned<AtomicUsize>,
    read_idx_cached: CacheAligned<UnsafeCell<usize>>,
    write_idx: CacheAligned<AtomicUsize>,
    write_idx_cached: CacheAligned<UnsafeCell<usize>>,
    capacity: usize, // avoids repeated calls to `data.len()` during push and pop operations

    // used for blocking implementation
    mtx: Mutex<()>,
    cv: Condvar,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

/// Used to guarantee cache alignment to reduce cache coherency traffic
#[repr(align(64))]
#[derive(PartialEq, Eq)]
pub struct CacheAligned<T>(T);

impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for CacheAligned<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> RingBuffer<T> {
    /// One slot must be reserved for determining if the queue is full
    /// a capacity of 1 will lead to a persistently full and useless queue
    pub fn new_channels(capacity: usize) -> (Producer<T>, Consumer<T>) {
        assert!(
            capacity > 1,
            "Capacity of 1 will result in a full and useless RingBuffer"
        );
        let data = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        let p_buffer = Arc::new(RingBuffer {
            data,
            read_idx: AtomicUsize::new(0).into(),
            read_idx_cached: UnsafeCell::new(0).into(),
            write_idx: AtomicUsize::new(0).into(),
            write_idx_cached: UnsafeCell::new(0).into(),
            capacity,
            mtx: Mutex::new(()),
            cv: Condvar::new(),
        });
        let c_buffer = Arc::clone(&p_buffer);
        (Producer { buffer: p_buffer }, Consumer { buffer: c_buffer })
    }

    // Rigtorp's `push` implementation in C++ (without caching optimization)
    // bool push(int val) {
    //     auto const writeIdx = writeIdx_.load(std::memory_order_relaxed);
    //     auto nextWriteIdx = writeIdx + 1;
    //     if (nextWriteIdx == data_.size()) {
    //         nextWriteIdx = 0;
    //     }
    //     if (nextWriteIdx == readIdx_.load(std::memory_order_acquire)) {
    //         return false;
    //     }
    //     data_[writeIdx] = val;
    //     writeIdx_.store(nextWriteIdx, std::memory_order_release);
    //     return true;
    // }

    /// push_back accepts a `T` value and writes it to the writer end of the queue
    /// Returns `None` on success and `Some(value)` if full, giving the caller
    /// their value back
    fn push_back(&self, value: T) -> Option<T> {
        let write_idx = self.write_idx.load(Ordering::Relaxed);

        // if we've hit the end of the ring buffer we will wrap back around to the beginning
        let next_write_idx = (write_idx + 1) % self.capacity;

        // SAFETY: We know that `read_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if next_write_idx == unsafe { *self.read_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.read_idx_cached.get() = self.read_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if next_write_idx == unsafe { *self.read_idx_cached.get() } {
                // this means the buffer is full and we'll return the value back to the caller
                // notice that we're comparing `next_write_idx` and not `write_idx`, this is what
                // determines empty vs full. We will have one "dummy" slot in use to indicate full.
                return Some(value);
            }
        }
        // `.get()` to return the `*mut MaybeUninit`, then dereference that and call `.write(value)`
        // SAFETY: We know that we have memory allocated in every vector slot, it may be uninitialized
        // at this point, but we're writing good data into it
        unsafe { (*self.data[write_idx].get()).write(value) };
        // increment the `write_idx`
        self.write_idx.store(next_write_idx, Ordering::Release);
        None
    }

    /// same as push_back but doesn't leverage the internal read_idx_cached
    /// used for benchmarking only
    fn push_back_no_cache(&self, value: T) -> Option<T> {
        let write_idx = self.write_idx.load(Ordering::Relaxed);

        // if we've hit the end of the ring buffer we will wrap back around to the beginning
        let next_write_idx = (write_idx + 1) % self.capacity;

        if next_write_idx == self.read_idx.load(Ordering::Acquire) {
            // this means the buffer is full and we'll return the value back to the caller
            // notice that we're comparing `next_write_idx` and not `write_idx`, this is what
            // determines empty vs full. We will have one "dummy" slot in use to indicate full.
            return Some(value);
        }

        // `.get()` to return the `*mut MaybeUninit`, then dereference that and call `.write(value)`
        // SAFETY: We know that we have memory allocated in every vector slot, it may be uninitialized
        // at this point, but we're writing good data into it
        unsafe { (*self.data[write_idx].get()).write(value) };
        // increment the `write_idx`
        self.write_idx.store(next_write_idx, Ordering::Release);
        None
    }

    /// push_back_blocking accepts a `T` value and writes it to the writer end of the queue
    /// Returns nothing but blocks the thread until a `pop` is called if the queue is full
    fn push_back_blocking(&self, value: T) {
        let write_idx = self.write_idx.load(Ordering::Relaxed);

        // if we've hit the end of the ring buffer we will wrap back around to the beginning
        let next_write_idx = (write_idx + 1) % self.capacity;

        // SAFETY: We know that `read_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if next_write_idx == unsafe { *self.read_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.read_idx_cached.get() = self.read_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if next_write_idx == unsafe { *self.read_idx_cached.get() } {
                // this means the buffer is full we will put the thread to sleep
                let guard = self.mtx.lock().unwrap();
                let _guard = self.cv.wait(guard).unwrap();
                return self.push_back_blocking(value);
            }
        }
        // `.get()` to return the `*mut MaybeUninit`, then dereference that and call `.write(value)`
        // SAFETY: We know that we have memory allocated in every vector slot, it may be uninitialized
        // at this point, but we're writing good data into it
        unsafe { (*self.data[write_idx].get()).write(value) };
        // increment the `write_idx`
        self.write_idx.store(next_write_idx, Ordering::Release);
        self.cv.notify_one();
    }

    // Rigtorp's `pop` implementation in C++ (without caching optimization)
    // bool pop(int &val) {
    //     auto const readIdx = readIdx_.load(std::memory_order_relaxed);
    //     if (readIdx == writeIdx_.load(std::memory_order_acquire)) {
    //         return false;
    //     }
    //     val = data_[readIdx];
    //     auto nextReadIdx = readIdx + 1;
    //     if (nextReadIdx == data_.size()) {
    //         nextReadIdx = 0;
    //     }
    //     readIdx_.store(nextReadIdx, std::memory_order_release);
    //     return true;
    // }

    /// pop_front returns `Some(value)` on success (there was something on the queue)
    /// to pop and `None` if nothing is there to take
    pub fn pop_front(&self) -> Option<T> {
        let read_idx = self.read_idx.load(Ordering::Relaxed);
        // SAFETY: We know that `write_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if read_idx == unsafe { *self.write_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.write_idx_cached.get() = self.write_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if read_idx == unsafe { *self.write_idx_cached.get() } {
                // this indicates that the queue is empty (see note about full in the `push_back`)
                // implementation
                return None;
            }
        }
        // SAFETY: We know that a value is there (the queue isn't empty), which means something
        // was written to the memory address
        let val = unsafe { (*(self.data[read_idx].get())).assume_init_read() };

        // calculate the next read index and wrap around if it hits the end of the list.
        let next_read_idx = (read_idx + 1) % self.capacity;
        // increment the read index
        self.read_idx.store(next_read_idx, Ordering::Release);
        Some(val)
    }

    /// same as pop_front but doesn't leverage the internal write_idx_cached
    /// used for benchmarking only
    pub fn pop_front_no_cache(&self) -> Option<T> {
        let read_idx = self.read_idx.load(Ordering::Relaxed);
        if read_idx == self.write_idx.load(Ordering::Acquire) {
            // this indicates that the queue is empty (see note about full in the `push_back`)
            // implementation
            return None;
        }
        // SAFETY: We know that a value is there (the queue isn't empty), which means something
        // was written to the memory address
        let val = unsafe { (*(self.data[read_idx].get())).assume_init_read() };

        // calculate the next read index and wrap around if it hits the end of the list.
        let next_read_idx = (read_idx + 1) % self.capacity;
        // increment the read index
        self.read_idx.store(next_read_idx, Ordering::Release);
        Some(val)
    }

    /// pop_front_blocking returns the first value on the queue.
    /// It will block the thread if the queue is empty until woken up by a called to `push`
    pub fn pop_front_blocking(&self) -> T {
        let read_idx = self.read_idx.load(Ordering::Relaxed);
        // SAFETY: We know that `write_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if read_idx == unsafe { *self.write_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.write_idx_cached.get() = self.write_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if read_idx == unsafe { *self.write_idx_cached.get() } {
                // this indicates that the queue is empty (see note about full in the `push_back`)
                // implementation
                let guard = self.mtx.lock().unwrap();
                let _guard = self.cv.wait(guard).unwrap();
                return self.pop_front_blocking();
            }
        }
        // SAFETY: We know that a value is there (the queue isn't empty), which means something
        // was written to the memory address
        let val = unsafe { (*(self.data[read_idx].get())).assume_init_read() };

        // calculate the next read index and wrap around if it hits the end of the list.
        let next_read_idx = (read_idx + 1) % self.capacity;
        // increment the read index
        self.read_idx.store(next_read_idx, Ordering::Release);
        self.cv.notify_one();
        val
    }

    /// should only be called by the consumer thread to preserve exclusive access to `read_idx`
    /// this will be accessed by `Consumer<T>.is_empty()` and `Self::pop_front()`
    fn is_empty(&self) -> bool {
        let read_idx = self.read_idx.load(Ordering::Relaxed);
        // SAFETY: We know that `write_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if read_idx == unsafe { *self.write_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.write_idx_cached.get() = self.write_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if read_idx == unsafe { *self.write_idx_cached.get() } {
                // this indicates that the queue is empty (see note about full in the `push_back`)
                // implementation
                return true;
            }
        }
        false
    }

    /// should only be called by the producer thread to preserve exclusive access to `write_idx`
    /// this will be accessed by `Producer<T>.is_full()` and `Self::push_back()`
    fn is_full(&self) -> bool {
        let write_idx = self.write_idx.load(Ordering::Relaxed);

        // if we've hit the end of the ring buffer we will wrap back around to the beginning
        let next_write_idx = (write_idx + 1) % self.capacity;

        // SAFETY: We know that `read_idx_cached` holds quality data as it's
        // initialized on `RingBuffer` creation and is updated with atomic load operations.
        // Use of `UnsafeCell` is primarily to allow interior mutability with a `&self` method.
        if next_write_idx == unsafe { *self.read_idx_cached.get() } {
            // SAFETY: see above
            unsafe { *self.read_idx_cached.get() = self.read_idx.load(Ordering::Acquire) };
            // SAFETY: see above
            if next_write_idx == unsafe { *self.read_idx_cached.get() } {
                // this means the buffer is full and we'll return the value back to the caller
                // notice that we're comparing `next_write_idx` and not `write_idx`, this is what
                // determines empty vs full. We will have one "dummy" slot in use to indicate full.
                return true;
            }
        }
        false
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_operations_normal() {
        let num_writes = 2500;
        let (tx, rx) = channel(4);

        let wjh = thread::spawn(move || {
            let mut i = 0;
            while i < num_writes {
                match tx.send(format!("[{i}] String Item")) {
                    Some(n) => {
                        println!("Failed to push '{n}', trying again...");
                        thread::yield_now();
                        continue;
                    }
                    None => {
                        i += 1;
                    }
                }
            }
            i
        });

        let rjh = thread::spawn(move || {
            let mut received = 0;
            while received < num_writes {
                if let Some(n) = rx.recv() {
                    println!("Popped: '{n}'");
                    received += 1;
                }
            }
            received
        });

        let write_count = wjh.join().unwrap();
        let read_count = rjh.join().unwrap();
        assert_eq!(write_count, read_count);
    }

    #[test]
    fn test_basic_operations_no_cache() {
        let num_writes = 2500;
        let (tx, rx) = channel(4);

        let wjh = thread::spawn(move || {
            let mut i = 0;
            while i < num_writes {
                match tx.send_no_cache(format!("[{i}] String Item")) {
                    Some(n) => {
                        println!("Failed to push '{n}', trying again...");
                        thread::yield_now();
                        continue;
                    }
                    None => {
                        i += 1;
                    }
                }
            }
            i
        });

        let rjh = thread::spawn(move || {
            let mut received = 0;
            while received < num_writes {
                if let Some(n) = rx.recv_no_cache() {
                    println!("Popped: '{n}'");
                    received += 1;
                }
            }
            received
        });

        let write_count = wjh.join().unwrap();
        let read_count = rjh.join().unwrap();
        assert_eq!(write_count, read_count);
    }

    #[test]
    fn test_basic_operations_blocking() {
        let num_writes = 2500;
        let (tx, rx) = channel(4);

        let wjh = thread::spawn(move || {
            println!("Write thread spawned!");
            let mut i = 0;
            while i < num_writes {
                tx.send_blocking(format!("[{i}] String Item"));
                println!("Sent {i}");
                i += 1;
            }
            i
        });

        let rjh = thread::spawn(move || {
            println!("Read thread spawned!");
            let mut received = 0;
            while received < num_writes {
                let n = rx.recv_blocking();
                println!("Received: '{n}'");
                received += 1;
            }
            received
        });

        let write_count = wjh.join().unwrap();
        let read_count = rjh.join().unwrap();
        assert_eq!(write_count, read_count);
    }

    // used for `test_drop_works` and `TestVal` dummy type to ensure values actually get dropped
    // using this in other tests will cause problems without making some changes
    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    struct TestVal {}

    impl Drop for TestVal {
        fn drop(&mut self) {
            DROP_COUNT.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_drop_works() {
        let vec_size = 100;
        // build a channel with capacity 100
        let (tx, _rx) = channel(100);
        // put my custom type in a vector to ensure heap allocated objects
        // are actually dropped
        let holder = vec![TestVal {}; vec_size];
        // push 50 copies of `holder` into the channel (half the capacity)
        for _ in 0..50 {
            tx.send(holder.clone());
        }
        // tx and _rx drop here with 50 unconsumed vector values
        drop(tx);
        drop(_rx);

        // assert is happening before `holder` is dropped, so those 10 aren't counted in it
        // We should see 50 vectors with `vec_size` values in each
        assert_eq!(50 * vec_size, DROP_COUNT.load(Ordering::Relaxed));
    }
}
