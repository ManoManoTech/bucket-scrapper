// src/utils/memory_limited_allocator.rs
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::collections::VecDeque;

// The core structure that tracks memory allocation
struct MemoryLimiter {
    total_capacity: usize,
    current_usage: usize,
    active_loans: usize,
    waiters: VecDeque<(usize, Waker)>,
}

impl MemoryLimiter {
    fn new(total_capacity: usize) -> Self {
        Self {
            total_capacity,
            current_usage: 0,
            active_loans: 0,
            waiters: VecDeque::new(),
        }
    }

    // Try to allocate memory
    fn try_allocate(&mut self, size: usize) -> bool {
        if size > (self.total_capacity as f64 * 0.85) as usize {
            panic!("Allocation of {} is unlikely to ever complete in a pool of {}, you must increase pool size", size, self.total_capacity);
        }

        if self.current_usage + size <= self.total_capacity {
            self.current_usage += size;
            self.active_loans += 1;
            true
        } else {
            false
        }
    }

    // Free memory
    fn free(&mut self, size: usize) {
        self.current_usage = self.current_usage.saturating_sub(size);
        self.active_loans = self.active_loans.saturating_sub(1);

        // Wake up waiters if possible
        let mut i = 0;
        while i < self.waiters.len() {
            let (wait_size, waker) = &self.waiters[i];
            if self.current_usage + wait_size <= self.total_capacity {
                // This waiter can be satisfied
                let (wait_size, waker) = self.waiters.remove(i).unwrap();
                self.current_usage += wait_size; // Pre-allocate for the waiter
                waker.wake_by_ref();
            } else {
                i += 1;
            }
        }
    }

    fn get_loan_count(&self) -> usize {
        self.active_loans
    }

    // Get number of waiters
    fn get_waiters_count(&self) -> usize {
        self.waiters.len()
    }

    fn get_waiters_total_size(&self) -> usize {
        self.waiters.iter().map(|(size, _)| size).sum()
    }
}

// Future for async allocation
struct AllocFuture {
    limiter: Arc<Mutex<MemoryLimiter>>,
    size: usize,
    waker: Option<Waker>,
}

impl Future for AllocFuture {
    type Output = LimitedVec;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Store the waker before locking to avoid borrow conflicts
        let waker = cx.waker().clone();
        self.waker = Some(waker.clone());

        let mut limiter = self.limiter.lock().unwrap();

        if limiter.try_allocate(self.size) {
            // We got our allocation
            return Poll::Ready(LimitedVec {
                vec: Vec::with_capacity(self.size),
                size: self.size,
                limiter: self.limiter.clone(),
            });
        }

        // Couldn't allocate, register waker
        limiter.waiters.push_back((self.size, waker));
        Poll::Pending
    }
}

impl Drop for AllocFuture {
    fn drop(&mut self) {
        if let Some(waker) = &self.waker {
            let mut limiter = self.limiter.lock().unwrap();
            // Remove our waiter if we're being dropped
            limiter.waiters.retain(|(_, w)| !waker.will_wake(w));
        }
    }
}

// The Vec wrapper that tracks and limits memory usage
pub struct LimitedVec {
    vec: Vec<u8>,
    size: usize, // The allocated size we're tracking
    limiter: Arc<Mutex<MemoryLimiter>>,
}

impl LimitedVec {
    // Access the underlying Vec<u8>
    pub fn as_vec(&self) -> &Vec<u8> {
        &self.vec
    }

    // Access the underlying Vec<u8> mutably
    pub fn as_vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.vec
    }
}

// When dropped, free the memory allocation in the limiter
impl Drop for LimitedVec {
    fn drop(&mut self) {
        if self.size > 0 {
            let mut limiter = self.limiter.lock().unwrap();
            limiter.free(self.size);
        }
    }
}

// Implement Deref and DerefMut to allow direct access to Vec methods
impl std::ops::Deref for LimitedVec {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl std::ops::DerefMut for LimitedVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.vec
    }
}

// The main entry point for memory-limited vector allocation
pub struct MemoryLimitedAllocator {
    limiter: Arc<Mutex<MemoryLimiter>>,
}

impl MemoryLimitedAllocator {
    pub fn new(total_capacity: usize) -> Self {
        Self {
            limiter: Arc::new(Mutex::new(MemoryLimiter::new(total_capacity))),
        }
    }

    // Allocate a vec of the given size, blocking if capacity limit is reached
    pub fn alloc_vec(&self, size: usize) -> impl Future<Output = LimitedVec> {
        AllocFuture {
            limiter: self.limiter.clone(),
            size,
            waker: None,
        }
    }

    // Try to allocate a vec immediately, returning None if at capacity
    pub fn try_alloc_vec(&self, size: usize) -> Option<LimitedVec> {
        let mut limiter = self.limiter.lock().unwrap();
        if limiter.try_allocate(size) {
            Some(LimitedVec {
                vec: Vec::with_capacity(size),
                size,
                limiter: self.limiter.clone(),
            })
        } else {
            None
        }
    }

    // Get current usage statistics
    pub fn stats(&self) -> (usize, usize) {
        let limiter = self.limiter.lock().unwrap();
        (limiter.current_usage, limiter.total_capacity)
    }

    // Get the number of waiters (tasks waiting for memory)
    pub fn waiters_count_and_size(&self) -> (usize, usize, usize) {
        let limiter = self.limiter.lock().unwrap();
        (limiter.get_waiters_count(), limiter.get_waiters_total_size(), limiter.get_loan_count())
    }
}