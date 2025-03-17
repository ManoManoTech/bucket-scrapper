use std::collections::VecDeque;
// src/utils/memory_limited_allocator.rs
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use rand::prelude::*;
use dashmap::DashMap;

// The core structure that tracks memory allocation
struct MemoryLimiter {
    total_capacity: usize,
    current_usage: usize,
    active_loans: usize,
    waiters: DashMap<u64, (usize, Waker)>,
}

impl MemoryLimiter {
    fn new(total_capacity: usize) -> Self {
        Self {
            total_capacity,
            current_usage: 0,
            active_loans: 0,
            waiters: DashMap::new(),
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
        let mut awaken_usage = 0;
        let mut waiters_to_pop = VecDeque::<u64>::new();
        for kv in self.waiters.iter() {
            let (waiter_id, (wait_size, waker)) = kv.pair();
            if self.current_usage + awaken_usage + *wait_size <= self.total_capacity {
                // This is likely to allocate properly
                waiters_to_pop.push_back(*waiter_id);
                awaken_usage += wait_size;
                waker.wake_by_ref();
            }
        }
        
        waiters_to_pop.iter().for_each(|waiter_id| {
            self.waiters.remove(&waiter_id);
        })
    }

    fn get_loan_count(&self) -> usize {
        self.active_loans
    }

    // Get number of waiters
    fn get_waiters_count(&self) -> usize {
        self.waiters.len()
    }

    fn get_waiters_total_size(&self) -> usize {
        self.waiters.iter().map(|kv| kv.value().0).sum()
    }
}

// Future for async allocation
struct AllocFuture {
    id: u64,
    limiter: Arc<Mutex<MemoryLimiter>>,
    size: usize,
}

impl Future for AllocFuture {
    type Output = LimitedVec;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
        limiter.waiters.insert(self.id, (self.size, cx.waker().clone()));
        Poll::Pending
    }
}

impl Drop for AllocFuture {
    fn drop(&mut self) {
        // Remove any lingering waker, if any
        let limiter = self.limiter.lock().unwrap();
        limiter.waiters.remove(&self.id);
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
        let mut rng = rand::rng();
        AllocFuture {
            id: rng.next_u64(),
            limiter: self.limiter.clone(),
            size,
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