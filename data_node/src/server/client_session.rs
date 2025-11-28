use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ClientSession {
    pub max_inflight: usize,
    num_inflight: AtomicUsize,
}

impl ClientSession {
    pub fn new(max_inflight: usize) -> Self {
        Self {
            max_inflight,
            num_inflight: AtomicUsize::new(0),
        }
    }
    pub fn inc_inflight(&self) {
        self.num_inflight.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_inflight(&self) {
        self.num_inflight.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn has_request_cap(&self) -> bool {
        self.num_inflight.load(Ordering::Relaxed) < self.max_inflight
    }
}