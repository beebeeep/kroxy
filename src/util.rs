use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(crate) struct Tracker<T> {
    inner: Arc<T>,
    ttl: Duration,
    expire: Instant,
}

impl<T> Tracker<T> {
    pub(crate) fn new(t: T, ttl: Duration) -> Self {
        Self {
            inner: Arc::new(t),
            ttl,
            expire: Instant::now().checked_add(ttl).expect("wrong ttl"),
        }
    }

    pub(crate) fn claim(&mut self) -> Arc<T> {
        self.expire = Instant::now().checked_add(self.ttl).expect("wrong ttl");
        self.inner.clone()
    }

    pub(crate) fn touch(&mut self) {
        self.expire = Instant::now().checked_add(self.ttl).expect("wrong ttl");
    }

    pub(crate) fn is_expired(&self) -> bool {
        Instant::now() > self.expire
    }

    pub(crate) fn is_used(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }
}
