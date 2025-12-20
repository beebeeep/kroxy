use std::time::{Duration, Instant};

pub(crate) struct Tracker<T: Clone> {
    inner: T,
    ttl: Duration,
    expire: Instant,
}

impl<T: Clone> Tracker<T> {
    pub(crate) fn new(t: T, ttl: Duration) -> Self {
        Self {
            inner: t,
            ttl,
            expire: Instant::now().checked_add(ttl).expect("wrong ttl"),
        }
    }

    pub(crate) fn claim(&mut self) -> T {
        self.expire = Instant::now().checked_add(self.ttl).expect("wrong ttl");
        self.inner.clone()
    }

    pub(crate) fn is_expired(&self) -> bool {
        Instant::now() > self.expire
    }
}
