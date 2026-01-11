use std::time::{Duration, Instant};

pub(crate) struct Tracker<T: Clone> {
    inner: T,
    ttl: Duration,
    expire: Instant,
    users: u32,
}

impl<T: Clone> Tracker<T> {
    pub(crate) fn new(t: T, ttl: Duration) -> Self {
        Self {
            inner: t,
            ttl,
            expire: Instant::now().checked_add(ttl).expect("wrong ttl"),
            users: 0,
        }
    }

    pub(crate) fn claim(&mut self) -> T {
        self.expire = Instant::now().checked_add(self.ttl).expect("wrong ttl");
        self.users += 1;
        self.inner.clone()
    }

    pub(crate) fn touch(&mut self) {
        self.expire = Instant::now().checked_add(self.ttl).expect("wrong ttl");
    }

    pub(crate) fn release(&mut self) {
        self.users = self.users.saturating_sub(1);
    }

    pub(crate) fn is_expired(&self) -> bool {
        Instant::now() > self.expire
    }

    pub(crate) fn is_used(&self) -> bool {
        self.users > 0
    }
}
