use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// A simple rate limiter that tracks the number of requests per IP address within a time window
pub struct IpRateLimiter {
    counts: HashMap<IpAddr, (u32, Instant)>,
    limit: u32,
}

/// Rate window is fixed to 1 minute for ergonomics
const WINDOW: Duration = Duration::from_secs(60);

impl IpRateLimiter {
    pub fn init(limit: u32) -> Self {
        Self {
            counts: HashMap::with_capacity(1024),
            limit,
        }
    }

    #[inline(always)]
    /// Returns `true` if the connection is allowed, `false` if over limit, `limit == 0` means unlimited
    pub fn check(&mut self, ip: IpAddr) -> bool {
        if self.limit == 0 {
            return true;
        }
        let now = Instant::now();
        // cleanup stale IPs to free memory
        if self.counts.len() > 8192 {
            self.counts
                .retain(|_, (_, start)| now.duration_since(*start) < WINDOW);
        }
        match self.counts.get_mut(&ip) {
            // put the new IP with count 1 and current time.
            None => {
                self.counts.insert(ip, (1, now));
                true
            }
            // update the count and time if within window, otherwise reset
            Some((count, start)) => {
                if now.duration_since(*start) >= WINDOW {
                    *count = 1;
                    *start = now;
                    true
                } else if *count < self.limit {
                    *count += 1;
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[test]
fn allows_up_to_limit_then_denies() {
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut limiter = IpRateLimiter::init(3);
    assert!(limiter.check(ip));
    assert!(limiter.check(ip));
    assert!(limiter.check(ip));
    assert!(!limiter.check(ip));
}

#[test]
fn is_per_ip() {
    let a: IpAddr = "10.0.0.2".parse().unwrap();
    let b: IpAddr = "10.0.0.3".parse().unwrap();
    let mut limiter = IpRateLimiter::init(1);
    assert!(limiter.check(a));
    assert!(!limiter.check(a));
    assert!(limiter.check(b));
}

#[test]
fn zero_limit_is_unlimited() {
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut limiter = IpRateLimiter::init(0);
    assert!(limiter.check(ip));
    assert!(limiter.check(ip));
}
