use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// A simple rate limiter that tracks the number of requests per IP address within a time window
pub struct IpRateLimiter {
    counts: HashMap<IpAddr, (u32, Instant)>,
    limit: u32,
    window: Duration,
}

impl IpRateLimiter {
    pub fn init(limit: u32, window: Duration) -> Self {
        Self {
            counts: HashMap::with_capacity(1 << 20),
            limit,
            window,
        }
    }

    #[inline(always)]
    /// Returns `true` if the connection is allowed, `false` if over limit
    pub fn check(&mut self, ip: IpAddr) -> bool {
        if self.limit == 0 {
            return false;
        }
        let now = Instant::now();
        // cleanup stale IPs to free memory
        if self.counts.len() > 8192 {
            let window = self.window;
            self.counts
                .retain(|_, (_, start)| now.duration_since(*start) < window);
        }
        match self.counts.get_mut(&ip) {
            // put the new IP with count 1 and current time.
            None => {
                self.counts.insert(ip, (1, now));
                true
            }
            // update the count and time if within window, otherwise reset
            Some((count, start)) => {
                if now.duration_since(*start) >= self.window {
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
