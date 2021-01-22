//! Provides a helper which computes a average of a series of values.
//!
//! This is intended to be used when measuring the system performance which is often tracked as
//! averages (e.g. execution duration of commands).
//!
//! An [Average](Average) is internally mutable without needing a mutable reference as we rely on
//! atomic intrinsics as provided by modern processors / compilers.
//!
//! # Example
//!
//! ```
//! # use jupiter::average::Average;
//! let avg = Average::new();
//! avg.add(10);
//! avg.add(20);
//! avg.add(30);
//!
//! assert_eq!(avg.avg(), 20);
//! assert_eq!(avg.count(), 3);
//! ```
use crate::fmt::format_micros;
use std::fmt;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};

/// Computes a sliding average of a series of values.
///
/// This is intended to record performance measurements and to keep track of the sliding average
/// as well as the total number of recorded values.
///
/// Note that this class overflows gracefully.
///
/// # Example
///
/// ```
/// # use jupiter::average::Average;
/// let avg = Average::new();
/// avg.add(10);
/// avg.add(20);
/// avg.add(30);
///
/// assert_eq!(avg.avg(), 20);
/// assert_eq!(avg.count(), 3);
/// ```
#[derive(Default)]
pub struct Average {
    sum_and_count: AtomicU64,
    count: AtomicU64,
}

impl Clone for Average {
    fn clone(&self) -> Self {
        Average {
            sum_and_count: AtomicU64::new(self.sum_and_count.load(Ordering::Relaxed)),
            count: AtomicU64::new(self.count.load(Ordering::Relaxed)),
        }
    }
}

impl Average {
    /// Creates a new average.
    pub fn new() -> Average {
        Average {
            sum_and_count: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn sum_and_count(&self) -> (i32, i32) {
        let last_sum_and_count = self.sum_and_count.load(Ordering::Relaxed);
        let count = (last_sum_and_count & 0xFFFFFFFF) as i32;
        let sum = ((last_sum_and_count >> 32) & 0xFFFFFFFF) as i32;

        (sum, count)
    }

    /// Adds another value to be added to the average calculation.
    ///
    /// Internally we simply update the global u64 counter to keep track of the total recorded
    /// values. Additionally, we have another u64 which is split into two i32 fields. One of these
    /// is used to keep the actual count of the sliding average and another is used to store the
    /// sum of the values.
    ///
    /// Whenever we recorded 100 values or the sum counter might overflow, we divide both values
    /// by two and add the new values. This yields a sliding average which is fit for our purposes.
    ///
    /// As the main task is to store the average duration of a task in microseconds, the i32 sum
    /// field shouldn't overflow under normal conditions.
    ///
    /// We perform this trickery (splitting a single field into two) so that this algorithm is
    /// completely lock and wait free, as we only utilize atomic load and store operations. This
    /// guarantees correctness while ensuring maximal performance.
    pub fn add(&self, value: i32) {
        self.count.fetch_add(1, Ordering::Relaxed);

        let (mut sum, mut count) = self.sum_and_count();

        while count > 100 || sum as i64 + value as i64 > std::i32::MAX as i64 {
            sum = count / 2 * sum / count;
            count /= 2;
        }

        sum += value;
        count += 1;

        let next_sum_and_count = (sum as u64 & 0xFFFFFFFF) << 32 | (count as u64 & 0xFFFFFFFF);
        self.sum_and_count
            .store(next_sum_and_count, Ordering::Relaxed);
    }

    /// Returns the total number of recorded values (unless an overflow of the internal u64 counter
    /// occurred).
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Computes the sliding average of the last 100 values.
    pub fn avg(&self) -> i32 {
        let (sum, count) = self.sum_and_count();

        if sum == 0 {
            0
        } else {
            sum / count
        }
    }
}

impl Display for Average {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_micros(self.avg(), f)?;
        write!(f, " ({})", self.count())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::average::Average;

    #[test]
    fn empty_average_is_properly_initialized() {
        let avg = Average::new();
        assert_eq!(avg.avg(), 0);
        assert_eq!(avg.count(), 0);
    }

    #[test]
    fn average_with_some_values_works() {
        let avg = Average::new();
        for i in 1..=10 {
            avg.add(i);
        }
        assert_eq!(avg.avg(), 5);
        assert_eq!(avg.count(), 10);
    }

    #[test]
    fn formatting_average_works() {
        let avg = Average::new();
        avg.add(10_123);
        assert_eq!(format!("{}", avg), "10.1 ms (1)");
    }

    #[test]
    fn average_with_many_values_keeps_count() {
        let avg = Average::new();
        for i in 1..=1000 {
            avg.add(i);
        }
        assert_eq!(avg.avg(), 928);
        assert_eq!(avg.count(), 1000);
    }

    #[test]
    fn average_overflows_sanely() {
        {
            let avg = Average::new();
            avg.add(std::i32::MAX);
            assert_eq!(avg.avg(), std::i32::MAX);
            avg.add(std::i32::MAX);
            assert_eq!(avg.avg(), std::i32::MAX);
            avg.add(std::i32::MAX / 2);
            avg.add(std::i32::MAX / 2);
            assert_eq!(avg.avg(), std::i32::MAX / 2);
        }
        {
            let avg = Average::new();
            avg.add(10);
            avg.add(std::i32::MAX - 50);
            avg.add(60);

            // If an overflow occurs, we compute the internal average (in this case the average of
            // 10 and std::i32::MAX - 50. We then accept the next value (60) and compute the average
            // between these two as result...
            let average_before_overflow = (std::i32::MAX - 50 + 10) / 2;
            let expected_average = (average_before_overflow + 60) / 2;
            assert_eq!(avg.avg(), expected_average);
        }
    }
}
