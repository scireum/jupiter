use std::fmt;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub fn format_micros(micros: i32, f: &mut dyn fmt::Write) -> fmt::Result {
    return if micros < 1_000 {
        write!(f, "{} us", micros)
    } else if micros < 10_000 {
        write!(f, "{:.2} ms", micros as f64 / 1_000.)
    } else if micros < 100_000 {
        write!(f, "{:.1} ms", micros as f64 / 1_000.)
    } else if micros < 1_000_000 {
        write!(f, "{} ms", micros / 1_000)
    } else if micros < 10_000_000 {
        write!(f, "{:.2} s", micros as f64 / 1_000_000.)
    } else if micros < 100_000_000 {
        write!(f, "{:.1} s", micros as f64 / 1_000_000.)
    } else {
        write!(f, "{} s", micros / 1_000_000)
    };
}

pub struct Watch {
    start: Instant
}

impl Watch {
    pub fn start() -> Watch {
        Watch { start: Instant::now() }
    }

    pub fn micros(&self) -> i32 {
        self.start.elapsed().as_micros() as i32
    }
}

impl Display for Watch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let micros = self.micros();
        format_micros(micros, f)
    }
}

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

        return (sum, count);
    }

    pub fn add(&self, value: i32) {
        self.count.fetch_add(1, Ordering::Relaxed);

        let (mut sum, mut count) = self.sum_and_count();

        while count > 100 || sum as i64 + value as i64 > std::i32::MAX as i64 {
            sum = count / 2 * sum / count;
            count = count / 2;
        }

        sum += value;
        count += 1;

        let next_sum_and_count = (sum as u64 & 0xFFFFFFFF) << 32 | (count as u64 & 0xFFFFFFFF);
        self.sum_and_count.store(next_sum_and_count, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn avg(&self) -> i32 {
        let (sum, count) = self.sum_and_count();

        return if sum == 0 {
            0
        } else {
            sum / count
        };
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
    use crate::watch::{Average, format_micros, Watch};

    fn format(micros: i32) -> String {
        let mut result = String::new();
        format_micros(micros, &mut result).unwrap_or_default();

        return result;
    }

    #[test]
    fn formatting_works_as_expected() {
        let w = Watch::start();
        println!("{}", w);
        assert_eq!(format(1), "1 us");
        assert_eq!(format(12), "12 us");
        assert_eq!(format(123), "123 us");
        assert_eq!(format(1230), "1.23 ms");
        assert_eq!(format(12300), "12.3 ms");
        assert_eq!(format(123000), "123 ms");
        assert_eq!(format(1230000), "1.23 s");
        assert_eq!(format(12300000), "12.3 s");
        assert_eq!(format(123000000), "123 s");
        println!("{}", w);
    }

    #[test]
    fn empty_average_is_properly_initialized() {
        let avg = Average::new();
        assert_eq!(avg.avg(), 0);
        assert_eq!(avg.count(), 0);
    }

    #[test]
    fn average_with_some_values_works() {
        let avg = Average::new();
        for i in 1..=10 { avg.add(i); }
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
        for i in 1..=1000 { avg.add(i); }
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
