//! Provides formatting helpers for durations and byte sizes.
use std::fmt::Write;
use std::time::Duration;

/// Formats a duration given in microseconds.
///
/// This function determines the ideal unit (ranging from microseconds to seconds) to provide
/// a concise representation.
///
/// Note that a helper function [format_short_duration](format_short_duration) is also provided
/// which directly returns a String. This function also provides some examples.
pub fn format_micros(micros: i32, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
    if micros < 1_000 {
        write!(f, "{} us", micros)
    } else if micros < 10_000 {
        write!(f, "{:.2} ms", micros as f32 / 1_000.)
    } else if micros < 100_000 {
        write!(f, "{:.1} ms", micros as f32 / 1_000.)
    } else if micros < 1_000_000 {
        write!(f, "{} ms", micros / 1_000)
    } else if micros < 10_000_000 {
        write!(f, "{:.2} s", micros as f32 / 1_000_000.)
    } else if micros < 100_000_000 {
        write!(f, "{:.1} s", micros as f32 / 1_000_000.)
    } else {
        write!(f, "{} s", micros / 1_000_000)
    }
}

/// Formats a duration given in microseconds and returns a String representation.
///
/// This function determines the ideal unit (ranging from microseconds to seconds) to provide
/// a concise representation.
///
/// Note that a helper function [format_micros](format_micros) is also provided
/// which directly consumes a **std::fmt::Write**.
///
/// # Examples
///
/// ```
/// assert_eq!(jupiter::fmt::format_short_duration(100), "100 us");
/// assert_eq!(jupiter::fmt::format_short_duration(8_192), "8.19 ms");
/// assert_eq!(jupiter::fmt::format_short_duration(32_768), "32.8 ms");
/// assert_eq!(jupiter::fmt::format_short_duration(128_123), "128 ms");
/// assert_eq!(jupiter::fmt::format_short_duration(1_128_123), "1.13 s");
/// assert_eq!(jupiter::fmt::format_short_duration(10_128_123), "10.1 s");
/// assert_eq!(jupiter::fmt::format_short_duration(101_000_000), "101 s");
/// ```
pub fn format_short_duration(duration_in_micros: i32) -> String {
    let mut result = String::new();
    let _ = format_micros(duration_in_micros, &mut result);
    result
}

/// Formats a given size in bytes.
///
/// This function determines the ideal unit (ranging from bytes to petabytes) to provide
/// a concise representation.
///
/// Note that a helper function [format_size](format_size) is also provided
/// which directly returns a String. This function also provides some examples.
pub fn format_bytes(size_in_bytes: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
    if size_in_bytes == 1 {
        return write!(f, "1 byte");
    } else if size_in_bytes < 1024 {
        return write!(f, "{} bytes", size_in_bytes);
    }

    let mut magnitude = 0;
    let mut size = size_in_bytes as f32;
    while size > 1024. && magnitude < 5 {
        size /= 1024.;
        magnitude += 1;
    }

    if size <= 10. {
        write!(f, "{:.2} ", size)?;
    } else if size <= 100. {
        write!(f, "{:.1} ", size)?;
    } else {
        write!(f, "{:.0} ", size)?;
    }

    match magnitude {
        0 => write!(f, "Bytes"),
        1 => write!(f, "KiB"),
        2 => write!(f, "MiB"),
        3 => write!(f, "GiB"),
        4 => write!(f, "TiB"),
        _ => write!(f, "PiB"),
    }
}

/// Formats a given size in bytes.
///
/// This function determines the ideal unit (ranging from bytes to petabytes) to provide
/// a concise representation.
///
/// Note that a helper function [format_bytes](format_bytes) is also provided
/// which directly consumes a **std::fmt::Write**.
///
/// # Examples
///
/// ```
/// assert_eq!(jupiter::fmt::format_size(0), "0 bytes");
/// assert_eq!(jupiter::fmt::format_size(1), "1 byte");
/// assert_eq!(jupiter::fmt::format_size(100), "100 bytes");
/// assert_eq!(jupiter::fmt::format_size(8_734), "8.53 KiB");
/// assert_eq!(jupiter::fmt::format_size(87_340), "85.3 KiB");
/// assert_eq!(jupiter::fmt::format_size(873_400), "853 KiB");
/// assert_eq!(jupiter::fmt::format_size(8_734_000), "8.33 MiB");
/// assert_eq!(jupiter::fmt::format_size(87_340_000), "83.3 MiB");
/// assert_eq!(jupiter::fmt::format_size(873_400_000), "833 MiB");
/// assert_eq!(jupiter::fmt::format_size(8_734_000_000), "8.13 GiB");
/// assert_eq!(jupiter::fmt::format_size(87_340_000_000), "81.3 GiB");
/// assert_eq!(jupiter::fmt::format_size(873_400_000_000), "813 GiB");
/// assert_eq!(jupiter::fmt::format_size(8_734_000_000_000), "7.94 TiB");
/// assert_eq!(jupiter::fmt::format_size(87_340_000_000_000), "79.4 TiB");
/// assert_eq!(jupiter::fmt::format_size(873_400_000_000_000), "794 TiB");
/// assert_eq!(jupiter::fmt::format_size(8_734_000_000_000_000), "7.76 PiB");
/// assert_eq!(jupiter::fmt::format_size(87_340_000_000_000_000), "77.6 PiB");
/// assert_eq!(jupiter::fmt::format_size(873_400_000_000_000_000), "776 PiB");
/// ```
pub fn format_size(size_in_bytes: usize) -> String {
    let mut result = String::new();
    let _ = format_bytes(size_in_bytes, &mut result);

    result
}

/// Parses a file size from a given string.
///
/// This string can have the following suffixes:
/// * **k** or **K**: multiplies the given value by 1024 thus treats the value as KiB
/// * **m** or **M**: multiplies the given value by 1.048.576 thus treats the value as MiB
/// * **g** or **G**: multiplies the given value by 1.073.741.824 thus treats the value as GiB
/// * **t** or **T**: multiplies the given value by 1.099.511.627.776 thus treats the value as TiB
///
/// Returns an **Err** if either a non-integer value is given or if an unknow suffix was provided.
///
/// # Examples
///
/// ```
/// assert_eq!(jupiter::fmt::parse_size("100").unwrap(), 100);
/// assert_eq!(jupiter::fmt::parse_size("100b").unwrap(), 100);
/// assert_eq!(jupiter::fmt::parse_size("8k").unwrap(), 8192);
/// assert_eq!(jupiter::fmt::parse_size("8m").unwrap(), 8 * 1024 * 1024);
/// assert_eq!(jupiter::fmt::parse_size("4 G").unwrap(), 4 * 1024 * 1024 * 1024);
/// assert_eq!(jupiter::fmt::parse_size("3 T").unwrap(), 3 * 1024 * 1024 * 1024 * 1024);
///
/// // An invalid suffix results in an error...
/// assert_eq!(jupiter::fmt::parse_size("3 Y").is_err(), true);
///
/// // Decimal numbers result in an error...
/// assert_eq!(jupiter::fmt::parse_size("1.2g").is_err(), true);
///
/// // Negative numbers result in an error...
/// assert_eq!(jupiter::fmt::parse_size("-1").is_err(), true);
/// ```
pub fn parse_size(str: impl AsRef<str>) -> anyhow::Result<usize> {
    lazy_static::lazy_static! {
        static ref NUMBER_AND_SUFFIX: regex::Regex =
            regex::Regex::new(r"^ *(\d+) *([bBkKmMgGtT]?) *$").unwrap();
    }

    match NUMBER_AND_SUFFIX.captures(str.as_ref()) {
        Some(captures) => {
            let number = captures[1].parse::<usize>().unwrap();
            match &captures[2] {
                "k" | "K" => Ok(number * 1024),
                "m" | "M" => Ok(number * 1024 * 1024),
                "g" | "G" => Ok(number * 1024 * 1024 * 1024),
                "t" | "T" => Ok(number * 1024 * 1024 * 1024 * 1024),
                _ => Ok(number),
            }
        }
        None => Err(anyhow::anyhow!(
            "Cannot parse '{}' into a size expression.\
             Expected a positive number and optionally 'b', 'k', 'm', 'g' or 't' as suffix.",
            str.as_ref()
        )),
    }
}

/// Parses a duration from a given string.
///
/// This string can have the following suffixes:
/// * **ms** or **MS**: treats the value as milliseconds
/// * **s** or **S**: treats the value as seconds
/// * **m** or **M**: treats the value as minutes
/// * **h** or **H**: treats the value as hours
/// * **d** or **D**: treats the value as days
///
/// Returns an **Err** if either a non-integer value is given or if an unknow suffix was provided.
///
/// # Examples
///
/// ```
/// # use std::time::Duration;
/// assert_eq!(jupiter::fmt::parse_duration("100 ms").unwrap(), Duration::from_millis(100));
/// assert_eq!(jupiter::fmt::parse_duration("12 s").unwrap(), Duration::from_secs(12));
/// assert_eq!(jupiter::fmt::parse_duration("3 M").unwrap(), Duration::from_secs(3 * 60));
/// assert_eq!(jupiter::fmt::parse_duration("2 H").unwrap(), Duration::from_secs(2 * 60 * 60));
/// assert_eq!(jupiter::fmt::parse_duration("5 d").unwrap(), Duration::from_secs(5 * 24 * 60 * 60));
///
/// // An invalid suffix results in an error...
/// assert_eq!(jupiter::fmt::parse_duration("3 Y").is_err(), true);
///
/// // Decimal numbers result in an error...
/// assert_eq!(jupiter::fmt::parse_duration("1.2s").is_err(), true);
///
/// // Negative numbers result in an error...
/// assert_eq!(jupiter::fmt::parse_duration("-1m").is_err(), true);
/// ```
pub fn parse_duration(str: impl AsRef<str>) -> anyhow::Result<Duration> {
    lazy_static::lazy_static! {
        static ref NUMBER_AND_SUFFIX: regex::Regex =
            regex::Regex::new(r"^ *(\d+) *((ms|s|m|h|d|MS|S|M|H|D)?) *$").unwrap();
    }

    match NUMBER_AND_SUFFIX.captures(str.as_ref()) {
        Some(captures) => {
            let number = captures[1].parse::<u64>().unwrap();
            match &captures[2] {
                "s" | "S" => Ok(Duration::from_secs(number)),
                "m" | "M" => Ok(Duration::from_secs(number * 60)),
                "h" | "H" => Ok(Duration::from_secs(number * 60 * 60)),
                "d" | "D" => Ok(Duration::from_secs(number * 60 * 60 * 24)),
                _ => Ok(Duration::from_millis(number)),
            }
        }
        None => Err(anyhow::anyhow!(
            "Cannot parse '{}' into a duration expression.\
             Expected a positive number an optionally 'ms', 's', 'm', 'h' or 'd' as suffix.",
            str.as_ref()
        )),
    }
}

/// Formats a duration into a string like "5d 3h 17m 2s 12ms".
///
/// As the format indicates this is mostly used for "shorter" durations which rather run in
/// seconds or minutes rather than several days. However, if required, we can still format such
/// a value, even if outputting milliseconds in this case is kind of questionable.
///
/// Also note that for formatting sort durations (in microseconds) we have a dedicated function
/// named [format_short_duration](format_short_duration).
///
/// # Examples
///
/// ```
/// # use std::time::Duration;
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_millis(13)), "13ms");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_millis(1013)), "1s 13ms");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_millis(62_013)), "1m 2s 13ms");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_secs(60 * 32 + 13)), "32m 13s");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_secs(60 * 61)), "1h 1m");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_secs(4 * 60 * 60)), "4h");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_secs(24 * 60 * 60 + 60 * 60 + 60)), "1d 1h 1m");
/// assert_eq!(jupiter::fmt::format_duration(Duration::from_secs(24 * 60 * 60 + 60 * 60 + 59)), "1d 1h 59s");
/// ```
pub fn format_duration(duration: Duration) -> String {
    let mut result = String::new();

    let mut value = duration.as_millis();
    {
        let days = value / (1000 * 60 * 60 * 24);
        if days > 0 {
            let _ = write!(result, "{}d", days);
            value %= 1000 * 60 * 60 * 24;
        }
    }
    {
        let hours = value / (1000 * 60 * 60);
        if hours > 0 {
            if !result.is_empty() {
                result.push(' ');
            }
            let _ = write!(result, "{}h", hours);
            value %= 1000 * 60 * 60;
        }
    }
    {
        let minutes = value / (1000 * 60);
        if minutes > 0 {
            if !result.is_empty() {
                result.push(' ');
            }
            let _ = write!(result, "{}m", minutes);
            value %= 1000 * 60;
        }
    }
    {
        let seconds = value / 1000;
        if seconds > 0 {
            if !result.is_empty() {
                result.push(' ');
            }
            let _ = write!(result, "{}s", seconds);
            value %= 1000;
        }
    }
    if value > 0 {
        if !result.is_empty() {
            result.push(' ');
        }
        let _ = write!(result, "{}ms", value);
    }

    result
}
