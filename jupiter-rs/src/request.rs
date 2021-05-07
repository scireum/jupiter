//! Provides a parser and wrapper for handling incoming RESP requests.
//!
//! A RESP request (that is "REdis Serialization Protocol") is quite simple. It starts with a
//! "*" followed by the number of arguments. For each argument, there is a "$" followed by the
//! number of bytes in the argument string followed by "\r\n". After the string data, yet another
//! CRLF (\r\n) is output.
//!
//! Therefore a simple request might look like:
//! * "PING" => `*1\r\n$4\r\nPING\r\n`
//! * "SET my-key 5 => `*3\r\n$3\r\SET\r\n$6\r\nmy-key\r\n$1\r\n5\r\n`
//!
//! As re receive these request via a network interface, which might also provide partial requests
//! we require a very fast and efficient algorithm to detect if the given data is a valid request
//! and what its parameters are.
//!
//! [Request::parse](Request::parse) implements this algorithm which can detect partial requests
//! in less than 100ns and also parse full requests well below 500ns! As internally all results are
//! only indices into the given byte buffer, only a single allocation for the list of offsets
//! is performed and no data is copied whatsoever.
//!
//! # Examples
//!
//! Parsing a simple request:
//! ```
//! # use bytes::BytesMut;
//! # use jupiter::request::Request;
//! let mut bytes = BytesMut::from("*1\r\n$4\r\nPING\r\n");
//! let result = Request::parse(&mut bytes).unwrap().unwrap();
//!
//! assert_eq!(result.command(), "PING");
//! assert_eq!(result.parameter_count(), 0);
//! ```
//!
//! Parsing a partial request:
//! ```
//! # use bytes::BytesMut;
//! # use jupiter::request::Request;
//! let mut bytes = BytesMut::from("*2\r\n$4\r\nPING\r\n$7\r\nTESTP");
//! let result = Request::parse(&mut bytes).unwrap();
//!
//! assert_eq!(result.is_none(), true);
//! ```
//!
//! Parsing an invalid request:
//! ```
//! # use bytes::BytesMut;
//! # use jupiter::request::Request;
//! let mut bytes = BytesMut::from("$4\r\nPING\r\n");
//! let result = Request::parse(&mut bytes);
//!
//! assert_eq!(result.is_err(), true);
//! ```
//!
//! Building a request for test environments:
//! ```
//! # use jupiter::request::Request;
//! let request = Request::example(vec!("PING"));
//! assert_eq!(request.command(), "PING");
//! ```
use std::fmt::{Display, Formatter};

use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};

/// Provides an internal representation of either the command or a parameter.
///
/// We only need to keep the byte offsets around as the underlying byte buffer is tied to
/// the request anyway.
#[derive(Copy, Clone, Debug)]
struct Range {
    start: usize,
    end: usize,
}

impl Range {
    /// Computes the start of the subsequent range by skipping over the CRLF.
    fn next_offset(&self) -> usize {
        self.end + 3
    }
}

impl Display for Range {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

/// Represents a parsed RESP request.
///
/// Note that we treat the 1st parameter as "command" and re-number all other parameters
/// accordingly. Therefore "SET x y" will have "SET" as command, "x" as first parameter
/// (index: 0) and "y" as second (index: 1).
pub struct Request {
    len: usize,
    data: Bytes,
    command: Range,
    arguments: Vec<Range>,
}

impl Request {
    const DOLLAR: u8 = b'$';
    const ASTERISK: u8 = b'*';
    const CR: u8 = b'\r';
    const ZERO_DIGIT: u8 = b'0';
    const NINE_DIGIT: u8 = b'9';

    /// Tries to parse a RESP request from the given byte buffer.
    ///
    /// If malformed data is detected, we return an **Err**. Otherwise we either return an
    /// empty optional, in case only a partial request is present or otherwise a full request
    /// which has then the form `Ok(Some(Request))`.
    ///
    ///
    /// # Examples
    ///
    /// Parsing a simple request:
    /// ```
    /// # use bytes::BytesMut;
    /// # use jupiter::request::Request;
    /// let mut bytes = BytesMut::from("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n");
    /// let result = Request::parse(&mut bytes).unwrap().unwrap();
    ///
    /// assert_eq!(result.command(), "SET");
    /// assert_eq!(result.parameter_count(), 2);
    /// assert_eq!(result.str_parameter(0).unwrap(), "x");
    /// assert_eq!(result.str_parameter(1).unwrap(), "y");
    /// assert_eq!(result.str_parameter(2).is_err(), true);
    /// ```
    pub fn parse(data: &BytesMut) -> anyhow::Result<Option<Request>> {
        let len = data.len();

        // Abort as early as possible if a partial request is present...
        if len < 4 || data[data.len() - 2] != Request::CR {
            Ok(None)
        } else {
            Request::parse_inner(data)
        }
    }

    /// Provides a helper function to create an example request in test environments.
    ///
    /// # Example
    /// ```
    /// # use jupiter::request::Request;
    /// let request = Request::example(vec!("PING"));
    /// assert_eq!(request.command(), "PING");
    /// ```
    pub fn example(data: Vec<&str>) -> Request {
        let mut input = String::new();
        input.push_str(&format!("*{}\r\n", data.len()));
        for param in data {
            input.push_str(&format!("${}\r\n{}\r\n", param.len(), param));
        }

        Request::parse(&BytesMut::from(input.as_str()))
            .unwrap()
            .unwrap()
    }

    fn parse_inner(data: &BytesMut) -> anyhow::Result<Option<Request>> {
        // Check general validity of the request...
        let mut offset = 0;
        if data[0] != Request::ASTERISK {
            return Err(anyhow!("A request must be an array of bulk strings!"));
        } else {
            offset += 1;
        }

        // Parse the number of arguments...
        let (mut num_args, range) = match Request::read_int(&data, offset)? {
            Some((num_args, range)) => (num_args - 1, range),
            _ => return Ok(None),
        };
        offset = range.next_offset();

        // Parse the first parameter as "command"...
        let command = match Request::read_bulk_string(&data, offset)? {
            Some(range) => range,
            _ => return Ok(None),
        };
        offset = command.next_offset();

        // Parse the remaining arguments...
        let mut arguments = Vec::with_capacity(num_args as usize);
        while num_args > 0 {
            if let Some(range) = Request::read_bulk_string(&data, offset)? {
                arguments.push(range);
                num_args -= 1;
                offset = range.next_offset();
            } else {
                return Ok(None);
            }
        }

        Ok(Some(Request {
            len: offset,
            data: data.clone().freeze(),
            command,
            arguments,
        }))
    }

    /// Tries to parse a number.
    ///
    /// This is either the number of arguments or the length of an argument string. Note that
    /// the algorithm and also the return type is a bit more complex as we have to handle the
    /// happy path (valid number being read) as well as an error (invalid number found) and also
    /// the partial request case (we didn't discover the final CR which marks the end of the
    /// number).
    fn read_int(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<(i32, Range)>> {
        let mut value: i32 = 0;
        let mut index = offset;
        while index < buffer.len() {
            let digit = buffer[index];
            if digit == Request::CR {
                return if buffer.len() > index {
                    Ok(Some((
                        value,
                        Range {
                            start: offset,
                            end: index - 1,
                        },
                    )))
                } else {
                    Ok(None)
                };
            }
            if !(Request::ZERO_DIGIT..=Request::NINE_DIGIT).contains(&digit) {
                return Err(anyhow!("Malformed integer at position {}", index));
            }

            value = value * 10 + (digit - Request::ZERO_DIGIT) as i32;
            index += 1;
        }

        Ok(None)
    }

    fn read_bulk_string(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<Range>> {
        if offset >= buffer.len() {
            return Ok(None);
        }
        if buffer[offset] != Request::DOLLAR {
            return Err(anyhow!("Expected a bulk string at {}", offset));
        }

        if let Some((length, range)) = Request::read_int(buffer, offset + 1)? {
            let next_offset = range.next_offset();
            if buffer.len() >= next_offset + length as usize + 2 {
                return Ok(Some(Range {
                    start: next_offset,
                    end: next_offset + length as usize - 1,
                }));
            }
        }

        Ok(None)
    }

    /// Returns the command in the request (this is the first parameter).
    pub fn command(&self) -> &str {
        std::str::from_utf8(&self.data[self.command.start..=self.command.end]).unwrap()
    }

    /// Returns the number of parameters (not counting the command itself).
    pub fn parameter_count(&self) -> usize {
        self.arguments.len()
    }

    /// Returns the n-th parameter (not including the command).
    ///
    /// Returns an **Err** if the requested index is outside of the range of detected
    /// parameters.
    pub fn parameter(&self, index: usize) -> Result<Bytes> {
        if index < self.arguments.len() {
            Ok(self
                .data
                .slice(self.arguments[index].start..=self.arguments[index].end))
        } else {
            Err(anyhow!(
                "Invalid parameter index {} (only {} are present)",
                index,
                self.arguments.len()
            ))
        }
    }

    /// Returns the n-th parameter as UTF-8 string.
    ///
    /// Returns an **Err** if either the requested index is out of range or if the parameter
    /// data isn't a valid UTF-8 sequence.
    pub fn str_parameter(&self, index: usize) -> Result<&str> {
        if index < self.arguments.len() {
            let range = self.arguments[index];
            std::str::from_utf8(&self.data[range.start..=range.end]).with_context(|| {
                format!(
                    "Failed to parse parameter {} (range {}) as UTF-8 string!",
                    index, range
                )
            })
        } else {
            Err(anyhow!(
                "Invalid parameter index {} (only {} are present)",
                index,
                self.arguments.len()
            ))
        }
    }

    /// Returns the n-th parameter as integer.
    ///
    /// Returns an **Err** if either the requested index is out of range or if the parameter
    /// data isn't a valid integer number.
    pub fn int_parameter(&self, index: usize) -> Result<i32> {
        let string = self.str_parameter(index)?;
        string.parse().with_context(|| {
            format!(
                "Failed to parse parameter {} ('{}') as integer!",
                index, string
            )
        })
    }

    /// Returns the total length on bytes for this request.
    pub fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use crate::request::Request;
    use bytes::BytesMut;

    #[test]
    fn a_command_is_successfully_parsed() {
        let request = Request::parse(&mut BytesMut::from(
            "*3\r\n$10\r\ntest.hello\r\n$5\r\nWorld\r\n$2\r\n42\r\n",
        ))
        .unwrap()
        .unwrap();

        assert_eq!(request.parameter_count(), 2);
        assert_eq!(request.command(), "test.hello");

        assert_eq!(request.str_parameter(0).unwrap(), "World");
        assert_eq!(
            std::str::from_utf8(request.parameter(0).unwrap().as_ref()).unwrap(),
            "World"
        );
        assert_eq!(request.int_parameter(1).unwrap(), 42);

        assert_eq!(request.str_parameter(2).is_err(), true);
        assert_eq!(request.int_parameter(2).is_err(), true);
        assert_eq!(request.parameter(2).is_err(), true);
    }

    #[test]
    fn missing_array_is_detected() {
        let result = Request::parse(&mut BytesMut::from("+GET\r\n"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn non_bulk_string_is_detected() {
        let result = Request::parse(&mut BytesMut::from("*1\r\n+GET\r\n"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn invalid_number_is_detected() {
        let result = Request::parse(&mut BytesMut::from("*GET\r\n"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn an_incomplete_command_is_skipped() {
        {
            let result = Request::parse(&mut BytesMut::from("")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1\r")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1\r\n")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*2\r\n$10\r\ntest.h")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result =
                Request::parse(&mut BytesMut::from("*2\r\n$10\r\ntest.hello\r\n")).unwrap();
            assert_eq!(result.is_none(), true);
        }
    }
}
