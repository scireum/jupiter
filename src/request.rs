use std::fmt::{Display, Formatter};

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes, BytesMut};

#[derive(Copy, Clone, Debug)]
struct Range {
    start: usize,
    end: usize,
}

impl Range {
    fn next_offset(&self) -> usize {
        self.end + 3
    }
}

impl Display for Range {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

pub struct Request {
    data: Bytes,
    command: Range,
    arguments: Vec<Range>,
}

const DOLLAR: u8 = b'$';
const ASTERISK: u8 = b'*';
const CR: u8 = b'\r';
const ZERO_DIGIT: u8 = b'0';
const NINE_DIGIT: u8 = b'9';

fn read_int(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<(i32, Range)>> {
    let mut len: i32 = 0;
    let mut index = offset;
    while index < buffer.len() {
        let digit = buffer[index];
        if digit == CR {
            return if buffer.len() >= index + 1 {
                Ok(Some((
                    len,
                    Range {
                        start: offset,
                        end: index - 1,
                    },
                )))
            } else {
                Ok(None)
            };
        }
        if digit < ZERO_DIGIT || digit > NINE_DIGIT {
            return Err(anyhow!("Malformed integer at position {}", index));
        }

        len = len * 10 + (digit - ZERO_DIGIT) as i32;
        index += 1;
    }

    Ok(None)
}

fn read_bulk_string(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<Range>> {
    if offset >= buffer.len() {
        return Ok(None);
    }
    if buffer[offset] != DOLLAR {
        return Err(anyhow!("Expected a bulk string at {}", offset));
    }

    if let Some((length, range)) = read_int(buffer, offset + 1)? {
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

impl Request {
    #[inline]
    pub fn parse(data: &mut BytesMut) -> anyhow::Result<Option<Request>> {
        let len = data.len();

        if len < 4 || data[data.len() - 2] != b'\r' {
            Ok(None)
        } else {
            Request::parse_inner(data)
        }
    }

    fn parse_inner(data: &mut BytesMut) -> anyhow::Result<Option<Request>> {
        let mut offset = 0;
        if data[0] != ASTERISK {
            return Err(anyhow!("A request must be an array of bulk strings!"));
        } else {
            offset += 1;
        }

        let (mut num_args, range) = match read_int(&data, offset)? {
            Some((num_args, range)) => (num_args - 1, range),
            _ => return Ok(None),
        };
        offset = range.next_offset();

        let command = match read_bulk_string(&data, offset)? {
            Some(range) => range,
            _ => return Ok(None),
        };
        offset = command.next_offset();

        let mut arguments = Vec::with_capacity(num_args as usize);
        while num_args > 0 {
            if let Some(range) = read_bulk_string(&data, offset)? {
                arguments.push(range);
                num_args -= 1;
                offset = range.next_offset();
            } else {
                return Ok(None);
            }
        }

        let result_data = data.to_bytes();
        if offset >= data.len() {
            data.clear();
        } else {
            data.advance(offset);
        }

        Ok(Some(Request {
            data: result_data,
            command,
            arguments,
        }))
    }

    pub fn command(&self) -> &str {
        std::str::from_utf8(&self.data[self.command.start..=self.command.end]).unwrap()
    }

    pub fn parameter_count(&self) -> usize {
        self.arguments.len()
    }

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

    pub fn int_parameter(&self, index: usize) -> Result<i32> {
        let string = self.str_parameter(index)?;
        string.parse().with_context(|| {
            format!(
                "Failed to parse parameter {} ('{}') as integer!",
                index, string
            )
        })
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
