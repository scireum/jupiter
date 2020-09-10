use std::error::Error;
use std::fmt::{Display, Formatter, Write};

use anyhow::anyhow;
use bytes::BytesMut;

#[derive(Debug)]
pub enum OutputError {
    IOError(std::fmt::Error),
    ProtocolError(anyhow::Error),
}

impl From<std::fmt::Error> for OutputError {
    fn from(err: std::fmt::Error) -> OutputError {
        OutputError::IOError(err)
    }
}

impl From<anyhow::Error> for OutputError {
    fn from(err: anyhow::Error) -> OutputError {
        OutputError::ProtocolError(err)
    }
}

impl Display for OutputError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OutputError::IOError(e) => write!(f, "IO error: {:?}", e),
            OutputError::ProtocolError(e) => write!(f, "Protocol error: {:?}", e),
        }
    }
}

impl Error for OutputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            OutputError::IOError(ref e) => Some(e),
            OutputError::ProtocolError(_) => None,
        }
    }
}

pub type OutputResult = std::result::Result<(), OutputError>;

pub struct Response {
    buffer: BytesMut,
    nesting: Vec<i32>,
}

impl Response {
    pub fn new() -> Self {
        Response {
            buffer: BytesMut::with_capacity(8192),
            nesting: vec![1],
        }
    }

    fn check_nesting(&mut self) -> OutputResult {
        let current_nesting = match self.nesting.last_mut() {
            Some(level) => level,
            None => {
                return Err(OutputError::ProtocolError(anyhow!(
                    "Invalid result nesting!"
                )))
            }
        };

        *current_nesting -= 1;
        return if *current_nesting > 0 {
            Ok(())
        } else if *current_nesting == 0 {
            self.nesting.pop();
            Ok(())
        } else {
            Err(OutputError::ProtocolError(anyhow!(
                "Invalid result nesting!"
            )))
        };
    }

    #[inline]
    fn reserve(&mut self, required_length: usize) {
        let len = self.buffer.len();
        let rem = self.buffer.capacity() - len;

        if rem < required_length {
            self.reserve_inner(required_length);
        }
    }

    fn reserve_inner(&mut self, required_length: usize) {
        let required_blocks = (required_length / 8192) + 1;
        self.buffer.reserve(required_blocks * 8192);
    }

    pub fn complete(mut self) -> Result<BytesMut, OutputError> {
        if !self.nesting.is_empty() {
            return Err(OutputError::ProtocolError(anyhow!(
                "Invalid result nesting!"
            )));
        }

        self.nesting.push(1);
        Ok(self.buffer)
    }

    pub fn array(&mut self, items: i32) -> OutputResult {
        self.check_nesting()?;
        self.nesting.push(items);
        self.reserve(16);
        self.buffer.write_char('*')?;
        write!(self.buffer, "{}\r\n", items)?;
        Ok(())
    }

    pub fn ok(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(5);
        self.buffer.write_str("+OK\r\n")?;
        Ok(())
    }

    pub fn zero(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(4);
        self.buffer.write_str(":0\r\n")?;
        Ok(())
    }

    pub fn one(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(4);
        self.buffer.write_str(":1\r\n")?;
        Ok(())
    }

    pub fn number(&mut self, number: i32) -> OutputResult {
        if number == 0 {
            self.zero()
        } else if number == 1 {
            self.one()
        } else {
            self.check_nesting()?;
            self.reserve(16);
            self.buffer.write_char(':')?;
            write!(self.buffer, "{}\r\n", number)?;
            Ok(())
        }
    }

    pub fn boolean(&mut self, boolean: bool) -> OutputResult {
        self.number(if boolean { 1 } else { 0 })
    }

    pub fn simple(&mut self, string: &str) -> OutputResult {
        if string.len() == 0 {
            self.empty_string()
        } else {
            self.check_nesting()?;
            self.reserve(3 + string.len());
            self.buffer.write_char('+')?;
            self.buffer.write_str(string)?;
            self.buffer.write_str("\r\n")?;

            Ok(())
        }
    }

    pub fn empty_string(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3);
        self.buffer.write_str("+\r\n")?;

        Ok(())
    }

    pub fn bulk(&mut self, string: &str) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3 + 16 + string.len());
        self.buffer.write_char('$')?;
        write!(self.buffer, "{}\r\n", string.len())?;
        self.buffer.write_str(string)?;
        self.buffer.write_str("\r\n")?;

        Ok(())
    }

    pub fn error(&mut self, string: &str) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3 + string.len());
        self.buffer.write_char('-')?;
        self.buffer.write_str(
            string
                .to_owned()
                .replace(&"\r", " ")
                .replace(&"\n", " ")
                .as_str(),
        )?;
        self.buffer.write_str("\r\n")?;

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buffer[..]).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::request::Request;
    use crate::response::Response;

    #[test]
    fn an_array_of_bulk_strings_can_be_read_by_request() {
        let mut response = Response::new();
        response.array(2).unwrap();
        response.bulk("Hello").unwrap();
        response.bulk("World").unwrap();

        assert_eq!(response.as_str(), "*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n");

        let mut buffer = response.complete().unwrap();
        let request = Request::parse(&mut buffer).unwrap().unwrap();
        assert_eq!(request.command(), "Hello");
        assert_eq!(request.parameter_count(), 1);
        assert_eq!(request.str_parameter(0).unwrap(), "World");
    }

    #[test]
    fn errors_are_sanitized() {
        let mut response = Response::new();
        response.error("Error\nProblem").unwrap();

        assert_eq!(response.as_str(), "-Error Problem\r\n");
    }

    #[test]
    fn incorrect_nesting_is_detected() {
        {
            let mut response = Response::new();
            response.array(2).unwrap();
            response.ok().unwrap();
            assert_eq!(response.complete().is_err(), true);
        }
        {
            let mut response = Response::new();
            response.ok().unwrap();
            assert_eq!(response.ok().is_err(), true);
        }
        {
            let mut response = Response::new();
            response.array(1).unwrap();
            response.ok().unwrap();
            assert_eq!(response.ok().is_err(), true);
        }
    }

    #[test]
    fn dynamic_buffer_allocation_works() {
        let many_x = "X".repeat(16_000);
        let many_y = "Y".repeat(16_000);

        let mut response = Response::new();
        response.array(2).unwrap();
        response.simple(many_x.as_str()).unwrap();
        response.bulk(many_y.as_str()).unwrap();

        assert_eq!(
            response.as_str(),
            format!("*2\r\n+{}\r\n$16000\r\n{}\r\n", many_x, many_y)
        );
    }
}
