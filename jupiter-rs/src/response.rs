//! Represents a memory backed RESP response.
//!
//! We use an internal buffer here so that we can build the complete response in one go without
//! blocking and then push the whole thing into the network with a single sys-call.
//!
//! Therefore we pre-allocate a buffer of 8k and grow this if needed. Note that so far, no
//! intermediate write will happen and the whole response is buffered in memory. As we expect
//! responses to be small, this is a good approach - however, when trying to send gigabytes,
//! this isn't probably the way to go.
//!
//! # Example
//!
//! ```
//! # use std::error::Error;
//! # use jupiter::response::Response;
//! # use jupiter::response::OutputError;
//!
//! # fn main() -> Result<(), OutputError> {
//! let mut response = Response::new();
//! response.ok()?;
//! assert_eq!(response.complete_string()?, "+OK\r\n");
//!
//! # Ok(())
//! # }
//! ```
//!
use std::error::Error;
use std::fmt::{Display, Formatter, Write};

use anyhow::anyhow;
use bytes::BytesMut;

/// Enumerates the possible errors when creating a response.
#[derive(Debug)]
pub enum OutputError {
    /// Represents any IO or formatting error while generating the response.
    IOError(std::fmt::Error),

    /// Represents a protocol error which most probably indicates an invalid nesting
    /// (e.g. when providing too few or too many result entries for an array..).
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

/// Represents the result type for all output operations.
///
/// The operations itself don't generate a result but might emit an **OutputError**.
pub type OutputResult = std::result::Result<(), OutputError>;

/// Represents a RESP response being built.
///
/// This is the buffer in which the response is stored along with a control structure which monitors
/// the nesting of the result and ensures that only valid response will be generated.
#[derive(Default)]
pub struct Response {
    buffer: BytesMut,
    nesting: Vec<i32>,
}

/// Represnets a separator used when outputting management data.
pub static SEPARATOR: &str =
    "-------------------------------------------------------------------------------\n";

impl Response {
    /// Creates a new response.
    ///
    /// Creates a new response instance which internally allocates a buffer of 8 kB. Note that
    /// this expects a single response element. If several elements are to be output, an
    /// [Response::array](Response::array) has to be used.
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
                )));
            }
        };

        *current_nesting -= 1;
        match *current_nesting {
            nesting if nesting > 0 => Ok(()),
            nesting if nesting == 0 => {
                let _ = self.nesting.pop();
                Ok(())
            }
            _ => Err(OutputError::ProtocolError(anyhow!(
                "Invalid result nesting!"
            ))),
        }
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

    /// Completes the request and returns the serialized response as bytes buffer.
    ///
    /// Note that this returns an error if the internal nesting is invalid (e.g. an array
    /// is missing elements).
    ///
    /// As this consumes **self**, this is the final operation to be performed on a response.
    pub fn complete(mut self) -> Result<BytesMut, OutputError> {
        if !self.nesting.is_empty() {
            return Err(OutputError::ProtocolError(anyhow!(
                "Invalid result nesting!"
            )));
        }

        self.nesting.push(1);
        Ok(self.buffer)
    }

    /// Provides a helper method which directly transforms the response into its string
    /// representation.
    ///
    /// This is only intended to be used in test environments to verify if a generated response as
    /// the expected size and shape.
    ///
    /// Note that this does not support responses which contain non UTF-8 data (which are generally
    /// supported by RESP).
    pub fn complete_string(self) -> Result<String, OutputError> {
        let buffer = self.complete()?;
        match std::str::from_utf8(&buffer[..]) {
            Ok(str) => Ok(str.to_owned()),
            Err(_) => Err(OutputError::ProtocolError(anyhow::anyhow!(
                "Non UTF-8 data found"
            ))),
        }
    }

    /// Starts an array with the given number of items.
    ///
    /// Note that this call has to be followed by the exact number of outer output calls in order
    /// to generate a valid response.
    ///
    /// # Example
    ///
    /// Properly building an array:
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.array(2)?;
    /// response.simple("Hello")?;
    /// response.simple("World")?;
    ///
    /// assert_eq!(response.complete_string()?, "*2\r\n+Hello\r\n+World\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    ///
    /// This will panic as the array is missing elements
    /// ```should_panic
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.array(3)?;
    /// response.simple("Hello")?;
    ///
    /// response.complete_string().unwrap();
    /// #    Ok(())
    /// # }
    /// ```
    pub fn array(&mut self, items: i32) -> OutputResult {
        self.check_nesting()?;
        if items > 0 {
            self.nesting.push(items);
        }
        self.reserve(16);
        self.buffer.write_char('*')?;
        write!(self.buffer, "{}\r\n", items)?;
        Ok(())
    }

    /// Emits "OK" as simple string.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.ok()?;
    /// assert_eq!(response.complete_string()?, "+OK\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn ok(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(5);
        self.buffer.write_str("+OK\r\n")?;
        Ok(())
    }

    /// Emits "0" as number.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.zero()?;
    /// assert_eq!(response.complete_string()?, ":0\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn zero(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(4);
        self.buffer.write_str(":0\r\n")?;
        Ok(())
    }

    /// Emits "1" as number.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.one()?;
    /// assert_eq!(response.complete_string()?, ":1\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn one(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(4);
        self.buffer.write_str(":1\r\n")?;
        Ok(())
    }

    /// Emits the given number.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.number(42)?;
    /// assert_eq!(response.complete_string()?, ":42\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn number(&mut self, number: i64) -> OutputResult {
        if number == 0 {
            self.zero()
        } else if number == 1 {
            self.one()
        } else {
            self.check_nesting()?;
            self.reserve(32);
            self.buffer.write_char(':')?;
            write!(self.buffer, "{}\r\n", number)?;
            Ok(())
        }
    }

    /// Emits "1" if the given value is **true** or "0" otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.boolean(true)?;
    /// assert_eq!(response.complete_string()?, ":1\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn boolean(&mut self, boolean: bool) -> OutputResult {
        self.number(if boolean { 1 } else { 0 })
    }

    /// Emits the given string as **simple string**.
    ///
    /// A simple string is encoded as "+STRING_VALUE". This requires that the given string
    /// is valid UTF-8 and that it does not contain any line breaks (CR or LF). This isn't enforced
    /// by this method. When in doubt, use [Response::bulk](Response::bulk).
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.simple("Hello World")?;
    /// assert_eq!(response.complete_string()?, "+Hello World\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn simple(&mut self, string: impl AsRef<str>) -> OutputResult {
        if string.as_ref().is_empty() {
            self.empty_string()
        } else {
            self.check_nesting()?;
            self.reserve(3 + string.as_ref().len());
            self.buffer.write_char('+')?;
            self.buffer.write_str(string.as_ref())?;
            self.buffer.write_str("\r\n")?;

            Ok(())
        }
    }

    /// Emits an empty string.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.empty_string()?;
    /// assert_eq!(response.complete_string()?, "+\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn empty_string(&mut self) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3);
        self.buffer.write_str("+\r\n")?;

        Ok(())
    }

    /// Emits the given string as bulk data.
    ///
    /// RESP doesn't provide any requirements for bulk string. These can therefore contain line
    /// breaks (CR and or LF) as well as non UTF-8 data (which isn't supported here).
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.bulk("Hello\nWorld")?;
    /// assert_eq!(response.complete_string()?, "$11\r\nHello\nWorld\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn bulk(&mut self, string: impl AsRef<str>) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3 + 16 + string.as_ref().len());
        self.buffer.write_char('$')?;
        write!(self.buffer, "{}\r\n", string.as_ref().len())?;
        self.buffer.write_str(string.as_ref())?;
        self.buffer.write_str("\r\n")?;

        Ok(())
    }

    /// Emits an error message.
    ///
    /// Errors are encoded as "-ERROR MESSAGE". Therefore, just like simple strings, these must not
    /// contain line breaks (CR or LF) or non UTF-8 data.
    ///
    /// This method will automatically transform CR and LF to " " so that we do not double fail
    /// (crash when reporting an error).
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::response::{OutputResult, Response};
    /// # fn main() -> OutputResult {
    /// let mut response = Response::new();
    /// response.error("Good bye,\ncruel World")?;
    /// assert_eq!(response.complete_string()?, "-Good bye, cruel World\r\n");
    /// #    Ok(())
    /// # }
    /// ```
    pub fn error(&mut self, string: impl AsRef<str>) -> OutputResult {
        self.check_nesting()?;
        self.reserve(3 + string.as_ref().len());
        self.buffer.write_char('-')?;
        self.buffer.write_str(
            string
                .as_ref()
                .to_owned()
                .replace(&"\r", " ")
                .replace(&"\n", " ")
                .as_str(),
        )?;
        self.buffer.write_str("\r\n")?;

        Ok(())
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

        assert_eq!(response.complete_string().unwrap(), "-Error Problem\r\n");
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
            response.complete_string().unwrap(),
            format!("*2\r\n+{}\r\n$16000\r\n{}\r\n", many_x, many_y)
        );
    }
}
