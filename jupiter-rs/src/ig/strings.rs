//! Memory efficient string types to be used in `Node`.
//!
//! Provides a memory efficient implementation for immutable strings to be stored within a
//! **Node**. Being immutable data, we can get away with storing
//! strings as slices of `u8` instead of `Vec`. Therefore we save 8 bytes to store the actual
//! capacity.
//!
//! As a `Node` occupies quite some bytes (at least 24 to store the `SymbolMap` which is internally
//! a `Vec` and therefore 3 * usize) we can store short strings inplace by using the first 23 bytes
//! to keep the data around and the last byte to store the actual length. Note that we do not need
//! to distinguish inline from boxed strings, as we simply can use two enum constants of `Node`
//! to keep track of the actual string type.
use memcmp::Memcmp;
use std::fmt::{Debug, Display, Formatter};

/// Provides a compact representation of a heap allocated immutable string.
///
/// In contrast to `String` which is internally a `Vec<u8>` we use a `Box<[u8]>` as the length
/// is known in advance and will never change (therefore we do not need to allocate a vector
/// with a larger capacity). This saves us from over-provisioning memory and also 8 more bytes
/// to store the capacity.
#[derive(Clone, Eq, PartialEq)]
pub struct BoxedString {
    data: Box<[u8]>,
}

impl BoxedString {
    /// Returns the length of the underlying string.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if the underlying string is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl From<&str> for BoxedString {
    /// Constructs a `BoxedString` from a given `&str`.
    ///
    /// # Safety
    /// Note that this is the only way to construct a `BoxedString`. This way, we know for
    /// sure that the `data` is always a valid UTF-8 byte sequence.
    fn from(value: &str) -> Self {
        BoxedString {
            data: value.as_bytes().into(),
        }
    }
}

impl AsRef<str> for BoxedString {
    /// Returns the underlying data as `&str`.
    ///
    /// # Safety
    /// Note that we know for sure, that the underlying data is valid UTF-8. Therefore we
    /// use a unchecked conversion here which is essentially a noop.
    fn as_ref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.data) }
    }
}

impl Debug for BoxedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())?;
        f.write_str(" (heap)")?;

        Ok(())
    }
}

impl Display for BoxedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// Provides an inline representation of short strings.
///
/// This struct is build to fit into the memory occupied by a [Node](crate::ig::node::Node).
///
/// It is the duty of the caller to not use this struct for longer strings.
pub struct InlineString {
    data: [u8; InlineString::MAX],
    size: u8,
}

impl InlineString {
    /// Defines the maximal number of bytes which can be stored in an `InlineString`.
    pub const MAX: usize = crate::ig::node::NODE_MAP_SIZE - 1;

    /// Returns the length of the underlying string.
    pub fn len(&self) -> usize {
        self.size as usize
    }

    /// Determines if the underlying string is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl From<&str> for InlineString {
    /// Constructs an `InlineString` from a given `&str`.
    ///
    /// # Safety
    /// Note that this is the only way to construct a `InlineString`. This way, we know for
    /// sure that the `data` is always a valid UTF-8 byte sequence.
    ///
    /// # Panics
    ///
    /// Panics if the given string is longer than `InlineString::MAX`.
    fn from(value: &str) -> Self {
        let mut data = [0_u8; InlineString::MAX];
        data[0..value.len()].clone_from_slice(&value.as_bytes()[0..value.len()]);

        InlineString {
            data,
            size: value.len() as u8,
        }
    }
}

impl Clone for InlineString {
    fn clone(&self) -> Self {
        InlineString {
            data: self.data,
            size: self.size,
        }
    }
}

impl PartialEq for InlineString {
    fn eq(&self, other: &Self) -> bool {
        if self.size == other.size {
            let size = self.size as usize;
            self.data[0..size].memcmp(&other.data[0..size])
        } else {
            false
        }
    }
}

impl AsRef<str> for InlineString {
    /// Returns the internal data as `&str`.
    ///
    /// # Safety
    /// Note that we know for sure, that the underlying data is valid UTF-8. Therefore we
    /// use a unchecked conversion here which is essentially a noop.
    fn as_ref(&self) -> &str {
        let size = self.size as usize;
        unsafe { std::str::from_utf8_unchecked(&self.data[0..size]) }
    }
}

impl Debug for InlineString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())?;
        f.write_str(" (inlined)")?;

        Ok(())
    }
}

impl Display for InlineString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::ig::strings::BoxedString;
    use crate::ig::strings::InlineString;

    #[test]
    fn boxed_strings_behave_like_strings() {
        for i in 0..256 {
            let data = "X".repeat(i);
            let boxed_data = BoxedString::from(data.as_str());
            assert_eq!(boxed_data, boxed_data);
            assert_ne!(boxed_data, BoxedString::from("OTHER"));
            assert_eq!(data.as_str(), boxed_data.as_ref());
            assert_eq!(data.len(), boxed_data.len());
            assert_eq!(data.is_empty(), boxed_data.is_empty());
        }
    }

    #[test]
    fn inline_strings_behave_like_strings() {
        for i in 0..=InlineString::MAX {
            let data = "X".repeat(i);
            let inline_data = InlineString::from(data.as_str());
            assert_eq!(inline_data, inline_data);
            assert_ne!(inline_data, InlineString::from("OTHER"));
            assert_eq!(data.as_str(), inline_data.as_ref());
            assert_eq!(data.len(), inline_data.len());
            assert_eq!(data.is_empty(), inline_data.is_empty());
        }
    }

    #[test]
    fn cloning_boxed_strings_works() {
        let original = BoxedString::from("Test String");
        assert_eq!(original.as_ref(), "Test String");
        assert_eq!(original.clone().as_ref(), "Test String");
    }

    #[test]
    fn cloning_inline_strings_works() {
        let original = InlineString::from("Test String");
        assert_eq!(original.as_ref(), "Test String");
        assert_eq!(original.clone().as_ref(), "Test String");
    }

    #[test]
    #[should_panic]
    fn inline_strings_panic_if_too_long() {
        let _ = InlineString::from("X".repeat(InlineString::MAX + 1).as_str());
    }
}
