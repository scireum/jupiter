//! Permits to load XML data using a pull principle.
//!
//! Using this facility permits to load large XML files in a very performant way. By providing
//! external work buffer a zero copy operation can be achieved (unless a non UTF-8 encoding is used).
//!
//! As the XML data being processed is most probably large and not needed all at once, the pull
//! strategy operates on a "per element" level for which then child elements can be queried and
//! processed like so:
//!
//! ```
//! use jupiter::ig::xml::PullReader;
//! use std::io::BufReader;
//!
//! let data = r#"
//! <xml>
//!     <entry><name>42</name><value>foo</value></entry>
//! </xml>
//! "#;
//!
//! let mut reader = PullReader::new(data.as_bytes());
//! let mut root_element = reader.root().unwrap();
//! if let Ok(Some(mut entry)) = root_element.next_child() {
//!     assert_eq!(entry.name().as_ref(), "entry");
//!     if let Ok(Some(mut child)) = entry.find_next_child("name") {
//!         assert_eq!(child.text().unwrap().contents().as_ref(), "42");
//!     } else {
//!         panic!("Invalid XML!");
//!     };
//!     if let Ok(Some(mut child)) = entry.next_child() {
//!         assert_eq!(child.name().as_ref(), "value");
//!         assert_eq!(child.text().unwrap().contents().as_ref(), "foo");
//!     } else {
//!         panic!("Invalid XML!");
//!     };
//! } else {
//!     panic!("Invalid XML!");
//! };
//!
//! ```
//!
//! # Memory Management
//!
//! The underlying **quick_xml** library uses an external buffer management when reading events.
//! While this is probably the most efficient way of dealing with memory management, we provide
//! an internal buffer manager which takes care of this task so that the code required to process
//! XML data isn't polluted with these tasks.
//!
//! We still re-use buffers as much as possible and therefore can still provide excellent
//! performance while also providing a convenient API. Note however, that in order to minimize the
//! performance overhead, we need some unsafe blocks.
use encoding_rs::Encoding;
use quick_xml::events::attributes::{Attribute, Attributes};
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Error, Reader};
use std::borrow::Cow;
use std::cell::RefCell;
use std::io::BufRead;

/// Describes the type used to represent a handle to a data buffer.
type BufferHandle = usize;

/// Takes care of managing buffers when parsing XML data.
///
/// The `quick_xml` reader requires a `Vec<u8>` buffer as mutable reference when reading an event.
/// This buffer has to be a valid reference as long as the resulting event lives. The
/// `BufferManager' is in charge of managing the buffers and especially to re-use buffers which have
/// already been allocated previously.
struct BufferManager {
    // Contains a list of all buffers which have been allocated so far. As a buffer manager lives
    // as long as the PullReader itself, this is a save operation.
    buffers: Vec<Vec<u8>>,

    // Keeps a list of indices into "buffers" which are known to be currently unused.
    free_buffers: Vec<BufferHandle>,
}

impl BufferManager {
    /// Allocates a new buffer.
    ///
    /// If possible an existing buffer is re-used to reduce pressure on the memory allocator.
    /// Therefore we attempt to pop a buffer from the free list and only allocate a new buffer,
    /// if the free list was already empty.
    fn alloc(&mut self) -> (&mut Vec<u8>, BufferHandle) {
        if let Some(index) = self.free_buffers.pop() {
            let mut_buffer = self.buffers.get_mut(index).unwrap();
            mut_buffer.clear();

            (mut_buffer, index)
        } else {
            let index = self.buffers.len();
            self.buffers.push(Vec::new());
            let mut_buffer = self.buffers.last_mut().unwrap();

            (mut_buffer, index)
        }
    }

    /// Releases the buffer to which this handle points.
    ///
    /// Note that this doesn't actually release any memory and just marks the buffer as free.
    fn release(&mut self, index: BufferHandle) {
        self.free_buffers.push(index);
    }
}

/// Provides a high level abstraction above `Reader` as provided by `quick_xml`.
///
/// This takes care of buffer management and encoding of all data and provides a convenient
/// API to process XML Data.
pub struct PullReader<B: BufRead> {
    reader: Reader<B>,
    buffer_manager: RefCell<BufferManager>,
}

/// Represents a matched element while processing XML data.
///
/// Using this type we can enforce proper nesting when handling a stream of XML events with
/// the help of the rust type system.
pub struct Element<'a, B: BufRead> {
    pending_close: bool,
    handle: BufferHandle,
    reader: &'a mut Reader<B>,
    buffer_manager: &'a RefCell<BufferManager>,
    data: BytesStart<'a>,
}

/// Provides an encoding aware view on the attributes of an `Element`.
pub struct AttributesView<'a> {
    encoding: &'static Encoding,
    attributes: Attributes<'a>,
}

/// Provides an encoding aware view on an attribute of an `Element`.
pub struct AttributeView<'a> {
    encoding: &'static Encoding,
    attribute: Attribute<'a>,
}

/// Provides an encoding aware handle which represents the text contents of an `Element`.
pub struct Text<'a> {
    encoding: &'static Encoding,
    handle: Option<BufferHandle>,
    buffer_manager: Option<&'a RefCell<BufferManager>>,
    data: Cow<'a, [u8]>,
}

impl<B: BufRead> PullReader<B> {
    /// Creates a new reader for the given input.
    ///
    /// This reader will automatically trim and enforce proper nesting in the XML data.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let data = r#"
    /// <xml>
    ///     <entry><name>42</name><value>foo</value></entry>
    /// </xml>
    /// "#;
    ///
    /// let mut reader = PullReader::new(data.as_bytes());
    /// ```
    pub fn new(input: B) -> Self {
        let mut reader = Reader::from_reader(input);
        let _ = reader
            .trim_text(true)
            .expand_empty_elements(true)
            .check_end_names(true);

        PullReader::from_reader(reader)
    }

    /// Creates a new reader based on the given reader object.
    pub fn from_reader(reader: Reader<B>) -> Self {
        PullReader {
            reader,
            buffer_manager: RefCell::new(BufferManager {
                buffers: Vec::new(),
                free_buffers: Vec::new(),
            }),
        }
    }

    /// Reads the root element of the underlying doc.
    ///
    /// This is mostly used so that all parsing functionality is centralized in `Element`.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new("<xml></xml>".as_bytes());
    /// assert_eq!(reader.root().unwrap().name().as_ref(), "xml");
    /// ```
    pub fn root<'a>(&'a mut self) -> quick_xml::Result<Element<'a, B>> {
        let mut buffer_manager = self.buffer_manager.borrow_mut();
        let (buffer, handle) = buffer_manager.alloc();
        loop {
            match self.reader.read_event(buffer)? {
                Event::Start(data) => {
                    return Ok(Element {
                        pending_close: false,
                        handle,
                        reader: &mut self.reader,
                        buffer_manager: &self.buffer_manager,
                        data: unsafe {
                            // Note that this unsafe block is required for two reasons.
                            // 1) We need to create a self referential data structure as the
                            //    &mut Vec<u8> we passed in into read_event as buffer, somehow
                            //    has to remain valid as long as the newly create element lives.
                            // 2) As we want to move data out of a loop, this currently tricks the
                            //    borrow checker anyway.
                            // We therefore cast away the lifetime of the resulting event data
                            // and thus of the mutably borrowed buffer above. However, we pass along
                            // the created handle and rely on the Drop handler of Element to invoke
                            // BufferManager::release on the Element itself is dropped.
                            std::mem::transmute::<BytesStart<'_>, BytesStart<'a>>(data)
                        },
                    });
                }
                Event::Eof => {
                    buffer_manager.release(handle);
                    return Err(Error::UnexpectedEof("root".to_owned()));
                }
                _ => {}
            }
        }
    }
}

impl<B: BufRead> Element<'_, B> {
    /// Returns the name of the element.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new("<xml></xml>".as_bytes());
    /// assert_eq!(reader.root().unwrap().name().as_ref(), "xml");
    /// ```
    pub fn name(&self) -> Cow<str> {
        self.reader.decode(self.data.name())
    }

    /// Provides access to the attributes of the element.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml test="foo"></xml>"#.as_bytes());
    /// let root = reader.root().unwrap();
    /// let attr = root.attributes().last().unwrap().unwrap();
    /// assert_eq!(attr.key(), "test");
    /// assert_eq!(attr.value().unwrap().contents().as_ref(), "foo");
    /// ```
    pub fn attributes(&self) -> AttributesView {
        AttributesView {
            encoding: self.reader.encoding(),
            attributes: self.data.attributes(),
        }
    }

    /// Returns the requested attribute.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml test="foo"></xml>"#.as_bytes());
    /// let root = reader.root().unwrap();
    /// assert_eq!(root.find_attribute("test").unwrap().unwrap().value().unwrap().contents().as_ref(), "foo");
    /// ```
    pub fn find_attribute(
        &self,
        name: impl AsRef<str>,
    ) -> quick_xml::Result<Option<AttributeView>> {
        for attribute in self.attributes() {
            match attribute {
                Ok(attribute) => {
                    if attribute.key().as_ref() == name.as_ref() {
                        return Ok(Some(attribute));
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Ok(None)
    }

    /// Returns the next child element of this element.
    ///
    /// If an empty value is returned, this element has no more children.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml><item></item></xml>"#.as_bytes());
    /// let mut root = reader.root().unwrap();
    /// assert_eq!(root.next_child().unwrap().unwrap().name().as_ref(), "item");
    /// assert_eq!(root.next_child().unwrap().is_none(), true);
    /// ```
    pub fn next_child<'a>(&'a mut self) -> quick_xml::Result<Option<Element<'a, B>>> {
        if self.pending_close {
            return Ok(None);
        }

        let mut buffer_manager = self.buffer_manager.borrow_mut();
        let (buffer, handle) = buffer_manager.alloc();
        loop {
            buffer.clear();
            match self.reader.read_event(buffer)? {
                Event::Start(data) => {
                    return Ok(Some(Element {
                        pending_close: false,
                        handle,
                        buffer_manager: self.buffer_manager,
                        reader: &mut self.reader,
                        data: unsafe {
                            // See PullReader::root() for an explanation why this is needed and
                            // why we think that this is safe code :-P
                            std::mem::transmute::<BytesStart<'_>, BytesStart<'a>>(data)
                        },
                    }));
                }
                Event::End(_) => {
                    buffer_manager.release(handle);
                    self.pending_close = true;
                    return Ok(None);
                }
                Event::Eof => {
                    buffer_manager.release(handle);
                    return Err(Error::UnexpectedEof("next_child".to_owned()));
                }
                _ => {}
            }
        }
    }

    /// Returns the next child element of this element with the given tag name.
    ///
    /// This will skip over all other tags until a match is found. If the end of the enclosing
    /// tag is reached, none is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml><item></item><test></test></xml>"#.as_bytes());
    /// let mut root = reader.root().unwrap();
    /// assert_eq!(root.find_next_child("test").unwrap().unwrap().name().as_ref(), "test");
    /// assert_eq!(root.find_next_child("test").unwrap().is_none(), true);
    /// ```
    pub fn find_next_child<'a>(
        &'a mut self,
        name: impl AsRef<str>,
    ) -> quick_xml::Result<Option<Element<'a, B>>> {
        let needle = self.reader.encoding().encode(name.as_ref()).0;
        while let Some(element) = self.next_child()? {
            if element.data.name() == needle.as_ref() {
                return Ok(Some(unsafe {
                    // This is sadly currently required due to the lexical scoping of the
                    // borrow checker. With future versions of Rust, this might be removed...
                    std::mem::transmute::<Element<'_, B>, Element<'a, B>>(element)
                }));
            }
        }

        Ok(None)
    }

    /// Tries to fetch the text contents of this element.
    ///
    /// Returns none if this element has no text at all.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml><item>test</item><empty></empty></xml>"#.as_bytes());
    /// let mut root = reader.root().unwrap();
    /// let mut item = root.next_child().unwrap().unwrap();
    /// assert_eq!(item.try_text().unwrap().unwrap().contents().as_ref(), "test");
    /// drop(item);
    ///
    /// let mut test = root.next_child().unwrap().unwrap();
    /// assert_eq!(test.try_text().unwrap().is_none(), true);
    /// ```
    pub fn try_text<'a>(&'a mut self) -> quick_xml::Result<Option<Text<'a>>> {
        if self.pending_close {
            return Ok(None);
        }

        let mut buffer_manager = self.buffer_manager.borrow_mut();
        let (buffer, handle) = buffer_manager.alloc();
        let mut depth = 1;
        loop {
            match self.reader.read_event(buffer)? {
                Event::Start(_) => depth += 1,
                Event::End(_) => {
                    depth -= 1;
                    if depth == 0 {
                        self.pending_close = true;
                        return Ok(None);
                    }
                }
                Event::Eof => return Err(Error::UnexpectedEof("try_text".to_owned())),
                Event::Text(data) | Event::CData(data) => {
                    return Ok(Some(Text {
                        encoding: self.reader.encoding(),
                        handle: Some(handle),
                        buffer_manager: Some(self.buffer_manager),
                        data: unsafe {
                            // See PullReader::root() for an explanation why this is needed and
                            // why we think that this is safe code :-P
                            std::mem::transmute::<Cow<'_, [u8]>, Cow<'a, [u8]>>(data.unescaped()?)
                        },
                    }));
                }
                _ => {}
            }
        }
    }

    /// Fetches the text contents of this element or returns an empty string if no text is present.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::xml::PullReader;
    /// let mut reader = PullReader::new(r#"<xml><item>test</item><empty></empty></xml>"#.as_bytes());
    /// let mut root = reader.root().unwrap();
    /// let mut item = root.next_child().unwrap().unwrap();
    /// assert_eq!(item.text().unwrap().contents().as_ref(), "test");
    /// drop(item);
    ///
    /// let mut test = root.next_child().unwrap().unwrap();
    /// assert_eq!(test.text().unwrap().contents().as_ref(), "");
    /// ```
    pub fn text(&mut self) -> quick_xml::Result<Text> {
        let encoding = self.reader.encoding();
        match self.try_text() {
            Ok(Some(text)) => Ok(text),
            Ok(None) => Ok(Text {
                handle: None,
                encoding,
                buffer_manager: None,
                data: Cow::Owned(Vec::new()),
            }),
            Err(error) => Err(error),
        }
    }
}

impl<B: BufRead> Drop for Element<'_, B> {
    /// Once an element is dropped, we consume all events up until the end tag of the element
    /// in the XML data.
    ///
    /// This permits to utilize the Rust type system to ensure proper nesting when parsing
    /// XML data.
    fn drop(&mut self) {
        let mut buffer_manager = self.buffer_manager.borrow_mut();
        buffer_manager.release(self.handle);

        if !self.pending_close {
            let mut depth = 1;
            let (tmp_buffer, tmp_handle) = buffer_manager.alloc();
            loop {
                if let Ok(event) = self.reader.read_event(tmp_buffer) {
                    match event {
                        Event::Start(_) => depth += 1,
                        Event::End(_) => {
                            depth -= 1;
                            if depth == 0 {
                                buffer_manager.release(tmp_handle);
                                return;
                            }
                        }
                        Event::Eof => {
                            buffer_manager.release(tmp_handle);
                            return;
                        }
                        Event::Text(_) => {}
                        Event::CData(_) => {}
                        _ => {}
                    }
                    tmp_buffer.clear();
                } else {
                    buffer_manager.release(tmp_handle);
                    return;
                }
            }
        }
    }
}

impl<'a> AttributeView<'a> {
    /// Returns the name of the attribute.
    pub fn key(&self) -> Cow<str> {
        self.encoding.decode(self.attribute.key).0
    }

    /// Returns the text contents of the attribute.
    pub fn value(&self) -> quick_xml::Result<Text> {
        Ok(Text {
            handle: None,
            buffer_manager: None,
            encoding: self.encoding,
            data: self.attribute.unescaped_value()?,
        })
    }
}

impl<'a> Iterator for AttributesView<'a> {
    type Item = quick_xml::Result<AttributeView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(attribute_result) = self.attributes.next() {
            match attribute_result {
                Ok(attribute) => Some(Ok(AttributeView {
                    encoding: self.encoding,
                    attribute,
                })),
                Err(error) => Some(Err(error)),
            }
        } else {
            None
        }
    }
}

impl<'a> Text<'a> {
    /// Returns the decoded text contents of this text element.
    pub fn contents(&self) -> Cow<str> {
        self.encoding.decode(self.data.as_ref()).0
    }
}

impl Drop for Text<'_> {
    /// In charge of releasing the internally allocated buffer for the underlying XML data.
    fn drop(&mut self) {
        if let Some(handle) = self.handle {
            self.buffer_manager.unwrap().borrow_mut().release(handle);
        }
    }
}
