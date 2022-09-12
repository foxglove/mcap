//! Includes functionality for reading MCAP records out of a [`std::io::Read`] implementation.
use crate::parse::{parse_attachment_header, parse_u64, ParseError};
use crate::records::{AttachmentHeader, InvalidOpcode, OpCode};
use std::error::Error;

pub const MAGIC: [u8; 8] = [0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

#[derive(Debug)]
pub enum LexError {
    IO(std::io::Error),
    ParseError(ParseError),
    TruncatedMidRecord(OpCode, Vec<u8>),
    InvalidStartMagic(Vec<u8>),
    RecordTooLargeForArchitecture(OpCode, u64),
    Exhausted,
    Lost,
    CalledReadInCallback,
    UnrecognizedCompression(String),
    InvalidOpcode(u8),
}
impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ParseError(err) => write!(f, "error parsing record or opcode: {}", err),
            Self::InvalidStartMagic(magic) => {
                write!(f, "expected magic bytes at start, got {:?}", magic)
            }
            Self::TruncatedMidRecord(opcode, bytes) => write!(
                f,
                "expected more data for record {:?}, found {:?}",
                opcode, bytes
            ),
            Self::RecordTooLargeForArchitecture(opcode, size) => write!(
                f,
                "encountered {:?} record too large for arch: {} < {}",
                opcode,
                usize::MAX,
                size
            ),
            Self::IO(err) => write!(f, "error reading next record: {}", err),
            Self::Lost => write!(f, "cannot continue lexing after last error"),
            Self::Exhausted => write!(f, "no more records to read"),
            Self::CalledReadInCallback => write!(
                f,
                "reader is None, did you call read_next within a callback?"
            ),
            Self::UnrecognizedCompression(name) => write!(f, "unrecognized compression: {}", name),
            Self::InvalidOpcode(opcode) => write!(f, "opcode is invalid: {}", opcode),
        }
    }
}

impl Error for LexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LexError::ParseError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ParseError> for LexError {
    fn from(err: ParseError) -> Self {
        Self::ParseError(err)
    }
}

impl From<InvalidOpcode> for LexError {
    fn from(err: InvalidOpcode) -> Self {
        Self::InvalidOpcode(err.0)
    }
}

impl From<std::io::Error> for LexError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}
enum LexerState {
    Start,
    Lexing,
    FooterSeen,
    Lost,
    End,
}

pub type AttachmentContentHandler<'a, R> =
    Box<dyn FnMut(AttachmentHeader, &mut std::io::Take<R>) -> bool + 'a>;

/// Reads out of a [`std::io::Read`] implementation one record at a time into
/// a buffer. The main interface for this struct is [`Lexer::read_next`].
pub struct Lexer<'a, R: std::io::Read> {
    reader: Option<R>,
    state: LexerState,
    expect_start_magic: bool,
    attachment_content_handler: Option<AttachmentContentHandler<'a, R>>,
}

impl<'a, R: std::io::Read> Lexer<'a, R> {
    /// Creates a new Lexer with default configuration values.
    pub fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            state: LexerState::Start,
            expect_start_magic: true,
            attachment_content_handler: None,
        }
    }

    /// if True, the resulting Lexer will not attempt to read or validate
    /// start magic. This is useful for lexing the content of Chunk records.
    pub fn expect_start_magic(mut self, expect_start_magic: bool) -> Self {
        self.expect_start_magic = expect_start_magic;
        self
    }

    /// Sets a callback to be called when Attachment records are encountered.
    /// In some contexts, an attachment might be too large to read into memory all at once.
    /// when set, [`Lexer::read_next`] will not return Attachment records.
    /// If attachment_content_handler returns False, lexing will be aborted.
    pub fn with_attachment_content_handler(
        mut self,
        attachment_content_handler: AttachmentContentHandler<'a, R>,
    ) -> Self {
        self.attachment_content_handler = Some(attachment_content_handler);
        self
    }

    /// Reads the data for the next record from an MCAP into `data`.
    /// This is most commonly used with [`crate::parse::parse_record`] to access the fields
    /// of the record.
    ///
    /// # Return
    /// - [`Ok(Some(records::OpCode))`] if the read has resulted in a valid record.
    /// - [`Ok(None)`] if the lexer has reached the end of the MCAP.
    /// - [`Err(LexError)`] if an error occurred when reading the next record.
    pub fn read_next(&mut self, out: &mut Vec<u8>) -> Result<Option<OpCode>, LexError> {
        match self.state {
            LexerState::Start => {
                if !self.expect_start_magic {
                    self.state = LexerState::Lexing;
                    return self.read_next(out);
                }
                let mut magic_buffer: [u8; 8] = [0; 8];
                let read_len = self.get_reader()?.read(&mut magic_buffer[..])?;

                if read_len != MAGIC.len() || magic_buffer != MAGIC {
                    self.state = LexerState::Lost;
                    return Err(LexError::InvalidStartMagic(magic_buffer.into()));
                }
                self.state = LexerState::Lexing;
                self.read_next(out)
            }
            LexerState::Lexing => {
                // read opcode and 64-bit length first.
                let mut opcode_and_length_buf: [u8; 1 + 8] = [0; 1 + 8];
                let read_len = self.get_reader()?.read(&mut opcode_and_length_buf[..])?;
                if read_len == 0 {
                    self.state = LexerState::End;
                    return Ok(None);
                }
                let opcode = OpCode::try_from(opcode_and_length_buf[0])?;
                if read_len < opcode_and_length_buf.len() {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord(
                        opcode,
                        opcode_and_length_buf[..read_len].into(),
                    ));
                }
                let (len, _) = parse_u64(&opcode_and_length_buf[1..])?;
                // Attachments are a special case, because they may be too large to fit into
                // memory. The user can specify a content handler callback, which can incrementally
                // read the attachment content out of the reader.
                if opcode == OpCode::Attachment {
                    if let Some(mut cb) = self.attachment_content_handler.take() {
                        let attachment_header_buf = read_attachment_header(self.get_reader()?)?;
                        let attachment_header =
                            parse_attachment_header(&attachment_header_buf[..])?;
                        // hand the user a limited reader which reaches EOF at the beginning of
                        // the next record.
                        let mut limited_reader = self
                            .reader
                            .take()
                            .expect("reader should be Some if get_reader succeeded above")
                            .take(len - (attachment_header_buf.len() as u64));
                        // the handler's return value tells us whether to continue iterating through
                        // the MCAP.
                        if !cb(attachment_header, &mut limited_reader) {
                            self.state = LexerState::End;
                            self.attachment_content_handler = Some(cb);
                            self.reader = Some(limited_reader.into_inner());
                            return Ok(None);
                        }
                        // The handler may not have read to the end of the attachment, so dump
                        // the rest of the reader.
                        let dump_res = std::io::copy(&mut limited_reader, &mut std::io::sink());
                        // restore the reader and handler state before checking the error.
                        self.attachment_content_handler = Some(cb);
                        self.reader = Some(limited_reader.into_inner());
                        if let Err(err) = dump_res {
                            self.state = LexerState::Lost;
                            return Err(LexError::IO(err));
                        }
                        // move on to the next record.
                        return self.read_next(out);
                    }
                }
                // On 32-bit architectures, a >4GB record might be too large to fit into memory.
                if len > usize::MAX as u64 {
                    self.state = LexerState::Lost;
                    return Err(LexError::RecordTooLargeForArchitecture(opcode, len));
                }
                out.resize(len as usize, 0);
                let read_len = self.get_reader()?.read(&mut out[..])?;
                if read_len < (len as usize) {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord(opcode, out.clone()));
                }
                if opcode == OpCode::Footer {
                    self.state = LexerState::FooterSeen;
                }
                Ok(Some(opcode))
            }
            LexerState::FooterSeen => Ok(None),
            LexerState::End => Err(LexError::Exhausted),
            LexerState::Lost => Err(LexError::Lost),
        }
    }

    /// Private helper to access the reader from its Option<> shell.
    /// self.reader is only ever None while the attachment_content_handler is being called,
    /// which is why the only way this can fail is if the user has tried to call [`Lexer::read_next`]
    /// from within the attachment content handler itself.
    fn get_reader(&mut self) -> Result<&mut R, LexError> {
        match self.reader.as_mut() {
            Some(r) => Ok(r),
            None => Err(LexError::CalledReadInCallback),
        }
    }
}

fn read_attachment_header<R: std::io::Read>(reader: &mut R) -> Result<Vec<u8>, LexError> {
    let mut buf: Vec<u8> = vec![0; 8 + 8 + 4];
    reader.read_exact(&mut buf[..])?;
    let name_len = u32::from_le_bytes(buf[(buf.len() - 4)..].try_into().unwrap());
    let old_len = buf.len();
    buf.resize(buf.len() + (name_len as usize) + 4, 0);
    reader.read_exact(&mut buf[old_len..])?;
    let content_type_len = u32::from_le_bytes(buf[(buf.len() - 4)..].try_into().unwrap());
    let old_len = buf.len();
    buf.resize(buf.len() + (content_type_len as usize) + 8, 0);
    reader.read_exact(&mut buf[old_len..])?;
    Ok(buf)
}
