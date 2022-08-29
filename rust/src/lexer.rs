use crate::parse::{parse_attachment_header, parse_u64, AttachmentHeader, OpCode, ParseError};

use std::{error::Error, io::SeekFrom};

const MAGIC: [u8; 8] = [0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

enum LexerState {
    Start,
    Lexing,
    FooterSeen,
    Lost,
    End,
}

pub struct Lexer<'a, R: std::io::Read> {
    reader: Option<R>,
    state: LexerState,
    last_opcode: Option<OpCode>,
    expect_start_magic: bool,
    attachment_content_handler:
        Option<Box<dyn FnMut(AttachmentHeader, &mut std::io::Take<R>) -> bool + 'a>>,
}

#[derive(Debug)]
pub enum LexError {
    IO(std::io::Error),
    ParseError(ParseError),
    TruncatedMidRecord(OpCode, Vec<u8>),
    InvalidStartMagic(Vec<u8>),
    RecordTooLargeForArchitecture(OpCode, u64),
    ErrorInAttachmentHandler(Box<dyn Error>),
    Exhausted,
    Lost,
    CalledReadInCallback,
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
            Self::ErrorInAttachmentHandler(err) => {
                write!(f, "failed in attachment handler: {}", err)
            }
            Self::CalledReadInCallback => write!(
                f,
                "reader is None, did you call read_next within a callback?"
            ),
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

impl From<std::io::Error> for LexError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

pub struct RawRecord {
    pub opcode: Option<OpCode>,
    pub buf: Vec<u8>,
}

impl RawRecord {
    pub fn new() -> Self {
        Self {
            opcode: None,
            buf: vec![],
        }
    }
}

impl<'a, R: std::io::Read> Lexer<'a, R> {
    // Creates a new Lexer with default configuration values.
    pub fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            last_opcode: None,
            state: LexerState::Start,
            expect_start_magic: true,
            attachment_content_handler: None,
        }
    }

    // if True, the resulting Lexer will not attempt to read or validate
    // start magic. This is useful for lexing the content of Chunk records.
    pub fn expect_start_magic(mut self, expect_start_magic: bool) -> Self {
        self.expect_start_magic = expect_start_magic;
        self
    }

    // Sets a callback to be called when Attachment records are encountered.
    // In some contexts, an attachment might be too large to read into memory all at once.
    // when set, read_next() will not return Attachment records.
    // If attachment_content_handler returns False, lexing will be aborted.
    pub fn with_attachment_content_handler(
        mut self,
        attachment_content_handler: Box<
            dyn FnMut(AttachmentHeader, &mut std::io::Take<R>) -> bool + 'a,
        >,
    ) -> Self {
        self.attachment_content_handler = Some(attachment_content_handler);
        self
    }

    /// Reads the data for the next record from an MCAP into the `record` buffer.
    /// This is most commonly used with `mcap::parse::parse_record()` to access the fields
    /// of the record.
    ///
    /// # Return
    /// - Ok(true) if the read has resulted in a valid record.
    /// - Ok(false) if the lexer has reached the end of the MCAP.
    /// - Err(err) if an error occurred when reading the next record.
    pub fn read_next(&mut self, record: &mut RawRecord) -> Result<bool, LexError> {
        record.opcode = None;
        match self.state {
            LexerState::Start => {
                if !self.expect_start_magic {
                    self.state = LexerState::Lexing;
                    return self.read_next(record);
                }
                let mut magic_buffer: [u8; 8] = [0; 8];
                let read_len = self.get_reader()?.read(&mut magic_buffer[..])?;

                if read_len != MAGIC.len() || magic_buffer != MAGIC {
                    self.state = LexerState::Lost;
                    return Err(LexError::InvalidStartMagic(record.buf.clone()));
                }
                self.state = LexerState::Lexing;
                return self.read_next(record);
            }
            LexerState::Lexing => {
                // read opcode and 64-bit length first.
                let mut opcode_and_length_buf: [u8; 1 + 8] = [0; 1 + 8];
                let read_len = self.get_reader()?.read(&mut opcode_and_length_buf[..])?;
                if read_len == 0 {
                    self.state = LexerState::End;
                    return Ok(false);
                }
                let opcode = OpCode::try_from(opcode_and_length_buf[0])?;
                record.opcode = Some(opcode);
                self.last_opcode = Some(opcode);
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
                            .unwrap()
                            .take(len - (attachment_header_buf.len() as u64));
                        // the handler's return value tells us whether to continue iterating through
                        // the MCAP.
                        if !cb(attachment_header, &mut limited_reader) {
                            self.state = LexerState::End;
                            return Ok(false);
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
                        return self.read_next(record);
                    }
                }
                // On 32-bit architectures, a >4GB record might be too large to fit into memory.
                if len > usize::MAX as u64 {
                    self.state = LexerState::Lost;
                    return Err(LexError::RecordTooLargeForArchitecture(opcode, len));
                }
                record.buf.resize(len as usize, 0);
                let read_len = self.get_reader()?.read(&mut record.buf[..])?;
                if read_len < (len as usize) {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord(opcode, record.buf.clone()));
                }
                if opcode == OpCode::Footer {
                    self.state = LexerState::FooterSeen;
                }
                Ok(true)
            }
            LexerState::FooterSeen => Ok(false),
            LexerState::End => Err(LexError::Exhausted),
            LexerState::Lost => Err(LexError::Lost),
        }
    }

    /// Private helper to access the reader from its Option<> shell.
    /// self.reader is only ever None while the attachment_content_handler is being called,
    /// which is why the only way this can fail is if the user has tried to call lexer.read_next()
    /// from within the attachment content handler itself.
    fn get_reader(&mut self) -> Result<&mut R, LexError> {
        match self.reader.as_mut() {
            Some(r) => Ok(r),
            None => Err(LexError::CalledReadInCallback),
        }
    }
}

impl<'a, R: std::io::Read + std::io::Seek + Copy> Lexer<'a, R> {
    // Seeks to a specific record before reading it.
    pub fn read_next_at(
        &mut self,
        record: &mut RawRecord,
        from: SeekFrom,
    ) -> Result<bool, LexError> {
        let new_pos = self.get_reader()?.seek(from)?;
        // determine the lexer state from the new position.
        if new_pos == 0 && !self.expect_start_magic {
            self.state = LexerState::Start;
        } else {
            self.state = LexerState::Lexing;
        }
        return self.read_next(record);
    }
}

fn read_attachment_header<R: std::io::Read>(reader: &mut R) -> Result<Vec<u8>, LexError> {
    let mut buf: Vec<u8> = vec![0; 8 + 8 + 4];
    reader.read(&mut buf[..])?;
    let name_len = u32::from_le_bytes(buf[(buf.len() - 4)..].try_into().unwrap());
    let old_len = buf.len();
    buf.resize(buf.len() + (name_len as usize) + 4, 0);
    reader.read(&mut buf[old_len..])?;
    let content_type_len = u32::from_le_bytes(buf[(buf.len() - 4)..].try_into().unwrap());
    let old_len = buf.len();
    buf.resize(buf.len() + (content_type_len as usize) + 8, 0);
    reader.read(&mut buf[old_len..])?;
    Ok(buf)
}
