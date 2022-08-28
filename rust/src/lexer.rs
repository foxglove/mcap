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

pub struct Lexer<R: std::io::Read> {
    reader: Option<R>,
    state: LexerState,
    last_opcode: Option<OpCode>,
    expect_start_magic: bool,
    attachment_content_handler: Option<
        Box<dyn FnMut(AttachmentHeader, &mut std::io::Take<R>) -> Result<bool, Box<dyn Error>>>,
    >,
}

#[derive(Debug)]
pub enum LexError {
    IO(std::io::Error),
    ParseError(ParseError),
    TruncatedMidRecord((OpCode, Vec<u8>)),
    InvalidStartMagic(Vec<u8>),
    RecordTooLargeForArchitecture(u64),
    ErrorInAttachmentHandler(Box<dyn Error>),
    Exhausted,
    Lost,
}
impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ParseError(err) => write!(f, "error parsing record or opcode: {}", err),
            Self::InvalidStartMagic(magic) => {
                write!(f, "expected magic bytes at start, got {:?}", magic)
            }
            Self::TruncatedMidRecord((opcode, bytes)) => write!(
                f,
                "expected more data for record {:?}, found {:?}",
                opcode, bytes
            ),
            Self::RecordTooLargeForArchitecture(size) => write!(
                f,
                "encountered record too large for arch: {} < {}",
                usize::MAX,
                size
            ),
            Self::IO(err) => write!(f, "error reading next record: {}", err),
            Self::Lost => write!(f, "cannot continue lexing after last error"),
            Self::Exhausted => write!(f, "no more records to read"),
            Self::ErrorInAttachmentHandler(err) => {
                write!(f, "failed in attachment handler: {}", err)
            }
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

impl<R: std::io::Read> Lexer<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            last_opcode: None,
            state: LexerState::Start,
            expect_start_magic: true,
            attachment_content_handler: None,
        }
    }

    pub fn expect_start_magic(mut self, expect_start_magic: bool) -> Self {
        self.expect_start_magic = expect_start_magic;
        self
    }

    pub fn with_attachment_content_handler(
        mut self,
        attachment_content_handler: Box<
            dyn FnMut(AttachmentHeader, &mut std::io::Take<R>) -> Result<bool, Box<dyn Error>>,
        >,
    ) -> Self {
        self.attachment_content_handler = Some(attachment_content_handler);
        self
    }

    pub fn read_next(&mut self, record: &mut RawRecord) -> Result<bool, LexError> {
        record.opcode = None;
        match self.state {
            LexerState::Start => {
                if !self.expect_start_magic {
                    self.state = LexerState::Lexing;
                    return self.read_next(record);
                }
                record.buf.resize(MAGIC.len(), 0);
                let read_len = self.reader.as_mut().unwrap().read(&mut record.buf[..])?;

                if read_len != MAGIC.len() || record.buf != MAGIC {
                    self.state = LexerState::Lost;
                    return Err(LexError::InvalidStartMagic(record.buf.clone()));
                }
                self.state = LexerState::Lexing;
                return self.read_next(record);
            }
            LexerState::Lexing => {
                record.buf.resize(1 + MAGIC.len(), 0);
                let read_len = self.reader.as_mut().unwrap().read(&mut record.buf[..])?;
                if read_len == 0 {
                    self.state = LexerState::End;
                    return Ok(false);
                }
                let opcode = OpCode::try_from(record.buf[0])?;
                record.opcode = Some(opcode);
                self.last_opcode = Some(opcode);
                if read_len < 1 + MAGIC.len() {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord((opcode, record.buf.clone())));
                }
                let (len, _) = parse_u64(&record.buf[1..])?;
                if len > usize::MAX as u64 {
                    self.state = LexerState::Lost;
                    return Err(LexError::RecordTooLargeForArchitecture(len));
                }
                if opcode == OpCode::Attachment {
                    if let Some(mut cb) = self.attachment_content_handler.take() {
                        let attachment_header_buf =
                            read_attachment_header(self.reader.as_mut().unwrap())?;
                        let attachment_header =
                            parse_attachment_header(&attachment_header_buf[..])?;

                        let mut limited_reader = self
                            .reader
                            .take()
                            .unwrap()
                            .take(len - (attachment_header_buf.len() as u64));
                        match cb(attachment_header, &mut limited_reader) {
                            Ok(false) => {
                                self.state = LexerState::End;
                                return Ok(false);
                            }
                            Ok(true) => {}
                            Err(err) => {
                                self.state = LexerState::End;
                                return Err(LexError::ErrorInAttachmentHandler(err));
                            }
                        };
                        let dump_res = std::io::copy(&mut limited_reader, &mut std::io::sink());
                        self.attachment_content_handler = Some(cb);
                        self.reader = Some(limited_reader.into_inner());
                        if let Err(err) = dump_res {
                            self.state = LexerState::Lost;
                            return Err(LexError::IO(err));
                        }
                        return self.read_next(record);
                    }
                }
                record.buf.resize(len as usize, 0);
                let read_len = self.reader.as_mut().unwrap().read(&mut record.buf[..])?;
                if read_len < (len as usize) {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord((opcode, record.buf.clone())));
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
}

impl<R: std::io::Read + std::io::Seek + Copy> Lexer<R> {
    pub fn read_next_at(
        &mut self,
        record: &mut RawRecord,
        from: SeekFrom,
    ) -> Result<bool, LexError> {
        let new_pos = self.reader.unwrap().seek(from)?;
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
