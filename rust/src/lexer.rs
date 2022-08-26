use crate::parse::{parse_u64, OpCode, ParseError};

use std::{error::Error, io::SeekFrom};

const MAGIC: [u8; 8] = [0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

enum LexerState {
    Start,
    Lexing,
    Lost,
    End,
}
pub struct Lexer<R> {
    reader: R,
    state: LexerState,
    last_opcode: Option<OpCode>,
    records_only: bool,
}

#[derive(Debug)]
pub enum LexError {
    IO(std::io::Error),
    ParseError(ParseError),
    TruncatedAfterRecord(Option<OpCode>),
    TruncatedMidRecord((OpCode, Vec<u8>)),
    InvalidStartMagic(Vec<u8>),
    RecordTooLargeForArchitecture(u64),
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
            Self::TruncatedAfterRecord(opcode) => {
                write!(f, "unexpected end after last record {:?}", opcode)
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
    pub fn new(reader: R, records_only: bool) -> Self {
        Self {
            reader: reader,
            last_opcode: None,
            state: LexerState::Start,
            records_only: records_only,
        }
    }

    pub fn read_next(&mut self, record: &mut RawRecord) -> Result<bool, LexError> {
        record.opcode = None;
        match self.state {
            LexerState::Start => {
                if self.records_only {
                    self.state = LexerState::Lexing;
                    return self.read_next(record);
                }
                record.buf.resize(MAGIC.len(), 0);
                let read_len = self.reader.read(&mut record.buf[..])?;

                if read_len != MAGIC.len() {
                    self.state = LexerState::Lost;
                    return Err(LexError::InvalidStartMagic(record.buf.clone()));
                }
                if record.buf != MAGIC {
                    self.state = LexerState::Lost;
                    return Err(LexError::InvalidStartMagic(record.buf.clone()));
                }
                self.state = LexerState::Lexing;
                return self.read_next(record);
            }
            LexerState::End => Err(LexError::Exhausted),
            LexerState::Lexing => {
                record.buf.resize(1 + MAGIC.len(), 0);
                let read_len = self.reader.read(&mut record.buf[..])?;
                if read_len == 0 {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedAfterRecord(self.last_opcode));
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
                record.buf.resize(len as usize, 0);
                let read_len = self.reader.read(&mut record.buf[..])?;
                if read_len < (len as usize) {
                    self.state = LexerState::Lost;
                    return Err(LexError::TruncatedMidRecord((opcode, record.buf.clone())));
                }
                Ok(match opcode {
                    OpCode::Footer => {
                        self.state = LexerState::End;
                        false
                    }
                    _ => true,
                })
            }
            LexerState::Lost => Err(LexError::Lost),
        }
    }
}

impl<R: std::io::Read + std::io::Seek> Lexer<R> {
    pub fn read_next_at(
        &mut self,
        record: &mut RawRecord,
        from: SeekFrom,
    ) -> Result<bool, LexError> {
        let new_pos = self.reader.seek(from)?;
        // determine the lexer state from the new position.
        if new_pos == 0 && !self.records_only {
            self.state = LexerState::Start;
        } else {
            self.state = LexerState::Lexing;
        }
        return self.read_next(record);
    }
}
