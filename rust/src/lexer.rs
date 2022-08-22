use crate::io::{ReadError, Reader};
use crate::records::{parse_u64, OpCode, ParseError};

use std::error::Error;

const MAGIC: [u8; 8] = [0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

enum LexerState {
    Start,
    Lexing,
    Lost,
    FooterSeen,
    End,
}
pub struct Lexer<'a, R>
where
    R: Reader<'a>,
{
    reader: &'a mut R,
    state: LexerState,
}

#[derive(Debug)]
pub enum LexError {
    IO(ReadError),
    ParseError(ParseError),
    InvalidStartMagic([u8; 8]),
    InvalidEndMagic([u8; 8]),
    Lost,
}
impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for LexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LexError::IO(err) => Some(err),
            LexError::ParseError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ReadError> for LexError {
    fn from(err: ReadError) -> Self {
        Self::IO(err)
    }
}
impl From<ParseError> for LexError {
    fn from(err: ParseError) -> Self {
        Self::ParseError(err)
    }
}

impl<'a, R> Lexer<'a, R>
where
    R: Reader<'a>,
{
    pub fn new(reader: &'a mut R) -> Self {
        Self {
            reader: reader,
            state: LexerState::Start,
        }
    }

    fn read_next_record(&mut self) -> Result<(OpCode, &'a [u8]), LexError> {
        let buf = self.reader.read(1 + 8)?;
        let opcode = OpCode::from_u8(buf[0])?;
        let (len, _) = parse_u64(&buf[1..])?;
        Ok((opcode, self.reader.read(len)?))
    }

    pub fn next(&mut self) -> Option<Result<(OpCode, &'a [u8]), LexError>> {
        match self.state {
            LexerState::Lost => {
                return Some(Err(LexError::Lost));
            }
            LexerState::End => {
                return None;
            }
            LexerState::FooterSeen => {
                let magic_buf: [u8; 8] = match self.reader.read(8) {
                    Ok(buf) => buf.try_into().unwrap(),
                    Err(err) => return Some(Err(LexError::IO(err))),
                };
                if magic_buf == MAGIC {
                    self.state = LexerState::End;
                    return None;
                } else {
                    self.state = LexerState::Lost;
                    return Some(Err(LexError::InvalidEndMagic(magic_buf)));
                }
            }
            LexerState::Start => {
                let magic_buf: [u8; 8] = match self.reader.read(8) {
                    Ok(buf) => buf.try_into().unwrap(),
                    Err(err) => return Some(Err(LexError::IO(err))),
                };
                if magic_buf != MAGIC {
                    return Some(Err(LexError::InvalidStartMagic(magic_buf)));
                }
                self.state = LexerState::Lexing;
                return Some(self.read_next_record());
            }
            LexerState::Lexing => {
                let res = match self.read_next_record() {
                    Err(err) => Err(err),
                    Ok((opcode, data)) => {
                        match opcode {
                            OpCode::Footer => {
                                self.state = LexerState::FooterSeen;
                            }
                            _ => (),
                        };
                        Ok((opcode, data))
                    }
                };
                Some(res)
            }
        }
    }
}
