use std::error::Error;
use std::io::SeekFrom;
use std::io::{Read, Seek};

#[derive(Debug)]
pub enum ReadError {
    IO(std::io::Error),
    UnexpectedEOF,
    SeekOutOfRange,
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IO(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ReadError {
    fn from(err: std::io::Error) -> ReadError {
        ReadError::IO(err)
    }
}

pub trait Reader<'a> {
    fn read<'b>(&'a mut self, size: u64) -> Result<&'b [u8], ReadError>
    where
        'a: 'b;
    fn seek(&mut self, pos: SeekFrom) -> Result<(), ReadError>;
}

pub struct BufReader<'a> {
    buffer: &'a [u8],
    cursor: u64,
}

impl<'a> BufReader<'a> {
    pub fn new(underlying_buffer: &'a [u8]) -> Self {
        Self {
            buffer: underlying_buffer,
            cursor: 0,
        }
    }
}

impl<'a> Reader<'a> for BufReader<'a> {
    fn read<'b>(&'a mut self, size: u64) -> Result<&'b [u8], ReadError>
    where
        'a: 'b,
    {
        if self.cursor + size > (self.buffer.len() as u64) {
            Err(ReadError::UnexpectedEOF)
        } else {
            let start = self.cursor as usize;
            let end = (self.cursor + size) as usize;
            self.cursor += size;
            Ok(&self.buffer[start..end])
        }
    }

    fn seek(&mut self, from: SeekFrom) -> Result<(), ReadError> {
        match from {
            SeekFrom::Start(pos) => {
                if pos as usize >= self.buffer.len() {
                    Err(ReadError::SeekOutOfRange)
                } else {
                    self.cursor = pos;
                    Ok(())
                }
            }
            SeekFrom::End(offset) => match (self.buffer.len() as i64) - offset {
                x if x < 0 => Err(ReadError::SeekOutOfRange),
                x if x > (self.buffer.len() as i64) => Err(ReadError::SeekOutOfRange),
                x => {
                    self.cursor = x as u64;
                    Ok(())
                }
            },
            SeekFrom::Current(offset) => match (self.cursor as i64) + offset {
                x if x < 0 => Err(ReadError::SeekOutOfRange),
                x if x > (self.buffer.len() as i64) => Err(ReadError::SeekOutOfRange),
                x => {
                    self.cursor = x as u64;
                    Ok(())
                }
            },
        }
    }
}

pub struct FileReader {
    file: std::fs::File,
    buf: Vec<u8>,
}

impl<'a> Reader<'a> for FileReader {
    fn read<'b>(&'a mut self, size: u64) -> Result<&'b [u8], ReadError>
    where
        'a: 'b,
    {
        self.buf.resize(size as usize, 0);
        let n = self.file.read(&mut self.buf)?;
        if n < size as usize {
            return Err(ReadError::UnexpectedEOF);
        }
        Ok(&self.buf[0..n])
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<(), ReadError> {
        self.file.seek(pos)?;
        Ok(())
    }
}
