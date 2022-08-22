use std::error::Error;
use std::io::SeekFrom;

#[derive(Debug)]
pub enum ReadError {
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
        None
    }
}

pub trait Reader<'a> {
    fn read<'b>(&mut self, size: u64) -> Result<&'b [u8], ReadError>
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
    fn read<'b>(&mut self, size: u64) -> Result<&'b [u8], ReadError>
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
