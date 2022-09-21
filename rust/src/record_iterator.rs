//! Ergonomic wrapper for reading and parsing records from a [`std::io::Read`] implementation.
use crate::{
    lexer::{LexError, Lexer},
    records::{parse_record, Chunk, Record},
};
use lifetime::IntoStatic;
use std::io::Read;

/// A wrapper for a [`std::io::Read`] implementation which parses out
/// records and returns them from an iterator.
pub struct RecordIterator<'a, R: std::io::Read> {
    lexer: Lexer<'a, R>,
    chunk_lexer: Option<Lexer<'a, std::io::Cursor<Vec<u8>>>>,
    raw_record: Vec<u8>,
    expand_chunks: bool,
}

impl<'a, R: std::io::Read> RecordIterator<'a, R> {
    pub fn new(reader: R) -> Self {
        Self {
            lexer: Lexer::new(reader),
            chunk_lexer: None,
            raw_record: Vec::new(),
            expand_chunks: true,
        }
    }
}

fn decompress(chunk: &Chunk) -> Result<Vec<u8>, LexError> {
    match chunk.compression.as_ref() {
        "" => Ok(chunk.records.clone().into()),
        "zstd" => Ok(zstd::decode_all(chunk.records.as_ref())?),
        "lz4" => {
            let mut buf: Vec<u8> = vec![];
            lz4::Decoder::new(chunk.records.as_ref())?.read_to_end(&mut buf)?;
            Ok(buf)
        }
        other => Err(LexError::UnrecognizedCompression(other.to_string())),
    }
}

impl<'a, R: std::io::Read> Iterator for RecordIterator<'a, R> {
    type Item = Result<Record<'static>, LexError>;
    fn next(&mut self) -> Option<Self::Item> {
        // If in the middle of the chunk, continue iterating through that chunk.
        if self.chunk_lexer.is_some() {
            return match self
                .chunk_lexer
                .as_mut()
                .unwrap()
                .read_next(&mut self.raw_record)
            {
                // The chunk is over - switch back to the base-level reader.
                Ok(None) => {
                    self.chunk_lexer = None;
                    self.next()
                }
                Ok(Some(opcode)) => match parse_record(opcode, &self.raw_record[..]) {
                    Ok(record) => Some(Ok(record.clone().into_static())),
                    Err(err) => Some(Err(err.into())),
                },
                Err(err) => Some(Err(err)),
            };
        }
        match self.lexer.read_next(&mut self.raw_record) {
            Ok(None) => None,
            Ok(Some(opcode)) => match parse_record(opcode, &self.raw_record[..]) {
                Ok(Record::Chunk(chunk)) if self.expand_chunks => match decompress(&chunk) {
                    Err(err) => Some(Err(err)),
                    Ok(buf) => {
                        self.chunk_lexer =
                            Some(Lexer::new(std::io::Cursor::new(buf)).expect_start_magic(false));
                        self.next()
                    }
                },
                Ok(record) => Some(Ok(record.clone().into_static())),
                Err(err) => Some(Err(err.into())),
            },
            Err(err) => Some(Err(err)),
        }
    }
}
