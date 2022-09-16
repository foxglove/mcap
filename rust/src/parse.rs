//! Functions for parsing byte buffers into their MCAP record contents.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;

/// Returned from [`parse_record`] when parsing fails.
#[derive(Debug)]
pub enum ParseError {
    StringEncoding(std::str::Utf8Error),
    OpCodeNotImplemented(u8),
    DataTooShort,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::DataTooShort => write!(f, "data ended unexpectedly before end of record"),
            Self::OpCodeNotImplemented(opcode) => write!(f, "opcode {:?} not supported", opcode),
            Self::StringEncoding(err) => write!(f, "string field not valid utf-8: {}", err),
        }
    }
}

impl Error for ParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParseError::StringEncoding(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(err: std::str::Utf8Error) -> Self {
        ParseError::StringEncoding(err)
    }
}

pub struct Parser<'a>(&'a [u8]);

impl<'a> Parser<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data)
    }
    pub fn get<T: Parseable<'a>>(&mut self) -> Result<T, ParseError> {
        let (res, remainder) = T::parse_from_front(self.0)?;
        self.0 = remainder;
        Ok(res)
    }

    pub fn get_byte_array(&mut self) -> Result<Cow<'a, [u8]>, ParseError> {
        let len: u32 = self.get()?;
        let (content, remainder) = split_checked(self.0, len as usize)?;
        self.0 = remainder;
        Ok(Cow::Borrowed(content))
    }

    pub fn get_long_byte_array(&mut self) -> Result<Cow<'a, [u8]>, ParseError> {
        let len: u64 = self.get()?;
        let (content, remainder) = split_checked(self.0, len as usize)?;
        self.0 = remainder;
        Ok(Cow::Borrowed(content))
    }

    fn get_front_bytes(&mut self, len: usize) -> Result<&'a [u8], ParseError> {
        let (result, remainder) = split_checked(self.0, len)?;
        self.0 = remainder;
        Ok(result)
    }

    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    pub fn into_inner(self) -> &'a [u8] {
        self.0
    }
}

pub trait Parseable<'a>
where
    Self: 'a + Sized,
{
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError>;
}

fn split_checked(value: &[u8], len: usize) -> Result<(&[u8], &[u8]), ParseError> {
    if len > value.len() {
        Err(ParseError::DataTooShort)
    } else {
        Ok(value.split_at(len))
    }
}

fn split_const_checked<const N: usize>(value: &[u8]) -> Result<(&[u8; N], &[u8]), ParseError> {
    let (first, remainder) = split_checked(value, N)?;
    Ok((
        first
            .try_into()
            .expect("split checked should ensure correct length"),
        remainder,
    ))
}

impl<'a> Parseable<'a> for u8 {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let (byte, remainder) = split_const_checked::<1>(value)?;
        Ok((byte[0], remainder))
    }
}

impl<'a> Parseable<'a> for u16 {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let (int_bytes, remainder) = split_const_checked::<{ std::mem::size_of::<u16>() }>(value)?;
        Ok((u16::from_le_bytes(*int_bytes), remainder))
    }
}

impl<'a> Parseable<'a> for u32 {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let (int_bytes, remainder) = split_const_checked::<{ std::mem::size_of::<u32>() }>(value)?;
        Ok((u32::from_le_bytes(*int_bytes), remainder))
    }
}

impl<'a> Parseable<'a> for u64 {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let (int_bytes, remainder) = split_const_checked::<{ std::mem::size_of::<u64>() }>(value)?;
        Ok((u64::from_le_bytes(*int_bytes), remainder))
    }
}

impl<'a> Parseable<'a> for Cow<'a, str> {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let mut p = Parser(value);
        let len: u32 = p.get()?;
        let str_bytes = p.get_front_bytes(len as usize)?;
        Ok((std::str::from_utf8(str_bytes)?.into(), p.into_inner()))
    }
}

impl<'a, T: Parseable<'a>> Parseable<'a> for Vec<T> {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let mut p = Parser::new(value);
        let len: u32 = p.get()?;
        let mut result: Vec<T> = Vec::new();
        let mut content_parser = Parser::new(p.get_front_bytes(len as usize)?);
        while !content_parser.is_empty() {
            result.push(content_parser.get()?);
        }
        Ok((result, p.into_inner()))
    }
}

impl<'a, A: Parseable<'a>, B: Parseable<'a>> Parseable<'a> for (A, B) {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let mut p = Parser::new(value);
        let a: A = p.get()?;
        let b: B = p.get()?;
        Ok(((a, b), p.into_inner()))
    }
}

impl<'a, K: Parseable<'a> + Ord, V: Parseable<'a>> Parseable<'a> for BTreeMap<K, V> {
    fn parse_from_front(value: &'a [u8]) -> Result<(Self, &'a [u8]), ParseError> {
        let mut p = Parser::new(value);
        let len: u32 = p.get()?;
        let mut content_parser = Parser::new(p.get_front_bytes(len as usize)?);
        let mut result: BTreeMap<K, V> = BTreeMap::new();
        while !content_parser.is_empty() {
            let key: K = content_parser.get()?;
            let value: V = content_parser.get()?;
            result.insert(key, value);
        }
        Ok((result, p.into_inner()))
    }
}
