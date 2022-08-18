use std::fmt;
use std::error::Error;

#[derive(Debug, Display)]
pub enum OpCode {
    Header = 0x01,
    Footer = 0x02,
    Schema = 0x03,
    Channel = 0x04,
    Message = 0x05,
    Chunk = 0x06,
    MessageIndex = 0x07,
    ChunkIndex = 0x08,
    Attachment = 0x09,
    AttachmentIndex = 0x0A,
    Statistics = 0x0B,
    Metadata = 0x0C,
    MetadataIndex = 0x0D,
    SummaryOffset = 0x0E,
    DataEnd = 0x0F,
}

pub enum RecordContentView<'a> {
    Header {
        library: &'a str,
        profile: &'a str,
    },
    Footer {
        summary_start: u64,
        summary_offset_start: u64,
        crc: u32,
    },
}

#[derive(Display, Debug)]
pub enum ParseError {
    Unimplemented,
    StringEncoding(std::str::Utf8Error),
    DataTooShort,
}

impl Error for ParseError {
    fn source(&self) => Option<&(dyn Error + 'static) {
        match self {
            StringEncoding(err) => Some(&err),
            _ => None,
        }
    }
}

fn parse_record<'a>(opcode: OpCode, data: &'a [u8]) -> Result<(RecordContentView<'a>, &'a[u8]), ParseError> {
    match opcode {
        OpCode::Header => parse_header(data),
        OpCode::Footer => parse_footer(data),
        _ => Err(ParseError::Unimplemented)
    }
}

fn parse_u32<'a>(data: &'a[u8]) -> Result<(u32, &'a[u8]), ParseError> {
    if data.len() < std::mem::size_of<u32>() {
        return Err(ParseError::DataTooShort)
    }
    let int_bytes, more = data.split_at(std::mem::size_of<u32>());
    return Ok((u32::from_le_bytes(len_bytes.try_into().unwrap()), more));
}

fn parse_u64<'a>(data: &'a[u8]) -> Result<(u64, &'a[u8]), ParseError> {
    if data.len() < std::mem::size_of<u64>() {
        return Err(ParseError::DataTooShort)
    }
    let int_bytes, more = data.split_at(std::mem::size_of<u64>());
    return Ok((u64::from_le_bytes(len_bytes.try_into().unwrap()), more));
}

fn parse_str<'a>(data: &'a[u8]) -> Result<(&'a str, &'a[u8]), ParseError> {
    let str_len, more = parse_u32(data)?;
    if more.len() < str_len {
        return Err(ParseError::DataTooShort)
    }
    let str_bytes, more = remaining.split_at(str_len as usize);
    return (str::from_utf8(str_bytes)?, more);
}

fn parse_header<'a>(data: &'a[u8]) -> Result<RecordContentView<'a>, ParseError> {
    let profile, more = parse_str(data)?;
    let library, more = parse_str(more)?;
    Ok((Record::Header { profile: profile, library: library }, more))
}

fn parse_footer<'a>(data: &'a[u8]) -> Result<RecordContentView<'a>, ParseError> {
    let summary_start, more = parse_u64(data)?;
    let summary_offset_start, more = parse_u64(more)?;
    let crc, more = parse_u32(more)?;
    Ok((Record::Footer { summary_start: summary_start, summary_offset_start: summary_offset_start, crc: crc}, more))
}
