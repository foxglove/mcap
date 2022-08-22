use std::error::Error;

#[derive(Debug)]
pub enum OpCode {
    Header,
    Footer,
    Schema,
    Channel,
    Message,
    Chunk,
    MessageIndex,
    ChunkIndex,
    Attachment,
    AttachmentIndex,
    Statistics,
    Metadata,
    MetadataIndex,
    SummaryOffset,
    DataEnd,
    UserOpcode(u8),
}

impl OpCode {
    pub fn from_u8(val: u8) -> Result<Self, ParseError> {
        match val {
            0x00 => Err(ParseError::InvalidOpcode(0x00)),
            0x01 => Ok(OpCode::Header),
            0x02 => Ok(OpCode::Footer),
            0x03 => Ok(OpCode::Schema),
            0x04 => Ok(OpCode::Channel),
            0x05 => Ok(OpCode::Message),
            0x06 => Ok(OpCode::Chunk),
            0x07 => Ok(OpCode::MessageIndex),
            0x08 => Ok(OpCode::ChunkIndex),
            0x09 => Ok(OpCode::Attachment),
            0x0A => Ok(OpCode::AttachmentIndex),
            0x0B => Ok(OpCode::Statistics),
            0x0C => Ok(OpCode::Metadata),
            0x0D => Ok(OpCode::MetadataIndex),
            0x0E => Ok(OpCode::SummaryOffset),
            0x0F => Ok(OpCode::DataEnd),
            x if x < 0x80 => Err(ParseError::InvalidOpcode(x)),
            x => Ok(OpCode::UserOpcode(x)),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum RecordContentView<'a> {
    Header {
        library: &'a str,
        profile: &'a str,
    },
    DataEnd {
        data_section_crc: u32,
    },
    Footer {
        summary_start: u64,
        summary_offset_start: u64,
        crc: u32,
    },
}

#[derive(Debug)]
pub enum ParseError {
    BadMagic,
    InvalidOpcode(u8),
    StringEncoding(std::str::Utf8Error),
    OpCodeNotImplemented(OpCode),
    DataTooShort,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
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

fn parse_u32<'a>(data: &'a [u8]) -> Result<(u32, &'a [u8]), ParseError> {
    if data.len() < std::mem::size_of::<u32>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, more) = data.split_at(std::mem::size_of::<u32>());
    return Ok((u32::from_le_bytes(int_bytes.try_into().unwrap()), more));
}

pub fn parse_u64<'a>(data: &'a [u8]) -> Result<(u64, &'a [u8]), ParseError> {
    if data.len() < std::mem::size_of::<u64>() {
        return Err(ParseError::DataTooShort);
    }
    let (int_bytes, more) = data.split_at(std::mem::size_of::<u64>());
    return Ok((u64::from_le_bytes(int_bytes.try_into().unwrap()), more));
}

fn parse_str<'a>(data: &'a [u8]) -> Result<(&'a str, &'a [u8]), ParseError> {
    let (str_len, more) = parse_u32(data)?;
    println!("str len: {}", str_len);
    if more.len() < str_len as usize {
        return Err(ParseError::DataTooShort);
    }
    let (str_bytes, more) = more.split_at(str_len as usize);
    return Ok((std::str::from_utf8(str_bytes)?, more));
}

fn parse_header<'a>(data: &'a [u8]) -> Result<RecordContentView<'a>, ParseError> {
    let (profile, more) = parse_str(data)?;
    let (library, _) = parse_str(more)?;
    Ok(RecordContentView::Header {
        profile: profile,
        library: library,
    })
}

fn parse_footer<'a>(data: &'a [u8]) -> Result<RecordContentView<'a>, ParseError> {
    let (summary_start, more) = parse_u64(data)?;
    let (summary_offset_start, more) = parse_u64(more)?;
    let (crc, _) = parse_u32(more)?;
    Ok(RecordContentView::Footer {
        summary_start: summary_start,
        summary_offset_start: summary_offset_start,
        crc: crc,
    })
}

fn parse_data_end<'a>(data: &'a [u8]) -> Result<RecordContentView<'a>, ParseError> {
    let (data_section_crc, _) = parse_u32(data)?;
    Ok(RecordContentView::DataEnd {
        data_section_crc: data_section_crc,
    })
}

pub fn parse_record<'a>(
    opcode: OpCode,
    data: &'a [u8],
) -> Result<RecordContentView<'a>, ParseError> {
    match opcode {
        OpCode::Header => parse_header(data),
        OpCode::Footer => parse_footer(data),
        OpCode::DataEnd => parse_data_end(data),
        _ => Err(ParseError::OpCodeNotImplemented(opcode)),
    }
}

#[cfg(test)]
mod tests {}
