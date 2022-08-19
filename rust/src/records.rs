use std::error::Error;

const MAGIC: [u8; 8] = [0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n'];

#[derive(Debug)]
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
    Unimplemented(u8),
    StringEncoding(std::str::Utf8Error),
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

fn parse_u64<'a>(data: &'a [u8]) -> Result<(u64, &'a [u8]), ParseError> {
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

pub struct RecordViewIterator<'a> {
    buf: &'a [u8],
}

impl<'a> RecordViewIterator<'a> {
    pub fn new(buf: &'a [u8]) -> Result<RecordViewIterator<'a>, ParseError> {
        if buf.len() < MAGIC.len() {
            return Err(ParseError::DataTooShort);
        }
        let (magic, more) = buf.split_at(MAGIC.len());
        if MAGIC != magic {
            return Err(ParseError::BadMagic);
        }
        Ok(RecordViewIterator { buf: more })
    }
}

impl<'a> Iterator for RecordViewIterator<'a> {
    type Item = Result<RecordContentView<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == 8 {
            if self.buf == MAGIC {
                return None;
            }
        }
        println!("getting next on buf len {}", self.buf.len());
        if self.buf.len() < 9 {
            return Some(Err(ParseError::DataTooShort));
        }
        let (opcode_bytes, more) = self.buf.split_at(1);
        match parse_u64(more) {
            Err(err) => Some(Err(err)),
            Ok((record_len, more)) => {
                if more.len() < record_len as usize {
                    return Some(Err(ParseError::DataTooShort));
                }
                let (content_bytes, remainder) = more.split_at(record_len as usize);
                let res = match opcode_bytes[0] {
                    x if x == OpCode::Header as u8 => Some(parse_header(content_bytes)),
                    x if x == OpCode::DataEnd as u8 => Some(parse_data_end(content_bytes)),
                    x if x == OpCode::Footer as u8 => Some(parse_footer(content_bytes)),
                    x => Some(Err(ParseError::Unimplemented(x))),
                };
                self.buf = remainder;
                return res;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::PathBuf;

    use super::*;

    fn test_asset_path(name: &'static str) -> PathBuf {
        let pkg_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        if let Some(repo_root) = pkg_path.parent() {
            let mut asset_path: PathBuf = repo_root.to_owned();
            asset_path.push("tests/conformance/data");
            asset_path.push(name);
            return asset_path;
        }
        panic!("expected CARGO_MANIFEST_DIR to be a real path with a parent")
    }

    fn read_test_asset(name: &'static str) -> Vec<u8> {
        let path = test_asset_path(name);
        let mut file = std::fs::File::open(path).unwrap();
        let mut out: Vec<u8> = Vec::new();
        file.read_to_end(&mut out).unwrap();
        return out;
    }

    #[test]
    fn no_data_read() {
        let mcap_data = read_test_asset("NoData/NoData.mcap");
        let expected: [RecordContentView; 3] = [
            RecordContentView::Header {
                library: "",
                profile: "",
            },
            RecordContentView::DataEnd {
                data_section_crc: 0,
            },
            RecordContentView::Footer {
                crc: 1875167664,
                summary_offset_start: 0,
                summary_start: 0,
            },
        ];
        let mut i: usize = 0;
        for res in RecordViewIterator::new(&mcap_data).unwrap() {
            assert!(
                res.is_ok(),
                "Could not parse record {}: {}",
                i,
                res.err().unwrap()
            );
            assert_eq!(res.unwrap(), expected[i]);
            i += 1;
        }
        assert_eq!(i, 3);
    }

    #[test]
    fn it_works() {
        let result = OpCode::Header;
        assert_eq!(format!("{:?}", result), "Header");
    }
}
