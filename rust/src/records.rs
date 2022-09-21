//! Struct definitions for all [MCAP Records](https://mcap.dev/specification/index.html#records).
//!
//! TODO: the `TryFrom<&[u8]>` implementations in this file could be derived with a custom macro.
use crate::parse::{ParseError, Parser};
use lifetime::IntoStatic;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::TryFrom;

pub type Timestamp = u64;

#[derive(Debug, Copy, PartialEq, Eq, Serialize, Deserialize, Clone)]
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

pub struct InvalidOpcode(pub u8);

impl TryFrom<u8> for OpCode {
    type Error = InvalidOpcode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Err(InvalidOpcode(value)),
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
            x if x < 0x80 => Err(InvalidOpcode(x)),
            x => Ok(OpCode::UserOpcode(x)),
        }
    }
}

pub type CowStr<'a> = Cow<'a, str>;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Header<'a> {
    pub library: CowStr<'a>,
    pub profile: CowStr<'a>,
}

impl<'a> TryFrom<&'a [u8]> for Header<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            library: p.get()?,
            profile: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Footer {
    pub summary_start: u64,
    pub summary_offset_start: u64,
    pub summary_crc: u32,
}
impl TryFrom<&[u8]> for Footer {
    type Error = ParseError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            summary_start: p.get()?,
            summary_offset_start: p.get()?,
            summary_crc: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Schema<'a> {
    pub id: u16,
    pub name: CowStr<'a>,
    pub encoding: CowStr<'a>,
    pub data: Cow<'a, [u8]>,
}

impl<'a> TryFrom<&'a [u8]> for Schema<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            id: p.get()?,
            name: p.get()?,
            encoding: p.get()?,
            data: p.get_byte_array()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Channel<'a> {
    pub id: u16,
    pub schema_id: u16,
    pub topic: CowStr<'a>,
    pub message_encoding: CowStr<'a>,
    pub metadata: BTreeMap<CowStr<'a>, CowStr<'a>>,
}

impl<'a> TryFrom<&'a [u8]> for Channel<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            id: p.get()?,
            schema_id: p.get()?,
            topic: p.get()?,
            message_encoding: p.get()?,
            metadata: p.get()?,
        })
    }
}

impl<'a> IntoStatic for Channel<'a> {
    type Static = Channel<'static>;

    fn into_static(self) -> Self::Static {
        Channel {
            id: self.id,
            schema_id: self.schema_id,
            topic: self.topic.into_static(),
            message_encoding: self.message_encoding.into_static(),
            metadata: self
                .metadata
                .iter()
                .map(|(k, v)| (k.clone().into_static(), v.clone().into_static()))
                .collect(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Message<'a> {
    pub channel_id: u16,
    pub sequence: u32,
    pub log_time: Timestamp,
    pub publish_time: Timestamp,
    pub data: Cow<'a, [u8]>,
}

impl<'a> TryFrom<&'a [u8]> for Message<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            channel_id: p.get()?,
            sequence: p.get()?,
            log_time: p.get()?,
            publish_time: p.get()?,
            data: Cow::Borrowed(p.into_inner()),
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Chunk<'a> {
    pub message_start_time: Timestamp,
    pub message_end_time: Timestamp,
    pub uncompressed_size: u64,
    pub uncompressed_crc: u32,
    pub compression: CowStr<'a>,
    pub records: Cow<'a, [u8]>,
}

impl<'a> TryFrom<&'a [u8]> for Chunk<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            message_start_time: p.get()?,
            message_end_time: p.get()?,
            uncompressed_size: p.get()?,
            uncompressed_crc: p.get()?,
            compression: p.get()?,
            records: p.get_long_byte_array()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct MessageIndex {
    pub channel_id: u16,
    pub records: Vec<(Timestamp, u64)>,
}

impl TryFrom<&[u8]> for MessageIndex {
    type Error = ParseError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            channel_id: p.get()?,
            records: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct ChunkIndex<'a> {
    pub message_start_time: Timestamp,
    pub message_end_time: Timestamp,
    pub chunk_start_offset: u64,
    pub chunk_length: u64,
    pub message_index_offsets: BTreeMap<u16, u64>,
    pub message_index_length: u64,
    pub compression: CowStr<'a>,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

impl<'a> TryFrom<&'a [u8]> for ChunkIndex<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            message_start_time: p.get()?,
            message_end_time: p.get()?,
            chunk_start_offset: p.get()?,
            chunk_length: p.get()?,
            message_index_offsets: p.get()?,
            message_index_length: p.get()?,
            compression: p.get()?,
            compressed_size: p.get()?,
            uncompressed_size: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Attachment<'a> {
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub name: CowStr<'a>,
    pub content_type: CowStr<'a>,
    pub data: Cow<'a, [u8]>,
    #[serde(skip_serializing)]
    pub crc: u32,
}

impl<'a> TryFrom<&'a [u8]> for Attachment<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            log_time: p.get()?,
            create_time: p.get()?,
            name: p.get()?,
            content_type: p.get()?,
            data: p.get_long_byte_array()?,
            crc: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct AttachmentIndex<'a> {
    pub offset: u64,
    pub length: u64,
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub data_size: u64,
    pub name: CowStr<'a>,
    pub content_type: CowStr<'a>,
}

impl<'a> TryFrom<&'a [u8]> for AttachmentIndex<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            offset: p.get()?,
            length: p.get()?,
            log_time: p.get()?,
            create_time: p.get()?,
            data_size: p.get()?,
            name: p.get()?,
            content_type: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Statistics {
    pub message_count: u64,
    pub schema_count: u16,
    pub channel_count: u32,
    pub attachment_count: u32,
    pub metadata_count: u32,
    pub chunk_count: u32,
    pub message_start_time: Timestamp,
    pub message_end_time: Timestamp,
    pub channel_message_counts: BTreeMap<u16, u64>,
}
impl TryFrom<&[u8]> for Statistics {
    type Error = ParseError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            message_count: p.get()?,
            schema_count: p.get()?,
            channel_count: p.get()?,
            attachment_count: p.get()?,
            metadata_count: p.get()?,
            chunk_count: p.get()?,
            message_start_time: p.get()?,
            message_end_time: p.get()?,
            channel_message_counts: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Metadata<'a> {
    pub name: CowStr<'a>,
    pub metadata: BTreeMap<CowStr<'a>, CowStr<'a>>,
}

impl<'a> TryFrom<&'a [u8]> for Metadata<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            name: p.get()?,
            metadata: p.get()?,
        })
    }
}

impl<'a> IntoStatic for Metadata<'a> {
    type Static = Metadata<'static>;
    fn into_static(self) -> Self::Static {
        Metadata {
            name: self.name.into_static(),
            metadata: self
                .metadata
                .iter()
                .map(|(k, v)| (k.clone().into_static(), v.clone().into_static()))
                .collect(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct MetadataIndex<'a> {
    pub offset: u64,
    pub length: u64,
    pub name: CowStr<'a>,
}

impl<'a> TryFrom<&'a [u8]> for MetadataIndex<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            offset: p.get()?,
            length: p.get()?,
            name: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct SummaryOffset {
    pub group_opcode: u8,
    pub group_start: u64,
    pub group_length: u64,
}

impl TryFrom<&[u8]> for SummaryOffset {
    type Error = ParseError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            group_opcode: p.get()?,
            group_start: p.get()?,
            group_length: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct DataEnd {
    pub data_section_crc: u32,
}

impl TryFrom<&[u8]> for DataEnd {
    type Error = ParseError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            data_section_crc: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct AttachmentHeader<'a> {
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub name: CowStr<'a>,
    pub content_type: CowStr<'a>,
    pub data_len: u64,
}

impl<'a> TryFrom<&'a [u8]> for AttachmentHeader<'a> {
    type Error = ParseError;
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut p = Parser::new(value);
        Ok(Self {
            log_time: p.get()?,
            create_time: p.get()?,
            name: p.get()?,
            content_type: p.get()?,
            data_len: p.get()?,
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub enum Record<'a> {
    Header(Header<'a>),
    Footer(Footer),
    Schema(Schema<'a>),
    Channel(Channel<'a>),
    Message(Message<'a>),
    Chunk(Chunk<'a>),
    MessageIndex(MessageIndex),
    ChunkIndex(ChunkIndex<'a>),
    Attachment(Attachment<'a>),
    AttachmentIndex(AttachmentIndex<'a>),
    Statistics(Statistics),
    Metadata(Metadata<'a>),
    MetadataIndex(MetadataIndex<'a>),
    SummaryOffset(SummaryOffset),
    DataEnd(DataEnd),
}

pub fn parse_record(opcode: OpCode, buf: &[u8]) -> Result<Record, ParseError> {
    match opcode {
        OpCode::Header => Ok(Record::Header(Header::try_from(buf)?)),
        OpCode::Footer => Ok(Record::Footer(Footer::try_from(buf)?)),
        OpCode::Schema => Ok(Record::Schema(Schema::try_from(buf)?)),
        OpCode::Channel => Ok(Record::Channel(Channel::try_from(buf)?)),
        OpCode::Message => Ok(Record::Message(Message::try_from(buf)?)),
        OpCode::Chunk => Ok(Record::Chunk(Chunk::try_from(buf)?)),
        OpCode::MessageIndex => Ok(Record::MessageIndex(MessageIndex::try_from(buf)?)),
        OpCode::ChunkIndex => Ok(Record::ChunkIndex(ChunkIndex::try_from(buf)?)),
        OpCode::Attachment => Ok(Record::Attachment(Attachment::try_from(buf)?)),
        OpCode::AttachmentIndex => Ok(Record::AttachmentIndex(AttachmentIndex::try_from(buf)?)),
        OpCode::Statistics => Ok(Record::Statistics(Statistics::try_from(buf)?)),
        OpCode::Metadata => Ok(Record::Metadata(Metadata::try_from(buf)?)),
        OpCode::MetadataIndex => Ok(Record::MetadataIndex(MetadataIndex::try_from(buf)?)),
        OpCode::SummaryOffset => Ok(Record::SummaryOffset(SummaryOffset::try_from(buf)?)),
        OpCode::DataEnd => Ok(Record::DataEnd(DataEnd::try_from(buf)?)),
        OpCode::UserOpcode(val) => Err(ParseError::OpCodeNotImplemented(val)),
    }
}

impl<'a> Record<'a> {
    pub fn opcode(&self) -> OpCode {
        match self {
            Self::Header(_) => OpCode::Header,
            Self::Footer(_) => OpCode::Footer,
            Self::Schema(_) => OpCode::Schema,
            Self::Channel(_) => OpCode::Channel,
            Self::Message(_) => OpCode::Message,
            Self::Chunk(_) => OpCode::Chunk,
            Self::MessageIndex(_) => OpCode::MessageIndex,
            Self::ChunkIndex(_) => OpCode::ChunkIndex,
            Self::Attachment(_) => OpCode::Attachment,
            Self::AttachmentIndex(_) => OpCode::AttachmentIndex,
            Self::Statistics(_) => OpCode::Statistics,
            Self::Metadata(_) => OpCode::Metadata,
            Self::MetadataIndex(_) => OpCode::MetadataIndex,
            Self::SummaryOffset(_) => OpCode::SummaryOffset,
            Self::DataEnd(_) => OpCode::DataEnd,
        }
    }
}
