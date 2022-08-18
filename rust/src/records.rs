//! Struct definitions for all [MCAP Records](https://mcap.dev/specification/index.html#records).
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

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Header<'a> {
    pub library: Cow<'a, str>,
    pub profile: Cow<'a, str>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Footer {
    pub summary_start: u64,
    pub summary_offset_start: u64,
    pub summary_crc: u32,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Schema<'a> {
    pub id: u16,
    pub name: Cow<'a, str>,
    pub encoding: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Channel<'a> {
    pub id: u16,
    pub schema_id: u16,
    pub topic: Cow<'a, str>,
    pub message_encoding: Cow<'a, str>,
    pub metadata: BTreeMap<Cow<'a, str>, Cow<'a, str>>,
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

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Chunk<'a> {
    pub message_start_time: Timestamp,
    pub message_end_time: Timestamp,
    pub uncompressed_size: u64,
    pub uncompressed_crc: u32,
    pub compression: Cow<'a, str>,
    pub records: Cow<'a, [u8]>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct MessageIndex {
    pub channel_id: u16,
    pub records: Vec<(Timestamp, u64)>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct ChunkIndex<'a> {
    pub message_start_time: Timestamp,
    pub message_end_time: Timestamp,
    pub chunk_start_offset: u64,
    pub chunk_length: u64,
    pub message_index_offsets: BTreeMap<u16, u64>,
    pub message_index_length: u64,
    pub compression: Cow<'a, str>,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct Attachment<'a> {
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub name: Cow<'a, str>,
    pub content_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    #[serde(skip_serializing)]
    pub crc: u32,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct AttachmentIndex<'a> {
    pub offset: u64,
    pub length: u64,
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub data_size: u64,
    pub name: Cow<'a, str>,
    pub content_type: Cow<'a, str>,
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

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Metadata<'a> {
    pub name: Cow<'a, str>,
    pub metadata: BTreeMap<Cow<'a, str>, Cow<'a, str>>,
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
    pub name: Cow<'a, str>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct SummaryOffset {
    pub group_opcode: u8,
    pub group_start: u64,
    pub group_length: u64,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct DataEnd {
    pub data_section_crc: u32,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, IntoStatic)]
pub struct AttachmentHeader<'a> {
    pub log_time: Timestamp,
    pub create_time: Timestamp,
    pub name: Cow<'a, str>,
    pub content_type: Cow<'a, str>,
    pub data_len: u64,
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
