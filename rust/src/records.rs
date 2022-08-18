use std::fmt;

#[repr(u8)]
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

impl fmt::Display for OpCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value: &str = match self {
            OpCode::Header => "Header",
            OpCode::Footer => "Footer",
            OpCode::Schema => "Schema",
            OpCode::Channel => "Channel",
            OpCode::Message => "Message",
            OpCode::Chunk => "Chunk",
            OpCode::MessageIndex => "MessageIndex",
            OpCode::ChunkIndex => "ChunkIndex",
            OpCode::Attachment => "Attachment",
            OpCode::AttachmentIndex => "AttachmentIndex",
            OpCode::Statistics => "Statistics",
            OpCode::Metadata => "Metadata",
            OpCode::MetadataIndex => "MetadataIndex",
            OpCode::SummaryOffset => "SummaryOffset",
            OpCode::DataEnd => "DataEnd",
        };
        write!(f, "{}", value)
    } 
}

