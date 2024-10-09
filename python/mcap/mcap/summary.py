from typing import Dict, List, Optional

from .records import (
    AttachmentIndex,
    Channel,
    ChunkIndex,
    MetadataIndex,
    Schema,
    Statistics,
)


class Summary:
    def __init__(self) -> None:
        """contains information from the summary section of an MCAP.
        :ivar schemas: a dict of schema ID to schema record.
        :ivar channels: a dict of channel ID to channel record.
        :ivar chunk_indexes: a list of ChunkIndex records, indicating the offset and content of
            Chunk records in the MCAP.
        :ivar attachment_indexes: a list of AttachmentIndex records, indicating the offset of
            attachments in the MCAP.
        :ivar metadata_indexes: a list of MetadataIndex records, indicating the offset of metadata
            records in the MCAP.
        """
        self.statistics: Optional[Statistics] = None
        self.schemas: Dict[int, Schema] = {}
        self.channels: Dict[int, Channel] = {}
        self.chunk_indexes: List[ChunkIndex] = []
        self.attachment_indexes: List[AttachmentIndex] = []
        self.metadata_indexes: List[MetadataIndex] = []
