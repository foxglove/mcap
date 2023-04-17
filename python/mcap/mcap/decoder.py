from abc import ABC

from typing import Optional, Callable, Any
from .records import Schema


class DecoderFactory(ABC):
    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], Any]]:
        """If the message encoding and schema arguments can be decoded by this decoder factory,
        returns a callable to decode message bytes."""
        return None
