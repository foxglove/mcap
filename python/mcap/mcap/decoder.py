from abc import ABC
from typing import Any, Callable, Optional

from .records import Schema


class DecoderFactory(ABC):
    """Provides functionality to an :py:class:`~mcap.reader.McapReader` to decode message contents.
    McapReader classes can be instantiated with a set of DecoderFactories,
    which are used within :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    """

    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], Any]]:
        """If the message encoding and schema arguments can be decoded by this decoder factory,
        returns a callable to decode message bytes."""
        return None
