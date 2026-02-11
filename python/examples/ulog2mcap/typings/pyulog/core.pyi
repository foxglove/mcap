"""Stub for pyulog.core (ULog parser)."""

from typing import Any, Dict, List, Optional, Union

class ULog:
    """Parser for ULog (PX4) log files."""

    # Constants
    HEADER_BYTES: bytes
    SYNC_BYTES: bytes
    MSG_TYPE_FORMAT: int
    MSG_TYPE_DATA: int
    MSG_TYPE_INFO: int
    MSG_TYPE_INFO_MULTIPLE: int
    MSG_TYPE_PARAMETER: int
    MSG_TYPE_PARAMETER_DEFAULT: int
    MSG_TYPE_ADD_LOGGED_MSG: int
    MSG_TYPE_REMOVE_LOGGED_MSG: int
    MSG_TYPE_SYNC: int
    MSG_TYPE_DROPOUT: int
    MSG_TYPE_LOGGING: int
    MSG_TYPE_LOGGING_TAGGED: int
    MSG_TYPE_FLAG_BITS: int

    # Properties
    @property
    def start_timestamp(self) -> int: ...
    @property
    def last_timestamp(self) -> int: ...
    @property
    def msg_info_dict(self) -> Dict[str, Any]: ...
    @property
    def msg_info_multiple_dict(self) -> Dict[str, Any]: ...
    @property
    def initial_parameters(self) -> Dict[str, Any]: ...
    @property
    def changed_parameters(self) -> List[tuple[int, str, Any]]: ...
    @property
    def message_formats(self) -> Dict[str, MessageFormat]: ...
    @property
    def logged_messages(self) -> List[MessageLogging]: ...
    @property
    def logged_messages_tagged(self) -> Dict[str, Any]: ...
    @property
    def dropouts(self) -> List[MessageDropout]: ...
    @property
    def data_list(self) -> List[Data]: ...
    @property
    def has_data_appended(self) -> bool: ...
    @property
    def file_corruption(self) -> bool: ...
    @property
    def has_default_parameters(self) -> bool: ...
    def __init__(
        self,
        log_file: Union[str, Any],
        message_name_filter_list: Optional[List[str]] = None,
        disable_str_exceptions: bool = True,
        parse_header_only: bool = False,
    ) -> None: ...
    @staticmethod
    def get_field_size(type_str: str) -> int: ...
    @staticmethod
    def parse_string(cstr: Union[bytes, bytearray]) -> str: ...
    def get_default_parameters(self, default_type: int) -> Dict[str, Any]: ...
    def get_dataset(self, name: str, multi_instance: int = 0) -> Data: ...
    def write_ulog(self, log_file: Union[str, Any]) -> None: ...

    class Data:
        """Topic data for a single topic and instance."""

        multi_id: int
        msg_id: int
        name: str
        field_data: List[Any]
        timestamp_idx: int
        data: Dict[str, Any]

        def __init__(self, message_add_logged_obj: Any) -> None: ...
        def list_value_changes(self, field_name: str) -> List[tuple[Any, Any]]: ...

    class MessageFormat:
        """ULog message format representation."""

        name: str
        fields: List[tuple[str, int, str]]

        def __init__(self, data: bytes, header: Any) -> None: ...

    class MessageLogging:
        """ULog logged string message representation."""

        log_level: int
        timestamp: int
        message: str

        def __init__(self, data: bytes, header: Any) -> None: ...
        def log_level_str(self) -> str: ...

    class MessageLoggingTagged:
        """ULog tagged log string message representation."""

        log_level: int
        tag: int
        timestamp: int
        message: str

        def __init__(self, data: bytes, header: Any) -> None: ...
        def log_level_str(self) -> str: ...

    class MessageDropout:
        """ULog dropout message representation."""

        duration: int
        timestamp: int

        def __init__(self, data: bytes, header: Any, timestamp: int) -> None: ...
