# This Kaitai Struct definition describes the MCAP binary file format. It can be used with tools
# like the web IDE (https://ide.kaitai.io/) or CLI visualizer
# (https://github.com/kaitai-io/kaitai_struct_visualizer) to parse and visualize MCAP files.

meta:
  id: mcap
  title: MCAP
  file-extension: mcap
  license: Apache-2.0
  endian: le
doc-ref: https://github.com/foxglove/mcap
doc: MCAP is a modular container format and logging library for pub/sub messages with arbitrary message serialization. It is primarily intended for use in robotics applications, and works well under various workloads, resource constraints, and durability requirements.

seq:
  - id: header_magic
    contents: [0x89, "MCAP0\r\n"]

  - id: records
    type: record
    repeat: until
    repeat-until: _.op == opcode::footer

  - id: footer_magic
    contents: [0x89, "MCAP0\r\n"]

enums:
  opcode:
    0x01: header
    0x02: footer
    0x03: schema
    0x04: channel
    0x05: message
    0x06: chunk
    0x07: message_index
    0x08: chunk_index
    0x09: attachment
    0x0a: attachment_index
    0x0b: statistics
    0x0c: metadata
    0x0d: metadata_index
    0x0e: summary_offset
    0x0f: data_end

types:
  prefixed_str:
    seq:
      - { id: len, type: u4 }
      - { id: str, type: str, size: len, encoding: UTF-8 }

  tuple_str_str:
    seq:
      - { id: key, type: prefixed_str }
      - { id: value, type: prefixed_str }

  map_str_str:
    types:
      entries:
        seq:
          - { id: entry, type: tuple_str_str, repeat: eos }
    seq:
      - { id: len, type: u4 }
      - { id: entry, size: len, type: entries }

  record:
    seq:
      - { id: op, type: u1, enum: opcode }
      - { id: len, type: u8 }
      - id: body
        size: len
        type:
          switch-on: op
          cases:
            opcode::header: header
            opcode::footer: footer
            opcode::schema: schema
            opcode::channel: channel
            opcode::message: message
            opcode::chunk: chunk
            opcode::message_index: message_index
            opcode::chunk_index: chunk_index
            opcode::attachment: attachment
            opcode::attachment_index: attachment_index
            opcode::statistics: statistics
            opcode::metadata: metadata
            opcode::metadata_index: metadata_index
            opcode::summary_offset: summary_offset
            opcode::data_end: data_end

  header:
    seq:
      - { id: profile, type: prefixed_str }
      - { id: library, type: prefixed_str }

  footer:
    seq:
      - { id: summary_start, type: u8 }
      - { id: summary_offset_start, type: u8 }
      - { id: summary_crc, type: u4 }

  schema:
    seq:
      - { id: id, type: u2 }
      - { id: name, type: prefixed_str }
      - { id: encoding, type: prefixed_str }
      - { id: data_len, type: u4 }
      - { id: data, size: data_len }

  channel:
    seq:
      - { id: id, type: u2 }
      - { id: schema_id, type: u2 }
      - { id: topic, type: prefixed_str }
      - { id: message_encoding, type: prefixed_str }
      - { id: metadata, type: map_str_str }

  message:
    seq:
      - { id: channel_id, type: u2 }
      - { id: sequence, type: u4 }
      - { id: log_time, type: u8 }
      - { id: publish_time, type: u8 }
      - { id: data, size-eos: true }

  chunk:
    types:
      uncompressed_chunk:
        seq:
          - { id: records, type: record, repeat: eos }
    seq:
      - { id: message_start_time, type: u8 }
      - { id: message_end_time, type: u8 }
      - { id: uncompressed_size, type: u8 }
      - { id: uncompressed_crc, type: u4 }
      - { id: compression, type: prefixed_str }
      - { id: records_size, type: u8 }
      - id: records
        size: records_size
        type:
          switch-on: compression.str
          cases:
            '""': uncompressed_chunk

  message_index:
    types:
      message_index_entry:
        seq:
          - { id: log_time, type: u8 }
          - { id: offset, type: u8 }
      message_index_entries:
        seq:
          - { id: entries, type: message_index_entry, repeat: eos }
    seq:
      - { id: channel_id, type: u2 }
      - { id: records_size, type: u4 }
      - { id: records, type: message_index_entries, size: records_size }

  chunk_index:
    types:
      message_index_offset:
        seq:
          - { id: channel_id, type: u2 }
          - { id: offset, type: u8 }
      message_index_offsets:
        seq:
          - { id: entry, type: message_index_offset, repeat: eos }
    seq:
      - { id: message_start_time, type: u8 }
      - { id: message_end_time, type: u8 }
      - { id: chunk_start_offset, type: u8 }
      - { id: chunk_length, type: u8 }
      - { id: message_index_offsets_size, type: u4 }
      - id: message_index_offsets
        size: message_index_offsets_size
        type: message_index_offsets
      - { id: message_index_length, type: u8 }
      - { id: compression, type: prefixed_str }
      - { id: compressed_size, type: u8 }
      - { id: uncompressed_size, type: u8 }
    instances:
      chunk:
        io: _root._io
        type: record
        pos: chunk_start_offset
        size: chunk_length

  attachment:
    seq:
      - { id: log_time, type: u8 }
      - { id: create_time, type: u8 }
      - { id: name, type: prefixed_str }
      - { id: content_type, type: prefixed_str }
      - { id: data_size, type: u8 }
      - { id: data, size: data_size }
      - { id: crc, type: u4 }

  attachment_index:
    seq:
      - { id: offset, type: u8 }
      - { id: length, type: u8 }
      - { id: log_time, type: u8 }
      - { id: create_time, type: u8 }
      - { id: data_size, type: u8 }
      - { id: name, type: prefixed_str }
      - { id: content_type, type: prefixed_str }
    instances:
      attachment:
        io: _root._io
        type: record
        pos: offset
        size: length

  statistics:
    types:
      channel_message_counts:
        seq:
          - id: entry
            type: channel_message_count
            repeat: eos
      channel_message_count:
        seq:
          - { id: channel_id, type: u2 }
          - { id: message_count, type: u8 }
    seq:
      - { id: message_count, type: u8 }
      - { id: schema_count, type: u2 }
      - { id: channel_count, type: u4 }
      - { id: attachment_count, type: u4 }
      - { id: metadata_count, type: u4 }
      - { id: chunk_count, type: u4 }
      - { id: message_start_time, type: u8 }
      - { id: message_end_type, type: u8 }
      - { id: channel_message_counts_size, type: u4 }
      - id: channel_message_counts
        size: channel_message_counts_size
        type: channel_message_counts

  metadata:
    seq:
      - { id: name, type: prefixed_str }
      - { id: metadata, type: map_str_str }

  metadata_index:
    seq:
      - { id: offset, type: u8 }
      - { id: length, type: u8 }
      - { id: name, type: prefixed_str }
    instances:
      metadata:
        io: _root._io
        type: record
        pos: offset
        size: length

  summary_offset:
    seq:
      - { id: group_opcode, type: u1, enum: opcode }
      - { id: group_start, type: u8 }
      - { id: group_length, type: u8 }
    types:
      records:
        seq:
          - { id: records, type: record, repeat: eos }
    instances:
      group:
        io: _root._io
        type: records
        pos: group_start
        size: group_length

  data_end:
    seq:
      - { id: data_section_crc, type: u4 }
