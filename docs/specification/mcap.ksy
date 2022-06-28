# This Kaitai Struct definition describes the MCAP binary file format. It can be used with tools
# like the web IDE (https://ide.kaitai.io/) or CLI visualizer
# (https://github.com/kaitai-io/kaitai_struct_visualizer) to parse and visualize MCAP files.

meta:
  id: mcap
  title: MCAP
  file-extension: mcap
  license: Apache-2.0
  endian: le
doc: |
  MCAP is a modular container format and logging library for pub/sub messages with
  arbitrary message serialization. It is primarily intended for use in robotics
  applications, and works well under various workloads, resource constraints, and
  durability requirements.

  Time values (`log_time`, `publish_time`, `create_time`) are represented in
  nanoseconds since a user-understood epoch (i.e. Unix epoch, robot boot time,
  etc.)
doc-ref: https://github.com/foxglove/mcap/tree/c1cc51d/docs/specification#readme
seq:
  - id: header_magic
    type: magic

  - id: records
    type: record
    repeat: until
    repeat-until: _.op == opcode::footer

  - id: footer_magic
    type: magic

instances:
  footer:
    pos: ofs_footer
    size-eos: true
    type: record
  ofs_footer:
    value: "_io.size - footer.op._sizeof - footer.len_body._sizeof - sizeof<footer> - sizeof<magic>"

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
  magic:
    seq:
      - id: magic
        contents: [0x89, "MCAP0\r\n"]

  prefixed_str:
    seq:
      - id: len_str
        type: u4
      - id: str
        type: str
        size: len_str
        encoding: UTF-8

  tuple_str_str:
    seq:
      - id: key
        type: prefixed_str
      - id: value
        type: prefixed_str

  map_str_str:
    seq:
      - id: len_entries
        type: u4
      - id: entries
        size: len_entries
        type: entries
    types:
      entries:
        seq:
          - id: entries
            type: tuple_str_str
            repeat: eos

  records:
    seq:
      - id: records
        type: record
        repeat: eos

  record:
    seq:
      - id: op
        type: u1
        enum: opcode
      - id: len_body
        type: u8
      - id: body
        size: len_body
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
      - id: profile
        type: prefixed_str
      - id: library
        type: prefixed_str

  footer:
    seq:
      - id: ofs_summary_section
        -orig-id: summary_start
        type: u8
      - id: ofs_summary_offset_section
        -orig-id: summary_offset_start
        type: u8
      - id: summary_crc32
        -orig-id: summary_crc
        type: u4
        doc: |
          A CRC-32 of all bytes from the start of the Summary section up through and
          including the end of the previous field (summary_offset_start) in the footer
          record. A value of 0 indicates the CRC-32 is not available.
    instances:
      summary_section:
        io: _root._io
        pos: ofs_summary_section
        size: "(ofs_summary_offset_section != 0 ? ofs_summary_offset_section : _root.ofs_footer) - ofs_summary_section"
        type: records
        if: ofs_summary_section != 0
      summary_offset_section:
        io: _root._io
        pos: ofs_summary_offset_section
        size: "_root.ofs_footer - ofs_summary_offset_section"
        type: records
        if: ofs_summary_offset_section != 0
      ofs_summary_crc32_input:
        value: "ofs_summary_section != 0 ? ofs_summary_section : _root.ofs_footer"
      summary_crc32_input:
        io: _root._io
        pos: ofs_summary_crc32_input
        size: "_root._io.size - ofs_summary_crc32_input - sizeof<magic> - summary_crc32._sizeof"

  schema:
    seq:
      - id: id
        type: u2
      - id: name
        type: prefixed_str
      - id: encoding
        type: prefixed_str
      - id: len_data
        type: u4
      - id: data
        size: len_data

  channel:
    seq:
      - id: id
        type: u2
      - id: schema_id
        type: u2
      - id: topic
        type: prefixed_str
      - id: message_encoding
        type: prefixed_str
      - id: metadata
        type: map_str_str

  message:
    seq:
      - id: channel_id
        type: u2
      - id: sequence
        type: u4
      - id: log_time
        type: u8
      - id: publish_time
        type: u8
      - id: data
        size-eos: true

  chunk:
    seq:
      - id: message_start_time
        type: u8
      - id: message_end_time
        type: u8
      - id: uncompressed_size
        type: u8
      - id: uncompressed_crc32
        -orig-id: uncompressed_crc
        type: u4
        doc: |
          CRC-32 checksum of uncompressed `records` field. A value of zero indicates that
          CRC validation should not be performed.
      - id: compression
        type: prefixed_str
      - id: len_records
        type: u8
      - id: records
        size: len_records
        type:
          switch-on: compression.str
          cases:
            '""': records

  message_index:
    seq:
      - id: channel_id
        type: u2
      - id: len_records
        type: u4
      - id: records
        size: len_records
        type: message_index_entries
    types:
      message_index_entry:
        seq:
          - id: log_time
            type: u8
          - id: offset
            type: u8
      message_index_entries:
        seq:
          - id: entries
            type: message_index_entry
            repeat: eos

  chunk_index:
    seq:
      - id: message_start_time
        type: u8
      - id: message_end_time
        type: u8
      - id: ofs_chunk
        -orig-id: chunk_start_offset
        type: u8
      - id: len_chunk
        -orig-id: chunk_length
        type: u8
      - id: len_message_index_offsets
        type: u4
      - id: message_index_offsets
        size: len_message_index_offsets
        type: message_index_offsets
      - id: message_index_length
        type: u8
      - id: compression
        type: prefixed_str
      - id: compressed_size
        type: u8
      - id: uncompressed_size
        type: u8
    instances:
      chunk:
        io: _root._io
        pos: ofs_chunk
        size: len_chunk
        type: record
    types:
      message_index_offset:
        seq:
          - id: channel_id
            type: u2
          - id: offset
            type: u8
      message_index_offsets:
        seq:
          - id: entries
            type: message_index_offset
            repeat: eos

  attachment:
    seq:
      - id: log_time
        type: u8
      - id: create_time
        type: u8
      - id: name
        type: prefixed_str
      - id: content_type
        type: prefixed_str
      - id: len_data
        type: u8
      - id: data
        size: len_data
      # Trigger _io.pos computation: https://github.com/kaitai-io/kaitai_struct/issues/721#issuecomment-623011059
      - id: invoke_crc32_input_end
        size: 0
        if: crc32_input_end >= 0
      - id: crc32
        -orig-id: crc
        type: u4
        doc: |
          CRC-32 checksum of preceding fields in the record. A value of zero indicates that
          CRC validation should not be performed.

    instances:
      crc32_input_end:
        value: _io.pos
      crc32_input:
        pos: 0
        size: crc32_input_end

  attachment_index:
    seq:
      - id: ofs_attachment
        -orig-id: offset
        type: u8
      - id: len_attachment
        -orig-id: length
        type: u8
      - id: log_time
        type: u8
      - id: create_time
        type: u8
      - id: data_size
        type: u8
      - id: name
        type: prefixed_str
      - id: content_type
        type: prefixed_str
    instances:
      attachment:
        io: _root._io
        pos: ofs_attachment
        size: len_attachment
        type: record

  statistics:
    seq:
      - id: message_count
        type: u8
      - id: schema_count
        type: u2
      - id: channel_count
        type: u4
      - id: attachment_count
        type: u4
      - id: metadata_count
        type: u4
      - id: chunk_count
        type: u4
      - id: message_start_time
        type: u8
      - id: message_end_time
        type: u8
      - id: len_channel_message_counts
        type: u4
      - id: channel_message_counts
        size: len_channel_message_counts
        type: channel_message_counts
    types:
      channel_message_counts:
        seq:
          - id: entries
            type: channel_message_count
            repeat: eos
      channel_message_count:
        seq:
          - id: channel_id
            type: u2
          - id: message_count
            type: u8

  metadata:
    seq:
      - id: name
        type: prefixed_str
      - id: metadata
        type: map_str_str

  metadata_index:
    seq:
      - id: ofs_metadata
        -orig-id: offset
        type: u8
      - id: len_metadata
        -orig-id: length
        type: u8
      - id: name
        type: prefixed_str
    instances:
      metadata:
        io: _root._io
        pos: ofs_metadata
        size: len_metadata
        type: record

  summary_offset:
    seq:
      - id: group_opcode
        type: u1
        enum: opcode
      - id: ofs_group
        -orig-id: group_start
        type: u8
      - id: len_group
        -orig-id: group_length
        type: u8
    instances:
      group:
        io: _root._io
        pos: ofs_group
        size: len_group
        type: records

  data_end:
    seq:
      - id: data_section_crc32
        -orig-id: data_section_crc
        type: u4
        doc: |
          CRC-32 of all bytes in the data section. A value of 0 indicates the CRC-32 is not
          available.
