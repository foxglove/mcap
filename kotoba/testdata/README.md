# Vendored MCAP golden

`NoData.hex` is the 75-byte foxglove/mcap conformance fixture
`tests/conformance/data/NoData/NoData.mcap` (empty profile and library, DataEnd
+ Footer, leading and trailing magic), stored as lowercase hex so Git LFS
(`*.mcap`) is not required.

The same bytes are reconstructed as document-vector chunks in `src/mcap.kotoba`
(`golden-nodata`). CI does not need Git LFS or a network fetch.

Recorded fields (little-endian):

- Header opcode `0x01`, content length 8, two empty strings
- DataEnd opcode `0x0F`, `data_section_crc` = 3959079795
- Footer opcode `0x02`, `summary_start` = 0, `summary_offset_start` = 0,
  `summary_crc` = 1875167664

`NoData.hex` is the same bytes as lowercase hex, one line.
