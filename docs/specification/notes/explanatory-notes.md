# Explanatory Notes

The following notes may be useful for users of the MCAP format, including implementers of readers and writers.

## Feature Explanations

The format is intended to support efficient, indexed reading of messages and generation of summary data in both local and remote contexts. "Seeking" should be imagined to incur either a disk seek or an HTTP range request to an object store -- the latter being significantly more costly. In both random access and summarization, features may be unsupported due to choices taken by the writer of the file. For instance, statistics may not include channel message counts, or there may be no message index present. If the index data section is empty, the index_offset in the file footer will be set to zero.

### Scanning for records on specific topics within an interval

The index is designed to support fast local and remote seek/filter operations with minimal seeking or range request overhead. The operation of the index for message reading is as follows:

1. Client queries for all messages on topics /a, /b, /c between t0 and t1
2. Reader reads the fixed-length footer off the end of the file
3. Reader parses the index_offset from the footer, and starts reading from that offset to the end of the file. During this read it will encounter the following in order:
   - A run of channel info records, one per channel in the file
   - A run of Message Group Index records, one per chunk in the file
   - The attachment index records
   - The statistics record

The reader in this case will stop after the chunk index records.

4. Using the channel info records at the start of the read, the reader converts topic names to channel IDs.
5. Using the chunk index records, the reader locates the chunks that must be read, based on the requested start times, channel IDs, and end times. These chunks will be a contiguous run.
6. Readers may access the message data in at least two ways,
   - “full scan”: Seek from the chunk index to the start of the chunk using chunk_offset. Read/decompress the entire chunk, discarding messages not on the requested channels. Skip through the index data and into the next chunk if it is targeted too.
   - “index scan”: Consult the message_index_offsets field in the chunk index record, and use it to locate specific message indexes after the chunk for the channels of interest. These message indexes can be used to obtain a list of offsets, which the reader can seek to and extract messages from.

Which of these options is preferable will tend to depend on the proportion of topics in use, as well as potentially whether the storage system is local or remote.

### Listing and accessing attachments

The format provides the ability to list attachments contained wihtin the file, and quickly extract them from the file contents. To list/select attachments in the file:

1. Read the fixed-length footer and seek to the start of the index data section.
2. Scan forward until encountering the attachment index, then read attachment index records until encountering a record that is not an attachment index.
3. The rcords covered in the previous read will include attachment names, types, sizes, and timestamps. These can be used to fill out a list of attachments for selection.
4. To select an attachment from th efile, seek to the associated offset in the file and unpack the file content from the attachment record.

### Accessing summary statistics

The format provides for fast local or remote access to summary information in the same style as "rosbag info", with the intent of functional parity with rosbag info. For reference, here is an example of the rosbag info output:

```
path:         demo.bag
version:      2.0
duration:     7.8s
start:        Mar 21 2017 19:26:20.10 (1490149580.10)
end:          Mar 21 2017 19:26:27.88 (1490149587.88)
size:         67.1 MB
messages:     1606
compression:        lz4 [79/79 chunks; 56.23%]
uncompressed:       119.1 MB @ 15.3 MB/s
compressed:    67.0 MB @  8.6 MB/s (56.23%)
types:        diagnostic_msgs/DiagnosticArray [60810da900de1dd6ddd437c3503511da]
              radar_driver/RadarTracks        [6a2de2f790cb8bb0e149d45d297462f8]
              sensor_msgs/CompressedImage     [8f7a12909da2c9d3332d540a0977563f]
              sensor_msgs/PointCloud2         [1158d486dd51d683ce2f1be655c3c181]
              sensor_msgs/Range               [c005c34273dc426c67a020a87bc24148]
              tf2_msgs/TFMessage              [94810edda583a504dfda3829e70d7eec]
topics:        /diagnostics               52 msgs    : diagnostic_msgs/DiagnosticArray
              /image_color/compressed       234 msgs    : sensor_msgs/CompressedImage
              /radar/points             156 msgs    : sensor_msgs/PointCloud2
              /radar/range              156 msgs    : sensor_msgs/Range
              /radar/tracks             156 msgs    : radar_driver/RadarTracks
              /tf                       774 msgs    : tf2_msgs/TFMessage
              /velodyne_points           78 msgs    : sensor_msgs/PointCloud2
```

The reader will recover this data from the index as follows:

1. Read the fixed length footer and seek to the index_offset.
2. Read the run of channel info records that follow to get topic names, types, and MD5 data (which in case of ROS1 will be in the user data section), as well as channel IDs to interpret the chunk index records.
3. After the channel infos are the chunk index records, if the file is chunked. From each chunk index record extract the compression algorithm and compressed/uncompressed size. From these the reader can compute the compression statistics shown in the rosbag info summary. For unchunked files this field is omitted.
4. The MCAP version of “rosbag info” will display information about included attachments as well. After reading the chunk index records, the attachment index records will be scanned and incorporated into the summary.
5. Finally, the statistics record is used to compute the start, end, total, and per-channel message counts. The per-channel message counts must be grouped/summed over topics for display.

The only difference between the chunked and unchunked versions of this output will be the chunk compression statistics (“compressed”, “uncompressed”, “compression”), which will be omitted in the case of unchunked files. The summary should be very fast to generate in either local or remote contexts, requiring no seeking around the file to visit chunks.

The above is not meant to prescribe a summary formatting, but to demonstrate that parity with the rosbag summary is supported by MCAP. There are other details we may consider including, like references to per-channel encryption or compression if these features get uptake. We could also enable more interaction with the channel info records, such as quickly obtaining schemas from the file for particular topics.
