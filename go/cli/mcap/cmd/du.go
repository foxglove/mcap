package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

const (
	mcapMagicSize      = 8 // len(mcap.Magic), appears at start and end of file
	recordEnvelopeSize = 9 // opcode (1 byte) + record length (8 bytes)
	// messageHeaderSize: channelID (2) + sequence (4) + logTime (8) + publishTime (8).
	messageHeaderSize = 22
	// messageOverhead: recordEnvelopeSize + messageHeaderSize (31 bytes before message data).
	messageOverhead = recordEnvelopeSize + messageHeaderSize
	// footerContentSize: SummaryStart (8) + SummaryOffsetStart (8) + SummaryCRC (4).
	footerContentSize = 20
	// footerRecordSize: recordEnvelopeSize + footerContentSize (29 bytes on disk).
	footerRecordSize      = recordEnvelopeSize + footerContentSize
	messageIndexEntrySize = 16 // timestamp (8) + offset (8)
)

type usage struct {
	reader io.ReadSeeker

	channels map[uint16]*mcap.Channel

	// total message size of all messages
	totalMessageSize uint64

	// total message size by topic name
	topicMessageSize map[string]uint64

	totalSize uint64

	// record kind to size
	recordKindSize map[string]uint64
}

func newUsage(reader io.ReadSeeker) *usage {
	return &usage{
		reader:           reader,
		channels:         make(map[uint16]*mcap.Channel),
		topicMessageSize: make(map[string]uint64),
		recordKindSize:   make(map[string]uint64),
		totalSize:        2 * mcapMagicSize, // leading + trailing magic
	}
}

func (instance *usage) processChunk(chunk *mcap.Chunk) error {
	compressionFormat := mcap.CompressionFormat(chunk.Compression)
	var uncompressedBytes []byte

	switch compressionFormat {
	case mcap.CompressionNone:
		uncompressedBytes = chunk.Records
	case mcap.CompressionZSTD:
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader, err := zstd.NewReader(compressedDataReader)
		if err != nil {
			return fmt.Errorf("could not make zstd decoder: %w", err)
		}
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			return fmt.Errorf("could not decompress: %w", err)
		}
	case mcap.CompressionLZ4:
		var err error
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader := lz4.NewReader(compressedDataReader)
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			return fmt.Errorf("could not decompress: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression format: %s", chunk.Compression)
	}

	if uint64(len(uncompressedBytes)) != chunk.UncompressedSize {
		return fmt.Errorf("uncompressed chunk data size != Chunk.uncompressed_size")
	}

	if chunk.UncompressedCRC != 0 {
		crc := crc32.ChecksumIEEE(uncompressedBytes)
		if crc != chunk.UncompressedCRC {
			return fmt.Errorf("invalid CRC: %x != %x", crc, chunk.UncompressedCRC)
		}
	}

	uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

	lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
		SkipMagic:         true,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		return fmt.Errorf("failed to make lexer for chunk bytes: %w", err)
	}
	defer lexer.Close()

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read next token: %w", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		switch tokenType {
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return fmt.Errorf("Error parsing Channel: %w", err)
			}

			instance.channels[channel.ID] = channel
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return fmt.Errorf("Error parsing Message: %w", err)
			}

			channel := instance.channels[message.ChannelID]
			if channel == nil {
				return fmt.Errorf("got a Message record for unknown channel: %d", message.ChannelID)
			}

			messageSize := uint64(len(message.Data))

			instance.totalMessageSize += messageSize
			instance.topicMessageSize[channel.Topic] += messageSize
		}
	}

	return nil
}

func (instance *usage) RunDu() error {
	lexer, err := mcap.NewLexer(instance.reader, &mcap.LexerOptions{
		SkipMagic:         false,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		return err
	}
	defer lexer.Close()

	msg := make([]byte, 1024)
	for {
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to read next token: %w", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		instance.totalSize += uint64(len(data))
		instance.recordKindSize[tokenType.String()] += uint64(len(data))

		switch tokenType {
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return fmt.Errorf("error parsing Channel: %w", err)
			}

			instance.channels[channel.ID] = channel
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return fmt.Errorf("error parsing Message: %w", err)
			}
			channel := instance.channels[message.ChannelID]
			if channel == nil {
				return fmt.Errorf("got a Message record for unknown channel: %d", message.ChannelID)
			}

			messageSize := uint64(len(message.Data))

			instance.totalMessageSize += messageSize
			instance.topicMessageSize[channel.Topic] += messageSize
		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				return fmt.Errorf("error parsing Chunk: %w", err)
			}
			err = instance.processChunk(chunk)
			if err != nil {
				return err
			}
		}
	}

	printRecordTable(instance.recordKindSize, instance.totalSize)
	printTopicTable(instance.topicMessageSize, instance.totalMessageSize)

	return nil
}

func printRecordTable(recordKindSize map[string]uint64, totalSize uint64) {
	fmt.Println("Top level record stats:")
	fmt.Println()

	rows := [][]string{}
	rows = append(rows, []string{
		"record",
		"sum bytes",
		"% of total file bytes",
	}, []string{
		"------",
		"---------",
		"---------------------",
	})

	for recordKind, size := range recordKindSize {
		row := []string{
			recordKind, fmt.Sprintf("%d", size),
			fmt.Sprintf("%f", float32(size)/float32(totalSize)*100.0),
		}

		rows = append(rows, row)
	}

	utils.FormatTable(os.Stdout, rows)
}

func printTopicTable(topicMessageSize map[string]uint64, totalMessageSize uint64) {
	fmt.Println()
	fmt.Println("Message size stats:")
	fmt.Println()

	rows := [][]string{}
	rows = append(rows, []string{
		"topic",
		"sum bytes (uncompressed)",
		"% of total message bytes (uncompressed)",
	}, []string{
		"-----",
		"------------------------",
		"---------------------------------------",
	})

	type topicInfo struct {
		name string
		size uint64
	}
	topicInfos := make([]topicInfo, 0, len(topicMessageSize))
	for topic, size := range topicMessageSize {
		topicInfos = append(topicInfos, topicInfo{topic, size})
	}

	// Sort for largest topics first
	sort.Slice(topicInfos, func(i, j int) bool {
		return topicInfos[i].size > topicInfos[j].size
	})

	for _, info := range topicInfos {
		row := []string{
			info.name,
			humanBytes(info.size),
			fmt.Sprintf("%f", float32(info.size)/float32(totalMessageSize)*100.0),
		}

		rows = append(rows, row)
	}

	utils.FormatTable(os.Stdout, rows)
}

// runDuFromIndex reads only the summary section and message indexes to compute
// space usage without decompressing any chunk data. Used by --rough.
func runDuFromIndex(rs io.ReadSeeker) error {
	// Get file size.
	fileSize, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader, err := mcap.NewReader(rs)
	if err != nil {
		return err
	}
	defer reader.Close()

	info, err := reader.Info()
	if err != nil {
		// If we can't read the summary section, fall back to full scan.
		if _, seekErr := rs.Seek(0, io.SeekStart); seekErr != nil {
			return seekErr
		}
		u := newUsage(rs)
		return u.RunDu()
	}

	// If no summary section or no chunk indexes, fall back to full scan.
	if info.Footer == nil || info.Footer.SummaryStart == 0 || len(info.ChunkIndexes) == 0 {
		if _, seekErr := rs.Seek(0, io.SeekStart); seekErr != nil {
			return seekErr
		}
		u := newUsage(rs)
		return u.RunDu()
	}

	// Compute Table 1: record-type breakdown from ChunkIndex metadata.
	// We use on-disk byte sizes and the actual file size as the denominator
	// for percentages (slightly different accounting from the default path,
	// which uses record content sizes only).
	totalFileSize := uint64(fileSize)
	recordKindSize := make(map[string]uint64)
	var totalChunkOnDisk, totalMIOnDisk uint64

	for _, ci := range info.ChunkIndexes {
		totalChunkOnDisk += ci.ChunkLength
		totalMIOnDisk += ci.MessageIndexLength
	}

	recordKindSize["chunk"] = totalChunkOnDisk
	recordKindSize["message index"] = totalMIOnDisk

	const minFileSize = mcapMagicSize + footerRecordSize + mcapMagicSize
	if totalFileSize >= minFileSize {
		footerStart := totalFileSize - mcapMagicSize - footerRecordSize
		if footerStart > info.Footer.SummaryStart {
			recordKindSize["summary section"] = footerStart - info.Footer.SummaryStart
		}
	}
	recordKindSize["footer"] = footerRecordSize

	// "other" = header record + DataEnd record + any unchunked records in
	// the data section. Computed as the remainder of the data section.
	dataSectionEnd := info.Footer.SummaryStart
	if dataSectionEnd > mcapMagicSize+totalChunkOnDisk+totalMIOnDisk {
		recordKindSize["other"] = dataSectionEnd - mcapMagicSize - totalChunkOnDisk - totalMIOnDisk
	}

	printRecordTable(recordKindSize, totalFileSize)

	// Compute Table 2: per-topic message sizes from MessageIndex records.
	channelTopics := make(map[uint16]string)
	for id, ch := range info.Channels {
		channelTopics[id] = ch.Topic
	}

	topicSizes, totalMsgSize, err := computeTopicSizesFromIndex(rs, info.ChunkIndexes, channelTopics)
	if err != nil {
		return err
	}

	printTopicTable(topicSizes, totalMsgSize)

	return nil
}

// computeTopicSizesFromIndex reads MessageIndex records from disk (in parallel
// when possible) and computes per-topic uncompressed message data sizes using
// offset differencing.
func computeTopicSizesFromIndex(
	rs io.ReadSeeker,
	chunkIndexes []*mcap.ChunkIndex,
	channelTopics map[uint16]string,
) (topicSizes map[string]uint64, totalSize uint64, err error) {
	// Try to use io.ReaderAt for goroutine-safe parallel reads.
	ra, isReaderAt := rs.(io.ReaderAt)

	if isReaderAt && len(chunkIndexes) > 1 {
		return computeTopicSizesParallel(ra, chunkIndexes, channelTopics)
	}
	return computeTopicSizesSequential(rs, chunkIndexes, channelTopics)
}

func computeTopicSizesParallel(
	ra io.ReaderAt,
	chunkIndexes []*mcap.ChunkIndex,
	channelTopics map[uint16]string,
) (topicSizes map[string]uint64, totalSize uint64, err error) {
	numWorkers := runtime.NumCPU()
	if numWorkers > len(chunkIndexes) {
		numWorkers = len(chunkIndexes)
	}

	type result struct {
		topicSizes map[string]uint64
		totalSize  uint64
		err        error
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	work := make(chan *mcap.ChunkIndex, len(chunkIndexes))
	results := make(chan result, len(chunkIndexes))

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ci := range work {
				ts, total, err := processChunkMessageIndexesAt(ra, ci, channelTopics)
				select {
				case results <- result{ts, total, err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, ci := range chunkIndexes {
		work <- ci
	}
	close(work)

	go func() {
		wg.Wait()
		close(results)
	}()

	topicSizes = make(map[string]uint64)

	for r := range results {
		if r.err != nil {
			cancel()
			return nil, 0, r.err
		}
		for topic, size := range r.topicSizes {
			topicSizes[topic] += size
		}
		totalSize += r.totalSize
	}

	return topicSizes, totalSize, nil
}

func computeTopicSizesSequential(
	rs io.ReadSeeker,
	chunkIndexes []*mcap.ChunkIndex,
	channelTopics map[uint16]string,
) (topicSizes map[string]uint64, totalSize uint64, err error) {
	topicSizes = make(map[string]uint64)
	var buf []byte

	for _, ci := range chunkIndexes {
		if ci.MessageIndexLength == 0 {
			continue
		}
		miOffset := int64(ci.ChunkStartOffset + ci.ChunkLength)
		if _, err := rs.Seek(miOffset, io.SeekStart); err != nil {
			return nil, 0, fmt.Errorf("failed to seek to message indexes: %w", err)
		}
		if uint64(cap(buf)) < ci.MessageIndexLength {
			buf = make([]byte, ci.MessageIndexLength)
		} else {
			buf = buf[:ci.MessageIndexLength]
		}
		if _, err := io.ReadFull(rs, buf); err != nil {
			return nil, 0, fmt.Errorf("failed to read message indexes: %w", err)
		}
		ts, total, parseErr := parseChunkMessageIndexes(buf, ci.UncompressedSize, channelTopics)
		if parseErr != nil {
			return nil, 0, parseErr
		}
		for topic, size := range ts {
			topicSizes[topic] += size
		}
		totalSize += total
	}

	return topicSizes, totalSize, nil
}

// processChunkMessageIndexesAt reads MessageIndex records for a single chunk
// using io.ReaderAt (goroutine-safe) and computes per-topic sizes.
func processChunkMessageIndexesAt(
	ra io.ReaderAt,
	ci *mcap.ChunkIndex,
	channelTopics map[uint16]string,
) (topicSizes map[string]uint64, totalSize uint64, err error) {
	if ci.MessageIndexLength == 0 {
		return nil, 0, nil
	}

	miOffset := int64(ci.ChunkStartOffset + ci.ChunkLength)
	buf := make([]byte, ci.MessageIndexLength)
	if _, err := ra.ReadAt(buf, miOffset); err != nil {
		return nil, 0, fmt.Errorf("failed to read message indexes at offset %d: %w", miOffset, err)
	}

	return parseChunkMessageIndexes(buf, ci.UncompressedSize, channelTopics)
}

// parseChunkMessageIndexes parses raw MessageIndex record bytes for a single
// chunk, sorts all message offsets, and computes per-topic data sizes via
// offset differencing.
//
// Each message record within a chunk is:
//
//	recordEnvelopeSize (opcode + length) + messageHeaderSize
//	(channelID + sequence + logTime + publishTime) + data (variable)
//
// So message.Data size = (next_offset - this_offset) - messageOverhead.
//
// NOTE: This approach assumes that no non-message records (Schema, Channel)
// are interleaved between messages within a chunk. The MCAP spec permits
// Schema and Channel records anywhere in a chunk, so if such records appear
// between two messages, the offset difference will include those extra bytes,
// inflating the computed data size for the preceding message. In practice,
// standard MCAP writers emit all Schema/Channel records before messages in
// each chunk, so this does not affect typical files. Omit --rough for exact
// results on files with non-standard record ordering.
func parseChunkMessageIndexes(
	buf []byte,
	uncompressedSize uint64,
	channelTopics map[uint16]string,
) (topicSizes map[string]uint64, totalSize uint64, err error) {
	type offsetEntry struct {
		offset    uint64
		channelID uint16
	}

	entries := make([]offsetEntry, 0, len(buf)/messageIndexEntrySize)
	pos := 0

	for pos+recordEnvelopeSize <= len(buf) {
		if buf[pos] != byte(mcap.OpMessageIndex) {
			return nil, 0, fmt.Errorf("unexpected opcode 0x%02x at offset %d, expected MessageIndex (0x%02x)",
				buf[pos], pos, byte(mcap.OpMessageIndex))
		}
		recordLen := binary.LittleEndian.Uint64(buf[pos+1 : pos+recordEnvelopeSize])
		recordEnd := pos + recordEnvelopeSize + int(recordLen)
		if recordEnd > len(buf) {
			return nil, 0, fmt.Errorf("message index record extends beyond buffer at offset %d", pos)
		}

		mi, err := mcap.ParseMessageIndex(buf[pos+recordEnvelopeSize : recordEnd])
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse message index: %w", err)
		}

		for _, entry := range mi.Records {
			entries = append(entries, offsetEntry{
				offset:    entry.Offset,
				channelID: mi.ChannelID,
			})
		}

		pos = recordEnd
	}

	if pos != len(buf) {
		return nil, 0, fmt.Errorf("message index buffer has %d trailing bytes", len(buf)-pos)
	}

	if len(entries) == 0 {
		return nil, 0, nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].offset < entries[j].offset
	})

	topicSizes = make(map[string]uint64)

	for i, entry := range entries {
		if entry.offset > uncompressedSize {
			return nil, 0, fmt.Errorf("message offset %d exceeds chunk uncompressed size %d",
				entry.offset, uncompressedSize)
		}

		var recordSize uint64
		if i+1 < len(entries) {
			recordSize = entries[i+1].offset - entry.offset
		} else {
			recordSize = uncompressedSize - entry.offset
		}

		if recordSize <= messageOverhead {
			continue
		}

		dataSize := recordSize - messageOverhead
		topic, ok := channelTopics[entry.channelID]
		if !ok {
			return nil, 0, fmt.Errorf("message references unknown channel: %d", entry.channelID)
		}
		topicSizes[topic] += dataSize
		totalSize += dataSize
	}

	return topicSizes, totalSize, nil
}

var duRough bool

var duCmd = &cobra.Command{
	Use:   "du <file>",
	Short: "Report space usage within an MCAP file",
	Long: `This command reports space usage within an mcap file. Space usage for messages is
calculated using the uncompressed size.

Use --rough for a faster approximation that skips chunk decompression. It may
over-count per-topic message sizes when non-message records (Schema, Channel)
are interleaved between messages within a chunk.`,
	Run: func(_ *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]
		err := utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
			if duRough {
				return runDuFromIndex(rs)
			}
			u := newUsage(rs)
			return u.RunDu()
		})
		if err != nil {
			die("Failed to read file %s: %v", filename, err)
		}
	},
}

func init() {
	duCmd.PersistentFlags().BoolVar(&duRough, "rough", false,
		"Fast approximation using message indexes "+
			"(skips decompression, may over-count if non-message records are interleaved in chunks)")
	rootCmd.AddCommand(duCmd)
}
