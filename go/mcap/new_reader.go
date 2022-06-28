package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

type ReadOptions struct {
	// optional callback which returns whether to include a given channel of messages in the results.
	ChannelFilter func(schema *Schema, channel *Channel) bool
	// optional callback which returns whether to include a given attachment in the results.
	AttachmentFilter func(name string) bool
	// optional callback which returns whether to include a given metadata record in the results.
	MetadataFilter func(name string) bool
	// Messages and Attachments with LogTimes in the range [StartTime, EndTime) will be included.
	StartTime uint64
	EndTime   uint64

	// Callbacks to handle parsed records of interest. If a non-nil error is returned,
	// reading will be aborted.
	OnMessage    func(*Schema, *Channel, *Message) error
	OnAttachment func(*Attachment) error
	OnMetadata   func(*Metadata) error
	OnStatistics func(*Statistics) error
}

type CallbackReader struct {
	opts ReadOptions
}

func NewCallbackReader(opts ReadOptions) (*CallbackReader, error) {
	if opts.OnMessage == nil && opts.ChannelFilter != nil {
		return nil, fmt.Errorf("must supply OnMessage if ChannelFilter is supplied")
	}
	if opts.OnAttachment == nil && opts.AttachmentFilter != nil {
		return nil, fmt.Errorf("must supply OnAttachment if AttachmentFilter is supplied")
	}
	if opts.OnMetadata == nil && opts.MetadataFilter != nil {
		return nil, fmt.Errorf("must supply OnMetadata if MetadataFilter is supplied")
	}
	return &CallbackReader{opts: opts}, nil
}

func (m *ReadOptions) upperTimeBound() uint64 {
	if m.EndTime == 0 {
		return math.MaxUint64
	}
	return m.EndTime
}

//
func (cbr *CallbackReader) Read(reader io.Reader) error {
	if readSeeker, ok := reader.(io.ReadSeeker); ok {
		return cbr.readIndexed(readSeeker)
	}
	return cbr.readUnindexed(reader)
}

func (cbr *CallbackReader) readUnindexed(r io.Reader) error {
	lexer, err := NewLexer(r)
	if err != nil {
		return err
	}
	schemas := make(map[uint16]*Schema)
	channels := make(map[uint16]*Channel)

	recordBuf := make([]byte, 1024)
	for {
		tokenType, record, err := lexer.Next(recordBuf)
		if err != nil {
			return fmt.Errorf("failed to read mcap: %w", err)
		}
		if len(record) > len(recordBuf) {
			recordBuf = record
		}
		switch tokenType {
		case TokenSchema:
			if cbr.opts.OnMessage == nil {
				continue
			}
			schema, err := ParseSchema(record)
			if err != nil {
				return fmt.Errorf("failed to parse schema record: %w", err)
			}
			if _, ok := schemas[schema.ID]; !ok {
				schemas[schema.ID] = schema
			}
		case TokenChannel:
			if cbr.opts.OnMessage == nil {
				continue
			}
			channel, err := ParseChannel(record)
			if err != nil {
				return fmt.Errorf("failed to parse channel record: %w", err)
			}
			if _, ok := channels[channel.ID]; !ok {
				schema, ok := schemas[channel.SchemaID]
				if !ok {
					return ErrUnknownSchema
				}
				if (cbr.opts.ChannelFilter != nil) && cbr.opts.ChannelFilter(schema, channel) {
					channels[channel.ID] = channel
				}
			}
		case TokenMessage:
			if cbr.opts.OnMessage == nil {
				continue
			}
			message, err := ParseMessage(record)
			if err != nil {
				return fmt.Errorf("failed to parse message record: %w", err)
			}
			if _, ok := channels[message.ChannelID]; !ok {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if message.LogTime <= cbr.opts.StartTime || message.LogTime > cbr.opts.upperTimeBound() {
				// timestamp out of bounds
				continue
			}
			channel := channels[message.ChannelID]
			schema := schemas[channel.SchemaID]
			if err := cbr.opts.OnMessage(schema, channel, message); err != nil {
				return err
			}
		case TokenAttachment:
			if cbr.opts.OnAttachment == nil {
				continue
			}
			attachment, err := ParseAttachment(record)
			if err != nil {
				return fmt.Errorf("failed to parse attachment record: %w", err)
			}
			if attachment.LogTime <= cbr.opts.StartTime || attachment.LogTime > cbr.opts.upperTimeBound() {
				continue
			}

			if err := cbr.opts.OnAttachment(attachment); err != nil {
				return err
			}
		case TokenMetadata:
			if cbr.opts.OnMetadata == nil {
				continue
			}
			metadata, err := ParseMetadata(record)
			if err != nil {
				return err
			}

			if err := cbr.opts.OnMetadata(metadata); err != nil {
				return err
			}
		case TokenDataEnd:
			if cbr.opts.OnStatistics == nil {
				return nil
			}
		case TokenStatistics:
			if cbr.opts.OnStatistics == nil {
				continue
			}
			statistics, err := ParseStatistics(record)
			if err != nil {
				return err
			}
			if err := cbr.opts.OnStatistics(statistics); err != nil {
				return err
			}
		case TokenFooter:
			return nil
		default:
			// skip all other tokens
		}
	}
}

type summary struct {
	statistics        *Statistics
	channels          map[uint16]*Channel
	schemas           map[uint16]*Schema
	chunkIndices      []*ChunkIndex
	attachmentIndices []*AttachmentIndex
	metadataIndices   []*MetadataIndex
}

func parseSummary(rs io.ReadSeeker) (*summary, error) {
	s := &summary{
		channels: make(map[uint16]*Channel),
		schemas:  make(map[uint16]*Schema),
	}
	_, err := rs.Seek(-8-4-8-8, io.SeekEnd) // magic, plus 20 bytes footer
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8+20)
	_, err = io.ReadFull(rs, buf)
	if err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}
	magic := buf[20:]
	if !bytes.Equal(magic, Magic) {
		return nil, fmt.Errorf("not an mcap file")
	}
	footer, err := ParseFooter(buf[:20])
	if err != nil {
		return nil, fmt.Errorf("failed to parse footer: %w", err)
	}

	// scan the whole summary section
	if footer.SummaryStart == 0 {
		return nil, nil
	}
	_, err = rs.Seek(int64(footer.SummaryStart), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to summary start")
	}
	lexer, err := NewLexer(rs, &LexerOptions{SkipMagic: true})
	if err != nil {
		return nil, err
	}
	for {
		tokenType, record, err := lexer.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to get next token: %w", err)
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			s.schemas[schema.ID] = schema
		case TokenChannel:
			channel, err := ParseChannel(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			s.channels[channel.ID] = channel
		case TokenAttachmentIndex:
			idx, err := ParseAttachmentIndex(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse attachment info: %w", err)
			}
			s.attachmentIndices = append(s.attachmentIndices, idx)
		case TokenChunkIndex:
			idx, err := ParseChunkIndex(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse attachment index: %w", err)
			}
			s.chunkIndices = append(s.chunkIndices, idx)
		case TokenStatistics:
			stats, err := ParseStatistics(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse statistics: %w", err)
			}
			s.statistics = stats
		case TokenFooter:
			return s, nil
		}
	}
}

type recordIndex interface {
	start() int64
	len() int64
}

func (ci *ChunkIndex) start() int64      { return (int64)(ci.ChunkStartOffset) }
func (ci *ChunkIndex) len() int64        { return (int64)(ci.ChunkLength + ci.MessageIndexLength) }
func (ai *AttachmentIndex) start() int64 { return (int64)(ai.Offset) }
func (ai *AttachmentIndex) len() int64   { return (int64)(ai.Length) }
func (mi *MetadataIndex) start() int64   { return (int64)(mi.Offset) }
func (mi *MetadataIndex) len() int64     { return (int64)(mi.Length) }

func (cbr *CallbackReader) findMatchingRecordIndices(s *summary) ([]recordIndex, error) {
	out := make([]recordIndex, 0)
	if cbr.opts.OnAttachment != nil {
		for _, attachmentIndex := range s.attachmentIndices {
			if attachmentIndex.LogTime < cbr.opts.StartTime {
				continue
			}
			if attachmentIndex.LogTime > cbr.opts.upperTimeBound() {
				continue
			}
			if cbr.opts.AttachmentFilter != nil && !cbr.opts.AttachmentFilter(attachmentIndex.Name) {
				continue
			}
			out = append(out, attachmentIndex)
		}
	}
	if cbr.opts.OnMetadata != nil {
		for _, metadataIndex := range s.metadataIndices {
			if cbr.opts.MetadataFilter != nil && !cbr.opts.MetadataFilter(metadataIndex.Name) {
				continue
			}
			out = append(out, metadataIndex)
		}
	}
	if cbr.opts.OnMessage != nil {
		for _, chunkIndex := range s.chunkIndices {
			if chunkIndex.MessageEndTime < cbr.opts.StartTime {
				continue
			}
			if chunkIndex.MessageStartTime >= cbr.opts.upperTimeBound() {
				continue
			}
			includeChunk := false
			for _, channel := range s.channels {
				if _, ok := chunkIndex.MessageIndexOffsets[channel.ID]; ok {
					schema, ok := s.schemas[channel.SchemaID]
					if !ok {
						return nil, fmt.Errorf("cannot find schema %d for channel id %d", channel.SchemaID, channel.ID)
					}
					if cbr.opts.ChannelFilter == nil || cbr.opts.ChannelFilter(schema, channel) {
						includeChunk = true
						break
					}
				}
			}
			if includeChunk {
				out = append(out, chunkIndex)
			}
		}
	}
	// sort by offset
	sort.SliceStable(out, func(i, j int) bool { return out[i].start() < out[j].start() })
	return out, nil
}

func maxRecordSize(recordIndices []recordIndex) int64 {
	var max int64
	for _, recordIndex := range recordIndices {
		if recordIndex.len() > max {
			max = recordIndex.len()
		}
	}
	return max
}

func (cbr *CallbackReader) parseMessageIndices(s *summary, buf []byte) ([]*MessageIndex, error) {
	var cursor int
	messageIndices := make([]*MessageIndex, 0)
	for cursor < len(buf) {
		if op := OpCode(buf[cursor]); op != OpMessageIndex {
			return nil, fmt.Errorf("expected a message index, got %v", op)
		}
		cursor++
		length := (int)(binary.LittleEndian.Uint64(buf[cursor : cursor+8]))
		cursor += 8
		messageIndex, err := ParseMessageIndex(buf[cursor : cursor+length])
		if err != nil {
			return nil, err
		}
		cursor += length
		recordInTimeRange := false
		for _, record := range messageIndex.Records {
			if record.Timestamp >= cbr.opts.StartTime && record.Timestamp < cbr.opts.upperTimeBound() {
				recordInTimeRange = true
				break
			}
		}
		if !recordInTimeRange {
			continue
		}
		channel, ok := s.channels[messageIndex.ChannelID]
		if !ok {
			return nil, fmt.Errorf("unknown channel ID in message index %d", messageIndex.ChannelID)
		}
		schema := s.schemas[channel.SchemaID]
		if cbr.opts.ChannelFilter == nil || cbr.opts.ChannelFilter(schema, channel) {
			messageIndices = append(messageIndices, messageIndex)
		}
	}
	return messageIndices, nil
}

func (cbr *CallbackReader) readMessagesFromChunk(chunk *Chunk, indices []*MessageIndex, s *summary) error {
	var decompressed []byte
	switch CompressionFormat(chunk.Compression) {
	case CompressionNone:
		decompressed = chunk.Records
	case CompressionZSTD:
		reader, err := zstd.NewReader(bytes.NewReader(chunk.Records))
		if err != nil {
			return fmt.Errorf("failed to read zstd chunk: %w", err)
		}
		defer reader.Close()
		decompressed, err = io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to decompress zstd chunk: %w", err)
		}
	case CompressionLZ4:
		reader := lz4.NewReader(bytes.NewReader(chunk.Records))
		var err error
		decompressed, err = io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("failed to decompress lz4 chunk: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression %s", chunk.Compression)
	}
	for _, index := range indices {
		for _, record := range index.Records {
			if record.Timestamp < cbr.opts.StartTime || record.Timestamp >= cbr.opts.upperTimeBound() {
				continue
			}
			cursor := record.Offset
			if op := OpCode(decompressed[cursor]); op != OpMessage {
				return fmt.Errorf("message index offset does not point to message")
			}
			cursor++
			length := binary.LittleEndian.Uint64(decompressed[cursor : cursor+8])
			cursor += 8
			message, err := ParseMessage(decompressed[cursor : cursor+length])
			if err != nil {
				return fmt.Errorf("failed to parse message in chunk %w", err)
			}
			channel, ok := s.channels[message.ChannelID]
			if !ok {
				return fmt.Errorf("unknown channel id %d", message.ChannelID)
			}
			schema := s.schemas[channel.SchemaID]
			if err := cbr.opts.OnMessage(schema, channel, message); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cbr *CallbackReader) readIndexed(rs io.ReadSeeker) error {
	summary, err := parseSummary(rs)
	if err != nil {
		return err
	}
	if summary == nil {
		// This MCAP has no summary and is effectively unindexed.
		_, err = rs.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek to start: %w", err)
		}
		return cbr.readUnindexed(rs)
	}

	// Figure out which records we need to read based on the summary.
	indicesToRead, err := cbr.findMatchingRecordIndices(summary)
	if err != nil {
		return err
	}

	buf := make([]byte, maxRecordSize(indicesToRead))

	for _, recordIndex := range indicesToRead {
		if _, err = rs.Seek(recordIndex.start(), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to start: %w", err)
		}
		recordBuf := buf[:recordIndex.len()]

		if _, err = io.ReadFull(rs, recordBuf); err != nil {
			return fmt.Errorf("failed to read record of length %d: %w", len(recordBuf), err)
		}

		switch v := recordIndex.(type) {
		case *ChunkIndex:
			chunk, err := ParseChunk(recordBuf[9:v.ChunkLength])
			if err != nil {
				return fmt.Errorf("failed to parse chunk record: %w", err)
			}
			messageIndices, err := cbr.parseMessageIndices(summary, recordBuf[v.ChunkLength:])
			if err != nil {
				return err
			}
			if err := cbr.readMessagesFromChunk(chunk, messageIndices, summary); err != nil {
				return err
			}
		case *AttachmentIndex:
			attachment, err := ParseAttachment(recordBuf)
			if err != nil {
				return fmt.Errorf("failed to parse attachment record: %w", err)
			}
			if err := cbr.opts.OnAttachment(attachment); err != nil {
				return err
			}
		case *MetadataIndex:
			metadata, err := ParseMetadata(recordBuf)
			if err != nil {
				return fmt.Errorf("failed to parse metadata record: %w", err)
			}
			if err := cbr.opts.OnMetadata(metadata); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unexpected type implementing recordIndex %T", v))
		}
	}
	return nil
}
