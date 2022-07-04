package mcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func readPrefixedString(data []byte, offset int) (s string, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return "", 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return "", 0, io.ErrShortBuffer
	}
	return string(data[offset+4 : offset+length+4]), offset + 4 + length, nil
}

func readPrefixedBytes(data []byte, offset int) (s []byte, newoffset int, err error) {
	if len(data[offset:]) < 4 {
		return nil, 0, io.ErrShortBuffer
	}
	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data[offset+4:]) < length {
		return nil, 0, io.ErrShortBuffer
	}
	return data[offset+4 : offset+length+4], offset + 4 + length, nil
}

func readPrefixedMap(data []byte, offset int) (result map[string]string, newoffset int, err error) {
	var key, value string
	var inset int
	m := make(map[string]string)
	maplen, offset, err := getUint32(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read map length: %w", err)
	}
	for uint32(offset+inset) < uint32(offset)+maplen {
		key, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read map key: %w", err)
		}
		value, inset, err = readPrefixedString(data[offset:], inset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read map value: %w", err)
		}
		m[key] = value
	}
	return m, offset + inset, nil
}

type Reader struct {
	l        *Lexer
	r        io.Reader
	rs       io.ReadSeeker
	channels map[uint16]*Channel
}

type ResolvedMessage struct {
	*Message
	Schema  *Schema
	Channel *Channel
}

type AttachmentReader struct {
	Attachment
	DataReader io.Reader
}

type ContentRecord interface {
	AsMessage() *ResolvedMessage
	AsAttachmentReader() *AttachmentReader
	AsMetadata() *Metadata
}

func (r *ResolvedMessage) AsMessage() *ResolvedMessage             { return r }
func (r *ResolvedMessage) AsAttachmentReader() *AttachmentReader   { return nil }
func (r *ResolvedMessage) AsMetadata() *Metadata                   { return nil }
func (ar *AttachmentReader) AsMessage() *ResolvedMessage           { return nil }
func (ar *AttachmentReader) AsAttachmentReader() *AttachmentReader { return ar }
func (ar *AttachmentReader) AsMetadata() *Metadata                 { return nil }
func (m *Metadata) AsMessage() *ResolvedMessage                    { return nil }
func (m *Metadata) AsAttachmentReader() *AttachmentReader          { return nil }
func (m *Metadata) AsMetadata() *Metadata                          { return m }

type ContentIterator interface {
	Next([]byte) (ContentRecord, error)
}

func Range(it ContentIterator, f func(ContentRecord) error) error {
	for {
		contentRecord, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read record: %w", err)
		}
		err = f(contentRecord)
		if err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}
}

func (r *Reader) unindexedContentIterator(config *contentIteratorConfig) *unindexedContentIterator {
	return &unindexedContentIterator{
		lexer:    r.l,
		channels: make(map[uint16]*Channel),
		schemas:  make(map[uint16]*Schema),
		config:   config,
	}
}

type contentIteratorConfig struct {
	startTime        uint64
	endTime          uint64
	messageFilter    func(*Schema, *Channel) bool
	attachmentFilter func(string) bool
	metadataFilter   func(string) bool
	forceIndexed     bool
	forceUnindexed   bool
}

func (cic *contentIteratorConfig) isWithinTimeBounds(ts uint64) bool {
	if cic.startTime == 0 && cic.endTime == 0 {
		return true
	}
	if ts < cic.startTime {
		return false
	}
	if ts >= cic.endTime {
		return false
	}
	return true
}

func (cic *contentIteratorConfig) shouldIncludeAttachment(ai *AttachmentIndex) bool {
	if cic.attachmentFilter == nil {
		return false
	}
	if !cic.isWithinTimeBounds(ai.LogTime) {
		return false
	}
	if !cic.attachmentFilter(ai.Name) {
		return false
	}
	return true
}

func (cic *contentIteratorConfig) shouldIncludeChunk(
	schemas map[uint16]*Schema,
	channels map[uint16]*Channel,
	ci *ChunkIndex,
) bool {
	if cic.messageFilter == nil {
		return false
	}
	if cic.startTime != 0 || cic.endTime != 0 {
		if ci.MessageEndTime < cic.startTime {
			return false
		}
		if ci.MessageStartTime > cic.endTime {
			return false
		}
	}
	for channelID := range ci.MessageIndexOffsets {
		if channel, ok := channels[channelID]; ok {
			if schema, ok := schemas[channel.SchemaID]; ok {
				if cic.messageFilter(schema, channel) {
					return true
				}
			}
		}
	}
	return false
}

func (cic *contentIteratorConfig) shouldIncludeMetadata(mi *MetadataIndex) bool {
	if cic.metadataFilter == nil {
		return false
	}
	return cic.metadataFilter(mi.Name)
}

type ContentIteratorOption func(*contentIteratorConfig)

func WithTimeBounds(start uint64, end uint64) ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.startTime = start
		c.endTime = end
	}
}

func WithMessagesMatching(messageFilter func(*Schema, *Channel) bool) ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.messageFilter = messageFilter
	}
}

func WithAllMessages() ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.messageFilter = func(*Schema, *Channel) bool { return true }
	}
}

func ForceIndexed() ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.forceIndexed = true
	}
}

func ForceUnindexed() ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.forceUnindexed = true
	}
}

func WithAttachmentsMatching(attachmentFilter func(name string) bool) ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.attachmentFilter = attachmentFilter
	}
}

func WithMetadataMatching(metadataFilter func(name string) bool) ContentIteratorOption {
	return func(c *contentIteratorConfig) {
		c.metadataFilter = metadataFilter
	}
}

func (r *Reader) Content(opts ...ContentIteratorOption) (ContentIterator, error) {
	config := contentIteratorConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	if config.forceIndexed && config.forceUnindexed {
		return nil, errors.New("cannot force indexed and unindexed at the same time")
	}
	if config.forceUnindexed {
		return r.unindexedContentIterator(&config), nil
	}
	if r.rs != nil {
		info, err := r.Info()
		if err != nil {
			return nil, err
		}
		if info != nil {
			return newIndexedContentIterator(r.rs, info, &config), nil
		}
		if config.forceIndexed {
			return nil, errors.New("tried to force an indexed read, but mcap has no summary")
		}
		_, err = r.rs.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek error: %w", err)
		}

		return r.unindexedContentIterator(&config), nil
	}
	if config.forceIndexed {
		return nil, errors.New("tried to force an indexed read, but source is not seekable")
	}
	return r.unindexedContentIterator(&config), nil
}

func (r *Reader) readHeader() (*Header, error) {
	_, err := r.rs.Seek(8, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to header: %w", err)
	}
	buf := make([]byte, 9)
	_, err = io.ReadFull(r.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read header length: %w", err)
	}
	if opcode := buf[0]; opcode != byte(OpHeader) {
		return nil, fmt.Errorf("unexpected opcode %d in header", opcode)
	}
	buf = make([]byte, binary.LittleEndian.Uint64(buf[1:]))
	_, err = io.ReadFull(r.rs, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	return ParseHeader(buf)
}

// parseIndexSection parses the index section of the file and populates the
// related fields of the structure. It must be called prior to any of the other
// access methods.
func (r *Reader) Info() (*Info, error) {
	info := Info{
		Schemas:  make(map[uint16]*Schema, 0),
		Channels: make(map[uint16]*Channel, 0),
	}
	if r.rs == nil {
		return nil, errors.New("parsing info from non-seekable sources unsupported")
	}
	header, err := r.readHeader()
	if err != nil {
		return nil, err
	}
	info.Header = header
	_, err = r.rs.Seek(-8-4-8-8, io.SeekEnd) // magic, plus 20 bytes footer
	if err != nil {
		return nil, fmt.Errorf("seek error: %w", err)
	}
	buf := make([]byte, 8+20)
	_, err = io.ReadFull(r.rs, buf)
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
	_, err = r.rs.Seek(int64(footer.SummaryStart), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to summary start")
	}
	recordBuf := make([]byte, 1024)
	for {
		tokenType, recordReader, recordLen, err := r.l.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next token: %w", err)
		}
		record, err := ReadIntoOrReplace(recordReader, recordLen, &recordBuf)
		if err != nil {
			return nil, fmt.Errorf("failed to read next record: %w", err)
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			info.Schemas[schema.ID] = schema
		case TokenChannel:
			channel, err := ParseChannel(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			info.Channels[channel.ID] = channel
		case TokenAttachmentIndex:
			idx, err := ParseAttachmentIndex(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse attachment index: %w", err)
			}
			info.AttachmentIndexes = append(info.AttachmentIndexes, idx)
		case TokenMetadataIndex:
			idx, err := ParseMetadataIndex(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse metadata index: %w", err)
			}
			info.MetadataIndexes = append(info.MetadataIndexes, idx)
		case TokenChunkIndex:
			idx, err := ParseChunkIndex(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse attachment index: %w", err)
			}
			// if the chunk overlaps with the requested parameters, load it
			info.ChunkIndexes = append(info.ChunkIndexes, idx)
		case TokenStatistics:
			stats, err := ParseStatistics(record)
			if err != nil {
				return nil, fmt.Errorf("failed to parse statistics: %w", err)
			}
			info.Statistics = stats
		case TokenFooter:
			return &info, nil
		}
	}
}

func NewReader(r io.Reader) (*Reader, error) {
	var rs io.ReadSeeker
	if readseeker, ok := r.(io.ReadSeeker); ok {
		rs = readseeker
	}
	lexer, err := NewLexer(r, &LexerOptions{
		EmitChunks: false,
	})
	if err != nil {
		return nil, err
	}
	return &Reader{
		l:        lexer,
		r:        r,
		rs:       rs,
		channels: make(map[uint16]*Channel),
	}, nil
}
