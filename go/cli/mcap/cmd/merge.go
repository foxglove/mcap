package cmd

import (
	"container/heap"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

type ErrDuplicateMetadataName struct {
	Name string
}

func (e ErrDuplicateMetadataName) Is(target error) bool {
	_, ok := target.(*ErrDuplicateMetadataName)
	return ok
}

func (e *ErrDuplicateMetadataName) Error() string {
	return fmt.Sprintf("metadata name '%s' was previously encountered. "+
		"Supply --allow-duplicate-metadata to override.", e.Name)
}

var (
	mergeCompression            string
	mergeChunkSize              int64
	mergeIncludeCRC             bool
	mergeChunked                bool
	mergeOutputFile             string
	mergeAllowDuplicateMetadata bool
	coalesceChannels            string
)

type mergeOpts struct {
	compression            string
	chunkSize              int64
	includeCRC             bool
	chunked                bool
	allowDuplicateMetadata bool
	coalesceChannels       string
}

// schemaID uniquely identifies a schema across the inputs.
type schemaID struct {
	inputID  int
	schemaID uint16
}

// channelID uniquely identifies a channel across the inputs.
type channelID struct {
	inputID   int
	channelID uint16
}

type HashSum = [md5.Size]byte

type mcapMerger struct {
	schemaIDs       map[schemaID]uint16
	channelIDs      map[channelID]uint16
	schemaIDByHash  map[HashSum]uint16
	channelIDByHash map[HashSum]uint16
	metadataHashes  map[string]bool
	metadataNames   map[string]bool
	nextChannelID   uint16
	nextSchemaID    uint16
	opts            mergeOpts
}

const (
	AutoCoalescing  = "auto"
	ForceCoalescing = "force"
	NoCoalescing    = "none"
)

func newMCAPMerger(opts mergeOpts) *mcapMerger {
	return &mcapMerger{
		schemaIDs:       make(map[schemaID]uint16),
		channelIDs:      make(map[channelID]uint16),
		schemaIDByHash:  make(map[HashSum]uint16),
		channelIDByHash: make(map[HashSum]uint16),
		metadataHashes:  make(map[string]bool),
		metadataNames:   make(map[string]bool),
		nextChannelID:   1,
		nextSchemaID:    1,
		opts:            opts,
	}
}

func (m *mcapMerger) outputChannelID(inputID int, inputChannelID uint16) (uint16, bool) {
	v, ok := m.channelIDs[channelID{
		inputID:   inputID,
		channelID: inputChannelID,
	}]
	return v, ok
}

func (m *mcapMerger) outputSchemaID(inputID int, inputSchemaID uint16) (uint16, bool) {
	if inputSchemaID == 0 {
		return 0, true
	}
	v, ok := m.schemaIDs[schemaID{
		inputID:  inputID,
		schemaID: inputSchemaID,
	}]
	return v, ok
}

func hashMetadata(metadata *mcap.Metadata) (string, error) {
	hasher := md5.New()
	hasher.Write([]byte(metadata.Name))
	bytes, err := json.Marshal(metadata.Metadata)
	if err != nil {
		return "", err
	}
	hasher.Write(bytes)
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash), nil
}

func (m *mcapMerger) addMetadata(w *mcap.Writer, metadata *mcap.Metadata) error {
	if m.metadataNames[metadata.Name] && !m.opts.allowDuplicateMetadata {
		return &ErrDuplicateMetadataName{Name: metadata.Name}
	}
	hash, err := hashMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to compute metadata hash: %w", err)
	}
	if !m.metadataHashes[hash] {
		err := w.WriteMetadata(metadata)
		if err != nil {
			return fmt.Errorf("failed to write metadata: %w", err)
		}
		m.metadataHashes[hash] = true
		m.metadataNames[metadata.Name] = true
	}
	return nil
}

func (m *mcapMerger) addAttachment(w *mcap.Writer, attachment *mcap.Attachment) error {
	err := w.WriteAttachment(attachment)
	if err != nil {
		return fmt.Errorf("failed to write attachment: %w", err)
	}
	return nil
}

func getChannelHash(channel *mcap.Channel, coalesceChannels string) HashSum {
	hasher := md5.New()
	schemaIDBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(schemaIDBytes, channel.SchemaID)
	hasher.Write(schemaIDBytes)
	hasher.Write([]byte(channel.Topic))
	hasher.Write([]byte(channel.MessageEncoding))

	switch coalesceChannels {
	case AutoCoalescing: // Include channel metadata in hash
		for key, value := range channel.Metadata {
			hasher.Write([]byte(key))
			hasher.Write([]byte(value))
		}
	case ForceCoalescing: // Channel metadata is not included in hash
		break
	default:
		die("Invalid value for --coalesce-channels: %s\n", coalesceChannels)
	}

	return HashSum(hasher.Sum(nil))
}

func (m *mcapMerger) addChannel(w *mcap.Writer, inputID int, channel *mcap.Channel) (uint16, error) {
	outputSchemaID, ok := m.outputSchemaID(inputID, channel.SchemaID)
	if !ok {
		return 0, fmt.Errorf("unknown schema on channel %d for input %d topic %s", channel.ID, inputID, channel.Topic)
	}
	key := channelID{inputID, channel.ID}
	newChannel := &mcap.Channel{
		ID:              m.nextChannelID, // substitute the next output channel ID
		SchemaID:        outputSchemaID,  // substitute the output schema ID
		Topic:           channel.Topic,
		MessageEncoding: channel.MessageEncoding,
		Metadata:        channel.Metadata,
	}

	if m.opts.coalesceChannels != NoCoalescing {
		channelHash := getChannelHash(newChannel, m.opts.coalesceChannels)
		channelID, channelKnown := m.channelIDByHash[channelHash]
		if channelKnown {
			m.channelIDs[key] = channelID
			return channelID, nil
		}
		m.channelIDByHash[channelHash] = m.nextChannelID
	}

	m.channelIDs[key] = m.nextChannelID
	err := w.WriteChannel(newChannel)
	if err != nil {
		return 0, fmt.Errorf("failed to write channel: %w", err)
	}
	m.nextChannelID++
	return newChannel.ID, nil
}

func getSchemaHash(schema *mcap.Schema) HashSum {
	hasher := md5.New()
	hasher.Write([]byte(schema.Name))
	hasher.Write([]byte(schema.Encoding))
	hasher.Write(schema.Data)
	return HashSum(hasher.Sum(nil))
}

func (m *mcapMerger) addSchema(w *mcap.Writer, inputID int, schema *mcap.Schema) error {
	key := schemaID{inputID, schema.ID}
	schemaHash := getSchemaHash(schema)
	schemaID, schemaKnown := m.schemaIDByHash[schemaHash]
	if schemaKnown {
		m.schemaIDs[key] = schemaID
		return nil
	}

	newSchema := &mcap.Schema{
		ID:       m.nextSchemaID, // substitute the next output schema ID
		Name:     schema.Name,
		Encoding: schema.Encoding,
		Data:     schema.Data,
	}
	m.schemaIDs[key] = m.nextSchemaID
	m.schemaIDByHash[schemaHash] = m.nextSchemaID
	err := w.WriteSchema(newSchema)
	if err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}
	m.nextSchemaID++
	return nil
}

func outputProfile(profiles []string) string {
	if len(profiles) == 0 {
		return ""
	}
	firstProfile := profiles[0]
	for _, profile := range profiles {
		if profile != firstProfile {
			return ""
		}
	}
	return firstProfile
}

func (m *mcapMerger) mergeInputs(w io.Writer, inputs []namedReader) error {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     m.opts.chunked,
		ChunkSize:   m.opts.chunkSize,
		Compression: mcap.CompressionFormat(m.opts.compression),
		IncludeCRC:  m.opts.includeCRC,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	iterators := make([]mcap.MessageIterator, len(inputs))
	profiles := make([]string, len(inputs))
	pq := utils.NewPriorityQueue(nil)

	// Reset struct members
	m.schemaIDByHash = make(map[HashSum]uint16)
	m.channelIDByHash = make(map[HashSum]uint16)
	m.schemaIDs = make(map[schemaID]uint16)
	m.channelIDs = make(map[channelID]uint16)
	m.nextChannelID = 1
	m.nextSchemaID = 1

	// for each input reader, initialize an mcap reader and read the first
	// message off. Insert the schema and channel into the output with
	// renumbered IDs, and load the message (with renumbered IDs) into the
	// priority queue.
	for inputID, input := range inputs {
		// include a lexer option to process attachments.
		// attachments are appended as they are encountered; log time order is
		// not preserved.
		opt := []mcap.ReaderOpt{
			mcap.WithLexerOptions(&mcap.LexerOptions{
				EmitChunks: false,
				AttachmentCallback: func(attReader *mcap.AttachmentReader) error {
					err := m.addAttachment(writer, &mcap.Attachment{
						LogTime:    attReader.LogTime,
						CreateTime: attReader.CreateTime,
						Name:       attReader.Name,
						MediaType:  attReader.MediaType,
						DataSize:   attReader.DataSize,
						Data:       attReader.Data(),
					})
					return err
				},
			}),
		}
		reader, err := mcap.NewReader(input.reader, opt...)
		if err != nil {
			return fmt.Errorf("failed to open reader on %s: %w", input.name, err)
		}
		defer reader.Close() //nolint:gocritic // we actually want these defered in the loop.
		profiles[inputID] = reader.Header().Profile
		opts := []mcap.ReadOpt{
			mcap.UsingIndex(false),
			mcap.WithMetadataCallback(func(metadata *mcap.Metadata) error {
				return m.addMetadata(writer, metadata)
			}),
		}
		iterator, err := reader.Messages(opts...)
		if err != nil {
			return err
		}
		iterators[inputID] = iterator
	}
	if err := writer.WriteHeader(&mcap.Header{Profile: outputProfile(profiles)}); err != nil {
		return err
	}
	for inputID, iterator := range iterators {
		inputName := inputs[inputID].name
		schema, channel, message, err := iterator.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// the file may be an empty mcap. if so, just ignore it.
				continue
			}
			return fmt.Errorf("error on input %s: %w", inputName, err)
		}
		if schema != nil {
			err = m.addSchema(writer, inputID, schema)
			if err != nil {
				return fmt.Errorf("failed to add initial schema for input %s: %w", inputName, err)
			}
		}
		message.ChannelID, err = m.addChannel(writer, inputID, channel)
		if err != nil {
			return fmt.Errorf("failed to add initial channel for input %s: %w", inputName, err)
		}
		// push the first message onto the priority queue
		heap.Push(pq, utils.NewTaggedMessage(inputID, message))
	}
	// there's one message per input on the heap now. Pop messages off,
	// replacing them with the next message from the corresponding input.
	for pq.Len() > 0 {
		// the message to be written. This is numbered with the correct channel
		// ID for the output, and schemas + channels for it have already been
		// written, so it can be written straight to the output.
		msg := heap.Pop(pq).(utils.TaggedMessage)
		err = writer.WriteMessage(msg.Message)
		if err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}

		// Pull the next message off the iterator, to replace the one just
		// popped from the queue. Before pushing this message, it must be
		// renumbered and the related channels/schemas may need to be inserted.
		newSchema, newChannel, newMessage, err := iterators[msg.InputID].Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// if the iterator is empty, skip this read. No further messages
				// on the input will be drawn from the heap, so we will not hit
				// this code on behalf of the same iterator again. Once this
				// happens for each input the queue will be empty and the loop
				// will break.
				continue
			}
			return fmt.Errorf("error on input on %s: %w", inputs[msg.InputID].name, err)
		}

		// if the channel is unknown, need to add it to the output
		var ok bool
		newMessage.ChannelID, ok = m.outputChannelID(msg.InputID, newChannel.ID)
		if !ok {
			if newSchema != nil {
				_, ok := m.outputSchemaID(msg.InputID, newSchema.ID)
				if !ok {
					// if the schema is unknown, add it to the output
					err := m.addSchema(writer, msg.InputID, newSchema)
					if err != nil {
						return fmt.Errorf("failed to add schema from %s: %w", inputs[msg.InputID].name, err)
					}
				}
			}
			newMessage.ChannelID, err = m.addChannel(writer, msg.InputID, newChannel)
			if err != nil {
				return fmt.Errorf("failed to add channel from %s: %w", inputs[msg.InputID].name, err)
			}
		}
		heap.Push(pq, utils.NewTaggedMessage(msg.InputID, newMessage))
	}
	return writer.Close()
}

type namedReader struct {
	name   string
	reader io.Reader
}

// mergeCmd represents the merge command.
var mergeCmd = &cobra.Command{
	Use:   "merge file1.mcap [file2.mcap] [file3.mcap]...",
	Short: "Merge a selection of MCAP files by record timestamp",
	Run: func(cmd *cobra.Command, args []string) {
		if mergeOutputFile == "" && !utils.StdoutRedirected() {
			die(PleaseRedirect)
		}
		var readers []namedReader
		for _, arg := range args {
			f, err := os.Open(arg)
			if err != nil {
				die("failed to open %s: %s\n", arg, err)
			}
			defer f.Close()
			readers = append(readers, namedReader{name: arg, reader: f})
		}
		opts := mergeOpts{
			compression:            mergeCompression,
			chunkSize:              mergeChunkSize,
			includeCRC:             mergeIncludeCRC,
			chunked:                mergeChunked,
			allowDuplicateMetadata: mergeAllowDuplicateMetadata,
			coalesceChannels:       coalesceChannels,
		}
		merger := newMCAPMerger(opts)
		var writer io.Writer
		if mergeOutputFile == "" {
			writer = os.Stdout
		} else {
			f, err := os.Create(mergeOutputFile)
			if err != nil {
				die("failed to open output file %s: %s\n", mergeOutputFile, err)
			}
			defer f.Close()
			writer = f
		}
		err := merger.mergeInputs(writer, readers)
		if err != nil {
			die("Merge failure: " + err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(mergeCmd)
	mergeCmd.PersistentFlags().StringVarP(
		&mergeCompression,
		"compression",
		"",
		"zstd",
		"chunk compression algorithm (supported: zstd, lz4, none)",
	)
	mergeCmd.PersistentFlags().StringVarP(
		&mergeOutputFile,
		"output-file",
		"o",
		"",
		"output file",
	)
	mergeCmd.PersistentFlags().Int64VarP(
		&mergeChunkSize,
		"chunk-size",
		"",
		8*1024*1024,
		"chunk size to target",
	)
	mergeCmd.PersistentFlags().BoolVarP(
		&mergeIncludeCRC,
		"include-crc",
		"",
		true,
		"include chunk CRC checksums in output",
	)
	mergeCmd.PersistentFlags().BoolVarP(
		&mergeChunked,
		"chunked",
		"",
		true,
		"chunk the output file",
	)
	mergeCmd.PersistentFlags().BoolVarP(
		&mergeAllowDuplicateMetadata,
		"allow-duplicate-metadata",
		"",
		false,
		"Allow duplicate-named metadata records to be merged in the output",
	)
	mergeCmd.PersistentFlags().StringVarP(
		&coalesceChannels,
		"coalesce-channels",
		"",
		"auto",
		`channel coalescing behavior (supported: auto, force, none).
 - auto: Coalesce channels with matching topic, schema and metadata
 - force: Same as auto but ignores metadata
 - none: Do not coalesce channels
`,
	)
}
