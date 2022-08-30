package cmd

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/foxglove/mcap/go/mcap/readopts"
	"github.com/spf13/cobra"
)

var (
	mergeProfile     string
	mergeCompression string
	mergeChunkSize   int64
	mergeIncludeCRC  bool
	mergeChunked     bool
	mergeOutputFile  string
)

type mergeOpts struct {
	profile     string
	compression string
	chunkSize   int64
	includeCRC  bool
	chunked     bool
}

// schemaID uniquely identifies a schema across the inputs
type schemaID struct {
	inputID  int
	schemaID uint16
}

// channelID uniquely identifies a channel across the inputs
type channelID struct {
	inputID   int
	channelID uint16
}

type mcapMerger struct {
	schemas    map[schemaID]*mcap.Schema
	channels   map[channelID]*mcap.Channel
	schemaIDs  map[schemaID]uint16
	channelIDs map[channelID]uint16

	outputChannelSchemas map[uint16]uint16

	nextChannelID uint16
	nextSchemaID  uint16
	opts          mergeOpts
}

func newMCAPMerger(opts mergeOpts) *mcapMerger {
	return &mcapMerger{
		schemas:              make(map[schemaID]*mcap.Schema),
		channels:             make(map[channelID]*mcap.Channel),
		schemaIDs:            make(map[schemaID]uint16),
		channelIDs:           make(map[channelID]uint16),
		outputChannelSchemas: make(map[uint16]uint16),
		nextChannelID:        1,
		nextSchemaID:         1,
		opts:                 opts,
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
	v, ok := m.schemaIDs[schemaID{
		inputID:  inputID,
		schemaID: inputSchemaID,
	}]
	return v, ok
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
	m.channels[key] = channel
	m.channelIDs[key] = m.nextChannelID
	err := w.WriteChannel(newChannel)
	if err != nil {
		return 0, fmt.Errorf("failed to write channel: %w", err)
	}
	m.nextChannelID++
	return newChannel.ID, nil
}

func (m *mcapMerger) addSchema(w *mcap.Writer, inputID int, schema *mcap.Schema) (uint16, error) {
	key := schemaID{inputID, schema.ID}
	newSchema := &mcap.Schema{
		ID:       m.nextSchemaID, // substitute the next output schema ID
		Name:     schema.Name,
		Encoding: schema.Encoding,
		Data:     schema.Data,
	}
	m.schemas[key] = newSchema
	m.schemaIDs[key] = m.nextSchemaID
	err := w.WriteSchema(newSchema)
	if err != nil {
		return 0, fmt.Errorf("failed to write schema: %w", err)
	}
	m.nextSchemaID++
	return newSchema.ID, nil
}

func buildIterator(r io.Reader) (mcap.MessageIterator, error) {
	reader, err := mcap.NewReader(r)
	if err != nil {
		return nil, err
	}
	iterator, err := reader.Messages(readopts.UsingIndex(false))
	if err != nil {
		return nil, err
	}
	return iterator, nil
}

func (m *mcapMerger) mergeInputs(w io.Writer, inputs []io.Reader) error {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     m.opts.chunked,
		ChunkSize:   m.opts.chunkSize,
		Compression: mcap.CompressionFormat(m.opts.compression),
		IncludeCRC:  m.opts.includeCRC,
	})
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	err = writer.WriteHeader(&mcap.Header{
		Profile: m.opts.profile,
	})
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	iterators := make([]mcap.MessageIterator, len(inputs))
	pq := utils.NewPriorityQueue(nil)

	// for each input reader, initialize an mcap reader and read the first
	// message off. Insert the schema and channel into the output with
	// renumbered IDs, and load the message (with renumbered IDs) into the
	// priority queue.
	for inputID, inputReader := range inputs {
		iterator, err := buildIterator(inputReader)
		if err != nil {
			return fmt.Errorf("failed to build iterator for input %d: %w", inputID, err)
		}
		schema, channel, message, err := iterator.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// the file may be an empty mcap. if so, just ignore it.
				continue
			}
			return fmt.Errorf("failed to read first message on input %d: %w", inputID, err)
		}
		schema.ID, err = m.addSchema(writer, inputID, schema)
		if err != nil {
			return fmt.Errorf("failed to add initial schema for input %d: %w", inputID, err)
		}
		message.ChannelID, err = m.addChannel(writer, inputID, channel)
		if err != nil {
			return fmt.Errorf("failed to add initial channel for input %d: %w", inputID, err)
		}
		iterators[inputID] = iterator

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
			return fmt.Errorf("failed to pull next message: %w", err)
		}

		// if the channel is unknown, need to add it to the output
		var ok bool
		newMessage.ChannelID, ok = m.outputChannelID(msg.InputID, newChannel.ID)
		if !ok {
			_, ok := m.outputSchemaID(msg.InputID, newSchema.ID)
			if !ok {
				// if the schema is unknown, add it to the output
				m.addSchema(writer, msg.InputID, newSchema)
			}
			newMessage.ChannelID, err = m.addChannel(writer, msg.InputID, newChannel)
			if err != nil {
				return fmt.Errorf("failed to add channel: %w", err)
			}
		}
		heap.Push(pq, utils.NewTaggedMessage(msg.InputID, newMessage))
	}
	return writer.Close()
}

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge file1.mcap [file2.mcap] [file3.mcap]...",
	Short: "Merge a selection of mcap files by record timestamp",
	Run: func(cmd *cobra.Command, args []string) {
		if mergeOutputFile == "" && !utils.StdoutRedirected() {
			die("Binary output can screw up your terminal. Supply -o or redirect to a file or pipe.")
		}
		var readers []io.Reader
		for _, arg := range args {
			f, err := os.Open(arg)
			if err != nil {
				die("failed to open %s: %s\n", arg, err)
			}
			defer f.Close()
			readers = append(readers, f)
		}
		opts := mergeOpts{
			profile:     mergeProfile,
			compression: mergeCompression,
			chunkSize:   mergeChunkSize,
			includeCRC:  mergeIncludeCRC,
			chunked:     mergeChunked,
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
			die(err.Error())
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
	mergeCmd.PersistentFlags().StringVarP(
		&mergeProfile,
		"profile",
		"",
		"",
		"profile to record in output header (default: empty string)",
	)
}
