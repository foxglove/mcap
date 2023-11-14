package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	concatCompression            string
	concatChunkSize              int64
	concatIncludeCRC             bool
	concatChunked                bool
	concatOutputFile             string
	concatAllowDuplicateMetadata bool
	concatCoalesceChannels       string
)

type concatOpts struct {
	compression            string
	chunkSize              int64
	includeCRC             bool
	chunked                bool
	allowDuplicateMetadata bool
	coalesceChannels       string
}

type mcapConcatenator struct {
	schemaIDs       map[schemaID]uint16
	channelIDs      map[channelID]uint16
	schemaIDByHash  map[HashSum]uint16
	channelIDByHash map[HashSum]uint16
	metadataHashes  map[string]bool
	metadataNames   map[string]bool
	nextChannelID   uint16
	nextSchemaID    uint16
	opts            concatOpts
}

func newMCAPConcatenator(opts concatOpts) *mcapConcatenator {
	return &mcapConcatenator{
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

func (m *mcapConcatenator) outputChannelID(inputID int, inputChannelID uint16) (uint16, bool) {
	v, ok := m.channelIDs[channelID{
		inputID:   inputID,
		channelID: inputChannelID,
	}]
	return v, ok
}

func (m *mcapConcatenator) outputSchemaID(inputID int, inputSchemaID uint16) (uint16, bool) {
	if inputSchemaID == 0 {
		return 0, true
	}
	v, ok := m.schemaIDs[schemaID{
		inputID:  inputID,
		schemaID: inputSchemaID,
	}]
	return v, ok
}

func (m *mcapConcatenator) addMetadata(w *mcap.Writer, metadata *mcap.Metadata) error {
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

func (m *mcapConcatenator) addChannel(w *mcap.Writer, inputID int, channel *mcap.Channel) (uint16, error) {
	outputSchemaID, ok := m.outputSchemaID(inputID, channel.SchemaID)
	if !ok {
		return 0, fmt.Errorf("unknown schema on channel %d for input %d topic %s",
			channel.ID, inputID, channel.Topic)
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

func (m *mcapConcatenator) addSchema(w *mcap.Writer, inputID int, schema *mcap.Schema) error {
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

func (m *mcapConcatenator) concatenateInputs(w io.Writer, inputs []namedReader) error {
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
		reader, err := mcap.NewReader(input.reader)
		if err != nil {
			return fmt.Errorf("failed to open reader on %s: %w", input.name, err)
		}
		defer reader.Close() //nolint:gocritic // we actually want these defered in the loop.
		profiles[inputID] = reader.Header().Profile
		opts := []mcap.ReadOpt{
			mcap.UsingIndex(false),
			mcap.WithMetadataCallback(func(metadata *mcap.Metadata) error {
				return m.addMetadata(writer, metadata)
			})}
		iterator, err := reader.Messages(opts...)
		if err != nil {
			return err
		}
		iterators[inputID] = iterator
	}
	if err := writer.WriteHeader(&mcap.Header{Profile: outputProfile(profiles)}); err != nil {
		return err
	}

	var lastTimestamp uint64
	const fileTimeGap uint64 = 100000000 // 100 milliseconds

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

		// Provides the offset to subtract from all of the timestamps in this file.
		var timestampOffset = message.LogTime - lastTimestamp

		message.LogTime = lastTimestamp

		err = writer.WriteMessage(message)
		if err != nil {
			return fmt.Errorf("failed to write initial message for input %s: %w", inputName, err)
		}

		for {
			newSchema, newChannel, newMessage, err := iterator.Next(nil)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("error on input %s: %w", inputName, err)
			}

			if newMessage.LogTime < timestampOffset {
				return fmt.Errorf("timestamp %d is less than offset %d, sort input files before concatenating",
					newMessage.LogTime, timestampOffset)
			}

			newMessage.LogTime -= timestampOffset
			lastTimestamp = newMessage.LogTime

			var ok bool
			newMessage.ChannelID, ok = m.outputChannelID(inputID, newChannel.ID)

			if !ok {
				if newSchema != nil {
					_, ok := m.outputSchemaID(inputID, newSchema.ID)
					if !ok {
						err := m.addSchema(writer, inputID, newSchema)
						if err != nil {
							return fmt.Errorf("failed to add schema from %s: %w", inputName, err)
						}
					}
				}
				newMessage.ChannelID, err = m.addChannel(writer, inputID, newChannel)
				if err != nil {
					return fmt.Errorf("failed to add channel from %s: %w", inputName, err)
				}
			}

			err = writer.WriteMessage(newMessage)
			if err != nil {
				return fmt.Errorf("failed to write message from %s: %w", inputName, err)
			}
		}

		// Add the gap so the records from one file aren't matched exactly with the previous.
		lastTimestamp += fileTimeGap
	}

	return writer.Close()
}

// concatCmd represents the merge command.
var concatCmd = &cobra.Command{
	Use:   "concat file1.mcap [file2.mcap] [file3.mcap]...",
	Short: "Concatenate a selection of MCAP files so the timestamps are sequential",
	Run: func(cmd *cobra.Command, args []string) {
		if concatOutputFile == "" && !utils.StdoutRedirected() {
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
		opts := concatOpts{
			compression:            concatCompression,
			chunkSize:              concatChunkSize,
			includeCRC:             concatIncludeCRC,
			chunked:                concatChunked,
			allowDuplicateMetadata: concatAllowDuplicateMetadata,
			coalesceChannels:       concatCoalesceChannels,
		}
		concatenator := newMCAPConcatenator(opts)
		var writer io.Writer
		if concatOutputFile == "" {
			writer = os.Stdout
		} else {
			f, err := os.Create(concatOutputFile)
			if err != nil {
				die("failed to open output file %s: %s\n", concatOutputFile, err)
			}
			defer f.Close()
			writer = f
		}
		err := concatenator.concatenateInputs(writer, readers)
		if err != nil {
			die("Merge failure: " + err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(concatCmd)
	concatCmd.PersistentFlags().StringVarP(
		&concatCompression,
		"compression",
		"",
		"zstd",
		"chunk compression algorithm (supported: zstd, lz4, none)",
	)
	concatCmd.PersistentFlags().StringVarP(
		&concatOutputFile,
		"output-file",
		"o",
		"",
		"output file",
	)
	concatCmd.PersistentFlags().Int64VarP(
		&concatChunkSize,
		"chunk-size",
		"",
		8*1024*1024,
		"chunk size to target",
	)
	concatCmd.PersistentFlags().BoolVarP(
		&concatIncludeCRC,
		"include-crc",
		"",
		true,
		"include chunk CRC checksums in output",
	)
	concatCmd.PersistentFlags().BoolVarP(
		&concatChunked,
		"chunked",
		"",
		true,
		"chunk the output file",
	)
	concatCmd.PersistentFlags().BoolVarP(
		&concatAllowDuplicateMetadata,
		"allow-duplicate-metadata",
		"",
		false,
		"Allow duplicate-named metadata records to be merged in the output",
	)
	concatCmd.PersistentFlags().StringVarP(
		&concatCoalesceChannels,
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
