package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

type filterFlags struct {
	output                      string
	includeTopics               []string
	excludeTopics               []string
	includeLastPerChannelTopics []string
	startSec                    uint64
	endSec                      uint64
	startNano                   uint64
	endNano                     uint64
	start                       string
	end                         string
	includeMetadata             bool
	includeAttachments          bool
	outputCompression           string
	chunkSize                   int64
	unchunked                   bool
}

type filterOpts struct {
	recover                     bool
	output                      string
	includeTopics               []regexp.Regexp
	excludeTopics               []regexp.Regexp
	includeLastPerChannelTopics []regexp.Regexp
	start                       uint64
	end                         uint64
	includeMetadata             bool
	includeAttachments          bool
	compressionFormat           mcap.CompressionFormat
	chunkSize                   int64
	unchunked                   bool
}

// parseDateOrNanos parses a string containing either an RFC3339-formatted date with timezone
// or a decimal number of nanoseconds. It returns a uint64 timestamp in nanoseconds.
func parseDateOrNanos(dateOrNanos string) (uint64, error) {
	intNanos, err := strconv.ParseUint(dateOrNanos, 10, 64)
	if err == nil {
		return intNanos, nil
	}
	date, err := time.Parse(time.RFC3339, dateOrNanos)
	if err != nil {
		return 0, err
	}
	return uint64(date.UnixNano()), nil
}

// parseTimestampArgs implements the semantics for setting start and end times in the CLI.
// a non-default value in `dateOrNanos` overrides `nanoseconds`, which overrides `seconds`.
func parseTimestampArgs(dateOrNanos string, nanoseconds uint64, seconds uint64) (uint64, error) {
	if dateOrNanos != "" {
		return parseDateOrNanos(dateOrNanos)
	}
	if nanoseconds != 0 {
		return nanoseconds, nil
	}
	return seconds * 1e9, nil
}

func buildFilterOptions(flags *filterFlags) (*filterOpts, error) {
	opts := &filterOpts{
		output:             flags.output,
		includeMetadata:    flags.includeMetadata,
		includeAttachments: flags.includeAttachments,
	}
	start, err := parseTimestampArgs(flags.start, flags.startNano, flags.startSec)
	if err != nil {
		return nil, fmt.Errorf("invalid start: %w", err)
	}
	opts.start = start
	end, err := parseTimestampArgs(flags.end, flags.endNano, flags.endSec)
	if err != nil {
		return nil, fmt.Errorf("invalid end: %w", err)
	}
	opts.end = end
	if opts.end == 0 {
		opts.end = math.MaxUint64
	}
	if opts.end < opts.start {
		return nil, errors.New("invalid time range query, end-time is before start-time")
	}
	if len(flags.includeTopics) > 0 && len(flags.excludeTopics) > 0 {
		return nil, errors.New("can only use one of --include-topic-regex and --exclude-topic-regex")
	}
	opts.compressionFormat = mcap.CompressionNone
	switch flags.outputCompression {
	case "zstd":
		opts.compressionFormat = mcap.CompressionZSTD
	case "lz4":
		opts.compressionFormat = mcap.CompressionLZ4
	case "none":
	case "":
		opts.compressionFormat = mcap.CompressionNone
	default:
		return nil, fmt.Errorf(
			"unrecognized compression format '%s': valid options are 'lz4', 'zstd', or 'none'",
			flags.outputCompression,
		)
	}

	includeTopics, err := compileMatchers(flags.includeTopics)
	if err != nil {
		return nil, fmt.Errorf("invalid included topic regex: %w", err)
	}
	opts.includeTopics = includeTopics

	excludeTopics, err := compileMatchers(flags.excludeTopics)
	if err != nil {
		return nil, fmt.Errorf("invalid excluded topic regex: %w", err)
	}
	opts.excludeTopics = excludeTopics

	includeLastPerChannelTopics, err := compileMatchers(flags.includeLastPerChannelTopics)
	if err != nil {
		return nil, fmt.Errorf("invalid last-per-channel topic regex: %w", err)
	}
	opts.includeLastPerChannelTopics = includeLastPerChannelTopics

	opts.chunkSize = flags.chunkSize
	opts.unchunked = flags.unchunked
	return opts, nil
}

func run(filterOptions *filterOpts, args []string) {
	var reader io.Reader
	if len(args) == 0 {
		stat, err := os.Stdin.Stat()
		if err != nil {
			die("failed to check stdin state: %s", err)
		}
		if stat.Mode()&os.ModeCharDevice == 0 {
			reader = os.Stdin
		} else {
			die("please supply a file. see --help for usage details.")
		}
	} else {
		closeFile, newReader, err := utils.GetReader(context.Background(), args[0])
		if err != nil {
			die("failed to open source for reading: %s", err)
		}
		defer func() {
			if closeErr := closeFile(); closeErr != nil {
				die("error closing read source: %s", closeErr)
			}
		}()
		reader = newReader
	}

	var writer io.Writer
	if filterOptions.output == "" {
		if !utils.StdoutRedirected() {
			die(PleaseRedirect)
		}
		writer = os.Stdout
	} else {
		newWriter, err := os.Create(filterOptions.output)
		if err != nil {
			die("failed to open %s for writing: %s", filterOptions.output, err)
		}
		defer func() {
			if err := newWriter.Close(); err != nil {
				die("error closing write target: %s", err)
			}
		}()
		writer = newWriter
	}

	err := filter(reader, writer, filterOptions)
	if err != nil {
		die("failed to filter: %s", err)
	}
}

func compileMatchers(regexStrings []string) ([]regexp.Regexp, error) {
	matchers := make([]regexp.Regexp, len(regexStrings))

	for i, regexString := range regexStrings {
		// auto-surround with ^$ if not specified.
		if regexString[:1] != "^" {
			regexString = "^" + regexString
		}
		if regexString[len(regexString)-1:] != "$" {
			regexString += "$"
		}
		regex, err := regexp.Compile(regexString)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid regex: %w", regexString, err)
		}
		matchers[i] = *regex
	}
	return matchers, nil
}

// includeTopic determines whether a topic should be included given the filter options.
// Precedence:
// - If include regexes are provided, only topics matching any include are included.
// - Else if exclude regexes are provided, topics not matching any exclude are included.
// - Else (no filters), include all topics.
func includeTopic(topic string, opts *filterOpts) bool {
	if len(opts.includeTopics) > 0 {
		for i := range opts.includeTopics {
			if opts.includeTopics[i].MatchString(topic) {
				return true
			}
		}
		return false
	}
	if len(opts.excludeTopics) > 0 {
		for i := range opts.excludeTopics {
			if opts.excludeTopics[i].MatchString(topic) {
				return false
			}
		}
		return true
	}
	return true
}

type markableSchema struct {
	*mcap.Schema
	written bool
}
type markableChannel struct {
	*mcap.Channel
	written bool
}

func filter(
	r io.Reader,
	w io.Writer,
	opts *filterOpts,
) error {
	// Dispatch to an indexed-reader path when the input is seekable; otherwise fall back
	// to the streaming lexer path.
	if rs, ok := r.(io.ReadSeeker); ok {
		return filterSeekable(rs, w, opts)
	}
	return filterStreaming(r, w, opts)
}

func filterSeekable(
	rs io.ReadSeeker,
	w io.Writer,
	opts *filterOpts,
) error {
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Compression: opts.compressionFormat,
		Chunked:     !opts.unchunked,
		ChunkSize:   opts.chunkSize,
	})
	if err != nil {
		return err
	}
	defer func() {
		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", err)
			return
		}
	}()

	reader, err := mcap.NewReader(rs)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Write header from source
	if err := mcapWriter.WriteHeader(reader.Header()); err != nil {
		return err
	}

	info, err := reader.Info()
	if err != nil {
		return err
	}

	// Build concrete topic list from regex include/exclude
	includeAll := len(opts.includeTopics) == 0 && len(opts.excludeTopics) == 0
	topicSet := map[string]struct{}{}
	if !includeAll {
		for _, ch := range info.Channels {
			if includeTopic(ch.Topic, opts) {
				topicSet[ch.Topic] = struct{}{}
			}
		}
	}

	readOpts := []mcap.ReadOpt{
		mcap.AfterNanos(opts.start),
		mcap.BeforeNanos(opts.end),
		mcap.InOrder(mcap.LogTimeOrder),
		mcap.UsingIndex(true),
	}
	if !includeAll {
		var topics []string
		for t := range topicSet {
			topics = append(topics, t)
		}
		readOpts = append(readOpts, mcap.WithTopics(topics))
	}
	if opts.includeMetadata {
		readOpts = append(readOpts, mcap.WithMetadataCallback(func(md *mcap.Metadata) error {
			if err := mcapWriter.WriteMetadata(md); err != nil {
				return err
			}
			return nil
		}))
	}

	writtenSchemas := make(map[uint16]bool)
	writtenChannels := make(map[uint16]bool)

	addMessage := func(schema *mcap.Schema, channel *mcap.Channel, msg *mcap.Message) error {
		// Ensure schema and channel are written before messages
		if !writtenChannels[channel.ID] {
			if channel.SchemaID != 0 && !writtenSchemas[channel.SchemaID] {
				if err := mcapWriter.WriteSchema(schema); err != nil {
					return err
				}
				writtenSchemas[channel.SchemaID] = true
			}
			if err := mcapWriter.WriteChannel(channel); err != nil {
				return err
			}
			writtenChannels[channel.ID] = true
		}
		if channel.SchemaID != 0 && !writtenSchemas[channel.SchemaID] {
			// This invariant should be upheld by the reader, assert on it here.
			die("message iterator returned second channel record with ID %d that has a differing schema ID %d", channel.ID, channel.SchemaID)
		}
		return mcapWriter.WriteMessage(msg)
	}

	msg := &mcap.Message{Data: make([]byte, 1024)}
	// If any lastPerChannelTopics are specified, we iterate backwards from the start time to find them.
	if len(opts.includeLastPerChannelTopics) > 0 {
		channelsToWrite := map[uint16]bool{}
		for _, ch := range info.Channels {
			// make sure the topic is not separately excluded by topic filters
			if includeAll || includeTopic(ch.Topic, opts) {
				for i := range opts.includeLastPerChannelTopics {
					matcher := opts.includeLastPerChannelTopics[i]
					if matcher.MatchString(ch.Topic) {
						channelsToWrite[ch.ID] = true
					}
				}
			}
		}
		topics := make([]string, 0, len(channelsToWrite))
		for id, _ := range channelsToWrite {
			topics = append(topics, info.Channels[id].Topic)
		}

		it, err := reader.Messages(
			mcap.BeforeNanos(opts.start),
			mcap.InOrder(mcap.ReverseLogTimeOrder),
			mcap.UsingIndex(true),
			mcap.WithTopics(topics),
		)
		if err != nil {
			return err
		}
		messagesToWrite := make([]*mcap.Message, 0, len(channelsToWrite))
		for {
			_, channel, msg, err := it.NextInto(nil)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if _, ok := channelsToWrite[channel.ID]; ok {
				messagesToWrite = append(messagesToWrite, msg)
				delete(channelsToWrite, channel.ID)
			}
			if len(channelsToWrite) == 0 {
				break
			}
		}
		// we now have all of the messages we need to write, but they should be written in log time order.
		sort.Slice(messagesToWrite, func(i, j int) bool {
			return messagesToWrite[i].LogTime < messagesToWrite[j].LogTime
		})
		for _, message := range messagesToWrite {
			channel := info.Channels[message.ChannelID]
			var schema *mcap.Schema
			if channel.SchemaID != 0 {
				schema = info.Schemas[channel.SchemaID]
			}
			if err := addMessage(schema, channel, message); err != nil {
				return err
			}
		}

	}

	it, err := reader.Messages(readOpts...)
	if err != nil {
		return err
	}

	for {
		schema, channel, newMsg, err := it.NextInto(msg)
		msg = newMsg
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err := addMessage(schema, channel, msg); err != nil {
			return err
		}
	}

	// Attachments via index
	if opts.includeAttachments {
		for _, aidx := range info.AttachmentIndexes {
			if aidx.LogTime < opts.start || aidx.LogTime >= opts.end {
				continue
			}
			ar, err := reader.GetAttachmentReader(aidx.Offset)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteAttachment(&mcap.Attachment{
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
				DataSize:   ar.DataSize,
				Data:       ar.Data(),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func filterStreaming(
	r io.Reader,
	w io.Writer,
	opts *filterOpts,
) error {
	if len(opts.includeLastPerChannelTopics) > 0 {
		return errors.New("including last-per-channel topics is not supported for streaming input")
	}

	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Compression: opts.compressionFormat,
		Chunked:     !opts.unchunked,
		ChunkSize:   opts.chunkSize,
	})
	if err != nil {
		return err
	}

	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			if !opts.includeAttachments {
				return nil
			}
			if ar.LogTime < opts.start {
				return nil
			}
			if ar.LogTime >= opts.end {
				return nil
			}
			err = mcapWriter.WriteAttachment(&mcap.Attachment{
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
				DataSize:   ar.DataSize,
				Data:       ar.Data(),
			})
			if err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	defer func() {
		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", err)
			return
		}
	}()

	buf := make([]byte, 1024)
	schemas := make(map[uint16]markableSchema)
	channels := make(map[uint16]markableChannel)

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(data) > len(buf) {
			buf = data
		}
		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteHeader(header); err != nil {
				return err
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				return err
			}
			schemas[schema.ID] = markableSchema{schema, false}
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return err
			}
			if includeTopic(channel.Topic, opts) {
				channels[channel.ID] = markableChannel{channel, false}
			}
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return err
			}
			if message.LogTime < opts.start {
				continue
			}
			if message.LogTime >= opts.end {
				continue
			}
			channel, ok := channels[message.ChannelID]
			if !ok {
				continue
			}
			if !channel.written {
				if channel.SchemaID != 0 {
					schema, ok := schemas[channel.SchemaID]
					if !ok {
						return fmt.Errorf("encountered channel with topic %s with unknown schema ID %d", channel.Topic, channel.SchemaID)
					}
					if !schema.written {
						if err := mcapWriter.WriteSchema(schema.Schema); err != nil {
							return err
						}
						schemas[channel.SchemaID] = markableSchema{schema.Schema, true}
					}
				}
				if err := mcapWriter.WriteChannel(channel.Channel); err != nil {
					return err
				}
				channels[message.ChannelID] = markableChannel{channel.Channel, true}
			}
			if err := mcapWriter.WriteMessage(message); err != nil {
				return err
			}
		case mcap.TokenMetadata:
			if !opts.includeMetadata {
				continue
			}
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteMetadata(metadata); err != nil {
				return err
			}
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			return nil
		case mcap.TokenChunk:
			return errors.New("expected lexer to remove chunk records from input stream")
		case mcap.TokenError:
			return errors.New("received error token but lexer did not return error on Next")
		}
	}
}

func init() {
	{
		var filterCmd = &cobra.Command{
			Use:   "filter [file]",
			Short: "Copy some filtered MCAP data to a new file",
			Long: `This subcommand filters an MCAP by topic and time range to a new file.
When multiple regexes are used, topics that match any regex are included (or excluded).
For inputs that support seeking, this command will also put messages in log time order.

usage:
  mcap filter in.mcap -o out.mcap -y /diagnostics -y /tf -y /camera_(front|back)`,
		}
		output := filterCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		includeTopics := filterCmd.PersistentFlags().StringArrayP(
			"include-topic-regex",
			"y",
			[]string{},
			"messages with topic names matching this regex will be included, can be supplied multiple times",
		)
		excludeTopics := filterCmd.PersistentFlags().StringArrayP(
			"exclude-topic-regex",
			"n",
			[]string{},
			"messages with topic names matching this regex will be excluded, can be supplied multiple times",
		)
		includeLastPerChannelTopics := filterCmd.PersistentFlags().StringArrayP(
			"last-per-channel-topic-regex",
			"l",
			[]string{},
			"For included topics matching this regex, the most recent message prior to the start time"+
				" will still be included. Not supported for streaming input.",
		)
		start := filterCmd.PersistentFlags().StringP(
			"start",
			"S",
			"",
			"only include messages logged at or after this time. Accepts integer nanoseconds or RFC3339-formatted date.",
		)
		startSec := filterCmd.PersistentFlags().Uint64P(
			"start-secs",
			"s",
			0,
			"only include messages logged at or after this time. Accepts integer seconds."+
				"Ignored if `--start` or `--start-nsecs` are used.",
		)
		startNano := filterCmd.PersistentFlags().Uint64(
			"start-nsecs",
			0,
			"deprecated, use --start. Only include messages logged at or after this time. Accepts integer nanoseconds.",
		)
		end := filterCmd.PersistentFlags().StringP(
			"end",
			"E",
			"",
			"Only include messages logged before this time. Accepts integer nanoseconds or RFC3339-formatted date.",
		)
		endSec := filterCmd.PersistentFlags().Uint64P(
			"end-secs",
			"e",
			0,
			"only include messages logged before this time. Accepts integer seconds."+
				"Ignored if `--end` or `--end-nsecs` are used.",
		)
		endNano := filterCmd.PersistentFlags().Uint64(
			"end-nsecs",
			0,
			"(Deprecated, use --end) Only include messages logged before this time. Accepts integer nanosconds.",
		)

		filterCmd.MarkFlagsMutuallyExclusive("start-secs", "start-nsecs")
		filterCmd.MarkFlagsMutuallyExclusive("end-secs", "end-nsecs")
		chunkSize := filterCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		includeMetadata := filterCmd.PersistentFlags().Bool(
			"include-metadata",
			false,
			"whether to include metadata in the output bag",
		)
		includeAttachments := filterCmd.PersistentFlags().Bool(
			"include-attachments",
			false,
			"whether to include attachments in the output mcap",
		)
		outputCompression := filterCmd.PersistentFlags().String(
			"output-compression",
			"zstd",
			"compression algorithm to use on output file",
		)
		filterCmd.Run = func(_ *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(&filterFlags{
				output:                      *output,
				includeTopics:               *includeTopics,
				excludeTopics:               *excludeTopics,
				includeLastPerChannelTopics: *includeLastPerChannelTopics,
				start:                       *start,
				startSec:                    *startSec,
				startNano:                   *startNano,
				end:                         *end,
				endSec:                      *endSec,
				endNano:                     *endNano,
				chunkSize:                   *chunkSize,
				includeMetadata:             *includeMetadata,
				includeAttachments:          *includeAttachments,
				outputCompression:           *outputCompression,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			run(filterOptions, args)
		}
		rootCmd.AddCommand(filterCmd)
	}

	{
		var compressCmd = &cobra.Command{
			Use:   "compress [file]",
			Short: "Create a compressed copy of an MCAP file",
			Long: `This subcommand copies data in an MCAP file to a new file, compressing the output.

usage:
  mcap compress in.mcap -o out.mcap`,
		}
		output := compressCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		chunkSize := compressCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		compression := compressCmd.PersistentFlags().String(
			"compression",
			"zstd",
			"compression algorithm to use on output file",
		)
		unchunked := compressCmd.PersistentFlags().Bool("unchunked", false, "do not chunk the output file")
		compressCmd.Run = func(_ *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(&filterFlags{
				output:             *output,
				chunkSize:          *chunkSize,
				outputCompression:  *compression,
				includeMetadata:    true,
				includeAttachments: true,
				unchunked:          *unchunked,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			run(filterOptions, args)
		}
		rootCmd.AddCommand(compressCmd)
	}

	{
		var decompressCmd = &cobra.Command{
			Use:   "decompress [file]",
			Short: "Create an uncompressed copy of an MCAP file",
			Long: `This subcommand copies data in an MCAP file to a new file, decompressing the output.

usage:
  mcap decompress in.mcap -o out.mcap`,
		}
		output := decompressCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		chunkSize := decompressCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		decompressCmd.Run = func(_ *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(&filterFlags{
				output:             *output,
				chunkSize:          *chunkSize,
				outputCompression:  "none",
				includeMetadata:    true,
				includeAttachments: true,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			run(filterOptions, args)
		}
		rootCmd.AddCommand(decompressCmd)
	}
}
