package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

type filterFlags struct {
	output             string
	includeTopics      []string
	excludeTopics      []string
	start              uint64
	end                uint64
	includeMetadata    bool
	includeAttachments bool
	outputCompression  string
	chunkSize          int64
}

type filterOpts struct {
	recover            bool
	output             string
	includeTopics      []regexp.Regexp
	excludeTopics      []regexp.Regexp
	start              uint64
	end                uint64
	includeMetadata    bool
	includeAttachments bool
	compressionFormat  mcap.CompressionFormat
	chunkSize          int64
}

func buildFilterOptions(flags filterFlags) (*filterOpts, error) {
	opts := &filterOpts{
		output:             flags.output,
		includeMetadata:    flags.includeMetadata,
		includeAttachments: flags.includeAttachments,
	}
	opts.start = flags.start * 1e9
	if flags.end == 0 {
		opts.end = math.MaxUint64
	} else {
		opts.end = flags.end * 1e9
	}
	if len(flags.includeTopics) > 0 && len(flags.excludeTopics) > 0 {
		return nil, errors.New("can only use one of --include-topic-regex and --exclude-topic-regex")
	}
	if flags.end < flags.start {
		return nil, errors.New("invalid time range query, end-time is before start-time")
	}
	opts.compressionFormat = mcap.CompressionNone
	switch flags.outputCompression {
	case "zstd":
		opts.compressionFormat = mcap.CompressionZSTD
	case "lz4":
		opts.compressionFormat = mcap.CompressionLZ4
	case "none":
		opts.compressionFormat = mcap.CompressionNone
	default:
		return nil, fmt.Errorf("unrecognized compression format '%s': valid options are 'lz4', 'zstd', or 'none'", flags.outputCompression)
	}

	includeTopics, err := compileMatchers(flags.includeTopics)
	if err != nil {
		return nil, err
	}
	opts.includeTopics = includeTopics

	excludeTopics, err := compileMatchers(flags.excludeTopics)
	if err != nil {
		return nil, err
	}
	opts.excludeTopics = excludeTopics
	opts.chunkSize = flags.chunkSize
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
		close, newReader, err := utils.GetReader(context.Background(), args[0])
		if err != nil {
			die("failed to open source for reading: %s", err)
		}
		defer func() {
			if closeErr := close(); closeErr != nil {
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
		die(error.Error(err))
	}
}

func compileMatchers(regexStrings []string) ([]regexp.Regexp, error) {
	var matchers []regexp.Regexp

	for _, regexString := range regexStrings {
		// auto-surround with ^$ if not specified.
		if regexString[:1] != "^" {
			regexString = "^" + regexString
		}
		if regexString[len(regexString)-1:] != "$" {
			regexString = regexString + "$"
		}
		regex, err := regexp.Compile(regexString)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, *regex)
	}
	return matchers, nil
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
	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{ValidateCRC: true, EmitInvalidChunks: opts.recover})
	if err != nil {
		return err
	}
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Compression: opts.compressionFormat,
		Chunked:     true,
		ChunkSize:   opts.chunkSize,
	})
	if err != nil {
		return err
	}

	var numMessages, numAttachments, numMetadata uint64

	defer func() {
		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to close mcap writer: %w", err)
			return
		}
		if opts.recover {
			fmt.Printf("Recovered %d messages, %d attachments, and %d metadata records.\n", numMessages, numAttachments, numMetadata)
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
			if opts.recover && errors.Is(err, io.ErrUnexpectedEOF) {
				fmt.Println("Input file was truncated.")
				return nil
			}
			if opts.recover && token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
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
			if err = mcapWriter.WriteHeader(header); err != nil {
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
			for _, matcher := range opts.includeTopics {
				if matcher.MatchString(channel.Topic) {
					channels[channel.ID] = markableChannel{channel, false}
				}
			}
			for _, matcher := range opts.excludeTopics {
				if !matcher.MatchString(channel.Topic) {
					channels[channel.ID] = markableChannel{channel, false}
				}
			}
			if len(opts.includeTopics) == 0 && len(opts.excludeTopics) == 0 {
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
				schema, ok := schemas[channel.SchemaID]
				if !ok {
					return fmt.Errorf("encountered channel with topic %s with unknown schema ID %d", channel.Topic, channel.SchemaID)
				}
				if !schema.written {
					if err = mcapWriter.WriteSchema(schema.Schema); err != nil {
						return err
					}
					schema.written = true
				}
				if err = mcapWriter.WriteChannel(channel.Channel); err != nil {
					return err
				}
				channel.written = true
			}
			if err = mcapWriter.WriteMessage(message); err != nil {
				return err
			}
			numMessages++
		case mcap.TokenAttachment:
			if !opts.includeAttachments {
				continue
			}
			attachment, err := mcap.ParseAttachment(data)
			if err != nil {
				return err
			}
			if attachment.LogTime < opts.start {
				continue
			}
			if attachment.LogTime >= opts.end {
				continue
			}
			if err = mcapWriter.WriteAttachment(attachment); err != nil {
				return err
			}
			numAttachments++
		case mcap.TokenMetadata:
			if !opts.includeMetadata {
				continue
			}
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			if err = mcapWriter.WriteMetadata(metadata); err != nil {
				return err
			}
			numMetadata++
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

usage:
  mcap filter in.mcap -o out.mcap -y /diagnostics -y /tf -y /camera_(front|back)`,
		}
		output := filterCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		includeTopics := filterCmd.PersistentFlags().StringArrayP("include-topic-regex", "y", []string{}, "messages with topic names matching this regex will be included, can be supplied multiple times")
		excludeTopics := filterCmd.PersistentFlags().StringArrayP("exclude-topic-regex", "n", []string{}, "messages with topic names matching this regex will be excluded, can be supplied multiple times")
		start := filterCmd.PersistentFlags().Uint64P("start-secs", "s", 0, "messages with log times after or equal to this timestamp will be included.")
		end := filterCmd.PersistentFlags().Uint64P("end-secs", "e", 0, "messages with log times before timestamp will be included.")
		chunkSize := filterCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		includeMetadata := filterCmd.PersistentFlags().Bool("include-metadata", false, "whether to include metadata in the output bag")
		includeAttachments := filterCmd.PersistentFlags().Bool("include-attachments", false, "whether to include attachments in the output mcap")
		outputCompression := filterCmd.PersistentFlags().String("output-compression", "zstd", "compression algorithm to use on output file")
		filterCmd.Run = func(cmd *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(filterFlags{
				output:             *output,
				includeTopics:      *includeTopics,
				excludeTopics:      *excludeTopics,
				start:              *start,
				end:                *end,
				chunkSize:          *chunkSize,
				includeMetadata:    *includeMetadata,
				includeAttachments: *includeAttachments,
				outputCompression:  *outputCompression,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			run(filterOptions, args)
		}
		rootCmd.AddCommand(filterCmd)
	}

	{
		var recoverCmd = &cobra.Command{
			Use:   "recover [file]",
			Short: "Recover data from a potentially corrupt MCAP file",
			Long: `This subcommand reads a potentially corrupt MCAP file and copies data to a new file.

usage:
  mcap recover in.mcap -o out.mcap`,
		}
		output := recoverCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		chunkSize := recoverCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		compression := recoverCmd.PersistentFlags().String("compression", "zstd", "compression algorithm to use on output file")
		recoverCmd.Run = func(cmd *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(filterFlags{
				output:             *output,
				chunkSize:          *chunkSize,
				outputCompression:  *compression,
				includeMetadata:    true,
				includeAttachments: true,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			filterOptions.recover = true
			run(filterOptions, args)
		}
		rootCmd.AddCommand(recoverCmd)
	}

	{
		var compressCmd = &cobra.Command{
			Use:   "compress [file]",
			Short: "Compress data in an MCAP file",
			Long: `This subcommand copies data in an MCAP file to a new file, compressing the output.

usage:
  mcap compress in.mcap -o out.mcap`,
		}
		output := compressCmd.PersistentFlags().StringP("output", "o", "", "output filename")
		chunkSize := compressCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
		compression := compressCmd.PersistentFlags().String("compression", "zstd", "compression algorithm to use on output file")
		compressCmd.Run = func(cmd *cobra.Command, args []string) {
			filterOptions, err := buildFilterOptions(filterFlags{
				output:             *output,
				chunkSize:          *chunkSize,
				outputCompression:  *compression,
				includeMetadata:    true,
				includeAttachments: true,
			})
			if err != nil {
				die("configuration error: %s", err)
			}
			run(filterOptions, args)
		}
		rootCmd.AddCommand(compressCmd)
	}
}
