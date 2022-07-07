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

var (
	filterOutput             string
	filterIncludeTopics      []string
	filterExcludeTopics      []string
	filterStart              uint64
	filterEnd                uint64
	filterIncludeMetadata    bool
	filterIncludeAttachments bool
	filterOutputCompression  string
)

type filterOpts struct {
	includeTopics      []regexp.Regexp
	excludeTopics      []regexp.Regexp
	start              uint64
	end                uint64
	includeMetadata    bool
	includeAttachments bool
	compressionFormat  mcap.CompressionFormat
}

func buildFilterOptions() (*filterOpts, error) {
	opts := &filterOpts{
		includeMetadata:    filterIncludeMetadata,
		includeAttachments: filterIncludeAttachments,
	}
	opts.start = filterStart * 1e9
	if filterEnd == 0 {
		opts.end = math.MaxUint64
	} else {
		opts.end = filterEnd * 1e9
	}
	if len(filterIncludeTopics) > 0 && len(filterExcludeTopics) > 0 {
		return nil, errors.New("can only use one of --include-topic-regex and --exclude-topic-regex")
	}
	if filterEnd < filterStart {
		return nil, errors.New("invalid time range query, end-time is before start-time")
	}
	opts.compressionFormat = mcap.CompressionNone
	switch filterOutputCompression {
	case "zstd":
		opts.compressionFormat = mcap.CompressionZSTD
	case "lz4":
		opts.compressionFormat = mcap.CompressionLZ4
	case "none":
		opts.compressionFormat = mcap.CompressionNone
	default:
		return nil, fmt.Errorf("unrecognized compression format '%s': valid options are 'lz4', 'zstd', or 'none'", filterOutputCompression)
	}

	includeTopics, err := compileMatchers(filterIncludeTopics)
	if err != nil {
		return nil, err
	}
	opts.includeTopics = includeTopics

	excludeTopics, err := compileMatchers(filterExcludeTopics)
	if err != nil {
		return nil, err
	}
	opts.excludeTopics = excludeTopics
	return opts, nil
}

// filterCmd represents the filter command
var filterCmd = &cobra.Command{
	Use:   "filter [file]",
	Short: "copy some filtered MCAP data to a new file",
	Long: `This subcommand filters an MCAP by topic and time range to a new file.
When multiple regexes are used, topics that match any regex are included (or excluded).

usage:
  mcap filter in.mcap -o out.mcap -y /diagnostics -y /tf -y /camera_(front|back)`,
	Run: func(cmd *cobra.Command, args []string) {
		filterOptions, err := buildFilterOptions()
		if err != nil {
			die("configuration error: %s", err)
		}
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
		if filterOutput == "" {
			if !utils.StdoutRedirected() {
				die("Binary output can screw up your terminal. Supply -o or redirect to a file or pipe")
			}
			writer = os.Stdout
		} else {
			newWriter, err := os.Create(filterOutput)
			if err != nil {
				die("failed to open %s for writing: %s", filterOutput, err)
			}
			defer func() {
				if err := newWriter.Close(); err != nil {
					die("error closing write target: %s", err)
				}
			}()
			writer = newWriter
		}

		err = filter(reader, writer, filterOptions)
		if err != nil {
			die(error.Error(err))
		}
	},
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
	lexer, err := mcap.NewLexer(r)
	if err != nil {
		return err
	}
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Compression: opts.compressionFormat,
	})
	if err != nil {
		return err
	}
	defer func() {
		err = mcapWriter.Close()
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
	rootCmd.AddCommand(filterCmd)

	filterCmd.PersistentFlags().StringVarP(&filterOutput, "output", "o", "", "output filename")
	filterCmd.PersistentFlags().StringArrayVarP(&filterIncludeTopics, "include-topic-regex", "y", []string{}, "messages with topic names matching this regex will be included, can be supplied multiple times")
	filterCmd.PersistentFlags().StringArrayVarP(&filterExcludeTopics, "exclude-topic-regex", "n", []string{}, "messages with topic names matching this regex will be excluded, can be supplied multiple times")
	filterCmd.PersistentFlags().Uint64VarP(&filterStart, "start-secs", "s", 0, "messages with log times after or equal to this timestamp will be included.")
	filterCmd.PersistentFlags().Uint64VarP(&filterEnd, "end-secs", "e", 0, "messages with log times before timestamp will be included.")
	filterCmd.PersistentFlags().BoolVar(&filterIncludeMetadata, "include-metadata", false, "whether to include metadata in the output bag")
	filterCmd.PersistentFlags().BoolVar(&filterIncludeAttachments, "include-attachments", false, "whether to include attachments in the output mcap")
	filterCmd.PersistentFlags().StringVar(&filterOutputCompression, "output-compression", "zstd", "compression algorithm to use on output file")
}
