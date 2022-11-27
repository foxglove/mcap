package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/cli/mcap/cmd/column_selectors"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	groupbyHeuristic         string
	groupbyTopics            []string
	groupbySizeThreshold     int
	groupbyOutput            string
	groupbyOutputCompression string
	groupbyChunkSize         int64
)

const (
	groupbyHeuristicManualTopics  = "manual-topics"
	groupbyHeuristicSingleChannel = "single-channel"
	groupbyHeuristicSingleSchema  = "single-schema"
	groupbyHeuristicTopicSize     = "topic-size"
	groupbyHeuristicSizeThreshold = "size-threshold"
)

// groupbyCmd represents the groupby command
var groupbyCmd = &cobra.Command{
	Use:   "groupby",
	Short: "reorganize messages in an MCAP into columns for optimized read performance",
	Long: `reorganize messages in an MCAP into columns. Instead of messages
appearing in the MCAP in roughly log-time order, like so:

	[Chunk [1  ][2][3    ]][Chunk [4][5  ][6     ]][Chunk [7    ][8  ][9]]

Messages will be sorted into series of chunks (called columns) by some heuristic, such as size.

	[Chunk [2][4][9]][Chunk [1  ][5  ][8  ]][Chunk [3    ][6     ][7    ]]

This can speed up access when trying to selectively read messages of a certain topic, where all
of those messages are small, for example.`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			die("please supply an input URI as an argument. See --help for usage details")
		}
		var selector mcap.ColumnSelector
		ctx := context.Background()
		// We open the input file twice for reading - once to scan the file and construct the
		// column selector, and a second time to rewrite it to the output file.
		err := utils.WithReader(ctx, args[0], func(remote bool, rs io.ReadSeeker) error {
			var err error
			switch groupbyHeuristic {
			case groupbyHeuristicManualTopics:
				selector, err = column_selectors.NewManualTopicColumnSelector(rs, groupbyTopics)
			case groupbyHeuristicSingleChannel:
				selector, err = column_selectors.NewColumnPerChannelSelector(rs)
			case groupbyHeuristicSingleSchema:
				selector = column_selectors.NewColumnPerSchemaSelector()
			case groupbyHeuristicTopicSize:
				selector, err = column_selectors.NewTopicSizeClassSelector(rs)
			case groupbyHeuristicSizeThreshold:
				selector, err = column_selectors.NewTopicSizeThresholdSelector(rs, groupbySizeThreshold)
			default:
				err = fmt.Errorf("selector for heuristic %s is not implemented", groupbyHeuristic)
			}
			return err
		})
		if err != nil {
			die("error constructing selector: %e", err)
		}
		outfile, err := os.Create(groupbyOutput)
		if err != nil {
			die("error opening output file: %e", err)
		}
		defer outfile.Close()
		writer, err := mcap.NewWriter(outfile, &mcap.WriterOptions{
			Chunked:        true,
			ChunkSize:      groupbyChunkSize,
			ColumnSelector: selector,
			Compression:    mcap.CompressionFormat(groupbyOutputCompression),
		})
		if err != nil {
			die("error constructing writer: %e", err)
		}
		err = utils.WithReader(ctx, args[0], func(remote bool, rs io.ReadSeeker) error {
			lexer, err := mcap.NewLexer(rs, &mcap.LexerOptions{
				AttachmentCallback: func(ar *mcap.AttachmentReader) error {
					return writer.WriteAttachment(&mcap.Attachment{
						LogTime:    ar.LogTime,
						CreateTime: ar.CreateTime,
						Name:       ar.Name,
						MediaType:  ar.MediaType,
						DataSize:   ar.DataSize,
						Data:       ar.Data(),
					})
				},
			})
			if err != nil {
				return err
			}
			buf := make([]byte, 1024)
			for {
				token, data, err := lexer.Next(buf)
				if err != nil {
					return err
				}
				switch token {
				case mcap.TokenHeader:
					header, err := mcap.ParseHeader(data)
					if err != nil {
						return err
					}
					if err := writer.WriteHeader(header); err != nil {
						return err
					}
				case mcap.TokenSchema:
					schema, err := mcap.ParseSchema(data)
					if err != nil {
						return err
					}
					if err := writer.WriteSchema(schema); err != nil {
						return err
					}
				case mcap.TokenChannel:
					channel, err := mcap.ParseChannel(data)
					if err != nil {
						return err
					}
					if err := writer.WriteChannel(channel); err != nil {
						return err
					}
				case mcap.TokenMessage:
					message, err := mcap.ParseMessage(data)
					if err != nil {
						return err
					}
					if err := writer.WriteMessage(message); err != nil {
						return err
					}
				case mcap.TokenMetadata:
					metadata, err := mcap.ParseMetadata(data)
					if err != nil {
						return err
					}
					if err := writer.WriteMetadata(metadata); err != nil {
						return err
					}
				case mcap.TokenDataEnd:
					writer.Close()
					return nil
				}
			}
		})
		if err != nil {
			die("error columnizing: %e", err)
		}
	},
}

func init() {
	groupbyCmd.PersistentFlags().StringVar(&groupbyHeuristic, "heuristic", "manual-topics", `
Select a heuristic to split messages into columns with. Choices are:

  manual-topics:  messages are divided into two groups:
  					1. with topics specified using the --topics argument
  					2. all other topics.
  single-channel: groups are mapped 1:1 with channels.
  single-schema:  groups are mapped 1:1 with schemas.
  topic-size:     messages are divided into groups of power-of-two size classes, where a message's
				  size class is defined by the average size of messages in its channel. All topics
				  with an average message size below 1KiB are grouped together.
  size-threshold: messages are divided into two groups by a threshold set by --size-threshold.
				  the threshold is applied on the average message size for that message's
				  channel.`)
	groupbyCmd.PersistentFlags().StringArrayVarP(&groupbyTopics, "topics", "t", nil, `
specify a topic to be sorted into the selected group. this argument is intended to be used multiple
times, eg:

mcap groupby <input> -o <output> -t /log -t /diagnostics -t /cam_front -t /cam_rear -t /lidar

Any topics not matching any list will be grouped together.`)
	groupbyCmd.PersistentFlags().IntVar(&groupbySizeThreshold, "size-threshold", 4096, `
the size threshold to choose whether a message goes into the "big" or "small" group.`)
	groupbyCmd.PersistentFlags().StringVarP(&groupbyOutput, "output", "o", "grouped.mcap", "output file to write to")
	groupbyCmd.PersistentFlags().StringVar(&groupbyOutputCompression, "compression", "zstd", "compression format")
	groupbyCmd.PersistentFlags().Int64Var(&groupbyChunkSize, "chunk-size", 4*1024*1024, "chunk size")
	rootCmd.AddCommand(groupbyCmd)
}
