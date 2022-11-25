/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
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
	columnizeHeuristic         string
	columnizeTopics            []string
	columnizeSizeThreshold     uint64
	columnizeOutput            string
	columnizeOutputCompression string
	columnizeChunkSize         int64
)

const (
	ColumnizeHeuristicManualTopics  = "manual-topics"
	ColumnizeHeuristicSingleTopic   = "single-topic"
	ColumnizeHeuristicSingleSchema  = "single-schema"
	ColumnizeHeuristicTopicSize     = "topic-size"
	ColumnizeHeuristicSizeThreshold = "size-threshold"
)

// columnizeCmd represents the columnize command
var columnizeCmd = &cobra.Command{
	Use:   "columnize",
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
			switch columnizeHeuristic {
			case ColumnizeHeuristicManualTopics:
				selector, err = column_selectors.NewManualTopicColumnSelector(rs, columnizeTopics)
			default:
				err = fmt.Errorf("selector for heuristic %s unimplemented", columnizeHeuristic)
			}
			return err
		})
		if err != nil {
			die("error constructing selector: %e", err)
		}
		outfile, err := os.Create(columnizeOutput)
		if err != nil {
			die("error opening output file: %e", err)
		}
		defer outfile.Close()
		writer, err := mcap.NewWriter(outfile, &mcap.WriterOptions{
			Chunked:        true,
			ChunkSize:      columnizeChunkSize,
			ColumnSelector: selector,
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
	columnizeCmd.PersistentFlags().StringVar(&columnizeHeuristic, "heuristic", "manual-topics", `
Select a heuristic to split messages into columns with. Choices are:

  manual-topics:  columns are specified manually by topic using the --column-topics argument.
  single-topic:   messages from each unique topic are organized into their own column.
  single-schema:  messages using each unique schema name are organized into their own column.
  topic-size:     messages are organized columns of power-of-two size classes, where a message's
				  size class is defined by the average size of messages in its channel.
  size-threshold: messages are broken into two columns by a threshold set by --size-threshold.
  				  the value being thresholded is the average message size for that message's
				  channel.`)
	columnizeCmd.PersistentFlags().StringArrayVarP(&columnizeTopics, "topics", "t", nil, `
specify a list of topics to be sorted into a single column, separated by a comma. This argument
is intended to be used multiple times, eg:

mcap columnize <input> -o <output> -t /log,/rosout,/diagnostics -t /cam_front,/cam_rear -t /lidar

Any topics not matching any list will be sorted into their own column together.`)
	columnizeCmd.PersistentFlags().Uint64Var(&columnizeSizeThreshold, "size-threshold", 4096, `
the size threshold to choose whether a message goes into the "big" or "small" column.`)
	columnizeCmd.PersistentFlags().StringVarP(&columnizeOutput, "output", "o", "columnized.mcap", "output file to write to")
	columnizeCmd.PersistentFlags().StringVar(&columnizeOutputCompression, "compression", "zstd", "compression format")
	columnizeCmd.PersistentFlags().Int64Var(&columnizeChunkSize, "chunk-size", 4*1024*1024, "chunk size")
	rootCmd.AddCommand(columnizeCmd)
}
