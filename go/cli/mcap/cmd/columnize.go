/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"github.com/spf13/cobra"
)

var (
	columnizeHeuristic          string
	columnizeTopics             []string
	columnizeSizeThreshold      uint64
	columnizeOutput             string
	columnizeIncludeMetadata    bool
	columnizeIncludeAttachments bool
	columnizeOutputCompression  string
	columnizeChunkSize          int64
)

const (
	ColumnizeHeuristicManualTopics  = "manual-topics"
	ColumnizeHeuristicSingleTopic   = "single-topic"
	ColumnizeHeuristicSingleSchema  = "single-schema"
	ColumnizeHeuristicTopicSize     = "topic-size"
	ColumnizeHeuristicSizeThreshold = "size-threshold"
)

func StackChunks(inPaths []string, outPath []string) error {
	return nil
}

type MultiFilter func(token mcap.TokenType, data []byte, writers []mcap.Writer) error

func SingleTopicSelector() ColumnSelector {
	topics := make([string]bool)
	return func(token mcap.TokenType, data []byte, writers []mcap.Writer) error {

	}
}

func splitter(r io.Reader, ws []io.Writer) {

}

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
		var decider ColumnDecider

		switch columnizeHeuristic {
		case ColumnizeHeuristicManualTopics:
			decider = manualTopicDecider()
		case ColumnizeHeuristicSingleTopic:
			decider = buildTopicDecider()
		case ColumnizeHeuristicSingleSchema:
			decider = buildSchemaDecider()
		case ColumnizeHeuristicTopicSize:
		case ColumnizeHeuristicSizeThreshold:
		default:
			die("invalid heuristic selection: %s", columnizeHeuristic)
		}:w http.ResponseWriter, r *http.Request
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
	columnizeCmd.PersistentFlags().StringVarP(&columnizeOutput, "output", "o", "", `output file
to write to`)
	rootCmd.AddCommand(columnizeCmd)
}
