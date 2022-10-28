package cmd

import (
	"bytes"
	"math"
	"os"
	"regexp"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	optimizeMode   string
	optimizeTopics []string
)

var optimizeCmd = &cobra.Command{
	Use:   "optimize [file]",
	Short: "optimize the mcap file for pre-loading",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("supply a file")
		}
		filename := args[0]
		f, err := os.Open(filename)
		if err != nil {
			die("failed to open file: %s", err)
		}
		includeTopics := []regexp.Regexp{}
		for _, topic := range optimizeTopics {
			re := regexp.MustCompile(topic)
			includeTopics = append(includeTopics, *re)
		}

		buf := &bytes.Buffer{}
		err = filter(f, buf, &filterOpts{
			includeTopics:     includeTopics,
			start:             0,
			end:               math.MaxUint64,
			compressionFormat: "zstd",
		})
		if err != nil {
			die("failed to filter input mcap")
		}
		_, err = f.Seek(0, 0)
		if err != nil {
			die("failed to seek to input start: %w", err)
		}

		err = utils.RewriteMCAP(os.Stdout, f, func(w *mcap.Writer) error {
			return w.WriteAttachment(&mcap.Attachment{
				Name: "preload_topics",
				Data: buf.Bytes(),
			})
		})
		if err != nil {
			die("failed to attach optimized mcap")
		}
	},
}

func init() {
	rootCmd.AddCommand(optimizeCmd)
	optimizeCmd.PersistentFlags().StringSliceVarP(&optimizeTopics, "topic", "t", []string{}, "topics to optimize for preloading")
}
