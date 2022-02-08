package cmd

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/libmcap"
	"github.com/spf13/cobra"
)

var (
	topics string
	start  int64
	end    int64
)

var catCmd = &cobra.Command{
	Use:   "cat [file]",
	Short: "Cat the messages in an mcap file to stdout",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		topics := strings.FieldsFunc(topics, func(c rune) bool { return c == ',' })
		f, err := os.Open(args[0])
		if err != nil {
			log.Fatal(err)
		}
		reader, err := libmcap.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		it, err := reader.Messages(start, end, topics, true)
		if err != nil {
			log.Fatal(err)
		}
		for {
			ci, msg, err := it.Next()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d %s %v...\n", msg.LogTime, ci.Topic, msg.Data[:10])
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)

	catCmd.PersistentFlags().Int64VarP(&start, "start-secs", "", 0, "start time")
	catCmd.PersistentFlags().Int64VarP(&end, "end-secs", "", math.MaxInt64, "end time")
	catCmd.PersistentFlags().StringVarP(&topics, "topics", "", "", "comma-separated list of topics")
}
