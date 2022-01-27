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
	Use:   "cat",
	Short: "Cat the messages in an mcap file to stdout",
	Run: func(cmd *cobra.Command, args []string) {
		topics := strings.FieldsFunc(topics, func(c rune) bool { return c == ',' })
		f, err := os.Open(args[0])
		if err != nil {
			log.Fatal(err)
		}
		reader := libmcap.NewReader(f)
		it, err := reader.Messages(start, end, topics, true)
		if err != nil {
			log.Fatal(err)
		}
		for {
			ci, msg, err := it.Next()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d %s %v...\n", msg.RecordTime, ci.TopicName, msg.Data[:10])
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)

	catCmd.PersistentFlags().Int64VarP(&start, "start seconds", "", 0, "start time (epoch seconds)")
	catCmd.PersistentFlags().Int64VarP(&end, "end seconds", "", math.MaxInt64, "end time (epoch seconds)")
	catCmd.PersistentFlags().StringVarP(&topics, "topics", "", "", "comma-separated list of topics")
}
