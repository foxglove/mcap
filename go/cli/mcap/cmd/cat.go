package cmd

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
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
		reader, err := mcap.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		it, err := reader.Messages(start, end, topics, false)
		if err != nil {
			log.Fatal(err)
		}
		buf := make([]byte, 1024*1024)
		for {
			schema, channel, message, err := it.Next(buf)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d %s [%s] %v...\n", message.LogTime, channel.Topic, schema.Name, message.Data[:10])
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)

	catCmd.PersistentFlags().Int64VarP(&start, "start-secs", "", 0, "start time")
	catCmd.PersistentFlags().Int64VarP(&end, "end-secs", "", math.MaxInt64, "end time")
	catCmd.PersistentFlags().StringVarP(&topics, "topics", "", "", "comma-separated list of topics")
}
