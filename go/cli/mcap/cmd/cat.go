package cmd

import (
	"fmt"
	"io"
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
	Run: func(cmd *cobra.Command, args []string) {
		var f io.Reader
		stat, err := os.Stdin.Stat()
		if err != nil {
			log.Fatal(err)
		}
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			f = os.Stdin
		} else {
			if len(args) != 1 {
				log.Fatal("supply a file")
			}
			f, err = os.Open(args[0])
			if err != nil {
				log.Fatal(err)
			}
		}
		reader, err := mcap.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		topics := strings.FieldsFunc(topics, func(c rune) bool { return c == ',' })
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
