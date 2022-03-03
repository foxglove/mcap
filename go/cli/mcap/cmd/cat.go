package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	topics string
	start  int64
	end    int64
)

func printMessages(ctx context.Context, it mcap.MessageIterator) error {
	buf := make([]byte, 1024*1024)
	for {
		schema, channel, message, err := it.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalf("Failed to read next message: %s", err)
		}
		fmt.Printf("%d %s [%s] %v...\n", message.LogTime, channel.Topic, schema.Name, message.Data[:10])
	}
	return nil
}

var catCmd = &cobra.Command{
	Use:   "cat [file]",
	Short: "Cat the messages in an mcap file to stdout",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		stat, err := os.Stdin.Stat()
		if err != nil {
			log.Fatal(err)
		}
		readingStdin := stat.Mode()&os.ModeCharDevice == 0
		// stdin is a special case, since we can't seek
		if readingStdin {
			reader, err := mcap.NewReader(os.Stdin)
			if err != nil {
				log.Fatalf("Failed to create reader: %s", err)
			}
			topics := strings.FieldsFunc(topics, func(c rune) bool { return c == ',' })
			it, err := reader.Messages(start*1e9, end*1e9, topics, false)
			if err != nil {
				log.Fatalf("Failed to read messages: %s", err)
			}
			err = printMessages(ctx, it)
			if err != nil {
				log.Fatalf("Failed to print messages: %s", err)
			}
			return
		}

		// otherwise, could be a remote or local file
		if len(args) != 1 {
			log.Fatal("supply a file")
		}
		filename := args[0]
		err = utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to create reader: %w", err)
			}
			topics := strings.FieldsFunc(topics, func(c rune) bool { return c == ',' })
			it, err := reader.Messages(start*1e9, end*1e9, topics, true)
			if err != nil {
				return fmt.Errorf("failed to read messages: %w", err)
			}
			err = printMessages(ctx, it)
			if err != nil {
				return fmt.Errorf("failed to print messages: %w", err)
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)

	catCmd.PersistentFlags().Int64VarP(&start, "start-secs", "", 0, "start time")
	catCmd.PersistentFlags().Int64VarP(&end, "end-secs", "", math.MaxInt64, "end time")
	catCmd.PersistentFlags().StringVarP(&topics, "topics", "", "", "comma-separated list of topics")
}
