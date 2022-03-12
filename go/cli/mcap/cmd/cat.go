package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/cli/mcap/utils/ros"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	topics     string
	start      int64
	end        int64
	formatJSON bool
)

type DecimalTime uint64

func digits(n uint64) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n = n / 10
		count++
	}
	return count
}

func (t DecimalTime) MarshalJSON() ([]byte, error) {
	seconds := uint64(t) / 1e9
	nanoseconds := uint64(t) % 1e9
	requiredLength := digits(seconds) + 1 + 9
	buf := make([]byte, 0, requiredLength)
	buf = strconv.AppendInt(buf, int64(seconds), 10)
	buf = append(buf, '.')
	for i := 0; i < 9-digits(nanoseconds); i++ {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(nanoseconds), 10)
	return buf, nil
}

type Message struct {
	Topic       string          `json:"topic"`
	Sequence    uint32          `json:"sequence"`
	LogTime     DecimalTime     `json:"log_time"`
	PublishTime DecimalTime     `json:"publish_time"`
	Data        json.RawMessage `json:"data"`
}

func printMessages(
	ctx context.Context,
	w io.Writer,
	it mcap.MessageIterator,
	formatJSON bool,
) error {
	msg := &bytes.Buffer{}
	msgReader := &bytes.Reader{}
	buf := make([]byte, 1024*1024)
	transcoders := make(map[uint16]*ros.JSONTranscoder)
	encoder := json.NewEncoder(w)
	target := Message{}
	for {
		schema, channel, message, err := it.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalf("Failed to read next message: %s", err)
		}
		if !formatJSON {
			fmt.Fprintf(w, "%d %s [%s] %v...\n", message.LogTime, channel.Topic, schema.Name, message.Data[:10])
			continue
		}
		if schema.Encoding != "ros1msg" {
			return fmt.Errorf("JSON output only supported for ros1msg schemas")
		}
		transcoder, ok := transcoders[channel.SchemaID]
		if !ok {
			packageName := strings.Split(schema.Name, "/")[0]
			transcoder, err = ros.NewJSONTranscoder(packageName, schema.Data)
			if err != nil {
				return fmt.Errorf("failed to build transcoder for %s: %w", channel.Topic, err)
			}
			transcoders[channel.SchemaID] = transcoder
		}
		msgReader.Reset(message.Data)
		err = transcoder.Transcode(msg, msgReader)
		if err != nil {
			return fmt.Errorf("failed to transcode %s record on %s: %w", schema.Name, channel.Topic, err)
		}
		target.Topic = channel.Topic
		target.Sequence = message.Sequence
		target.LogTime = DecimalTime(message.LogTime)
		target.PublishTime = DecimalTime(message.PublishTime)
		target.Data = msg.Bytes()
		err = encoder.Encode(target)
		if err != nil {
			return fmt.Errorf("failed to write encoded message")
		}
		msg.Reset()
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
			err = printMessages(ctx, os.Stdout, it, formatJSON)
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
			err = printMessages(ctx, os.Stdout, it, formatJSON)
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
	catCmd.PersistentFlags().BoolVarP(&formatJSON, "json", "", false, "print messages as JSON")
}
