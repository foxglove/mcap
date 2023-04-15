package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/cli/mcap/utils/ros"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/foxglove/mcap/go/mcap/readopts"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	catTopics     string
	catStart      int64
	catEnd        int64
	catFormatJSON bool
)

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

func formatDecimalTime(t uint64) []byte {
	seconds := t / 1e9
	nanoseconds := t % 1e9
	requiredLength := digits(seconds) + 1 + 9
	buf := make([]byte, 0, requiredLength)
	buf = strconv.AppendInt(buf, int64(seconds), 10)
	buf = append(buf, '.')
	for i := 0; i < 9-digits(nanoseconds); i++ {
		buf = append(buf, '0')
	}
	buf = strconv.AppendInt(buf, int64(nanoseconds), 10)
	return buf
}

type Message struct {
	Topic       string          `json:"topic"`
	Sequence    uint32          `json:"sequence"`
	LogTime     uint64          `json:"log_time"`
	PublishTime uint64          `json:"publish_time"`
	Data        json.RawMessage `json:"data"`
	buf         *bytes.Buffer
}

type jsonOutputWriter struct {
	w   io.Writer
	buf *bytes.Buffer
}

func newJSONOutputWriter(w io.Writer) *jsonOutputWriter {
	return &jsonOutputWriter{
		w:   w,
		buf: &bytes.Buffer{},
	}
}

func (w *jsonOutputWriter) writeMessage(
	topic string,
	sequence uint32,
	logTime uint64,
	publishTime uint64,
	data []byte,
) error {
	w.buf.Reset()
	_, err := w.buf.Write([]byte("{"))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`"topic":`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`"`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(topic))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`",`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`"sequence":`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(strconv.FormatUint(uint64(sequence), 10)))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`,"log_time":`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write(formatDecimalTime(logTime))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`,"publish_time":`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write(formatDecimalTime(publishTime))
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte(`,"data":`))
	if err != nil {
		return err
	}

	_, err = w.buf.Write(data)
	if err != nil {
		return err
	}

	_, err = w.buf.Write([]byte("}\n"))
	if err != nil {
		return err
	}

	_, err = io.Copy(w.w, w.buf)
	if err != nil {
		return err
	}

	return nil
}

func getReadOpts(useIndex bool) []readopts.ReadOpt {
	topics := strings.FieldsFunc(catTopics, func(c rune) bool { return c == ',' })
	opts := []readopts.ReadOpt{readopts.UsingIndex(useIndex), readopts.WithTopics(topics)}
	if catStart != 0 {
		opts = append(opts, readopts.After(catStart*1e9))
	}
	if catEnd != math.MaxInt64 {
		opts = append(opts, readopts.Before(catEnd*1e9))
	}
	return opts
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
	descriptors := make(map[uint16]protoreflect.MessageDescriptor)
	jsonWriter := newJSONOutputWriter(w)
	for {
		schema, channel, message, err := it.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			die("Failed to read next message: %s", err)
		}
		if !formatJSON {
			if len(message.Data) > 10 {
				fmt.Fprintf(w, "%d %s [%s] %v...\n", message.LogTime, channel.Topic, schema.Name, message.Data[:10])
			} else {
				fmt.Fprintf(w, "%d %s [%s] %v\n", message.LogTime, channel.Topic, schema.Name, message.Data)
			}
			continue
		}
		if schema == nil || schema.Encoding == "" {
			switch channel.MessageEncoding {
			case "json":
				if _, err = msg.Write(message.Data); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			default:
				return fmt.Errorf("For schema-less channels, JSON output is only supported with 'json' message encoding. Found: %s", channel.MessageEncoding)
			}
		} else {
			switch schema.Encoding {
			case "ros1msg":
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
			case "protobuf":
				messageDescriptor, ok := descriptors[channel.SchemaID]
				if !ok {
					fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
					if err := proto.Unmarshal(schema.Data, fileDescriptorSet); err != nil {
						return fmt.Errorf("failed to build file descriptor set: %w", err)
					}
					files, err := protodesc.FileOptions{}.NewFiles(fileDescriptorSet)
					if err != nil {
						return fmt.Errorf("failed to create file descriptor: %w", err)
					}
					descriptor, err := files.FindDescriptorByName(protoreflect.FullName(schema.Name))
					if err != nil {
						return fmt.Errorf("failed to find descriptor: %w", err)
					}
					messageDescriptor = descriptor.(protoreflect.MessageDescriptor)
					descriptors[channel.SchemaID] = messageDescriptor
				}
				protoMsg := dynamicpb.NewMessage(messageDescriptor.(protoreflect.MessageDescriptor))
				if err := proto.Unmarshal(message.Data, protoMsg); err != nil {
					return fmt.Errorf("failed to parse message: %w", err)
				}
				bytes, err := protojson.Marshal(protoMsg)
				if err != nil {
					return fmt.Errorf("failed to marshal message: %w", err)
				}
				if _, err = msg.Write(bytes); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			case "jsonschema":
				if _, err = msg.Write(message.Data); err != nil {
					return fmt.Errorf("failed to write message bytes: %w", err)
				}
			default:
				return fmt.Errorf("JSON output only supported for ros1msg, protobuf, and jsonschema schemas. Found: %s", schema.Encoding)
			}
		}
		err = jsonWriter.writeMessage(
			channel.Topic,
			message.Sequence,
			message.LogTime,
			message.PublishTime,
			msg.Bytes(),
		)
		if err != nil {
			return fmt.Errorf("failed to write encoded message: %s", err)
		}
		msg.Reset()
	}
	return nil
}

var catCmd = &cobra.Command{
	Use:   "cat [file]",
	Short: "Cat the messages in an MCAP file to stdout",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		stat, err := os.Stdin.Stat()
		if err != nil {
			die("Failed to stat() stdin: %s", err)
		}
		// read stdin if no filename has been provided and data is available on stdin.
		readingStdin := (stat.Mode()&os.ModeCharDevice == 0 && len(args) == 0)
		// stdin is a special case, since we can't seek

		output := bufio.NewWriter(os.Stdout)
		defer output.Flush()

		if readingStdin {
			reader, err := mcap.NewReader(os.Stdin)
			if err != nil {
				die("Failed to create reader: %s", err)
			}
			defer reader.Close()
			it, err := reader.Messages(getReadOpts(false)...)
			if err != nil {
				die("Failed to read messages: %s", err)
			}
			err = printMessages(ctx, output, it, catFormatJSON)
			if err != nil {
				die("Failed to print messages: %s", err)
			}
			return
		}

		// otherwise, could be a remote or local file
		if len(args) != 1 {
			die("supply a file")
		}
		filename := args[0]
		err = utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			reader, err := mcap.NewReader(rs)
			if err != nil {
				return fmt.Errorf("failed to create reader: %w", err)
			}
			defer reader.Close()
			it, err := reader.Messages(getReadOpts(true)...)
			if err != nil {
				return fmt.Errorf("failed to read messages: %w", err)
			}
			err = printMessages(ctx, output, it, catFormatJSON)
			if err != nil {
				return fmt.Errorf("failed to print messages: %w", err)
			}
			return nil
		})
		if err != nil {
			die("Error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)

	catCmd.PersistentFlags().Int64VarP(&catStart, "start-secs", "", 0, "start time")
	catCmd.PersistentFlags().Int64VarP(&catEnd, "end-secs", "", math.MaxInt64, "end time")
	catCmd.PersistentFlags().StringVarP(&catTopics, "topics", "", "", "comma-separated list of topics")
	catCmd.PersistentFlags().BoolVarP(&catFormatJSON, "json", "", false,
		`print messages as JSON. Supported message encodings: ros1, protobuf, and json.`)
}
