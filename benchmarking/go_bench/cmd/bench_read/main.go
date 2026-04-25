package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/foxglove/mcap/go/mcap"
)

func run() error {
	if len(os.Args) < 2 || len(os.Args) > 6 {
		return fmt.Errorf("Usage: %s <input_file> [mode] [num_messages] [payload_size] [filter]", os.Args[0])
	}

	filename := os.Args[1]
	mode := "unknown"
	numMessagesStr := "0"
	payloadSizeStr := "0"
	filter := ""
	if len(os.Args) >= 3 {
		mode = os.Args[2]
	}
	if len(os.Args) >= 4 {
		numMessagesStr = os.Args[3]
	}
	if len(os.Args) >= 5 {
		payloadSizeStr = os.Args[4]
	}
	if len(os.Args) >= 6 {
		filter = os.Args[5]
	}

	// Timed: file open + message iteration
	start := time.Now()

	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	reader, err := mcap.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Close()

	var opts []mcap.ReadOpt
	switch filter {
	case "":
		// no filter — read all messages
	case "topic":
		opts = append(opts,
			mcap.WithTopics([]string{"/imu"}),
			mcap.UsingIndex(true),
		)
	case "timerange":
		opts = append(opts,
			mcap.AfterNanos(3000000000),
			mcap.BeforeNanos(5000000000),
			mcap.UsingIndex(true),
		)
	case "topic_timerange":
		opts = append(opts,
			mcap.WithTopics([]string{"/lidar"}),
			mcap.AfterNanos(4000000000),
			mcap.BeforeNanos(6000000000),
			mcap.UsingIndex(true),
		)
	default:
		return fmt.Errorf("unknown filter mode: %s (expected topic, timerange, or topic_timerange)", filter)
	}

	it, err := reader.Messages(opts...)
	if err != nil {
		return fmt.Errorf("failed to create message iterator: %w", err)
	}

	msgCount := int64(0)
	msg := &mcap.Message{}
	for {
		_, _, _, err = it.NextInto(msg)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read message: %w", err)
		}
		// Touch data to prevent dead-code elimination
		if len(msg.Data) == 0 {
			fmt.Fprintf(os.Stderr, "Empty message\n")
		}
		msgCount++
	}

	elapsed := time.Since(start)

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fi.Size()

	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)

	fmt.Printf("read\tgo\t%s\t%s\t%s\t%d\t%d\t%.6f\t%d\n",
		mode, numMessagesStr, payloadSizeStr, fileSize, elapsed.Nanoseconds(), elapsed.Seconds(), rusage.Maxrss)

	_ = msgCount

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
