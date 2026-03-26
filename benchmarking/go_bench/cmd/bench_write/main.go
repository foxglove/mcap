package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/foxglove/mcap/go/mcap"
)

type scheduledMsg struct {
	timestamp uint64
	channelID uint16
}

func run() error {
	if len(os.Args) != 6 {
		return fmt.Errorf("Usage: %s <output_file> <mode> <num_messages> <payload_size> <uniform|varied>\n  mode: unchunked | chunked | zstd | lz4", os.Args[0])
	}

	variedFill := os.Args[5] == "varied"

	filename := os.Args[1]
	mode := os.Args[2]
	mixed := os.Args[4] == "mixed"

	var numMessages int64
	var payloadSize int64
	if !mixed {
		var err error
		numMessages, err = strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid num_messages: %w", err)
		}
		payloadSize, err = strconv.ParseInt(os.Args[4], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid payload_size: %w", err)
		}
	}

	var opts mcap.WriterOptions
	opts.IncludeCRC = true
	opts.OverrideLibrary = true

	switch mode {
	case "unchunked":
		opts.Chunked = false
	case "chunked":
		opts.Chunked = true
		opts.ChunkSize = 786432
		opts.Compression = mcap.CompressionNone
	case "zstd":
		opts.Chunked = true
		opts.ChunkSize = 786432
		opts.Compression = mcap.CompressionZSTD
	case "lz4":
		opts.Chunked = true
		opts.ChunkSize = 786432
		opts.Compression = mcap.CompressionLZ4
	default:
		return fmt.Errorf("Unknown mode: %s", mode)
	}

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	w, err := mcap.NewWriter(f, &opts)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	// Header (not timed)
	if err := w.WriteHeader(&mcap.Header{
		Profile: "bench",
		Library: "go-bench",
	}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if mixed {
		// --- Mixed payload mode: simulate a 10-second robot recording ---

		// Schemas
		type schemaInfo struct {
			id   uint16
			name string
		}
		schemas := []schemaInfo{
			{1, "IMU"},
			{2, "Odometry"},
			{3, "TFMessage"},
			{4, "PointCloud2"},
			{5, "CompressedImage"},
		}
		for _, s := range schemas {
			if err := w.WriteSchema(&mcap.Schema{
				ID:       s.id,
				Name:     s.name,
				Encoding: "jsonschema",
				Data:     []byte(`{"type":"object"}`),
			}); err != nil {
				return fmt.Errorf("failed to write schema %s: %w", s.name, err)
			}
		}

		// Channels
		type channelInfo struct {
			id       uint16
			schemaID uint16
			topic    string
		}
		channels := []channelInfo{
			{1, 1, "/imu"},
			{2, 2, "/odom"},
			{3, 3, "/tf"},
			{4, 4, "/lidar"},
			{5, 5, "/camera/compressed"},
		}
		for _, c := range channels {
			if err := w.WriteChannel(&mcap.Channel{
				ID:              c.id,
				SchemaID:        c.schemaID,
				Topic:           c.topic,
				MessageEncoding: "json",
			}); err != nil {
				return fmt.Errorf("failed to write channel %s: %w", c.topic, err)
			}
		}

		// Pre-generate message schedule
		type chanSpec struct {
			channelID uint16
			periodNs  uint64
			count     int
		}
		chanSpecs := []chanSpec{
			{1, 5000000, 2000},
			{2, 20000000, 500},
			{3, 10000000, 1000},
			{4, 100000000, 100},
			{5, 66666667, 150},
		}

		schedule := make([]scheduledMsg, 0, 3750)
		for _, cs := range chanSpecs {
			for i := 0; i < cs.count; i++ {
				schedule = append(schedule, scheduledMsg{
					timestamp: uint64(i) * cs.periodNs,
					channelID: cs.channelID,
				})
			}
		}
		sort.Slice(schedule, func(i, j int) bool {
			if schedule[i].timestamp != schedule[j].timestamp {
				return schedule[i].timestamp < schedule[j].timestamp
			}
			return schedule[i].channelID < schedule[j].channelID
		})

		// Pre-allocate payload buffers
		payloadSizes := []int{96, 296, 80, 160, 320, 800, 1600, 230400, 524288}
		payloads := make(map[int][]byte, len(payloadSizes))
		for _, sz := range payloadSizes {
			buf := make([]byte, sz)
			fillPayload(buf, variedFill)
			payloads[sz] = buf
		}

		// TF payload sizes cycle
		tfSizes := []int{80, 160, 320, 800, 1600}

		// Channel ID -> fixed payload size (0 means variable/TF)
		fixedPayload := map[uint16]int{
			1: 96,
			2: 296,
			4: 230400,
			5: 524288,
		}

		// Per-channel sequence counters
		chanSeq := make([]uint32, 6) // index by channelID (1-based)

		// Timed: message loop + close
		start := time.Now()

		for _, msg := range schedule {
			seq := chanSeq[msg.channelID]
			chanSeq[msg.channelID] = seq + 1
			var data []byte
			if msg.channelID == 3 {
				data = payloads[tfSizes[seq%uint32(len(tfSizes))]]
			} else {
				data = payloads[fixedPayload[msg.channelID]]
			}
			if err := w.WriteMessage(&mcap.Message{
				ChannelID:   msg.channelID,
				Sequence:    seq,
				LogTime:     msg.timestamp,
				PublishTime: msg.timestamp,
				Data:        data,
			}); err != nil {
				return fmt.Errorf("failed to write message %d: %w", seq, err)
			}
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}

		elapsed := time.Since(start)

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := fi.Size()

		var rusage syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)

		fmt.Printf("write\tgo\t%s\t%d\t%v\t%d\t%d\t%.6f\t%d\n",
			mode, 3750, "mixed", fileSize, elapsed.Nanoseconds(), elapsed.Seconds(), rusage.Maxrss)
	} else {
		// --- Fixed payload mode ---
		payload := make([]byte, payloadSize)
		fillPayload(payload, variedFill)

		if err := w.WriteSchema(&mcap.Schema{
			ID:       1,
			Name:     "BenchMsg",
			Encoding: "jsonschema",
			Data:     []byte(`{"type":"object"}`),
		}); err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}

		if err := w.WriteChannel(&mcap.Channel{
			ID:              1,
			SchemaID:        1,
			Topic:           "/bench",
			MessageEncoding: "json",
		}); err != nil {
			return fmt.Errorf("failed to write channel: %w", err)
		}

		// Timed: message loop + close
		start := time.Now()

		for i := int64(0); i < numMessages; i++ {
			logTime := uint64(i) * 1000
			if err := w.WriteMessage(&mcap.Message{
				ChannelID:   1,
				Sequence:    uint32(i),
				LogTime:     logTime,
				PublishTime: logTime,
				Data:        payload,
			}); err != nil {
				return fmt.Errorf("failed to write message %d: %w", i, err)
			}
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}

		elapsed := time.Since(start)

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := fi.Size()

		var rusage syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)

		fmt.Printf("write\tgo\t%s\t%d\t%d\t%d\t%d\t%.6f\t%d\n",
			mode, numMessages, payloadSize, fileSize, elapsed.Nanoseconds(), elapsed.Seconds(), rusage.Maxrss)
	}

	return nil
}

func fillPayload(buf []byte, varied bool) {
	for i := range buf {
		if varied {
			buf[i] = byte((i*137 + 43) & 0xff)
		} else {
			buf[i] = 0x42
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
