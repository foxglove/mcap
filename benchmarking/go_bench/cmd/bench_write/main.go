package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"os"
	"runtime"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/foxglove/mcap/go/mcap"
)

// Shared payload blob parameters; must match gen_blob.py and the other
// language benches. Message i's payload is the window of the blob starting
// at (i * blobStride) % blobWindowSpan, so all implementations feed
// identical bytes to their writers.
const (
	blobSize       = 16777216
	blobMaxPayload = 524288
	blobWindowSpan = blobSize - blobMaxPayload
	blobStride     = 7919
)

func payloadOffset(msgIndex int64) int64 {
	return (msgIndex * blobStride) % blobWindowSpan
}

func loadBlob(path string) ([]byte, error) {
	blob, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob file: %w", err)
	}
	if len(blob) != blobSize {
		return nil, fmt.Errorf("blob file %s is not exactly %d bytes", path, blobSize)
	}
	return blob, nil
}

type scheduledMsg struct {
	timestamp uint64
	channelID uint16
}

func run() error {
	if len(os.Args) != 6 {
		return fmt.Errorf("Usage: %s <output_file> <mode> <num_messages> <payload_size> <blob_file>\n  mode: unchunked | chunked | zstd | lz4", os.Args[0])
	}

	blob, err := loadBlob(os.Args[5])
	if err != nil {
		return err
	}

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
		if payloadSize > blobMaxPayload {
			return fmt.Errorf("payload_size must be <= %d", blobMaxPayload)
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

	// Buffer file writes so per-record write() system calls do not dominate the
	// unchunked mode, matching the buffered output of the other benches (the
	// Rust bench's BufWriter, C++'s libc-buffered FILE*).
	bw := bufio.NewWriterSize(f, 1<<20)
	w, err := mcap.NewWriter(bw, &opts)
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

		// TF payload sizes cycle
		tfSizes := []int{80, 160, 320, 800, 1600}

		// Channel ID -> fixed payload size (0 means variable/TF)
		fixedPayload := map[uint16]int{
			1: 96,
			2: 296,
			4: 230400,
			5: 524288,
		}

		// Not timed: CRC of the payload stream for cross-language verification
		var payloadCrc uint32
		{
			crcSeq := make([]uint32, 6) // index by channelID (1-based)
			for i, msg := range schedule {
				seq := crcSeq[msg.channelID]
				crcSeq[msg.channelID] = seq + 1
				size := fixedPayload[msg.channelID]
				if msg.channelID == 3 {
					size = tfSizes[seq%uint32(len(tfSizes))]
				}
				off := payloadOffset(int64(i))
				payloadCrc = crc32.Update(payloadCrc, crc32.IEEETable, blob[off:off+int64(size)])
			}
		}

		// Per-channel sequence counters
		chanSeq := make([]uint32, 6) // index by channelID (1-based)

		// Timed: message loop + close
		start := time.Now()

		for i, msg := range schedule {
			seq := chanSeq[msg.channelID]
			chanSeq[msg.channelID] = seq + 1
			size := fixedPayload[msg.channelID]
			if msg.channelID == 3 {
				size = tfSizes[seq%uint32(len(tfSizes))]
			}
			off := payloadOffset(int64(i))
			data := blob[off : off+int64(size)]
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
		// The flush is part of the timed write, like the other benches.
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		elapsed := time.Since(start)

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := fi.Size()

		fmt.Printf("write\tgo\t%s\t%d\t%v\t%d\t%d\t%.6f\t%d\t%d\n",
			mode, 3750, "mixed", fileSize, elapsed.Nanoseconds(), elapsed.Seconds(), peakRssKb(),
			payloadCrc)
	} else {
		// --- Fixed payload mode ---
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

		// Not timed: CRC of the payload stream for cross-language verification
		var payloadCrc uint32
		for i := int64(0); i < numMessages; i++ {
			off := payloadOffset(i)
			payloadCrc = crc32.Update(payloadCrc, crc32.IEEETable, blob[off:off+payloadSize])
		}

		// Timed: message loop + close
		start := time.Now()

		for i := int64(0); i < numMessages; i++ {
			logTime := uint64(i) * 1000
			off := payloadOffset(i)
			if err := w.WriteMessage(&mcap.Message{
				ChannelID:   1,
				Sequence:    uint32(i),
				LogTime:     logTime,
				PublishTime: logTime,
				Data:        blob[off : off+payloadSize],
			}); err != nil {
				return fmt.Errorf("failed to write message %d: %w", i, err)
			}
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}
		// The flush is part of the timed write, like the other benches.
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		elapsed := time.Since(start)

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := fi.Size()

		fmt.Printf("write\tgo\t%s\t%d\t%d\t%d\t%d\t%.6f\t%d\t%d\n",
			mode, numMessages, payloadSize, fileSize, elapsed.Nanoseconds(), elapsed.Seconds(), peakRssKb(),
			payloadCrc)
	}

	return nil
}

// peakRssKb returns peak RSS in KB. Rusage.Maxrss is KB on Linux but
// bytes on macOS.
func peakRssKb() int64 {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	rss := int64(rusage.Maxrss)
	if runtime.GOOS == "darwin" {
		rss /= 1024
	}
	return rss
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
