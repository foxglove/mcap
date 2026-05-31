// Cross-language correlation check (Go).
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/foxglove/mcap/go/mcap"
)

func fill(buf []byte, seq uint64) {
	for i := range buf {
		buf[i] = byte((uint64(i) + seq) & 0xff)
	}
}

func main() {
	if len(os.Args) != 7 {
		fmt.Fprintln(os.Stderr, "usage: main <write|read> <file> <num> <size> <chunk> <none|zstd>")
		os.Exit(1)
	}
	op := os.Args[1]
	file := os.Args[2]
	num, _ := strconv.Atoi(os.Args[3])
	size, _ := strconv.Atoi(os.Args[4])
	chunk, _ := strconv.ParseInt(os.Args[5], 10, 64)
	comp := os.Args[6]

	if op == "write" {
		f, err := os.Create(file)
		check(err)
		compression := mcap.CompressionNone
		if comp == "zstd" {
			compression = mcap.CompressionZSTD
		}
		w, err := mcap.NewWriter(f, &mcap.WriterOptions{
			Chunked:     true,
			ChunkSize:   chunk,
			Compression: compression,
		})
		check(err)
		check(w.WriteHeader(&mcap.Header{Profile: "xl", Library: "go"}))
		check(w.WriteSchema(&mcap.Schema{ID: 1, Name: "Bench", Encoding: "jsonschema", Data: []byte("{}")}))
		check(w.WriteChannel(&mcap.Channel{ID: 0, SchemaID: 1, Topic: "/bench", MessageEncoding: "json"}))
		buf := make([]byte, size)
		fill(buf, 0) // one reusable payload, generated outside timing
		start := time.Now()
		for i := 0; i < num; i++ {
			check(w.WriteMessage(&mcap.Message{
				ChannelID:   0,
				Sequence:    uint32(i),
				LogTime:     uint64(i) * 1000,
				PublishTime: uint64(i) * 1000,
				Data:        buf,
			}))
		}
		check(w.Close())
		wall := time.Since(start).Seconds()
		check(f.Close())
		fi, _ := os.Stat(file)
		fmt.Printf("go\twrite\t%s\t%d\t%d\t%d\t%.6f\n", comp, num, num*size, fi.Size(), wall)
	} else {
		f, err := os.Open(file)
		check(err)
		r, err := mcap.NewReader(f)
		check(err)
		it, err := r.Messages()
		check(err)
		start := time.Now()
		var count, nbytes uint64
		msg := &mcap.Message{}
		for {
			_, _, m, err := it.NextInto(msg)
			if errors.Is(err, io.EOF) {
				break
			}
			check(err)
			count++
			nbytes += uint64(len(m.Data))
		}
		wall := time.Since(start).Seconds()
		r.Close()
		check(f.Close())
		fmt.Printf("go\tread\t%s\t%d\t%d\t0\t%.6f\n", comp, count, nbytes, wall)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
