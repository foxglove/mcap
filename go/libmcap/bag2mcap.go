package libmcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/pierrec/lz4/v4"
)

var (
	BagMagic = []byte("#ROSBAG V2.0\n")
)

type BagOp byte

const (
	OpBagHeader      = 0x03
	OpBagChunk       = 0x05
	OpBagConnection  = 0x07
	OpBagMessageData = 0x02
	OpBagIndexData   = 0x04
	OpBagChunkInfo   = 0x06
)

func extractHeaderValue(header []byte, key []byte) ([]byte, error) {
	var fieldlen uint32
	offset := 0
	for offset < len(header) {
		fieldlen, offset = getUint32(header, offset)
		field := header[offset : offset+int(fieldlen)]
		parts := bytes.SplitN(field, []byte{'='}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header field: %s", field)
		}
		if bytes.Equal(key, parts[0]) {
			return parts[1], nil
		}
		offset += int(fieldlen)
	}
	return nil, fmt.Errorf("key %s not found", key)
}

func processBag(
	r io.Reader,
	connectionCallback func([]byte, []byte) error,
	msgcallback func([]byte, []byte) error,
	checkmagic bool,
) error {
	if checkmagic {
		magic := make([]byte, len(BagMagic))
		_, err := io.ReadFull(r, magic)
		if err != nil {
			log.Fatal(err)
		}
		if !bytes.Equal(magic, BagMagic) {
			log.Fatal("not a bag")
		}
	}

	headerbuf := make([]byte, 1024)
	buf := make([]byte, 8)
	for {
		// header len
		_, err := io.ReadFull(r, buf[:4])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		headerlen := binary.LittleEndian.Uint32(buf[:4])

		// header
		if len(headerbuf) < int(headerlen) {
			headerbuf = make([]byte, headerlen*2)
		}
		_, err = io.ReadFull(r, headerbuf[:headerlen])
		if err != nil {
			return err
		}

		header := headerbuf[:headerlen]

		// data len
		_, err = io.ReadFull(r, buf[4:8])
		if err != nil {
			return err
		}
		datalen := binary.LittleEndian.Uint32(buf[4:8])

		// opcode
		opcode, err := extractHeaderValue(header, []byte("op"))
		if err != nil {
			return err
		}

		// data
		data := make([]byte, datalen)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return err
		}
		switch opcode[0] {
		case OpBagHeader:
			continue
		case OpBagChunk:
			compression, err := extractHeaderValue(header, []byte("compression"))
			if err != nil {
				return err
			}
			var reader io.Reader
			switch string(compression) {
			case "lz4":
				reader = lz4.NewReader(bytes.NewReader(data))
			case "none":
				reader = bytes.NewReader(data)
			default:
				return fmt.Errorf("unsupported compression: %s", compression)
			}
			err = processBag(reader, connectionCallback, msgcallback, false)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
		case OpBagConnection:
			err := connectionCallback(header, data)
			if err != nil {
				return err
			}
		case OpBagMessageData:
			err := msgcallback(header, data)
			if err != nil {
				return err
			}
		case OpBagIndexData:
			continue
		case OpBagChunkInfo:
			continue
		}
	}
	return nil
}

func Bag2MCAP(r io.Reader, w io.Writer) error {
	writer, err := NewWriter(w, &WriterOptions{
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
		Compression: CompressionLZ4,
		IncludeCRC:  true,
	})
	if err != nil {
		return err
	}
	defer writer.Close()

	err = writer.WriteHeader("ros1", "golang-bag2mcap", map[string]string{"name": "my funky mcap file"})
	if err != nil {
		return err
	}
	seq := uint32(0)
	return processBag(r,
		func(header, data []byte) error {
			conn, err := extractHeaderValue(header, []byte("conn"))
			if err != nil {
				return err
			}
			connID := binary.LittleEndian.Uint16(conn)
			topic, err := extractHeaderValue(header, []byte("topic"))
			if err != nil {
				return err
			}
			typ, err := extractHeaderValue(data, []byte("type"))
			if err != nil {
				return err
			}
			md5sum, err := extractHeaderValue(data, []byte("md5sum"))
			if err != nil {
				return err
			}
			msgdef, err := extractHeaderValue(data, []byte("message_definition"))
			if err != nil {
				return err
			}
			channelInfo := &ChannelInfo{
				ChannelID:  connID,
				TopicName:  string(topic),
				Encoding:   "ros1",
				SchemaName: string(typ),
				Schema:     msgdef,
				UserData: map[string]string{
					"md5sum": string(md5sum),
				},
			}
			return writer.WriteChannelInfo(channelInfo)
		},
		func(header, data []byte) error {
			conn, err := extractHeaderValue(header, []byte("conn"))
			if err != nil {
				return err
			}
			connID := binary.LittleEndian.Uint16(conn)
			time, err := extractHeaderValue(header, []byte("time"))
			if err != nil {
				return err
			}
			nsecs := rostimeToNanos(time)
			err = writer.WriteMessage(&Message{
				ChannelID:   connID,
				Sequence:    seq,
				RecordTime:  nsecs,
				PublishTime: nsecs,
				Data:        data,
			})
			if err != nil {
				return err
			}
			seq++
			return nil
		},
		true,
	)
}

func rostimeToNanos(time []byte) uint64 {
	secs := binary.LittleEndian.Uint32(time)
	nsecs := binary.LittleEndian.Uint32(time[4:])
	return uint64(secs)*1000000000 + uint64(nsecs)
}
