package ros

import (
	"bytes"
	"compress/bzip2"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/foxglove/mcap/go/mcap"
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

var (
	headerCompression = []byte("compression")
	headerOp          = []byte("op")
	headerTopic       = []byte("topic")
	headerConn        = []byte("conn")
	headerTime        = []byte("time")
)

func headerToMap(header []byte) (map[string]string, error) {
	offset := 0
	m := make(map[string]string)
	for offset < len(header) {
		fieldlen := binary.LittleEndian.Uint32(header[offset : offset+4])
		offset += 4
		index := bytes.IndexByte(header[offset:offset+int(fieldlen)], '=')
		if index < 0 {
			return nil, fmt.Errorf("missing kv separator")
		}
		key := string(header[offset : offset+index])
		m[key] = string(header[offset+index+1 : offset+int(fieldlen)])
		offset += int(fieldlen)
	}
	return m, nil
}

func getUint32(buf []byte, offset int) (result uint32, newoffset int, err error) {
	if len(buf[offset:]) < 4 {
		return 0, 0, fmt.Errorf("short buffer")
	}
	return binary.LittleEndian.Uint32(buf[offset:]), offset + 4, nil
}

type ResettableReader interface {
	io.Reader
	Reset(io.Reader)
}

type resettableByteReader struct {
	r io.Reader
}

func (r *resettableByteReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *resettableByteReader) Reset(reader io.Reader) {
	r.r = reader
}

func extractHeaderValue(header []byte, key []byte) ([]byte, error) {
	var fieldlen uint32
	var err error
	offset := 0
	for offset < len(header) {
		fieldlen, offset, err = getUint32(header, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to extract field length: %w", err)
		}
		field := header[offset : offset+int(fieldlen)]
		separatorIdx := bytes.Index(field, []byte{'='})
		if separatorIdx < 0 {
			return nil, fmt.Errorf("no field separator found")
		}
		k := field[:separatorIdx]
		v := field[separatorIdx+1:]
		if bytes.Equal(key, k) {
			return v, nil
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

	var inChunk bool
	header := make([]byte, 1024)
	buf := make([]byte, 8)
	data := make([]byte, 1024*1024)
	chunkData := make([]byte, 1024*1024)

	var chunkReader ResettableReader
	var activeReader, baseReader io.Reader
	baseReader = r
	activeReader = r
	for {
		// header len
		_, err := io.ReadFull(activeReader, buf[:4])
		if err != nil {
			if errors.Is(err, io.EOF) {
				if inChunk {
					activeReader = baseReader
					inChunk = false
					continue
				}
				break
			}
			return err
		}
		headerlen := binary.LittleEndian.Uint32(buf[:4])

		// header
		if len(header) < int(headerlen) {
			header = make([]byte, headerlen*2)
		}
		_, err = io.ReadFull(activeReader, header[:headerlen])
		if err != nil {
			return err
		}

		headerData := header[:headerlen]

		// data len
		_, err = io.ReadFull(activeReader, buf[4:8])
		if err != nil {
			return err
		}
		datalen := binary.LittleEndian.Uint32(buf[4:8])

		// opcode
		opcode, err := extractHeaderValue(headerData, headerOp)
		if err != nil {
			return err
		}

		if opcode[0] == OpBagChunk {
			// data
			if len(chunkData) < int(datalen) {
				chunkData = make([]byte, datalen*2)
			}
			_, err = io.ReadFull(activeReader, chunkData[:datalen])
			if err != nil {
				return err
			}
		} else {
			if len(data) < int(datalen) {
				data = make([]byte, datalen*2)
			}
			_, err = io.ReadFull(activeReader, data[:datalen])
			if err != nil {
				return err
			}
		}

		switch opcode[0] {
		case OpBagHeader:
			continue
		case OpBagChunk:
			compression, err := extractHeaderValue(headerData, headerCompression)
			if err != nil {
				return err
			}
			r := bytes.NewReader(chunkData[:datalen])
			switch string(compression) {
			case "lz4":
				if chunkReader == nil {
					chunkReader = lz4.NewReader(r)
				} else {
					chunkReader.Reset(r)
				}
			case "bz2":
				if chunkReader == nil {
					chunkReader = &resettableByteReader{bzip2.NewReader(r)}
				} else {
					chunkReader.Reset(r)
				}
			case "none":
				if chunkReader == nil {
					chunkReader = &resettableByteReader{r}
				} else {
					chunkReader.Reset(r)
				}
			default:
				return fmt.Errorf("unsupported compression: %s", compression)
			}
			activeReader = chunkReader
			inChunk = true
			continue
		case OpBagConnection:
			err := connectionCallback(headerData, data[:datalen])
			if err != nil {
				return err
			}
		case OpBagMessageData:
			err := msgcallback(headerData, data[:datalen])
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

func Bag2MCAP(w io.Writer, r io.Reader, opts *mcap.WriterOptions) error {
	writer, err := mcap.NewWriter(w, opts)
	if err != nil {
		return err
	}
	defer writer.Close()

	err = writer.WriteHeader(&mcap.Header{
		Profile: "ros1",
	})
	if err != nil {
		return err
	}
	seq := uint32(0)
	schemas := make(map[string]uint16)
	return processBag(r,
		func(header, data []byte) error {
			conn, err := extractHeaderValue(header, headerConn)
			if err != nil {
				return err
			}
			connID := binary.LittleEndian.Uint16(conn)
			topic, err := extractHeaderValue(header, headerTopic)
			if err != nil {
				return err
			}
			connectionDataHeader, err := headerToMap(data)
			if err != nil {
				return fmt.Errorf("failed to parse connection data: %w", err)
			}
			typ := connectionDataHeader["type"]
			delete(connectionDataHeader, "type")
			msgdef := connectionDataHeader["message_definition"]
			delete(connectionDataHeader, "message_definition")

			key := fmt.Sprintf("%s/%s", topic, connectionDataHeader["md5sum"])
			if _, ok := schemas[key]; !ok {
				schemaID := uint16(len(schemas) + 1)
				msgdefCopy := make([]byte, len(msgdef))
				copy(msgdefCopy, msgdef)
				err := writer.WriteSchema(&mcap.Schema{
					ID:       schemaID,
					Encoding: "ros1msg",
					Name:     typ,
					Data:     msgdefCopy,
				})
				if err != nil {
					return err
				}
				schemas[key] = schemaID
			}
			channelInfo := &mcap.Channel{
				ID:              connID,
				Topic:           string(topic),
				MessageEncoding: "ros1",
				SchemaID:        schemas[key],
				Metadata:        connectionDataHeader,
			}
			return writer.WriteChannel(channelInfo)
		},
		func(header, data []byte) error {
			conn, err := extractHeaderValue(header, headerConn)
			if err != nil {
				return err
			}
			connID := binary.LittleEndian.Uint16(conn)
			time, err := extractHeaderValue(header, headerTime)
			if err != nil {
				return err
			}
			nsecs := rosTimeToNanoseconds(time)
			err = writer.WriteMessage(&mcap.Message{
				ChannelID:   connID,
				Sequence:    seq,
				LogTime:     nsecs,
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

func rosTimeToNanoseconds(time []byte) uint64 {
	secs := binary.LittleEndian.Uint32(time)
	nsecs := binary.LittleEndian.Uint32(time[4:])
	return uint64(secs)*1000000000 + uint64(nsecs)
}
