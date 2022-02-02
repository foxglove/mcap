package ros

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
)

type transcoder func(io.Writer, io.Reader) error

type field struct {
	name   string
	xcoder transcoder
}
type ROSJSONTranscoder struct {
	buf    []byte
	fields []field
}

func NewTranscoder(fields []*Field) (transcoder, error) {
	xcoder := &ROSJSONTranscoder{
		buf: make([]byte, 8),
	}
	transforms := []field{}
	for _, f := range fields {
		switch f.Type {
		case "int8":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.int8,
			})
		case "int16":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.int16,
			})
		case "int32":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.int32,
			})
		case "int64":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.int64,
			})
		case "uint8":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.uint8,
			})
		case "uint16":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.uint16,
			})
		case "uint32":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.uint32,
			})
		case "uint64":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.uint64,
			})
		case "float32":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.float32,
			})
		case "float64":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.float64,
			})
		case "string":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.string,
			})
		case "time":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.time,
			})
		case "duration":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.duration,
			})
		case "array":
			transforms = append(transforms, field{
				name:   f.Name,
				xcoder: xcoder.array(xcoder.string, -1),
			})
		default:
			return nil, fmt.Errorf("unsupported field type: %s", f.Type)
		}
	}
	return xcoder.message(transforms), nil
}

func (t *ROSJSONTranscoder) int8(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:1])
	if err != nil {
		return err
	}
	s := strconv.Itoa(int(t.buf[0]))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) int16(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:2])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint16(t.buf[:2])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) int32(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:4])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint32(t.buf[:4])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) int64(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:8])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint64(t.buf[:4])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) uint8(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:1])
	if err != nil {
		return err
	}
	s := strconv.Itoa(int(t.buf[0]))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) uint16(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:2])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint16(t.buf[:2])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) uint32(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:4])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint32(t.buf[:4])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) uint64(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:8])
	if err != nil {
		return err
	}
	x := binary.LittleEndian.Uint32(t.buf[:8])
	s := strconv.Itoa(int(x))
	_, err = w.Write([]byte(s))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) float32(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:4])
	if err != nil {
		return err
	}
	bits := binary.LittleEndian.Uint32(t.buf[:4])
	float := math.Float32frombits(bits)
	_, err = w.Write([]byte(strconv.FormatFloat(float64(float), 'f', -1, 32)))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) float64(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:8])
	if err != nil {
		return err
	}
	bits := binary.LittleEndian.Uint64(t.buf[:8])
	float := math.Float64frombits(bits)
	_, err = w.Write([]byte(strconv.FormatFloat(float64(float), 'f', -1, 64)))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) string(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:4])
	if err != nil {
		return err
	}
	strlen := binary.LittleEndian.Uint32(t.buf[:4])
	buf := make([]byte, strlen+2)
	buf[0] = '"'
	buf[len(buf)-1] = '"'
	_, err = io.ReadFull(r, buf[1:len(buf)-1])
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) time(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:8])
	if err != nil {
		return err
	}
	secs := binary.LittleEndian.Uint32(t.buf[:4])
	nsecs := binary.LittleEndian.Uint32(t.buf[4:8])
	_, err = w.Write([]byte(fmt.Sprintf("\"%d.%09d\"", secs, nsecs)))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) duration(w io.Writer, r io.Reader) error {
	_, err := io.ReadFull(r, t.buf[:8])
	if err != nil {
		return err
	}
	secs := binary.LittleEndian.Uint32(t.buf[:4])
	nsecs := binary.LittleEndian.Uint32(t.buf[4:8])
	_, err = w.Write([]byte(fmt.Sprintf("\"%d.%09d\"", secs, nsecs)))
	if err != nil {
		return err
	}
	return nil
}

func (t *ROSJSONTranscoder) array(xcoder transcoder, size int) func(w io.Writer, r io.Reader) error {
	return func(w io.Writer, r io.Reader) error {
		_, err := w.Write([]byte("["))
		if err != nil {
			return err
		}
		if size == -1 {
			// variable length array
			_, err := io.ReadFull(r, t.buf[:4])
			if err != nil {
				return err
			}
			count := int(binary.LittleEndian.Uint32(t.buf[:4]))
			for i := 0; i < count; i++ {
				err = xcoder(w, r)
				if err != nil {
					return err
				}
			}
		} else {
			// fixed length array
			for i := 0; i < size; i++ {
				if i > 0 {
					_, err = w.Write([]byte(","))
					if err != nil {
						return err
					}
				}
				err = xcoder(w, r)
				if err != nil {
					return err
				}
			}
		}
		_, err = w.Write([]byte("]"))
		if err != nil {
			return err
		}
		return nil
	}
}

func (t *ROSJSONTranscoder) message(fields []field) func(w io.Writer, r io.Reader) error {
	return func(w io.Writer, r io.Reader) error {
		_, err := w.Write([]byte("{"))
		if err != nil {
			return err
		}
		for i, f := range fields {
			if i > 0 {
				_, err := w.Write([]byte(","))
				if err != nil {
					return err
				}
			}
			_, err := w.Write([]byte(fmt.Sprintf("\"%s\":", f.name)))
			if err != nil {
				return err
			}
			err = f.xcoder(w, r)
			if err != nil {
				return err
			}
		}
		_, err = w.Write([]byte("}"))
		if err != nil {
			return err
		}
		return nil
	}
}
