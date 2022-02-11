package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/foxglove/mcap/go/libmcap"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func prettifyJSON(src []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := json.Indent(buf, src, " ", " ")
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func toSnakeCase(s string) string {
	snake := matchFirstCap.ReplaceAllString(s, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type Field struct {
	Name  string
	Value interface{}
}

func (x Field) MarshalJSON() ([]byte, error) {
	t := reflect.TypeOf(x.Value)
	var v interface{}
	switch t.Name() {
	case "string":
		v = fmt.Sprintf("\"%s\"", x.Value)
	case "uint8", "uint16", "uint32", "uint64":
		v = fmt.Sprintf("\"%d\"", x.Value)
	case "OpCode":
		v = fmt.Sprintf("\"%d\"", x.Value)
	case "CompressionFormat":
		v = fmt.Sprintf("\"%s\"", x.Value)
	default:
		switch t.Kind() {
		case reflect.Map:
			keyType := t.Key().Kind()
			valueType := t.Elem().Kind()
			m := make(map[string]string)
			switch {
			case keyType == reflect.String && valueType == reflect.String:
				for k, v := range x.Value.(map[string]string) {
					m[k] = v
				}
			case keyType == reflect.Uint16 && valueType == reflect.Uint32:
				for k, v := range x.Value.(map[uint16]uint32) {
					m[fmt.Sprintf("%d", k)] = fmt.Sprintf("%d", v)
				}
			case keyType == reflect.Uint16 && valueType == reflect.Uint64:
				for k, v := range x.Value.(map[uint16]uint64) {
					m[fmt.Sprintf("%d", k)] = fmt.Sprintf("%d", v)
				}
			default:
				return nil, fmt.Errorf("unrecognized types: %s, %s", keyType, valueType)
			}

			bytes, err := json.Marshal(m)
			if err != nil {
				return nil, err
			}
			v = string(bytes)
		case reflect.Slice:
			switch elemType := t.Elem(); elemType.Name() {
			case "uint8":
				val := x.Value.([]uint8)
				ints := make([]string, len(x.Value.([]uint8)))
				for i, v := range val {
					ints[i] = fmt.Sprintf("%d", v)
				}
				bytes, err := json.Marshal(ints)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal []uint8: %w", err)
				}
				v = string(bytes)
			case "MessageIndexEntry":
				results := [][]string{}
				entries := x.Value.([]libmcap.MessageIndexEntry)
				for _, entry := range entries {
					results = append(results, []string{
						fmt.Sprintf("\"%d\"", entry.Timestamp),
						fmt.Sprintf("\"%d\"", entry.Offset),
					})
				}
				bytes, err := json.Marshal(results)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal MessageIndexEntry: %w", err)
				}
				v = string(bytes)
			default:
				return nil, fmt.Errorf("unrecognized slice type: %s", elemType.Name())
			}
		default:
			v = x.Value
		}
	}
	bytes := []byte(fmt.Sprintf(`[%q, %s]`, x.Name, v))
	return bytes, nil
}

type Record struct {
	V interface{}
}

type TextOutput struct {
	Records []struct {
		Type   string        `json:"type"`
		Fields []interface{} `json:"fields"`
	} `json:"records"`
}

func (r Record) MarshalJSON() ([]byte, error) {
	t := reflect.TypeOf(r.V)
	v := reflect.ValueOf(r.V)
	fields := make([]Field, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		if name := toSnakeCase(t.Field(i).Name); name != "crc" {
			fields = append(fields, Field{
				Name:  toSnakeCase(t.Field(i).Name),
				Value: v.Field(i).Interface(),
			})
		}
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	record := struct {
		Type   string  `json:"type"`
		Fields []Field `json:"fields"`
	}{
		Type:   t.Name(),
		Fields: fields,
	}
	bytes, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func mcapToJSON(w io.Writer, filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()
	lexer, err := libmcap.NewLexer(f)
	if err != nil {
		return err
	}
	records := []Record{}
	for {
		tokenType, data, err := lexer.Next(nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		switch tokenType {
		case libmcap.TokenHeader:
			header, err := libmcap.ParseHeader(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*header})
		case libmcap.TokenFooter:
			footer, err := libmcap.ParseFooter(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*footer})
		case libmcap.TokenSchema:
			schema, err := libmcap.ParseSchema(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*schema})
		case libmcap.TokenChannel:
			channelInfo, err := libmcap.ParseChannel(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*channelInfo})
		case libmcap.TokenMessage:
			message, err := libmcap.ParseMessage(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*message})
		case libmcap.TokenChunk:
			chunk, err := libmcap.ParseChunk(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*chunk})
		case libmcap.TokenMessageIndex:
			_, err := libmcap.ParseMessageIndex(data)
			if err != nil {
				return err
			}
			// TODO: these should be omitted, but aren't present in JSON
			// records = append(records, Record{*messageIndex})
		case libmcap.TokenChunkIndex:
			chunkIndex, err := libmcap.ParseChunkIndex(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*chunkIndex})
		case libmcap.TokenAttachment:
			attachment, err := libmcap.ParseAttachment(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*attachment})
		case libmcap.TokenAttachmentIndex:
			attachmentIndex, err := libmcap.ParseAttachmentIndex(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*attachmentIndex})
		case libmcap.TokenStatistics:
			statistics, err := libmcap.ParseStatistics(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*statistics})
		case libmcap.TokenMetadata:
			metadata, err := libmcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*metadata})
		case libmcap.TokenMetadataIndex:
			metadataIndex, err := libmcap.ParseMetadataIndex(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*metadataIndex})
		case libmcap.TokenSummaryOffset:
			summaryOffset, err := libmcap.ParseSummaryOffset(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*summaryOffset})
		case libmcap.TokenDataEnd:
			dataEnd, err := libmcap.ParseDataEnd(data)
			if err != nil {
				return err
			}
			records = append(records, Record{*dataEnd})
		case libmcap.TokenError:
			if err != nil {
				return fmt.Errorf("error token: %w", err)
			}
		}
	}

	serializedOutput, err := json.Marshal(map[string]interface{}{
		"records": records,
	})
	if err != nil {
		return err
	}
	_, err = w.Write(serializedOutput)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	filepath := os.Args[1]
	err := mcapToJSON(os.Stdout, filepath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
