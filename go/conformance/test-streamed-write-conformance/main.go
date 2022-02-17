package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/foxglove/mcap/go/mcap"
)

const (
	UseChunks               = "ch"
	UseMessageIndex         = "mx"
	UseStatistics           = "st"
	UseRepeatedSchemas      = "rsh"
	UseRepeatedChannelInfos = "rch"
	UseAttachmentIndex      = "ax"
	UseMetadataIndex        = "mdx"
	UseChunkIndex           = "chx"
	UseSummaryOffset        = "sum"
	AddExtraDataToRecords   = "pad"
)

func parseOptions(features []string) (*mcap.WriterOptions, error) {
	options := mcap.WriterOptions{
		IncludeCRC:               true,
		Chunked:                  false,
		SkipMessageIndexing:      true,
		SkipStatistics:           true,
		SkipRepeatedSchemas:      true,
		SkipRepeatedChannelInfos: true,
		SkipAttachmentIndex:      true,
		SkipMetadataIndex:        true,
		SkipChunkIndex:           true,
		SkipSummaryOffsets:       true,
	}
	for _, feature := range features {
		switch feature {
		case UseChunks:
			options.Chunked = true
		case UseMessageIndex:
			options.SkipMessageIndexing = false
		case UseStatistics:
			options.SkipStatistics = false
		case UseRepeatedSchemas:
			options.SkipRepeatedSchemas = false
		case UseRepeatedChannelInfos:
			options.SkipRepeatedChannelInfos = false
		case UseAttachmentIndex:
			options.SkipAttachmentIndex = false
		case UseMetadataIndex:
			options.SkipMetadataIndex = false
		case UseChunkIndex:
			options.SkipChunkIndex = false
		case UseSummaryOffset:
			options.SkipSummaryOffsets = false
		case AddExtraDataToRecords:
			continue
		default:
			return nil, UnknownField(feature)
		}
	}
	return &options, nil
}

func UnknownField(field string) error {
	return fmt.Errorf("unknown field: %s", field)
}

type InputField struct {
	Name  string
	Value interface{}
}

func (x *InputField) UnmarshalJSON(date []byte) error {
	xs := []interface{}{}
	err := json.Unmarshal(date, &xs)
	if err != nil {
		return err
	}
	if len(xs) != 2 {
		return fmt.Errorf("invalid field: %v", xs)
	}
	x.Name = xs[0].(string)
	x.Value = xs[1]
	return nil
}

type InputRecord struct {
	Type   string       `json:"type"`
	Fields []InputField `json:"fields"`
}

type TextInput struct {
	Records []InputRecord `json:"records"`
	Meta    struct {
		Variant struct {
			Features []string `json:"features"`
		} `json:"variant"`
	} `json:"meta"`
}

func parseUint16(s string) (uint16, error) {
	x, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("failed to parse uint16: %w", err)
	}
	return uint16(x), nil
}

func parseBytes(numbers []interface{}) ([]byte, error) {
	result := []byte{}
	for _, num := range numbers {
		x, err := strconv.ParseInt(num.(string), 10, 8)
		if err != nil {
			return nil, err
		}
		result = append(result, byte(x))
	}
	return result, nil
}

func parseUint64(s string) (uint64, error) {
	x, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse uint64: %w", err)
	}
	return uint64(x), nil
}

func parseUint32(s string) (uint32, error) {
	x, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse uint16: %w", err)
	}
	return uint32(x), nil
}

func parseHeader(fields []InputField) (*mcap.Header, error) {
	header := mcap.Header{}
	for _, field := range fields {
		switch field.Name {
		case "profile":
			header.Profile = field.Value.(string)
		case "library":
			header.Library = field.Value.(string)
		}
	}
	return &header, nil
}
func parseSchema(fields []InputField) (*mcap.Schema, error) {
	schema := mcap.Schema{}
	for _, field := range fields {
		key := field.Name
		value := field.Value
		switch key {
		case "id":
			schemaID, err := parseUint16(value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema ID: %w", err)
			}
			schema.ID = schemaID
		case "name":
			schema.Name = value.(string)
		case "encoding":
			schema.Encoding = value.(string)
		case "data":
			data, err := parseBytes(value.([]interface{}))
			if err != nil {
				return nil, fmt.Errorf("failed to decode schema data: %w", err)
			}
			schema.Data = data
		default:
			return nil, UnknownField(field.Name)
		}
	}
	return &schema, nil
}
func parseChannel(fields []InputField) (*mcap.Channel, error) {
	channel := mcap.Channel{}
	for _, field := range fields {
		switch field.Name {
		case "id":
			channelID, err := parseUint16(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel ID: %w", err)
			}
			channel.ID = channelID
		case "schema_id":
			schemaID, err := parseUint16(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema ID: %w", err)
			}
			channel.SchemaID = schemaID
		case "topic":
			channel.Topic = field.Value.(string)
		case "message_encoding":
			channel.MessageEncoding = field.Value.(string)
		case "metadata":
			metadata := field.Value.(map[string]interface{})
			m := make(map[string]string)
			for k, v := range metadata {
				m[k] = v.(string)
			}
			channel.Metadata = m
		default:
			return nil, UnknownField(field.Name)
		}
	}
	return &channel, nil
}
func parseMessage(fields []InputField) (*mcap.Message, error) {
	message := mcap.Message{}
	for _, field := range fields {
		switch field.Name {
		case "channel_id":
			channelID, err := parseUint16(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to parse channel ID: %w", err)
			}
			message.ChannelID = channelID
		case "sequence":
			sequence, err := parseUint32(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to parse sequence: %w", err)
			}
			message.Sequence = sequence
		case "log_time":
			logTime, err := parseUint64(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to log time: %w", err)
			}
			message.LogTime = logTime
		case "publish_time":
			publishTime, err := parseUint64(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to log time: %w", err)
			}
			message.PublishTime = publishTime
		case "data":
			data, err := parseBytes(field.Value.([]interface{}))
			if err != nil {
				return nil, fmt.Errorf("failed to parse data: %w", err)
			}
			message.Data = data
		default:
			return nil, UnknownField(field.Name)
		}
	}
	return &message, nil
}

func parseAttachment(fields []InputField) (*mcap.Attachment, error) {
	attachment := mcap.Attachment{}
	for _, field := range fields {
		switch field.Name {
		case "log_time":
			logTime, err := parseUint64(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to log time: %w", err)
			}
			attachment.LogTime = logTime
		case "create_time":
			createTime, err := parseUint64(field.Value.(string))
			if err != nil {
				return nil, fmt.Errorf("failed to log time: %w", err)
			}
			attachment.CreateTime = createTime
		case "name":
			attachment.Name = field.Value.(string)
		case "content_type":
			attachment.ContentType = field.Value.(string)
		case "data":
			data, err := parseBytes(field.Value.([]interface{}))
			if err != nil {
				return nil, err
			}
			attachment.Data = data
		case "crc":
			crc, err := parseUint32(field.Value.(string))
			if err != nil {
				return nil, err
			}
			attachment.CRC = crc
		default:
			return nil, UnknownField(field.Name)
		}
	}
	return &attachment, nil
}

func parseDataEnd(fields []InputField) (*mcap.DataEnd, error) {
	dataEnd := mcap.DataEnd{}
	for _, field := range fields {
		switch field.Name {
		case "data_section_crc":
			crc, err := parseUint32(field.Value.(string))
			if err != nil {
				return nil, err
			}
			dataEnd.DataSectionCRC = crc
		default:
			return nil, UnknownField(field.Name)
		}
	}
	return &dataEnd, nil
}

func jsonToMCAP(w io.Writer, filepath string) error {
	input, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}
	textInput := TextInput{}
	err = json.Unmarshal(input, &textInput)
	if err != nil {
		return err
	}
	features := textInput.Meta.Variant.Features
	opts, err := parseOptions(features)
	if err != nil {
		return err
	}
	writer, err := mcap.NewWriter(w, opts)
	if err != nil {
		return err
	}
	if len(textInput.Records) == 0 {
		return fmt.Errorf("empty records")
	}
	for _, record := range textInput.Records {
		switch record.Type {
		case "Header":
			header, err := parseHeader(record.Fields)
			if err != nil {
				return err
			}
			err = writer.WriteHeader(header)
			if err != nil {
				return err
			}
		case "Schema":
			schema, err := parseSchema(record.Fields)
			if err != nil {
				return err
			}
			err = writer.WriteSchema(schema)
			if err != nil {
				return err
			}
		case "Channel":
			channel, err := parseChannel(record.Fields)
			if err != nil {
				return err
			}
			err = writer.WriteChannel(channel)
			if err != nil {
				return err
			}
		case "Message":
			message, err := parseMessage(record.Fields)
			if err != nil {
				return err
			}
			err = writer.WriteMessage(message)
			if err != nil {
				return err
			}
		case "Attachment":
			attachment, err := parseAttachment(record.Fields)
			if err != nil {
				return err
			}
			err = writer.WriteAttachment(attachment)
			if err != nil {
				return err
			}
		case "DataEnd":
			err = writer.Close()
			if err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("unrecognized record type: %s", record.Type)
		}
	}
	return fmt.Errorf("missing data end")
}

func main() {
	filepath := os.Args[1]
	err := jsonToMCAP(os.Stdout, filepath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
