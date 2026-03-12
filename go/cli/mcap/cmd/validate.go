package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/foxglove/go-rosbag/ros1msg"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	validateTopics  string
	validateSample  float64
	validateMaxErrs int
)

type mcapValidator struct {
	// Cached decoders per schema ID
	protobufDescriptors  map[uint16]protoreflect.MessageDescriptor
	ros1Transcoders      map[uint16]*ros1msg.JSONTranscoder
	jsonSchemaValidators map[uint16]*jsonschema.Schema

	// Track schemas and channels
	schemas  map[uint16]*mcap.Schema
	channels map[uint16]*mcap.Channel

	// Deduplicate warnings for unsupported encodings
	warnedEncodings map[string]bool

	// Track channels per encoding for summary
	channelsByEncoding map[string]int

	// Results
	diagnosis       Diagnosis
	messagesChecked uint64
	messagesPassed  uint64
	messagesFailed  uint64
	messagesSkipped uint64
}

func newMcapValidator() *mcapValidator {
	return &mcapValidator{
		protobufDescriptors:  make(map[uint16]protoreflect.MessageDescriptor),
		ros1Transcoders:      make(map[uint16]*ros1msg.JSONTranscoder),
		jsonSchemaValidators: make(map[uint16]*jsonschema.Schema),
		schemas:              make(map[uint16]*mcap.Schema),
		channels:             make(map[uint16]*mcap.Channel),
		warnedEncodings:      make(map[string]bool),
		channelsByEncoding:   make(map[string]int),
	}
}

func (v *mcapValidator) warn(format string, args ...any) {
	color.Yellow(format, args...)
	v.diagnosis.Warnings = append(v.diagnosis.Warnings, fmt.Sprintf(format, args...))
}

func (v *mcapValidator) error(format string, args ...any) {
	color.Red(format, args...)
	v.diagnosis.Errors = append(v.diagnosis.Errors, fmt.Sprintf(format, args...))
}

func formatLogTime(t uint64) string {
	sec := t / 1e9
	nsec := t % 1e9
	return fmt.Sprintf("%d.%09d", sec, nsec)
}

// getProtobufDescriptor returns a cached protobuf MessageDescriptor for the given schema,
// or initializes one from the schema's FileDescriptorSet data.
func (v *mcapValidator) getProtobufDescriptor(schema *mcap.Schema) (protoreflect.MessageDescriptor, error) {
	if desc, ok := v.protobufDescriptors[schema.ID]; ok {
		return desc, nil
	}
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(schema.Data, fileDescriptorSet); err != nil {
		return nil, fmt.Errorf("failed to parse FileDescriptorSet: %w", err)
	}
	files, err := protodesc.FileOptions{}.NewFiles(fileDescriptorSet)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %w", err)
	}
	descriptor, err := files.FindDescriptorByName(protoreflect.FullName(schema.Name))
	if err != nil {
		return nil, fmt.Errorf("failed to find descriptor for %q: %w", schema.Name, err)
	}
	msgDesc, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor for %q is not a message descriptor", schema.Name)
	}
	v.protobufDescriptors[schema.ID] = msgDesc
	return msgDesc, nil
}

// getROS1Transcoder returns a cached ROS1 JSONTranscoder for the given schema,
// or initializes one from the schema's message definition data.
func (v *mcapValidator) getROS1Transcoder(schema *mcap.Schema) (*ros1msg.JSONTranscoder, error) {
	if tc, ok := v.ros1Transcoders[schema.ID]; ok {
		return tc, nil
	}
	packageName := strings.Split(schema.Name, "/")[0]
	tc, err := ros1msg.NewJSONTranscoder(packageName, schema.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to build ROS1 transcoder for %q: %w", schema.Name, err)
	}
	v.ros1Transcoders[schema.ID] = tc
	return tc, nil
}

// getJSONSchemaValidator returns a cached JSON Schema validator for the given schema,
// or compiles one from the schema's JSON Schema data.
func (v *mcapValidator) getJSONSchemaValidator(schema *mcap.Schema) (*jsonschema.Schema, error) {
	if s, ok := v.jsonSchemaValidators[schema.ID]; ok {
		return s, nil
	}
	compiler := jsonschema.NewCompiler()
	schemaID := fmt.Sprintf("schema-%d.json", schema.ID)
	if err := compiler.AddResource(schemaID, bytes.NewReader(schema.Data)); err != nil {
		return nil, fmt.Errorf("failed to add JSON Schema resource: %w", err)
	}
	compiled, err := compiler.Compile(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JSON Schema: %w", err)
	}
	v.jsonSchemaValidators[schema.ID] = compiled
	return compiled, nil
}

// validateMessage attempts to decode/validate a single message against its schema.
// Returns nil if the message is valid, or an error describing the validation failure.
func (v *mcapValidator) validateMessage(
	schema *mcap.Schema,
	channel *mcap.Channel,
	message *mcap.Message,
) error {
	// Schema-less channel
	if schema == nil || schema.Encoding == "" {
		if channel.MessageEncoding == "json" {
			if !json.Valid(message.Data) {
				var js json.RawMessage
				err := json.Unmarshal(message.Data, &js)
				if err != nil {
					return fmt.Errorf("invalid JSON: %w", err)
				}
				return fmt.Errorf("invalid JSON")
			}
			return nil
		}
		// Non-JSON schema-less channel — nothing to validate
		return nil
	}

	switch schema.Encoding {
	case "protobuf":
		desc, err := v.getProtobufDescriptor(schema)
		if err != nil {
			return err
		}
		protoMsg := dynamicpb.NewMessage(desc)
		if err := proto.Unmarshal(message.Data, protoMsg); err != nil {
			return fmt.Errorf("protobuf unmarshal failed: %w", err)
		}
		return nil

	case "ros1msg":
		tc, err := v.getROS1Transcoder(schema)
		if err != nil {
			return err
		}
		if err := tc.Transcode(io.Discard, bytes.NewReader(message.Data)); err != nil {
			return fmt.Errorf("ROS1 transcode failed: %w", err)
		}
		return nil

	case "jsonschema":
		compiled, err := v.getJSONSchemaValidator(schema)
		if err != nil {
			return err
		}
		var val any
		if err := json.Unmarshal(message.Data, &val); err != nil {
			return fmt.Errorf("failed to parse message as JSON: %w", err)
		}
		if err := compiled.Validate(val); err != nil {
			var validationErr *jsonschema.ValidationError
			if errors.As(err, &validationErr) {
				return formatJSONSchemaError(validationErr)
			}
			return fmt.Errorf("JSON Schema validation failed: %w", err)
		}
		return nil

	default:
		// Unsupported encoding — caller handles this as a skip
		return nil
	}
}

// formatJSONSchemaError converts a jsonschema.ValidationError tree into a readable error.
func formatJSONSchemaError(ve *jsonschema.ValidationError) error {
	var msgs []string
	collectValidationErrors(ve, &msgs)
	if len(msgs) == 0 {
		return fmt.Errorf("JSON Schema validation failed: %s", ve.Error())
	}
	return fmt.Errorf("JSON Schema validation failed:\n%s", strings.Join(msgs, "\n"))
}

func collectValidationErrors(ve *jsonschema.ValidationError, msgs *[]string) {
	if len(ve.Causes) == 0 {
		*msgs = append(*msgs, fmt.Sprintf("      at '%s': %s", ve.InstanceLocation, ve.Message))
		return
	}
	for _, cause := range ve.Causes {
		collectValidationErrors(cause, msgs)
	}
}

// shouldSample returns true if this message should be validated based on the sample rate.
// Uses a deterministic hash of the message's log time for reproducibility.
func shouldSample(logTime uint64, sampleRate float64) bool {
	if sampleRate >= 1.0 {
		return true
	}
	if sampleRate <= 0.0 {
		return false
	}
	h := fnv.New32a()
	b := make([]byte, 8)
	b[0] = byte(logTime)
	b[1] = byte(logTime >> 8)
	b[2] = byte(logTime >> 16)
	b[3] = byte(logTime >> 24)
	b[4] = byte(logTime >> 32)
	b[5] = byte(logTime >> 40)
	b[6] = byte(logTime >> 48)
	b[7] = byte(logTime >> 56)
	h.Write(b)
	// Normalize hash to [0, 1)
	normalized := float64(h.Sum32()) / float64(1<<32)
	return normalized < sampleRate
}

// isUnsupportedEncoding returns true for schema encodings we cannot validate.
func isUnsupportedEncoding(encoding string) bool {
	switch encoding {
	case "protobuf", "ros1msg", "jsonschema", "":
		return false
	default:
		return true
	}
}

// Validate reads all messages and validates them against their schemas.
func (v *mcapValidator) Validate(rs io.ReadSeeker, topics []string, sampleRate float64, maxErrors int) Diagnosis {
	reader, err := mcap.NewReader(rs)
	if err != nil {
		v.error("Failed to create MCAP reader: %s", err)
		return v.diagnosis
	}
	defer reader.Close()

	opts := []mcap.ReadOpt{mcap.UsingIndex(true)}
	if len(topics) > 0 {
		opts = append(opts, mcap.WithTopics(topics))
	}

	it, err := reader.Messages(opts...)
	if err != nil {
		v.error("Failed to create message iterator: %s", err)
		return v.diagnosis
	}

	message := mcap.Message{Data: make([]byte, 0, 1024*1024)}
	for {
		schema, channel, _, err := it.NextInto(&message)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			v.error("Failed to read message: %s", err)
			return v.diagnosis
		}

		// Track channel/schema for later
		v.channels[channel.ID] = channel
		if schema != nil {
			v.schemas[schema.ID] = schema
		}

		// Track channels per encoding for summary
		encoding := ""
		if schema != nil {
			encoding = schema.Encoding
		}
		if _, seen := v.channelsByEncoding[encoding]; !seen {
			v.channelsByEncoding[encoding] = 0
		}
		v.channelsByEncoding[encoding]++

		// Handle unsupported encodings
		if schema != nil && isUnsupportedEncoding(schema.Encoding) {
			if !v.warnedEncodings[schema.Encoding] {
				v.warnedEncodings[schema.Encoding] = true
				v.warn("Skipping validation for channels with schema encoding %q (not supported)", schema.Encoding)
			}
			v.messagesSkipped++
			continue
		}

		// Sampling
		if !shouldSample(message.LogTime, sampleRate) {
			v.messagesSkipped++
			continue
		}

		v.messagesChecked++

		validationErr := v.validateMessage(schema, channel, &message)
		if validationErr != nil {
			v.messagesFailed++
			schemaDesc := "no schema"
			if schema != nil {
				schemaDesc = fmt.Sprintf("%q [%s]", schema.Name, schema.Encoding)
			}
			v.error("Topic %q (channel %d, schema %s):\n  Message seq=%d at log_time %s:\n    %s",
				channel.Topic,
				channel.ID,
				schemaDesc,
				message.Sequence,
				formatLogTime(message.LogTime),
				validationErr,
			)

			if maxErrors > 0 && len(v.diagnosis.Errors) >= maxErrors {
				v.warn("Stopping after %d errors (--max-errors)", maxErrors)
				return v.diagnosis
			}
		} else {
			v.messagesPassed++
			if verbose {
				fmt.Printf("  OK: %s seq=%d log_time=%s\n",
					channel.Topic, message.Sequence, formatLogTime(message.LogTime))
			}
		}
	}

	return v.diagnosis
}

func (v *mcapValidator) printSummary() {
	fmt.Println()
	fmt.Println("Validation complete:")
	fmt.Printf("  Checked: %d  Passed: %d  Failed: %d  Skipped: %d\n",
		v.messagesChecked, v.messagesPassed, v.messagesFailed, v.messagesSkipped)

	// Summarize encodings validated
	encodingSummary := []string{}
	// Count unique channels per encoding
	channelEncodings := make(map[string]map[uint16]bool)
	for chID, ch := range v.channels {
		enc := ""
		if schema, ok := v.schemas[ch.SchemaID]; ok {
			enc = schema.Encoding
		}
		if channelEncodings[enc] == nil {
			channelEncodings[enc] = make(map[uint16]bool)
		}
		channelEncodings[enc][chID] = true
	}
	for enc, chs := range channelEncodings {
		label := enc
		if label == "" {
			label = "schema-less"
		}
		encodingSummary = append(encodingSummary, fmt.Sprintf("%s (%d channels)", label, len(chs)))
	}
	if len(encodingSummary) > 0 {
		fmt.Printf("  Schemas: %s\n", strings.Join(encodingSummary, ", "))
	}
}

var validateCmd = &cobra.Command{
	Use:   "validate <file>",
	Short: "Validate message data against schemas in an MCAP file",
	Long: `Validate that message payloads in an MCAP file conform to their declared schemas.

This command attempts to decode every message against its channel's schema and
reports any messages that fail validation. Supported schema encodings: protobuf,
ros1msg, and jsonschema. Unsupported encodings are skipped with a warning.

For structural validation of the MCAP file format itself, use "mcap doctor".`,
	Run: func(_ *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			fmt.Println("An MCAP file argument is required.")
			os.Exit(1)
		}

		topics := strings.FieldsFunc(validateTopics, func(c rune) bool { return c == ',' })

		filename := args[0]
		err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
			validator := newMcapValidator()
			if remote {
				color.Yellow("Will read full remote file")
			}
			fmt.Printf("Validating messages in %s...\n", filename)
			diagnosis := validator.Validate(rs, topics, validateSample, validateMaxErrs)
			validator.printSummary()
			if len(diagnosis.Errors) > 0 {
				return fmt.Errorf("encountered %d validation errors", len(diagnosis.Errors))
			}
			return nil
		})
		if err != nil {
			die("Validate command failed: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.PersistentFlags().StringVar(&validateTopics, "topics", "", "Comma-separated list of topics to validate (default: all)")
	validateCmd.PersistentFlags().Float64Var(&validateSample, "sample", 1.0, "Fraction of messages to validate (0.0-1.0)")
	validateCmd.PersistentFlags().IntVar(&validateMaxErrs, "max-errors", 0, "Stop after N validation errors (0 = no limit)")
}
