package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildProtobufMessage creates a valid serialized protobuf message using the
// cardboard.pb.bin test descriptor.
func buildProtobufMessage(t *testing.T) (schemaData []byte, msgData []byte) {
	t.Helper()
	schemaData, err := os.ReadFile("../testdata/cardboard.pb.bin")
	require.NoError(t, err)

	// Parse the FileDescriptorSet to create a valid message
	fds := &descriptorpb.FileDescriptorSet{}
	require.NoError(t, proto.Unmarshal(schemaData, fds))
	files, err := protodesc.FileOptions{}.NewFiles(fds)
	require.NoError(t, err)
	desc, err := files.FindDescriptorByName(protoreflect.FullName("cardboard.Box"))
	require.NoError(t, err)
	msgDesc := desc.(protoreflect.MessageDescriptor)

	// Build a simple Box message with a cost
	msg := dynamicpb.NewMessage(msgDesc)
	costField := msgDesc.Fields().ByName("cost")
	costMsg := dynamicpb.NewMessage(costField.Message())
	costMsg.Set(costField.Message().Fields().ByName("dollars"), protoreflect.ValueOfInt32(42))
	msg.Set(costField, protoreflect.ValueOfMessage(costMsg))

	msgData, err = proto.Marshal(msg)
	require.NoError(t, err)
	return schemaData, msgData
}

func writeMCAPWithMessages(t *testing.T, schemas []*mcap.Schema, channels []*mcap.Channel, messages []*mcap.Message) *bytes.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024,
	})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&mcap.Header{
		Profile: "",
		Library: "validate-test",
	}))

	for _, s := range schemas {
		require.NoError(t, writer.WriteSchema(s))
	}
	for _, ch := range channels {
		require.NoError(t, writer.WriteChannel(ch))
	}
	for _, msg := range messages {
		require.NoError(t, writer.WriteMessage(msg))
	}
	require.NoError(t, writer.Close())
	return bytes.NewReader(buf.Bytes())
}

func TestValidateProtobufValid(t *testing.T) {
	schemaData, msgData := buildProtobufMessage(t)

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "cardboard.Box", Encoding: "protobuf", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/boxes", MessageEncoding: "protobuf"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Empty(t, diagnosis.Errors)
	assert.Equal(t, uint64(1), validator.messagesChecked)
	assert.Equal(t, uint64(1), validator.messagesPassed)
	assert.Equal(t, uint64(0), validator.messagesFailed)
}

func TestValidateProtobufInvalid(t *testing.T) {
	schemaData, _ := buildProtobufMessage(t)

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "cardboard.Box", Encoding: "protobuf", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/boxes", MessageEncoding: "protobuf"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: []byte{0xff, 0xff, 0xff}}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Len(t, diagnosis.Errors, 1)
	assert.Contains(t, diagnosis.Errors[0], "protobuf unmarshal failed")
	assert.Equal(t, uint64(1), validator.messagesFailed)
}

func TestValidateJSONSchemaValid(t *testing.T) {
	schemaData := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"value": {"type": "number"}
		},
		"required": ["name", "value"]
	}`)
	msgData, _ := json.Marshal(map[string]any{"name": "sensor", "value": 42.5})

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "SensorReading", Encoding: "jsonschema", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/sensors", MessageEncoding: "json"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Empty(t, diagnosis.Errors)
	assert.Equal(t, uint64(1), validator.messagesPassed)
}

func TestValidateJSONSchemaInvalid(t *testing.T) {
	schemaData := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"value": {"type": "number"}
		},
		"required": ["name", "value"]
	}`)
	// Missing required "value" field
	msgData, _ := json.Marshal(map[string]any{"name": "sensor"})

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "SensorReading", Encoding: "jsonschema", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/sensors", MessageEncoding: "json"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Len(t, diagnosis.Errors, 1)
	assert.Contains(t, diagnosis.Errors[0], "JSON Schema validation failed")
	assert.Equal(t, uint64(1), validator.messagesFailed)
}

func TestValidateJSONSchemaInvalidJSON(t *testing.T) {
	schemaData := []byte(`{"type": "object"}`)
	msgData := []byte(`{invalid json}`)

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "Config", Encoding: "jsonschema", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/config", MessageEncoding: "json"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Len(t, diagnosis.Errors, 1)
	assert.Contains(t, diagnosis.Errors[0], "failed to parse message as JSON")
}

func TestValidateUnsupportedEncodingWarns(t *testing.T) {
	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "MyMsg", Encoding: "flatbuffer", Data: []byte("some schema")}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/fb", MessageEncoding: "flatbuffer"}},
		[]*mcap.Message{
			{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: []byte{1, 2, 3}},
			{ChannelID: 1, Sequence: 2, LogTime: 200, PublishTime: 200, Data: []byte{4, 5, 6}},
		},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	// Should produce warnings, not errors
	assert.Empty(t, diagnosis.Errors)
	assert.Len(t, diagnosis.Warnings, 1)
	assert.Contains(t, diagnosis.Warnings[0], "flatbuffer")
	assert.Equal(t, uint64(2), validator.messagesSkipped)
	assert.Equal(t, uint64(0), validator.messagesChecked)
}

func TestValidateSchemalessJSONValid(t *testing.T) {
	msgData := []byte(`{"hello": "world"}`)

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{},
		[]*mcap.Channel{{ID: 1, SchemaID: 0, Topic: "/json", MessageEncoding: "json"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Empty(t, diagnosis.Errors)
	assert.Equal(t, uint64(1), validator.messagesPassed)
}

func TestValidateSchemalessJSONInvalid(t *testing.T) {
	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{},
		[]*mcap.Channel{{ID: 1, SchemaID: 0, Topic: "/json", MessageEncoding: "json"}},
		[]*mcap.Message{{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: []byte(`{bad`)}},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 0)
	assert.Len(t, diagnosis.Errors, 1)
	assert.Contains(t, diagnosis.Errors[0], "invalid JSON")
}

func TestValidateTopicFilter(t *testing.T) {
	schemaData, msgData := buildProtobufMessage(t)

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "cardboard.Box", Encoding: "protobuf", Data: schemaData}},
		[]*mcap.Channel{
			{ID: 1, SchemaID: 1, Topic: "/boxes", MessageEncoding: "protobuf"},
			{ID: 2, SchemaID: 1, Topic: "/other", MessageEncoding: "protobuf"},
		},
		[]*mcap.Message{
			{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: msgData},
			{ChannelID: 2, Sequence: 1, LogTime: 200, PublishTime: 200, Data: msgData},
		},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, []string{"/boxes"}, 1.0, 0)
	assert.Empty(t, diagnosis.Errors)
	// Only the /boxes message should be checked
	assert.Equal(t, uint64(1), validator.messagesChecked)
}

func TestValidateMaxErrors(t *testing.T) {
	schemaData, _ := buildProtobufMessage(t)
	garbage := []byte{0xff, 0xff, 0xff}

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "cardboard.Box", Encoding: "protobuf", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/boxes", MessageEncoding: "protobuf"}},
		[]*mcap.Message{
			{ChannelID: 1, Sequence: 1, LogTime: 100, PublishTime: 100, Data: garbage},
			{ChannelID: 1, Sequence: 2, LogTime: 200, PublishTime: 200, Data: garbage},
			{ChannelID: 1, Sequence: 3, LogTime: 300, PublishTime: 300, Data: garbage},
		},
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 1.0, 2)
	// Should stop after 2 errors
	assert.Len(t, diagnosis.Errors, 2)
	// Plus a warning about stopping
	assert.Len(t, diagnosis.Warnings, 1)
	assert.Contains(t, diagnosis.Warnings[0], "Stopping after 2 errors")
}

func TestValidateSampling(t *testing.T) {
	schemaData, msgData := buildProtobufMessage(t)

	// Create many messages
	messages := make([]*mcap.Message, 100)
	for i := range messages {
		messages[i] = &mcap.Message{
			ChannelID:   1,
			Sequence:    uint32(i),
			LogTime:     uint64(i * 1000),
			PublishTime: uint64(i * 1000),
			Data:        msgData,
		}
	}

	rs := writeMCAPWithMessages(t,
		[]*mcap.Schema{{ID: 1, Name: "cardboard.Box", Encoding: "protobuf", Data: schemaData}},
		[]*mcap.Channel{{ID: 1, SchemaID: 1, Topic: "/boxes", MessageEncoding: "protobuf"}},
		messages,
	)

	validator := newMcapValidator()
	diagnosis := validator.Validate(rs, nil, 0.5, 0)
	assert.Empty(t, diagnosis.Errors)
	// With 50% sample rate, we should get roughly 50 messages checked, but allow wide range
	assert.Greater(t, validator.messagesChecked, uint64(20))
	assert.Less(t, validator.messagesChecked, uint64(80))
	assert.Equal(t, validator.messagesChecked+validator.messagesSkipped, uint64(100))
}

func TestShouldSampleDeterministic(t *testing.T) {
	// Same inputs should always produce same outputs
	for i := uint64(0); i < 100; i++ {
		r1 := shouldSample(i, 0.5)
		r2 := shouldSample(i, 0.5)
		assert.Equal(t, r1, r2, "shouldSample should be deterministic for logTime=%d", i)
	}
}

func TestShouldSampleEdgeCases(t *testing.T) {
	// Rate 1.0 always samples
	for i := uint64(0); i < 10; i++ {
		assert.True(t, shouldSample(i, 1.0))
	}
	// Rate 0.0 never samples
	for i := uint64(0); i < 10; i++ {
		assert.False(t, shouldSample(i, 0.0))
	}
}
