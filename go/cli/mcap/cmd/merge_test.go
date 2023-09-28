package cmd

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/foxglove/mcap/go/mcap/readopts"
	"github.com/stretchr/testify/assert"
)

func prepInput(t *testing.T, w io.Writer, schema *mcap.Schema, channelID uint16, topic string) {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)

	assert.Nil(t, writer.WriteHeader(&mcap.Header{Profile: "testprofile"}))
	if schema.ID != 0 {
		assert.Nil(t, writer.WriteSchema(schema))
	}
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       channelID,
		SchemaID: schema.ID,
		Topic:    topic,
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: channelID,
			LogTime:   uint64(i),
		}))
	}
	assert.Nil(t, writer.Close())
}

func TestMCAPMerging(t *testing.T) {
	for _, chunked := range []bool{true, false} {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		buf3 := &bytes.Buffer{}
		prepInput(t, buf1, &mcap.Schema{ID:1}, 1, "/foo")
		prepInput(t, buf2, &mcap.Schema{ID:1}, 1, "/bar")
		prepInput(t, buf3, &mcap.Schema{ID:1}, 1, "/baz")
		merger := newMCAPMerger(mergeOpts{
			chunked: chunked,
		})
		output := &bytes.Buffer{}
		inputs := []namedReader{
			{"buf1", buf1},
			{"buf2", buf2},
			{"buf3", buf3},
		}
		assert.Nil(t, merger.mergeInputs(output, inputs))

		// output should now be a well-formed mcap
		reader, err := mcap.NewReader(output)
		assert.Nil(t, err)
		assert.Equal(t, reader.Header().Profile, "testprofile")
		it, err := reader.Messages(readopts.UsingIndex(false))
		assert.Nil(t, err)

		messages := make(map[string]int)
		err = mcap.Range(it, func(schema *mcap.Schema, channel *mcap.Channel, message *mcap.Message) error {
			messages[channel.Topic]++
			return nil
		})
		assert.Nil(t, err)
		assert.Equal(t, 100, messages["/foo"])
		assert.Equal(t, 100, messages["/bar"])
		assert.Equal(t, 100, messages["/baz"])
		reader.Close()
	}
}

func TestChannelsWithSameSchema(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{Profile: "testprofile"}))

	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:   1,
		Name: "foo",
	}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID:   2,
		Name: "bar",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 2,
		Topic:    "/bar1",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       2,
		SchemaID: 2,
		Topic:    "/bar2",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       3,
		SchemaID: 1,
		Topic:    "/foo",
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 1,
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 2,
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 3,
	}))
	assert.Nil(t, writer.Close())
	merger := newMCAPMerger(mergeOpts{
		chunked: true,
	})
	output := &bytes.Buffer{}
	assert.Nil(t, merger.mergeInputs(output, []namedReader{{"buf", buf}}))
	reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
	assert.Nil(t, err)
	info, err := reader.Info()
	assert.Nil(t, err)

	assert.NotNil(t, info.Schemas)
	assert.Equal(t, 2, len(info.Schemas))
	assert.Equal(t, info.Schemas[1].Name, "bar")
	assert.Equal(t, info.Schemas[2].Name, "foo")
}

func TestMultiChannelInput(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	prepInput(t, buf1, &mcap.Schema{ID:1}, 1, "/foo")
	prepInput(t, buf2, &mcap.Schema{ID:1}, 1, "/bar")
	merger := newMCAPMerger(mergeOpts{})
	multiChannelInput := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", buf1},
		{"buf2", buf2},
	}
	assert.Nil(t, merger.mergeInputs(multiChannelInput, inputs))
	buf3 := &bytes.Buffer{}
	prepInput(t, buf3, &mcap.Schema{ID:2}, 2, "/baz")
	output := &bytes.Buffer{}
	inputs2 := []namedReader{
		{"multiChannelInput", multiChannelInput},
		{"buf3", buf3},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs2))
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(readopts.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	err = mcap.Range(it, func(schema *mcap.Schema, channel *mcap.Channel, message *mcap.Message) error {
		messages[channel.Topic]++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, messages["/foo"])
	assert.Equal(t, 100, messages["/bar"])
	assert.Equal(t, 100, messages["/baz"])
}
func TestSchemalessChannelInput(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	prepInput(t, buf1, &mcap.Schema{ID:0}, 1, "/foo")
	prepInput(t, buf2, &mcap.Schema{ID:1}, 1, "/bar")
	merger := newMCAPMerger(mergeOpts{})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", buf1},
		{"buf2", buf2},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))

	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(readopts.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	schemaIDs := make(map[uint16]int)
	err = mcap.Range(it, func(schema *mcap.Schema, channel *mcap.Channel, message *mcap.Message) error {
		messages[channel.Topic]++
		schemaIDs[channel.SchemaID]++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 100, messages["/foo"])
	assert.Equal(t, 100, messages["/bar"])
	assert.Equal(t, 100, schemaIDs[0])
	assert.Equal(t, 100, schemaIDs[1])
}

func TestMultipleSchemalessChannelSingleInput(t *testing.T) {
	buf := &bytes.Buffer{}
	writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{Profile: "testprofile"}))

	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 0,
		Topic:    "/foo",
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       2,
		SchemaID: 0,
		Topic:    "/bar",
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 1,
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 2,
	}))
	assert.Nil(t, writer.Close())

	merger := newMCAPMerger(mergeOpts{})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf", buf},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))

	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(readopts.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	schemaIDs := make(map[uint16]int)
	err = mcap.Range(it, func(schema *mcap.Schema, channel *mcap.Channel, message *mcap.Message) error {
		messages[channel.Topic]++
		schemaIDs[channel.SchemaID]++
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, messages["/foo"])
	assert.Equal(t, 1, messages["/bar"])
	assert.Equal(t, 2, schemaIDs[0])
}

func TestBadInputGivesNamedErrors(t *testing.T) {
	cases := []struct {
		assertion   string
		input       func() *bytes.Buffer
		errContains string
	}{
		{
			"bad magic",
			func() *bytes.Buffer {
				buf := &bytes.Buffer{}
				prepInput(t, buf, &mcap.Schema{ID:0}, 1, "/foo")
				buf.Bytes()[0] = 0x00
				return buf
			},
			"Invalid magic",
		},
		{
			"bad content",
			func() *bytes.Buffer {
				buf := &bytes.Buffer{}
				prepInput(t, buf, &mcap.Schema{ID:0}, 1, "/foo")
				for i := 3000; i < 4000; i++ {
					buf.Bytes()[i] = 0x00
				}
				return buf
			},
			"invalid zero opcode",
		},
	}
	for _, c := range cases {
		for _, chunked := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s chunked %v", c.assertion, chunked), func(t *testing.T) {
				buf := c.input()
				merger := newMCAPMerger(mergeOpts{
					chunked: chunked,
				})
				inputs := []namedReader{
					{"filename", buf},
				}
				output := &bytes.Buffer{}
				err := merger.mergeInputs(output, inputs)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, "filename")
				assert.ErrorContains(t, err, c.errContains)
			})
		}
	}
}

func TestSameSchemasNotDuplicated(t *testing.T) {
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	buf3 := &bytes.Buffer{}
	prepInput(t, buf1, &mcap.Schema{ID:1, Name: "SchemaA"}, 1, "/foo")
	prepInput(t, buf2, &mcap.Schema{ID:1, Name: "SchemaA"}, 1, "/bar")
	prepInput(t, buf3, &mcap.Schema{ID:1, Name: "SchemaB"}, 1, "/baz")
	merger := newMCAPMerger(mergeOpts{})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", buf1},
		{"buf2", buf2},
		{"buf3", buf3},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))
	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(readopts.UsingIndex(false))
	assert.Nil(t, err)
	schemas := make(map[uint16]bool)
	var schemaNames []string
	err = mcap.Range(it, func(schema *mcap.Schema, channel *mcap.Channel, message *mcap.Message) error {
		_, ok := schemas[schema.ID];
		if !ok {
			schemas[schema.ID] = true
			schemaNames = append(schemaNames, schema.Name)
		}
		return nil
	})
	assert.Equal(t, 2, len(schemas))
	assert.Equal(t, schemaNames, []string{"SchemaA", "SchemaB"})
}
