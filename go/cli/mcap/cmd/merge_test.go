package cmd

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

type prepInputOptions struct {
	writerOptions *mcap.WriterOptions
	attachment    *mcap.Attachment
}

func withWriterOptions(writerOpts *mcap.WriterOptions) func(*prepInputOptions) {
	return func(inputOpts *prepInputOptions) {
		if writerOpts != nil {
			inputOpts.writerOptions = writerOpts
		}
	}
}

func withAttachment(attachment *mcap.Attachment) func(*prepInputOptions) {
	return func(inputOpts *prepInputOptions) {
		if attachment != nil {
			inputOpts.attachment = attachment
		}
	}
}

func prepInput(t *testing.T, w io.Writer, schema *mcap.Schema, channel *mcap.Channel, opts ...func(*prepInputOptions)) {
	options := prepInputOptions{
		writerOptions: &mcap.WriterOptions{
			Chunked: true,
		},
	}
	for _, opt := range opts {
		opt(&options)
	}

	writer, err := mcap.NewWriter(w, options.writerOptions)
	assert.Nil(t, err)

	assert.Nil(t, writer.WriteHeader(&mcap.Header{Profile: "testprofile"}))
	if schema.ID != 0 {
		assert.Nil(t, writer.WriteSchema(schema))
	}
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:              channel.ID,
		SchemaID:        schema.ID,
		Topic:           channel.Topic,
		MessageEncoding: channel.MessageEncoding,
		Metadata:        channel.Metadata,
	}))
	for i := 0; i < 100; i++ {
		assert.Nil(t, writer.WriteMessage(&mcap.Message{
			ChannelID: channel.ID,
			LogTime:   uint64(i),
		}))
	}

	assert.Nil(t, writer.WriteMetadata(&mcap.Metadata{
		Name: "a",
		Metadata: map[string]string{
			"b":     "c",
			"topic": channel.Topic,
		},
	}))

	if options.attachment != nil {
		err = writer.WriteAttachment(options.attachment)
		assert.Nil(t, err)
	}

	assert.Nil(t, writer.Close())
}

func TestMCAPMerging(t *testing.T) {
	cases := []struct {
		assertion        string
		opts             mergeOpts
		expectedError    error
		expectedMetadata int
	}{
		{
			"allow duplicates",
			mergeOpts{
				allowDuplicateMetadata: true,
			},
			nil,
			3,
		},
		{
			"disallow duplicates",
			mergeOpts{
				allowDuplicateMetadata: false,
			},
			&ErrDuplicateMetadataName{Name: "a"},
			0,
		},
	}

	for _, c := range cases {
		for _, chunked := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s chunked %v", c.assertion, chunked), func(t *testing.T) {
				buf1 := &bytes.Buffer{}
				buf2 := &bytes.Buffer{}
				buf3 := &bytes.Buffer{}
				prepInput(t, buf1, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/foo"})
				prepInput(t, buf2, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/bar"})
				prepInput(t, buf3, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/baz"},
					withAttachment(&mcap.Attachment{
						LogTime:    1,
						CreateTime: 2,
						Name:       "mock.bytes",
						MediaType:  "application/octet-stream",
						DataSize:   3,
						Data:       bytes.NewBuffer([]byte{1, 2, 3}),
					}))

				c.opts.chunked = chunked
				c.opts.coalesceChannels = "none"
				merger := newMCAPMerger(c.opts)
				output := &bytes.Buffer{}
				inputs := []namedReader{
					{"buf1", bytes.NewReader(buf1.Bytes())},
					{"buf2", bytes.NewReader(buf2.Bytes())},
					{"buf3", bytes.NewReader(buf3.Bytes())},
				}
				assert.ErrorIs(t, merger.mergeInputs(output, inputs), c.expectedError)
				if c.expectedError != nil {
					return
				}

				// output should now be a well-formed mcap
				reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
				assert.Nil(t, err)
				assert.Equal(t, reader.Header().Profile, "testprofile")
				it, err := reader.Messages(mcap.UsingIndex(false))
				assert.Nil(t, err)

				messages := make(map[string]int)
				err = mcap.Range(it, func(_ *mcap.Schema, channel *mcap.Channel, _ *mcap.Message) error {
					messages[channel.Topic]++
					return nil
				})
				assert.Nil(t, err)
				assert.Equal(t, 100, messages["/foo"])
				assert.Equal(t, 100, messages["/bar"])
				assert.Equal(t, 100, messages["/baz"])

				info, err := reader.Info()
				assert.Nil(t, err)
				assert.Equal(t, c.expectedMetadata, len(info.MetadataIndexes))
				for _, idx := range info.MetadataIndexes {
					_, err := reader.GetMetadata(idx.Offset)
					assert.Nil(t, err)
				}
				reader.Close()
			})
		}
	}
}

func TestAttachmentMerging(t *testing.T) {
	cases := []struct {
		assertion  string
		writerOpts *mcap.WriterOptions
	}{
		{
			"merge attachments from indexed inputs",
			nil,
		},
		{
			"merge attachments from unindexed inputs",
			&mcap.WriterOptions{
				Chunked:                  true,
				SkipMessageIndexing:      true,
				SkipStatistics:           true,
				SkipRepeatedSchemas:      true,
				SkipRepeatedChannelInfos: true,
				SkipAttachmentIndex:      true,
				SkipMetadataIndex:        true,
				SkipChunkIndex:           true,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf1 := &bytes.Buffer{}
			buf2 := &bytes.Buffer{}
			prepInput(t, buf1, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/foo"},
				withWriterOptions(c.writerOpts),
				withAttachment(&mcap.Attachment{
					LogTime:    1,
					CreateTime: 2,
					Name:       "mock.bytes",
					MediaType:  "application/octet-stream",
					DataSize:   3,
					Data:       bytes.NewBuffer([]byte{1, 2, 3}),
				}))
			prepInput(t, buf2, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/bar"},
				withWriterOptions(c.writerOpts),
				withAttachment(&mcap.Attachment{
					LogTime:    1,
					CreateTime: 2,
					Name:       "mock.bytes",
					MediaType:  "application/octet-stream",
					DataSize:   3,
					Data:       bytes.NewBuffer([]byte{1, 2, 3}),
				}))

			opts := mergeOpts{coalesceChannels: "none", allowDuplicateMetadata: true}
			merger := newMCAPMerger(opts)
			output := &bytes.Buffer{}
			inputs := []namedReader{
				{"buf1", bytes.NewReader(buf1.Bytes())},
				{"buf2", bytes.NewReader(buf2.Bytes())},
			}

			err := merger.mergeInputs(output, inputs)
			assert.Nil(t, err)

			reader, err := mcap.NewReader(bytes.NewReader(output.Bytes()))
			assert.Nil(t, err)
			defer reader.Close()

			info, err := reader.Info()
			assert.Nil(t, err)

			assert.Equal(t, 2, len(info.AttachmentIndexes))
			for _, attIndex := range info.AttachmentIndexes {
				assert.Equal(t, &mcap.Attachment{
					LogTime:    1,
					CreateTime: 2,
					Name:       "mock.bytes",
					MediaType:  "application/octet-stream",
					DataSize:   3,
				}, &mcap.Attachment{
					LogTime:    attIndex.LogTime,
					CreateTime: attIndex.CreateTime,
					Name:       attIndex.Name,
					MediaType:  attIndex.MediaType,
					DataSize:   attIndex.DataSize,
				})

				attReader, err := reader.GetAttachmentReader(attIndex.Offset)
				assert.Nil(t, err)
				data, err := io.ReadAll(attReader.Data())
				assert.Nil(t, err)
				assert.Equal(t, []byte{1, 2, 3}, data)
			}
		})
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
		chunked:          true,
		coalesceChannels: "none",
	})
	output := &bytes.Buffer{}
	assert.Nil(t, merger.mergeInputs(output, []namedReader{{"buf", bytes.NewReader(buf.Bytes())}}))
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
	prepInput(t, buf1, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/foo"})
	prepInput(t, buf2, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/bar"})
	merger := newMCAPMerger(mergeOpts{
		allowDuplicateMetadata: true,
		coalesceChannels:       "none",
	})
	multiChannelInput := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", bytes.NewReader(buf1.Bytes())},
		{"buf2", bytes.NewReader(buf2.Bytes())},
	}
	assert.Nil(t, merger.mergeInputs(multiChannelInput, inputs))
	buf3 := &bytes.Buffer{}
	prepInput(t, buf3, &mcap.Schema{ID: 2}, &mcap.Channel{ID: 2, Topic: "/baz"})
	output := &bytes.Buffer{}
	inputs2 := []namedReader{
		{"multiChannelInput", bytes.NewReader(multiChannelInput.Bytes())},
		{"buf3", bytes.NewReader(buf3.Bytes())},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs2))
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	defer reader.Close()
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(mcap.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	err = mcap.Range(it, func(_ *mcap.Schema, channel *mcap.Channel, _ *mcap.Message) error {
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
	prepInput(t, buf1, &mcap.Schema{ID: 0}, &mcap.Channel{ID: 1, Topic: "/foo"})
	prepInput(t, buf2, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/bar"})
	merger := newMCAPMerger(mergeOpts{
		allowDuplicateMetadata: true,
		coalesceChannels:       "none",
	})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", bytes.NewReader(buf1.Bytes())},
		{"buf2", bytes.NewReader(buf2.Bytes())},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))

	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(mcap.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	schemaIDs := make(map[uint16]int)
	err = mcap.Range(it, func(_ *mcap.Schema, channel *mcap.Channel, _ *mcap.Message) error {
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

	merger := newMCAPMerger(mergeOpts{coalesceChannels: "none"})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf", bytes.NewReader(buf.Bytes())},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))

	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(mcap.UsingIndex(false))
	assert.Nil(t, err)
	messages := make(map[string]int)
	schemaIDs := make(map[uint16]int)
	err = mcap.Range(it, func(_ *mcap.Schema, channel *mcap.Channel, _ *mcap.Message) error {
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
				prepInput(t, buf, &mcap.Schema{ID: 0}, &mcap.Channel{ID: 1, Topic: "/foo"})
				buf.Bytes()[0] = 0x00
				return buf
			},
			"Invalid magic",
		},
		{
			"bad content",
			func() *bytes.Buffer {
				buf := &bytes.Buffer{}
				prepInput(t, buf, &mcap.Schema{ID: 0}, &mcap.Channel{ID: 1, Topic: "/foo"})
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
					chunked:          chunked,
					coalesceChannels: "none",
				})
				inputs := []namedReader{
					{"filename", bytes.NewReader(buf.Bytes())},
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
	prepInput(t, buf1, &mcap.Schema{ID: 1, Name: "SchemaA"}, &mcap.Channel{ID: 1, Topic: "/foo"})
	prepInput(t, buf2, &mcap.Schema{ID: 1, Name: "SchemaA"}, &mcap.Channel{ID: 1, Topic: "/bar"})
	prepInput(t, buf3, &mcap.Schema{ID: 1, Name: "SchemaB"}, &mcap.Channel{ID: 1, Topic: "/baz"})
	merger := newMCAPMerger(mergeOpts{
		allowDuplicateMetadata: true,
		coalesceChannels:       "none",
	})
	output := &bytes.Buffer{}
	inputs := []namedReader{
		{"buf1", bytes.NewReader(buf1.Bytes())},
		{"buf2", bytes.NewReader(buf2.Bytes())},
		{"buf3", bytes.NewReader(buf3.Bytes())},
	}
	assert.Nil(t, merger.mergeInputs(output, inputs))
	// output should now be a well-formed mcap
	reader, err := mcap.NewReader(output)
	assert.Nil(t, err)
	assert.Equal(t, reader.Header().Profile, "testprofile")
	it, err := reader.Messages(mcap.UsingIndex(false))
	assert.Nil(t, err)
	schemas := make(map[uint16]bool)
	var schemaNames []string
	err = mcap.Range(it, func(schema *mcap.Schema, _ *mcap.Channel, _ *mcap.Message) error {
		_, ok := schemas[schema.ID]
		if !ok {
			schemas[schema.ID] = true
			schemaNames = append(schemaNames, schema.Name)
		}
		return nil
	})
	if err != nil {
		die("failed to iterate through schemas: %s", err)
	}
	assert.Equal(t, 2, len(schemas))
	assert.Equal(t, schemaNames, []string{"SchemaA", "SchemaB"})
}

func TestChannelCoalesceBehavior(t *testing.T) {
	expectedMsgCountByChannel := map[string]map[uint16]int{
		"none":  {1: 100, 2: 100, 3: 100, 4: 100},
		"auto":  {1: 200, 2: 100, 3: 100},
		"force": {1: 300, 2: 100},
	}

	for coalesceChannels, messagesByChannel := range expectedMsgCountByChannel {
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		buf3 := &bytes.Buffer{}
		buf4 := &bytes.Buffer{}
		prepInput(t, buf1, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 1, Topic: "/foo"})
		prepInput(t, buf2, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 2, Topic: "/foo"})
		prepInput(t, buf3, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 3, Topic: "/foo", Metadata: map[string]string{"k": "v"}})
		prepInput(t, buf4, &mcap.Schema{ID: 1}, &mcap.Channel{ID: 4, Topic: "/bar"})
		output := &bytes.Buffer{}
		inputs := []namedReader{
			{"buf1", bytes.NewReader(buf1.Bytes())},
			{"buf2", bytes.NewReader(buf2.Bytes())},
			{"buf3", bytes.NewReader(buf3.Bytes())},
			{"buf4", bytes.NewReader(buf4.Bytes())},
		}
		merger := newMCAPMerger(mergeOpts{coalesceChannels: coalesceChannels, allowDuplicateMetadata: true})
		assert.Nil(t, merger.mergeInputs(output, inputs))
		// output should now be a well-formed mcap
		reader, err := mcap.NewReader(output)
		assert.Nil(t, err)
		assert.Equal(t, reader.Header().Profile, "testprofile")
		it, err := reader.Messages(mcap.UsingIndex(false))
		assert.Nil(t, err)
		messages := make(map[uint16]int)
		err = mcap.Range(it, func(_ *mcap.Schema, channel *mcap.Channel, _ *mcap.Message) error {
			messages[channel.ID]++
			return nil
		})
		assert.Nil(t, err)
		assert.Equal(t, messagesByChannel, messages)
	}
}
