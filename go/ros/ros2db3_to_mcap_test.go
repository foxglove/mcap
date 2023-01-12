package ros

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/foxglove/mcap/go/mcap/readopts"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestDB3MCAPConversion(t *testing.T) {
	cases := []struct {
		assertion            string
		inputFile            string
		searchDir            string
		expectedTopic        string
		expectedSchemaName   string
		expectedMessageCount int
	}{
		{
			"galactic bag",
			"../../testdata/db3/chatter.db3",
			"./testdata/galactic",
			"/chatter",
			"std_msgs/msg/String",
			7,
		},
		{
			"eloquent bag",
			"../../testdata/db3/eloquent-twist.db3",
			"./testdata/eloquent",
			"/turtle1/cmd_vel",
			"geometry_msgs/msg/Twist",
			4,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			buf := &bytes.Buffer{}
			db, err := sql.Open("sqlite3", c.inputFile)
			assert.Nil(t, err)
			opts := &mcap.WriterOptions{
				IncludeCRC:  true,
				Chunked:     true,
				ChunkSize:   1024,
				Compression: "lz4",
			}

			err = DB3ToMCAP(buf, db, opts, []string{c.searchDir})
			assert.Nil(t, err)

			reader, err := mcap.NewReader(bytes.NewReader(buf.Bytes()))
			assert.Nil(t, err)

			info, err := reader.Info()
			assert.Nil(t, err)
			assert.Equal(t, uint64(c.expectedMessageCount), info.Statistics.MessageCount)
			assert.Equal(t, 1, len(info.Channels))
			assert.Equal(t, c.expectedTopic, info.Channels[1].Topic)
			messageCount := 0
			it, err := reader.Messages(readopts.WithTopics([]string{c.expectedTopic}))
			assert.Nil(t, err)
			for {
				schema, channel, message, err := it.Next(nil)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					t.Errorf("failed to pull message from serialized file: %s", err)
				}
				assert.NotEmpty(t, message.Data)
				assert.Equal(t, channel.Topic, c.expectedTopic)
				assert.Equal(t, schema.Name, c.expectedSchemaName)
				messageCount++
			}
			assert.Equal(t, c.expectedMessageCount, messageCount)
		})
	}
}

func TestMergesNonNewlineDelimitedSchemas(t *testing.T) {
	schemas, err := getSchemas(
		"msg", []string{"./testdata/galactic"},
		[]string{"package_a/msg/NoNewline"})
	assert.Nil(t, err)
	schema := schemas["package_a/msg/NoNewline"]
	expected := `
string data
package_b/NoNewline SpaceMe
package_b/TypeB FancyType
================================================================================
MSG: package_b/NoNewline
string data
================================================================================
MSG: package_b/TypeB
int32 foo
`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(string(schema)))
}

func TestSchemaComposition(t *testing.T) {
	t.Run("schema dependencies are resolved", func(t *testing.T) {
		schemas, err := getSchemas("msg", []string{"./testdata/galactic"}, []string{"package_a/msg/TypeA"})
		assert.Nil(t, err)

		schema := schemas["package_a/msg/TypeA"]
		expectedSchema := `
string data
package_b/TypeB FancyType
================================================================================
MSG: package_b/TypeB
int32 foo
`
		assert.Equal(t, strings.TrimSpace(expectedSchema), strings.TrimSpace(string(schema)))
	})
}

func TestMessageTopicRegex(t *testing.T) {
	cases := []struct {
		assertion string
		input     string
		match     bool
	}{
		{
			"message topic",
			"turtlesim/msg/Pose",
			true,
		},
		{
			"message topic",
			"action_msgs/msg/GoalStatusArray",
			true,
		},
		{
			"action topic",
			"action_msgs/action/GoalStatusArray",
			false,
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			assert.Equal(t, c.match, messageTopicRegex.MatchString(c.input))
		})
	}
}

func TestSchemaFinding(t *testing.T) {
	content, err := getSchema("msg", "jupiter_msgs/msg/Descriptor", []string{"./testdata/get_schema_workspace"})
	assert.Nil(t, err)
	assert.Equal(t, "# is a descriptor\n", string(content))
}
