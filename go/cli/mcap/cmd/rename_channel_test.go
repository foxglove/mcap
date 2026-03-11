package cmd

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestMCAPForRename(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	writer, err := mcap.NewWriter(f, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "s1",
		Encoding: "ros1msg",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       2,
		Name:     "s2",
		Encoding: "ros1msg",
		Data:     []byte{},
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 1,
		Topic:    "/foo",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       2,
		SchemaID: 2,
		Topic:    "/bar",
	}))
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   1,
		Sequence:    1,
		LogTime:     1,
		PublishTime: 1,
		Data:        []byte{1, 2, 3},
	}))
	require.NoError(t, writer.WriteMessage(&mcap.Message{
		ChannelID:   2,
		Sequence:    2,
		LogTime:     2,
		PublishTime: 2,
		Data:        []byte{4, 5, 6},
	}))
	require.NoError(t, writer.Close())
}

func readTopicsFromMCAP(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	lexer, err := mcap.NewLexer(f, &mcap.LexerOptions{})
	require.NoError(t, err)

	topics := []string{}
	for {
		tokenType, token, err := lexer.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if tokenType == mcap.TokenChannel {
			channel, err := mcap.ParseChannel(token)
			require.NoError(t, err)
			topics = append(topics, channel.Topic)
		}
		if tokenType == mcap.TokenDataEnd {
			break
		}
	}
	return topics
}

func TestRewriteChannelTopics(t *testing.T) {
	t.Run("renames matching topic", func(t *testing.T) {
		input := &bytes.Buffer{}
		writer, err := mcap.NewWriter(input, &mcap.WriterOptions{Chunked: true})
		require.NoError(t, err)
		require.NoError(t, writer.WriteHeader(&mcap.Header{}))
		require.NoError(t, writer.WriteSchema(&mcap.Schema{
			ID: 1, Name: "s1", Encoding: "ros1msg", Data: []byte{},
		}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 1, SchemaID: 1, Topic: "/foo",
		}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 2, SchemaID: 1, Topic: "/bar",
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 1, Sequence: 1, LogTime: 1, PublishTime: 1, Data: []byte{1},
		}))
		require.NoError(t, writer.Close())

		output := &bytes.Buffer{}
		renamed, err := rewriteChannelTopics(output, input, "/foo", "/foo_renamed")
		require.NoError(t, err)
		assert.Equal(t, 1, renamed)

		// Verify output has the renamed topic
		lexer, err := mcap.NewLexer(bytes.NewReader(output.Bytes()), &mcap.LexerOptions{})
		require.NoError(t, err)
		var topics []string
		for {
			tokenType, token, err := lexer.Next(nil)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			if tokenType == mcap.TokenChannel {
				ch, err := mcap.ParseChannel(token)
				require.NoError(t, err)
				topics = append(topics, ch.Topic)
			}
			if tokenType == mcap.TokenDataEnd {
				break
			}
		}
		assert.Equal(t, []string{"/foo_renamed", "/bar"}, topics)
	})

	t.Run("errors when from and to are identical", func(t *testing.T) {
		output := &bytes.Buffer{}
		_, err := rewriteChannelTopics(output, &bytes.Buffer{}, "/foo", "/foo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "identical")
	})

	t.Run("errors on topic collision when source is before target", func(t *testing.T) {
		// Regression: source channel (/foo) appears before target channel (/bar).
		// The collision must still be detected.
		input := &bytes.Buffer{}
		writer, err := mcap.NewWriter(input, &mcap.WriterOptions{Chunked: true})
		require.NoError(t, err)
		require.NoError(t, writer.WriteHeader(&mcap.Header{}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 1, SchemaID: 0, Topic: "/foo",
		}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 2, SchemaID: 0, Topic: "/bar",
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 1, Sequence: 1, LogTime: 1, PublishTime: 1, Data: []byte{1},
		}))
		require.NoError(t, writer.Close())

		output := &bytes.Buffer{}
		_, err = rewriteChannelTopics(output, input, "/foo", "/bar")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("errors on topic collision when target is before source", func(t *testing.T) {
		input := &bytes.Buffer{}
		writer, err := mcap.NewWriter(input, &mcap.WriterOptions{Chunked: true})
		require.NoError(t, err)
		require.NoError(t, writer.WriteHeader(&mcap.Header{}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 1, SchemaID: 0, Topic: "/bar",
		}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 2, SchemaID: 0, Topic: "/foo",
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 2, Sequence: 1, LogTime: 1, PublishTime: 1, Data: []byte{1},
		}))
		require.NoError(t, writer.Close())

		output := &bytes.Buffer{}
		_, err = rewriteChannelTopics(output, input, "/foo", "/bar")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("errors on topic collision with no messages", func(t *testing.T) {
		input := &bytes.Buffer{}
		writer, err := mcap.NewWriter(input, &mcap.WriterOptions{Chunked: true})
		require.NoError(t, err)
		require.NoError(t, writer.WriteHeader(&mcap.Header{}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 1, SchemaID: 0, Topic: "/foo",
		}))
		require.NoError(t, writer.WriteChannel(&mcap.Channel{
			ID: 2, SchemaID: 0, Topic: "/bar",
		}))
		require.NoError(t, writer.Close())

		output := &bytes.Buffer{}
		_, err = rewriteChannelTopics(output, input, "/foo", "/bar")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})
}

func TestRenameChannelInFile_OutputFile(t *testing.T) {
	tmp := t.TempDir()
	inputPath := filepath.Join(tmp, "input.mcap")
	outputPath := filepath.Join(tmp, "output.mcap")
	writeTestMCAPForRename(t, inputPath)

	err := renameChannelInFile(inputPath, outputPath, "/foo", "/foo_renamed")
	require.NoError(t, err)
	assert.Equal(t, []string{"/foo", "/bar"}, readTopicsFromMCAP(t, inputPath))
	assert.Equal(t, []string{"/foo_renamed", "/bar"}, readTopicsFromMCAP(t, outputPath))
}

func TestRenameChannelInFile_InPlace(t *testing.T) {
	tmp := t.TempDir()
	inputPath := filepath.Join(tmp, "input.mcap")
	writeTestMCAPForRename(t, inputPath)

	err := renameChannelInFile(inputPath, "", "/foo", "/foo_renamed")
	require.NoError(t, err)
	assert.Equal(t, []string{"/foo_renamed", "/bar"}, readTopicsFromMCAP(t, inputPath))
}

func TestRenameChannelInFile_MissingTopic(t *testing.T) {
	tmp := t.TempDir()
	inputPath := filepath.Join(tmp, "input.mcap")
	outputPath := filepath.Join(tmp, "output.mcap")
	writeTestMCAPForRename(t, inputPath)

	err := renameChannelInFile(inputPath, outputPath, "/missing", "/newname")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not found")
	_, statErr := os.Stat(outputPath)
	assert.True(t, os.IsNotExist(statErr))
}

func TestRenameChannelInFile_TopicCollision(t *testing.T) {
	tmp := t.TempDir()
	inputPath := filepath.Join(tmp, "input.mcap")
	outputPath := filepath.Join(tmp, "output.mcap")
	writeTestMCAPForRename(t, inputPath)

	err := renameChannelInFile(inputPath, outputPath, "/foo", "/bar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
	_, statErr := os.Stat(outputPath)
	assert.True(t, os.IsNotExist(statErr))
}
