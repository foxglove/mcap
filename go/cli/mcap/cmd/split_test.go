package cmd

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

type testFS struct {
	m map[string]io.ReadWriteCloser
}

func newTFS() testFS {
	return testFS{
		m: make(map[string]io.ReadWriteCloser),
	}
}

type nopReadWriteCloser struct {
	w io.ReadWriter
}

func (wc nopReadWriteCloser) Write(p []byte) (int, error) {
	return wc.w.Write(p)
}

func (wc nopReadWriteCloser) Read(p []byte) (int, error) {
	return wc.w.Read(p)
}

func (wc nopReadWriteCloser) Close() error {
	return nil
}

func (tfs testFS) inMemoryWriteCloserProvider() func(string) (io.WriteCloser, error) {
	return func(fileName string) (io.WriteCloser, error) {
		buf := &bytes.Buffer{}
		wc := nopReadWriteCloser{buf}
		tfs.m[fileName] = wc
		return wc, nil
	}
}

func TestSplitCommand(t *testing.T) {
	input := &bytes.Buffer{}
	writer, err := mcap.NewWriter(input, &mcap.WriterOptions{
		Chunked: true,
	})
	assert.Nil(t, err)
	assert.Nil(t, writer.WriteHeader(&mcap.Header{}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID: 0,
	}))
	assert.Nil(t, writer.WriteSchema(&mcap.Schema{
		ID: 1,
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       0,
		SchemaID: 0,
	}))
	assert.Nil(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 1,
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 0,
	}))
	assert.Nil(t, writer.WriteMessage(&mcap.Message{
		ChannelID: 1,
	}))
	assert.Nil(t, writer.Close())

	tfs := newTFS()
	err = splitMCAP(tfs.inMemoryWriteCloserProvider(), input, &mcap.WriterOptions{})
	assert.Nil(t, err)

	t.Run("each output is a valid mcap", func(t *testing.T) {
		for _, v := range tfs.m {
			lexer, err := mcap.NewLexer(v, &mcap.LexerOptions{})
			assert.Nil(t, err)
			var messages, channels, schemas int
		Top:
			for {
				tokenType, _, err := lexer.Next(nil)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					t.Error(err)
				}
				switch tokenType {
				case mcap.TokenMessage:
					messages++
				case mcap.TokenSchema:
					schemas++
				case mcap.TokenChannel:
					channels++
				case mcap.TokenFooter:
					break Top
				}
			}
			assert.Equal(t, 1, messages, "unexpected message count")
			assert.Equal(t, 2, channels, "unexpected channel count")
			assert.Equal(t, 2, schemas, "unexpected schema count")
		}
	})
}
