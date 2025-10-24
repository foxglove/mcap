package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetScheme(t *testing.T) {
	cases := []struct {
		assertion        string
		input            string
		expectedScheme   string
		expectedBucket   string
		expectedFilename string
	}{
		{
			"local file",
			"foo/bar/baz.txt",
			"",
			"",
			"foo/bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo/bar/baz.txt",
			"gs",
			"foo",
			"bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo-bar.com123/bar/baz.txt",
			"gs",
			"foo-bar.com123",
			"bar/baz.txt",
		},
		{
			"remote file",
			"s3://foo-bar/bar/baz.txt",
			"s3",
			"foo-bar",
			"bar/baz.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			scheme, bucket, filename := GetScheme(c.input)
			assert.Equal(t, c.expectedScheme, scheme)
			assert.Equal(t, c.expectedBucket, bucket)
			assert.Equal(t, c.expectedFilename, filename)
		})
	}
}

func TestDefaultString(t *testing.T) {
	cases := []struct {
		assertion string
		args      []string
		output    string
	}{
		{
			"first string",
			[]string{"hello", "goodbye"},
			"hello",
		},
		{
			"second string",
			[]string{"", "hello"},
			"hello",
		},
		{
			"empty",
			[]string{"", ""},
			"",
		},
	}

	for _, c := range cases {
		assert.Equal(t, c.output, DefaultString(c.args...))
	}
}
