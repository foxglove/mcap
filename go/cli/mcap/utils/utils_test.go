package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSchemFromURI(t *testing.T) {
	cases := []struct {
		assertion      string
		input          string
		expectedScheme string
		expectedPath   string
	}{
		{
			"local file",
			"foo/bar/baz.txt",
			"",
			"foo/bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo/bar/baz.txt",
			"gs",
			"foo/bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo-bar.com123/bar/baz.txt",
			"gs",
			"foo-bar.com123/bar/baz.txt",
		},
		{
			"remote file",
			"s3://foo-bar.com/bar/baz.txt",
			"s3",
			"foo-bar.com/bar/baz.txt",
		},
		{
			"remote file",
			"http://foo-bar.com/bar/baz.txt",
			"http",
			"foo-bar.com/bar/baz.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			scheme, path := GetSchemeFromURI(c.input)
			assert.Equal(t, c.expectedScheme, scheme)
			assert.Equal(t, c.expectedPath, path)
		})
	}
}

func TestGetBucketFromPath(t *testing.T) {
	cases := []struct {
		assertion        string
		input            string
		expectedBucket   string
		expectedFilename string
	}{
		{
			"Simple structure",
			"foo/bar.txt",
			"foo",
			"bar.txt",
		},
		{
			"Complex structure",
			"foo.com/bar/baz.txt",
			"foo.com",
			"bar/baz.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			bucket, filename := GetBucketFromPath(c.input)
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
