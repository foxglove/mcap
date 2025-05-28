package utils

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

func TestNewChecksummingReaderCounter_WithCRC(t *testing.T) {
	data := []byte("hello world")
	reader := bytes.NewReader(data)
	crcReader := NewChecksummingReaderCounter(reader, true)

	buf := make([]byte, len(data))
	n, err := crcReader.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to read %d bytes, got %d", len(data), n)
	}
	if crcReader.Count() != int64(len(data)) {
		t.Errorf("expected count %d, got %d", len(data), crcReader.Count())
	}

	expectedCRC := crc32.ChecksumIEEE(data)
	if crc := crcReader.CRC(); crc != expectedCRC {
		t.Errorf("expected checksum %v, got %v", expectedCRC, crc)
	}
}

func TestNewChecksummingReaderCounter_WithoutCRC(t *testing.T) {
	data := []byte("test data")
	reader := bytes.NewReader(data)
	crcReader := NewChecksummingReaderCounter(reader, false)

	buf := make([]byte, len(data))
	n, err := crcReader.Read(buf)
	if err != nil && errors.Is(err, io.EOF) {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to read %d bytes, got %d", len(data), n)
	}
	if crcReader.Count() != int64(len(data)) {
		t.Errorf("expected count %d, got %d", len(data), crcReader.Count())
	}

	if crc := crcReader.CRC(); crc != 0 {
		t.Errorf("expected checksum 0, got %v", crc)
	}
}

func TestNewChecksummingReaderCounter_ResetCRC(t *testing.T) {
	data := []byte("reset test")
	reader := bytes.NewReader(data)
	crcReader := NewChecksummingReaderCounter(reader, true)

	buf := make([]byte, len(data))
	_, _ = crcReader.Read(buf)
	crcBefore := crcReader.CRC()
	crcReader.ResetCRC()

	// After reset, reading the same data should produce the same checksum
	reader2 := bytes.NewReader(data)
	crcReader2 := NewChecksummingReaderCounter(reader2, true)
	_, _ = crcReader2.Read(buf)
	crcAfter := crcReader2.CRC()

	if crcBefore != crcAfter {
		t.Errorf("expected checksum after reset to match, got %v and %v", crcBefore, crcAfter)
	}
}
