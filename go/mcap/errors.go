package mcap

import (
	"errors"
	"fmt"
	"io"
)

var ErrBadOffset = errors.New("invalid offset")
var ErrMetadataNotFound = errors.New("metadata not found")

// ErrNestedChunk indicates the lexer has detected a nested chunk.
var ErrNestedChunk = errors.New("detected nested chunk")
var ErrChunkTooLarge = errors.New("chunk exceeds configured maximum size")
var ErrRecordTooLarge = errors.New("record exceeds configured maximum size")
var ErrInvalidZeroOpcode = errors.New("invalid zero opcode")

// ErrUnknownSchema is returned when a schema ID is not known to the writer.
var ErrUnknownSchema = errors.New("unknown schema")

// ErrAttachmentDataSizeIncorrect is returned when the length of a written
// attachment does not match the length supplied.
var ErrAttachmentDataSizeIncorrect = errors.New("attachment content length incorrect")

var ErrLengthOutOfRange = errors.New("length out of int32 range")

// ErrUnexpectedToken indicated when an unexpected token was found in an MCAP file.
type ErrUnexpectedToken struct {
	err error
}

func NewErrUnexpectedToken(err error) error {
	return &ErrUnexpectedToken{err}
}

func (e *ErrUnexpectedToken) Error() string {
	return e.err.Error()
}

func (e *ErrUnexpectedToken) Is(target error) bool {
	var err *ErrUnexpectedToken
	if errors.As(target, &err) {
		return true
	}
	return errors.Is(e.err, target)
}

// ErrBadMagic indicates invalid magic bytes were detected.
type ErrBadMagic struct {
	location magicLocation
	actual   []byte
}

func (e *ErrBadMagic) Error() string {
	return fmt.Sprintf("Invalid magic at %s of file, found: %v", e.location, e.actual)
}

func (e *ErrBadMagic) Is(err error) bool {
	_, ok := err.(*ErrBadMagic)
	return ok
}

// ErrTruncatedRecord indicates not enough data was available to parse a certain record.
type ErrTruncatedRecord struct {
	opcode      OpCode
	actualLen   int
	expectedLen uint64
}

func (e *ErrTruncatedRecord) Error() string {
	if e.expectedLen == 0 {
		return fmt.Sprintf(
			"MCAP truncated in record length field after %s opcode (%x), received %d bytes",
			e.opcode.String(),
			byte(e.opcode),
			e.actualLen,
		)
	}
	return fmt.Sprintf(
		"MCAP truncated in %s (0x%x) record content with expected length %d, data ended after %d bytes",
		e.opcode.String(),
		byte(e.opcode),
		e.expectedLen,
		e.actualLen,
	)
}

func (e *ErrTruncatedRecord) Unwrap() error {
	return io.ErrUnexpectedEOF
}
