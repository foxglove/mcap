package cmd

// This file contains the common types and helpers used by both the merge and the concat commands.

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
)

type ErrDuplicateMetadataName struct {
	Name string
}

func (e ErrDuplicateMetadataName) Is(target error) bool {
	_, ok := target.(*ErrDuplicateMetadataName)
	return ok
}

func (e *ErrDuplicateMetadataName) Error() string {
	return fmt.Sprintf("metadata name '%s' was previously encountered. "+
		"Supply --allow-duplicate-metadata to override.", e.Name)
}

// schemaID uniquely identifies a schema across the inputs.
type schemaID struct {
	inputID  int
	schemaID uint16
}

// channelID uniquely identifies a channel across the inputs.
type channelID struct {
	inputID   int
	channelID uint16
}

type HashSum = [md5.Size]byte

const (
	AutoCoalescing  = "auto"
	ForceCoalescing = "force"
	NoCoalescing    = "none"
)

func hashMetadata(metadata *mcap.Metadata) (string, error) {
	hasher := md5.New()
	hasher.Write([]byte(metadata.Name))
	bytes, err := json.Marshal(metadata.Metadata)
	if err != nil {
		return "", err
	}
	hasher.Write(bytes)
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash), nil
}

func getChannelHash(channel *mcap.Channel, coalesceChannels string) HashSum {
	hasher := md5.New()
	schemaIDBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(schemaIDBytes, channel.SchemaID)
	hasher.Write(schemaIDBytes)
	hasher.Write([]byte(channel.Topic))
	hasher.Write([]byte(channel.MessageEncoding))

	switch coalesceChannels {
	case AutoCoalescing: // Include channel metadata in hash
		for key, value := range channel.Metadata {
			hasher.Write([]byte(key))
			hasher.Write([]byte(value))
		}
	case ForceCoalescing: // Channel metadata is not included in hash
		break
	default:
		die("Invalid value for --coalesce-channels: %s\n", coalesceChannels)
	}

	return HashSum(hasher.Sum(nil))
}

func getSchemaHash(schema *mcap.Schema) HashSum {
	hasher := md5.New()
	hasher.Write([]byte(schema.Name))
	hasher.Write([]byte(schema.Encoding))
	hasher.Write(schema.Data)
	return HashSum(hasher.Sum(nil))
}

func outputProfile(profiles []string) string {
	if len(profiles) == 0 {
		return ""
	}
	firstProfile := profiles[0]
	for _, profile := range profiles {
		if profile != firstProfile {
			return ""
		}
	}
	return firstProfile
}

type namedReader struct {
	name   string
	reader io.Reader
}
