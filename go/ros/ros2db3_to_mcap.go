package ros

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
)

var (
	messageTopicRegex = regexp.MustCompile(`\w+/msg/.*`)
)

func getSchema(encoding string, rosType string, directories []string) ([]byte, error) {
	parts := strings.FieldsFunc(rosType, func(c rune) bool { return c == '/' })
	if len(parts) < 3 {
		return nil, fmt.Errorf("expected type %s to match <package>/msg/<type>", rosType)
	}
	baseType := parts[2]
	rosPkg := parts[0]

	for _, dir := range directories {
		var content []byte
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if content != nil {
				return nil
			}
			mode := info.Mode()
			if !mode.IsRegular() && (mode&fs.ModeSymlink == 0) {
				// This is not a file or a symlink, skip it.
				return nil
			}
			parts := strings.Split(path, string(os.PathSeparator))
			expectedParts := []string{rosPkg, "msg", baseType + "." + encoding}
			if len(parts) < len(expectedParts) {
				return nil
			}
			for i, expectedPart := range expectedParts {
				if parts[len(parts)-len(expectedParts)+i] != expectedPart {
					return nil
				}
			}
			content, err = os.ReadFile(path)
			return err
		})
		if err != nil {
			return nil, err
		}
		if content != nil {
			return content, nil
		}
	}
	return nil, errors.New("schema not found")
}

func getSchemas(encoding string, directories []string, types []string) (map[string][]byte, error) {
	messageDefinitions := make(map[string][]byte)
	for _, rosType := range types {
		rosPackage := strings.Split(rosType, "/")[0]
		messageDefinition := &bytes.Buffer{}
		schema, err := getSchema(encoding, rosType, directories)
		if err != nil {
			return nil, fmt.Errorf("failed to find schema for %s: %w", rosType, err)
		}
		subdefinitions := []struct {
			parentPackage string
			rosType       string
			schema        []byte
		}{
			{parentPackage: rosPackage, rosType: rosType, schema: schema},
		}
		first := true
		for len(subdefinitions) > 0 {
			subdefinition := subdefinitions[0]
			if !first {
				// if the previous write did not end with a newline, add one now
				if messageDefinition.Bytes()[messageDefinition.Len()-1] != '\n' {
					err := messageDefinition.WriteByte('\n')
					if err != nil {
						return nil, fmt.Errorf("failed to write newline")
					}
				}
				_, err := messageDefinition.Write(MessageDefinitionSeparator)
				if err != nil {
					return nil, fmt.Errorf("failed to write separator: %w", err)
				}
				_, err = fmt.Fprintf(messageDefinition, "MSG: %s\n", strings.Replace(subdefinition.rosType, "/msg/", "/", 1))
				if err != nil {
					return nil, fmt.Errorf("failed to write MSG header to message definition: %w", err)
				}
			}
			_, err = messageDefinition.Write(subdefinition.schema)
			if err != nil {
				return nil, fmt.Errorf("failed to write subdefinition: %w", err)
			}
			first = false
			subdefinitions = subdefinitions[1:]

			lines := strings.FieldsFunc(string(subdefinition.schema), func(c rune) bool { return c == '\n' })
			for _, line := range lines {
				line = strings.TrimSpace(line)

				// skip empty lines
				if line == "" {
					continue
				}

				// skip comments
				if strings.HasPrefix(line, "#") {
					continue
				}

				// must be a field
				parts := strings.FieldsFunc(line, func(c rune) bool { return c == ' ' })
				if len(parts) < 1 {
					return nil, fmt.Errorf("malformed field: %s. Message definition: %s", line, string(subdefinition.schema))
				}
				fieldType := parts[0]

				// it may be an array, but we just care about the base type
				baseType := fieldType
				arrayParts := strings.FieldsFunc(fieldType, func(c rune) bool { return c == '[' })
				if len(arrayParts) > 1 {
					baseType = arrayParts[0]
				}

				// if it's a primitive, no action required
				if Primitives[baseType] {
					continue
				}

				parentPackage := subdefinition.parentPackage
				if parts := strings.Split(subdefinition.rosType, "/"); len(parts) > 1 {
					parentPackage = parts[0]
				}

				// if it's not a primitive, we need to look it up
				qualifiedType := fieldToQualifiedROSType(baseType, parentPackage)
				fieldSchema, err := getSchema(encoding, qualifiedType, directories)
				if err != nil {
					return nil, fmt.Errorf("failed to find schema for %s: %w", baseType, err)
				}
				subdefinitions = append(subdefinitions, struct {
					parentPackage string
					rosType       string
					schema        []byte
				}{
					parentPackage: parentPackage,
					rosType:       qualifiedType,
					schema:        fieldSchema,
				})
			}
		}
		messageDefinitions[rosType] = messageDefinition.Bytes()
	}
	return messageDefinitions, nil
}

func fieldToQualifiedROSType(fieldType, rosPackage string) string {
	parts := strings.FieldsFunc(fieldType, func(c rune) bool { return c == '/' })
	if len(parts) == 1 {
		return path.Join(rosPackage, "msg", fieldType)
	}
	return path.Join(parts[0], "msg", parts[1])
}

type topicsRecord struct {
	id                  uint16
	name                string
	typ                 string
	serializationFormat string
	offeredQOSProfiles  *string
}

func checkHasQOSProfiles(db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRow(
		`select count(*) from pragma_table_info('topics') where name = 'offered_qos_profiles'`,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func getTopics(db *sql.DB) ([]topicsRecord, error) {
	hasQOSProfiles, err := checkHasQOSProfiles(db)
	if err != nil {
		return nil, err
	}
	var rows *sql.Rows
	if hasQOSProfiles {
		rows, err = db.Query(
			`select id, name, type, serialization_format, offered_qos_profiles from topics`,
		)
	} else {
		rows, err = db.Query(
			`select id, name, type, serialization_format from topics`,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	topics := []topicsRecord{}
	for rows.Next() {
		record := topicsRecord{}
		if hasQOSProfiles {
			err = rows.Scan(
				&record.id,
				&record.name,
				&record.typ,
				&record.serializationFormat,
				&record.offeredQOSProfiles,
			)
		} else {
			err = rows.Scan(
				&record.id,
				&record.name,
				&record.typ,
				&record.serializationFormat,
			)
		}
		if err != nil {
			return nil, err
		}
		if messageTopicRegex.MatchString(record.typ) {
			topics = append(topics, record)
		}
	}
	return topics, nil
}

func transformMessages(db *sql.DB, f func(*sql.Rows) error) error {
	rows, err := db.Query(`
	select
	messages.topic_id,
	messages.timestamp,
	messages.data
	from messages
	inner join topics
	on messages.topic_id = topics.id
	order by messages.timestamp asc
	`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err := f(rows)
		if err != nil {
			return err
		}
	}
	return nil
}

func DB3ToMCAP(w io.Writer, db *sql.DB, opts *mcap.WriterOptions, searchdirs []string) error {
	topics, err := getTopics(db)
	if err != nil {
		return err
	}
	types := make([]string, len(topics))
	for i := range topics {
		types[i] = topics[i].typ
	}
	schemas, err := getSchemas("msg", searchdirs, types)
	if err != nil {
		return err
	}

	writer, err := mcap.NewWriter(w, opts)
	if err != nil {
		return err
	}
	defer writer.Close()
	err = writer.WriteHeader(&mcap.Header{
		Profile: "ros2",
	})
	if err != nil {
		return err
	}
	// for each topic, write a schema and channel info to the output.
	for i, t := range topics {
		schemaID := uint16(i + 1)
		schema, ok := schemas[t.typ]
		if !ok {
			return fmt.Errorf("unrecognized schema for %s", t.typ)
		}
		err = writer.WriteSchema(&mcap.Schema{
			ID:       schemaID,
			Data:     schema,
			Name:     t.typ,
			Encoding: "ros2msg",
		})
		if err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
		metadata := make(map[string]string)
		if t.offeredQOSProfiles != nil {
			metadata["offered_qos_profiles"] = *t.offeredQOSProfiles
		}
		err = writer.WriteChannel(&mcap.Channel{
			ID:              t.id,
			Topic:           t.name,
			MessageEncoding: t.serializationFormat,
			SchemaID:        schemaID,
			Metadata:        metadata,
		})
		if err != nil {
			return fmt.Errorf("failed to write channel info: %w", err)
		}
	}
	seq := make(map[uint16]uint32)
	err = transformMessages(db, func(rows *sql.Rows) error {
		var topicID uint16
		var messageTimestamp int64
		var messageData []byte
		err := rows.Scan(
			&topicID,
			&messageTimestamp,
			&messageData,
		)
		if err != nil {
			return err
		}
		err = writer.WriteMessage(&mcap.Message{
			ChannelID:   topicID,
			Sequence:    seq[topicID],
			LogTime:     uint64(messageTimestamp),
			PublishTime: uint64(messageTimestamp),
			Data:        messageData,
		})
		if err != nil {
			return err
		}
		seq[topicID]++
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
