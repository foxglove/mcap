package ros

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/foxglove/mcap/go/libmcap"
)

// collectMessageSchemas collects message schemas from the provided list of
// directories, for type names matching those in the list of types.
func collectMessageSchemas(directories []string, types []string) (map[string][]byte, error) {
	if len(directories) == 0 {
		return nil, fmt.Errorf("no directories provided")
	}
	targets := make(map[string]bool)
	for _, t := range types {
		targets[t] = true
	}
	schemas := make(map[string][]byte)
	interfaceDirs := make(map[string]string)
	for _, dir := range directories {
		err := filepath.Walk(dir, func(filepath string, info os.FileInfo, err error) error {
			if info.IsDir() && info.Name() == "rosidl_interfaces" { // cspell:disable-line
				interfaceDirs[dir] = filepath
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// look up each requested type in the interface directories
	for _, t := range types {
		parts := strings.Split(t, "/")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid type name %s", t)
		}
		packageName := parts[0]
		resourceType := parts[1]
		typeName := parts[2]
		for parentPath, dirPath := range interfaceDirs {
			packageFile := path.Join(dirPath, packageName)
			packageData, err := os.ReadFile(packageFile)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			packagePaths := strings.Split(string(packageData), "\n")
			for _, packagePath := range packagePaths {
				targetPath := path.Join(resourceType, typeName+"."+resourceType)
				if packagePath == targetPath {
					schemaPath := path.Join(parentPath, "share", packageName, targetPath)
					schema, err := os.ReadFile(schemaPath)
					if err != nil {
						return nil, err
					}
					schemas[t] = schema
				}
			}
		}
	}

	// ensure all requested schemas were found, or we won't be able to create a valid file
	for _, t := range types {
		if _, ok := schemas[t]; !ok {
			return nil, fmt.Errorf("no schema found for type %s", t)
		}
	}

	return schemas, nil
}

type topicsRecord struct {
	id                  uint16
	name                string
	typ                 string
	serializationFormat string
	offeredQOSProfiles  string
}

func getTopics(db *sql.DB) ([]topicsRecord, error) {
	rows, err := db.Query(
		`select id, name, type, serialization_format, offered_qos_profiles from topics`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	topics := []topicsRecord{}
	for rows.Next() {
		record := topicsRecord{}
		err := rows.Scan(
			&record.id,
			&record.name,
			&record.typ,
			&record.serializationFormat,
			&record.offeredQOSProfiles,
		)
		if err != nil {
			return nil, err
		}
		topics = append(topics, record)
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

func DB3ToMCAP(w io.Writer, db *sql.DB, opts *libmcap.WriterOptions, searchdirs []string) error {
	writer, err := libmcap.NewWriter(w, opts)
	if err != nil {
		return err
	}
	defer writer.Close()
	err = writer.WriteHeader(&libmcap.Header{
		Profile: "ros2",
		Library: "golang-db3-mcap",
	})
	if err != nil {
		return err
	}

	topics, err := getTopics(db)
	if err != nil {
		return err
	}
	types := make([]string, len(topics))
	for i := range topics {
		types[i] = topics[i].typ
	}
	schemas, err := collectMessageSchemas(searchdirs, types)
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
		err = writer.WriteSchema(&libmcap.Schema{
			ID:       schemaID,
			Data:     schema,
			Name:     t.typ,
			Encoding: "msg",
		})
		if err != nil {
			return fmt.Errorf("failed to write schema: %w", err)
		}
		err = writer.WriteChannel(&libmcap.Channel{
			ID:              t.id,
			Topic:           t.name,
			MessageEncoding: t.serializationFormat,
			SchemaID:        schemaID,
			Metadata: map[string]string{
				"offered_qos_profiles": t.offeredQOSProfiles,
			},
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
		err = writer.WriteMessage(&libmcap.Message{
			ChannelID:   topicID,
			Sequence:    seq[topicID],
			LogTime:     uint64(messageTimestamp),
			PublishTime: uint64(messageTimestamp),
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
