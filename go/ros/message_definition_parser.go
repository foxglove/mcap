package ros

import (
	"fmt"
	"strings"
)

var rostypes = map[string]bool{
	"int8":     true,
	"uint8":    true,
	"int16":    true,
	"uint16":   true,
	"int32":    true,
	"uint32":   true,
	"int64":    true,
	"uint64":   true,
	"float32":  true,
	"float64":  true,
	"string":   true,
	"time":     true,
	"duration": true,
}

type Field struct {
	Name string
	Type string
}

func parsePrimitive(s string) (*Field, error) {
	parts := strings.Split(s, " ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid primitive: %s", s)
	}
	return &Field{
		Name: parts[1],
		Type: parts[0],
	}, nil
}

func splitLines(text string, pred func(line string) bool) []string {
	output := []string{}
	chunk := strings.Builder{}
	for _, line := range strings.Split(text, "\n") {
		if pred(line) {
			output = append(output, chunk.String())
			chunk.Reset()
		} else {
			chunk.WriteString(line + "\n")
		}
	}
	output = append(output, chunk.String())
	return output
}

func parse(knowntypes map[string]string, top string) ([]*Field, error) {
	lines := strings.FieldsFunc(top, func(c rune) bool { return c == '\n' })
	fields := []*Field{}
	for _, line := range lines {
		parts := strings.Split(line, " ")
		if len(parts) < 2 {
			return nil, fmt.Errorf("malformed field: %s", line)
		}
		rostype := parts[0]
		fieldname := parts[1]
		if _, ok := rostypes[rostype]; ok {
			fields = append(fields, &Field{
				Name: fieldname,
				Type: rostype,
			})
		} else if definition, ok := knowntypes[rostype]; ok {
			subfields, err := parse(knowntypes, definition)
			if err != nil {
				return nil, fmt.Errorf("failed to parse subfields '%s': %w", definition, err)
			}
			fields = append(fields, subfields...)
		} else if strings.Contains(rostype, "]") {
			// assume array
			fields = append(fields, &Field{
				Name: fieldname,
				Type: rostype,
			})
		} else {
			return nil, fmt.Errorf("unresolved definition: %s", line)
		}
	}
	return fields, nil
}

// ParseMessageDefinition parses a ROS message definition into a flat list of
// the fields that form it. The definition is expected to look something like
// this (in general, it must be a ROS message def):
//
// Bar barfield
// ============
// MSG: Foo
// int16 myint
// ============
// MSG: Bar
// string mystring
//
// Parsing works by first splitting the input on lines that begin with =. Except
// for the first (top-level) definition, these begin with a line MSG:
// <fieldname>. These definitions are collected into a map and mutually resolved.
func ParseMessageDefinition(messageDefinition string) ([]*Field, error) {
	definitions := splitLines(messageDefinition, func(def string) bool {
		return strings.HasPrefix(def, "=")
	})
	if len(definitions) < 1 {
		return nil, fmt.Errorf("invalid ROS message definition")
	}
	top := definitions[0]
	knowntypes := make(map[string]string)
	for _, def := range definitions[1:] {
		lines := strings.Split(def, "\n")
		if len(lines) < 2 {
			return nil, fmt.Errorf("malformed definition: %s", def)
		}
		header := lines[0]
		body := strings.Join(lines[1:], "\n")
		if !strings.HasPrefix(header, "MSG: ") {
			return nil, fmt.Errorf("malformed definition: %s", def)
		}
		rostype := strings.TrimPrefix(header, "MSG: ")
		knowntypes[rostype] = strings.TrimSpace(body)

	}
	return parse(knowntypes, top)
}
