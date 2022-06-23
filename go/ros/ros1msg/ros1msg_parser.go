package ros1msg

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/foxglove/mcap/go/ros"
)

// Field names are restricted to "an alphabetical character followed by any mixture of alphanumeric and underscores",
// per http://wiki.ros.org/msg#Fields
var fieldMatcher = regexp.MustCompile(`([^ ]+) +([a-zA-Z][a-zA-Z0-9_]+)`)

type Type struct {
	BaseType  string
	IsArray   bool
	FixedSize int
	IsRecord  bool
	Items     *Type
	Fields    []Field
}

type Field struct {
	Name string
	Type Type
}

func resolveDependentFields(
	parentPackage string,
	dependencies map[string]string,
	subdefinition string,
) ([]Field, error) {
	fields := []Field{}
	for i, line := range strings.Split(subdefinition, "\n") {
		line := strings.TrimSpace(line)
		// empty line
		if line == "" {
			continue
		}
		// comment
		if strings.HasPrefix(line, "#") {
			continue
		}
		// constant
		if strings.Contains(strings.Split(line, "#")[0], "=") {
			continue
		}

		// must be a field
		matches := fieldMatcher.FindStringSubmatch(line)
		if len(matches) < 3 {
			return nil, fmt.Errorf("malformed field on line %d: %s", i, line)
		}
		fieldType := matches[1]
		fieldName := matches[2]

		var isRecord bool
		var recordFields []Field
		var arrayItems *Type
		var err error
		inputType := fieldType

		// check if this is an array
		isArray, baseType, fixedSize := parseArrayType(fieldType)
		if isArray {
			fieldType = baseType
		}

		if _, ok := ros.Primitives[fieldType]; !ok {
			// There are three ways the field type can relate to the type
			// names listed in dependencies.
			// 1. They can match exactly, either as qualified (including package) or unqualified types.
			// 2. The type can be unqualified in the fieldType and qualified in
			// the dependency. In this situation we need to qualify the field
			// type with its parent package.
			// 3. The type may be "Header". This is a special case that needs to
			// translate to std_msgs/Header.
			typeIsQualified := strings.Contains(fieldType, "/")
			if typeIsQualified {
				parentPackage = strings.Split(fieldType, "/")[0]
			}
			subdefinition, typeIsPresent := dependencies[fieldType]
			switch {
			case typeIsPresent:
				break
			case fieldType == "Header":
				subdefinition, ok = dependencies["std_msgs/Header"]
				if !ok {
					return nil, fmt.Errorf("dependency Header not found")
				}
			case !typeIsPresent && !typeIsQualified:
				qualifiedType := parentPackage + "/" + fieldType
				subdefinition, ok = dependencies[qualifiedType]
				if !ok {
					return nil, fmt.Errorf("dependency %s not found", qualifiedType)
				}
			}
			recordFields, err = resolveDependentFields(
				parentPackage,
				dependencies,
				subdefinition,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve dependent record: %w", err)
			}
			isRecord = true
		}

		// if isArray, then the "record fields" above are for the array items.
		// Otherwise we are dealing with a record and they are for the record
		// itself.
		if isArray {
			arrayItems = &Type{
				BaseType:  fieldType,
				IsArray:   false,
				FixedSize: 0,
				IsRecord:  isRecord,
				Items:     nil, // nested arrays not allowed
				Fields:    recordFields,
			}
			fields = append(fields, Field{
				Name: fieldName,
				Type: Type{
					BaseType:  inputType,
					IsArray:   true,
					FixedSize: fixedSize,
					IsRecord:  false,
					Items:     arrayItems,
				},
			})
		} else {
			fields = append(fields, Field{
				Name: fieldName,
				Type: Type{
					BaseType:  inputType,
					IsArray:   isArray,
					FixedSize: fixedSize,
					IsRecord:  isRecord,
					Items:     arrayItems,
					Fields:    recordFields,
				},
			})
		}
	}
	return fields, nil
}

func ParseMessageDefinition(parentPackage string, data []byte) ([]Field, error) {
	// split the definition on lines starting with =, and load each section
	// after the first (the top-level definition) into a map. Then, mutually
	// resolve the definitions.
	definitions := splitLines(string(data), func(line string) bool {
		return strings.HasPrefix(strings.TrimSpace(line), "=")
	})
	definition := definitions[0]
	subdefinitions := definitions[1:]
	dependencies := make(map[string]string)
	for _, subdefinition := range subdefinitions {
		lines := strings.Split(subdefinition, "\n")
		header := strings.TrimSpace(lines[0])
		rosType := strings.TrimPrefix(header, "MSG: ")
		dependencies[rosType] = strings.Join(lines[1:], "\n")
	}
	fields, err := resolveDependentFields(parentPackage, dependencies, definition)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependent records: %w", err)
	}
	return fields, nil
}

func splitLines(s string, predicate func(string) bool) []string {
	chunks := []string{}
	chunk := &strings.Builder{}
	for _, line := range strings.Split(s, "\n") {
		if predicate(line) {
			chunks = append(chunks, chunk.String())
			chunk.Reset()
			continue
		}
		chunk.WriteString(line + "\n")
	}
	if chunk.Len() > 0 {
		chunks = append(chunks, chunk.String())
	}
	return chunks
}

func parseArrayType(s string) (isArray bool, baseType string, fixedSize int) {
	if !strings.Contains(s, "[") || !strings.Contains(s, "]") {
		return false, "", 0
	}
	leftBracketIndex := strings.Index(s, "[")
	rightBracketIndex := strings.Index(s, "]")
	baseType = s[:leftBracketIndex]
	size := s[leftBracketIndex+1 : rightBracketIndex]
	if size == "" {
		return true, baseType, 0
	}
	fixedSize, err := strconv.Atoi(size)
	if err != nil {
		return false, "", 0
	}
	return true, baseType, fixedSize
}
