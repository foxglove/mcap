package ros

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func heredoc(s string) string {
	result := ""
	for i, line := range strings.Split(strings.TrimPrefix(s, "\n"), "\n") {
		if i > 0 {
			result += "\n"
		}
		result += strings.TrimSpace(line)
	}
	return result
}

func TestParseMessageDefinition(t *testing.T) {
	cases := []struct {
		assertion string
		input     string
		output    []*Field
	}{
		{
			"compound input",
			heredoc(`Bar barfield
			=======
			MSG: Foo
			int16 myint
			========
			MSG: Bar
			string mystring
			string[10] mystringarray
			Foo myfoo`),
			[]*Field{
				{
					Name: "mystring",
					Type: "string",
				},
				{
					Name: "mystringarray",
					Type: "string[10]",
				},
				{
					Name: "myint",
					Type: "int16",
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			fields, err := ParseMessageDefinition(c.input)
			assert.Nil(t, err)
			assert.Equal(t, len(c.output), len(fields))
			for i, field := range fields {
				assert.Equal(t, c.output[i].Name, field.Name)
				assert.Equal(t, c.output[i].Type, field.Type)
			}
		})
	}
}

func TestParsePrimitive(t *testing.T) {
	cases := []struct {
		assertion string
		input     string
		output    *Field
	}{
		{
			"primitive type",
			"int8 field",
			&Field{
				Type: "int8",
				Name: "field",
			},
		},
		{
			"array type",
			"int8[] field",
			&Field{
				Type: "int8[]",
				Name: "field",
			},
		},
		{
			"fixed length array",
			"int8[10] field",
			&Field{
				Type: "int8[10]",
				Name: "field",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			field, err := parsePrimitive(c.input)
			assert.Nil(t, err)
			assert.Equal(t, c.output.Name, field.Name)
			assert.Equal(t, c.output.Type, field.Type)
		})
	}
}

func TestMessageDefinitionParser(t *testing.T) {}
