package ros1msg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestROS1MSGParser(t *testing.T) {
	cases := []struct {
		assertion         string
		parentPackage     string
		messageDefinition string
		fields            []Field
	}{
		{
			"simple string",
			"",
			"string foo",
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "string",
					},
				},
			},
		},
		{
			"two primitive fields",
			"",
			`string foo
			int32 bar`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "string",
					},
				},
				{
					Name: "bar",
					Type: Type{
						BaseType: "int32",
					},
				},
			},
		},
		{
			"primitive variable-length array",
			"",
			`bool[] foo`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "bool[]",
						IsArray:  true,
						Items: &Type{
							BaseType: "bool",
						},
					},
				},
			},
		},
		{
			"primitive fixed-length array",
			"",
			`bool[2] foo`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType:  "bool[2]",
						IsArray:   true,
						FixedSize: 2,
						Items: &Type{
							BaseType: "bool",
						},
					},
				},
			},
		},
		{
			"dependent type",
			"",
			`Foo foo
			===
			MSG: Foo
			string bar
			`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "Foo",
						IsRecord: true,
						Fields: []Field{
							{
								Name: "bar",
								Type: Type{
									BaseType: "string",
								},
							},
						},
					},
				},
			},
		},
		{
			"2x dependent type",
			"",
			`Foo foo
			===
			MSG: Foo
			Baz bar
			===
			MSG: Baz
			string spam
			`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "Foo",
						IsRecord: true,
						Fields: []Field{
							{
								Name: "bar",
								Type: Type{
									BaseType: "Baz",
									IsRecord: true,
									Fields: []Field{
										{
											Name: "spam",
											Type: Type{
												BaseType: "string",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"uses a header",
			"",
			`Header header
			===
			MSG: std_msgs/Header
			uint32 seq
			time stamp
			string frame_id
			`,
			[]Field{
				{
					Name: "header",
					Type: Type{
						BaseType: "Header",
						IsRecord: true,
						Fields: []Field{
							{
								Name: "seq",
								Type: Type{
									BaseType: "uint32",
								},
							},
							{
								Name: "stamp",
								Type: Type{
									BaseType: "time",
								},
							},
							{
								Name: "frame_id",
								Type: Type{
									BaseType: "string",
								},
							},
						},
					},
				},
			},
		},
		{
			"uses a relative type",
			"my_package",
			`MyType foo
			===
			MSG: my_package/MyType
			string bar
			`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "MyType",
						IsRecord: true,
						Fields: []Field{
							{
								Name: "bar",
								Type: Type{
									BaseType: "string",
								},
							},
						},
					},
				},
			},
		},
		{
			"relative type inherited from subdefinition",
			"",
			`my_package/MyType foo
			===
			MSG: my_package/MyType
			MyOtherType bar
			==
			MSG: my_package/MyOtherType
			string baz`,
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "my_package/MyType",
						IsRecord: true,
						Fields: []Field{
							{
								Name: "bar",
								Type: Type{
									BaseType: "MyOtherType",
									IsRecord: true,
									Fields: []Field{
										{
											Name: "baz",
											Type: Type{
												BaseType: "string",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"uses tabs instead of spaces",
			"",
			"string foo\t# no spaces for me",
			[]Field{
				{
					Name: "foo",
					Type: Type{
						BaseType: "string",
					},
				},
			},
		},
		{
			"very short field name",
			"",
			"string f",
			[]Field{
				{
					Name: "f",
					Type: Type{
						BaseType: "string",
					},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			fields, err := ParseMessageDefinition(c.parentPackage, []byte(c.messageDefinition))
			assert.Nil(t, err)
			assert.Equal(t, c.fields, fields)
		})
	}
}
