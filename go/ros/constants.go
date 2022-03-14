package ros

var Primitives = map[string]bool{
	"bool":     true,
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
	"char":     true,
	"byte":     true,
}

var MessageDefinitionSeparator = []byte(
	"================================================================================\n",
)
