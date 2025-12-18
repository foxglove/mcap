package cmd

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const cardboardProto = `// file: cardboard.proto
syntax = "proto3";
package cardboard;
message Cost {
  optional int32 dollars = 1;
  optional int32 cents = 2;
}
message Box {
  message Aesthetics {
    enum Color {
      RED = 0;
      BLUE = 1;
      GREEN = 2;
    }
    enum Shape {
      SQUARE = 0;
      RECTANGULAR = 1;
      HEXAGONAL = 2;
    }
    optional .cardboard.Box.Aesthetics.Color color = 1;
    optional .cardboard.Box.Aesthetics.Shape shape = 2;
  }
  optional .cardboard.Box.Aesthetics aesthetics = 1;
  optional .cardboard.Cost cost = 2;
}
`

func TestPrintDescriptor(t *testing.T) {
	descriptorBytes, err := os.ReadFile("../testdata/cardboard.pb.bin")
	require.NoError(t, err)
	descriptor, err := parseDescriptor(descriptorBytes)
	require.NoError(t, err)
	buf := bytes.NewBuffer(nil)
	printDescriptor(buf, descriptor)
	require.Equal(t, cardboardProto, buf.String())
}

func TestPrintDescriptorWithDependency(t *testing.T) {
	descriptorBytes, err := os.ReadFile("../testdata/shipping.pb.bin")
	require.NoError(t, err)
	descriptor, err := parseDescriptor(descriptorBytes)
	require.NoError(t, err)
	buf := bytes.NewBuffer(nil)
	printDescriptor(buf, descriptor)
	require.Equal(t, cardboardProto+`--------------------
// file: shipping.proto
syntax = "proto3";
package shipping;
import "cardboard.proto";
message Item {
  optional .cardboard.Box box = 1;
  optional int32 grams = 2;
}
`, buf.String())
}
