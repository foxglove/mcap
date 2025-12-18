package cmd

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrintDescriptor(t *testing.T) {
	descriptorBytes, err := os.ReadFile("../testdata/nested.pb.bin")
	require.NoError(t, err)
	descriptor, err := parseDescriptor(descriptorBytes)
	require.NoError(t, err)
	buf := bytes.NewBuffer(nil)
	printDescriptor(buf, descriptor)
	require.Equal(t, `syntax = "proto3";

message cardboard.Cost {
  optional int32 dollars = 1;
  optional int32 cents = 2;
}
message cardboard.Box {
  message cardboard.Aesthetics {
    enum cardboard.Color {
      RED = 0;
      BLUE = 1;
      GREEN = 2;
    }
    enum cardboard.Shape {
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
`, buf.String())
}
