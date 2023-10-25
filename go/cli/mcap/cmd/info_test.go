package cmd

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestInfo(t *testing.T) {
	cases := []struct {
		assertion string
		inputfile string
		expected  string
	}{
		{
			"OneMessage",
			"../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx-pad-rch-rsh-st-sum.mcap",
			`library:
profile:
messages:  1
duration:  0s
start:     0.000000002
end:       0.000000002
compression:
	: [1/1 chunks] [115.00 B/115.00 B (0.00%)]
channels:
	(1) example  1 msgs (+Inf Hz)   : Example [c]
attachments: 0
metadata: 0`,
		},
		{
			"OneSchemalessMessage",
			"../../../../tests/conformance/data/OneSchemalessMessage/OneSchemalessMessage-ch-chx-mx-pad-rch-st.mcap",
			`library:
profile:
messages:  1
duration:  0s
start:     0.000000002
end:       0.000000002
compression:
	: [1/1 chunks] [70.00 B/70.00 B (0.00%)]
channels:
	(1) example  1 msgs (+Inf Hz)   : <no schema>
attachments: 0
metadata: 0`,
		},
		{
			"OneSchemalessMessage_NoChannels",
			"../../../../tests/conformance/data/OneSchemalessMessage/OneSchemalessMessage.mcap",
			`library:
profile:
channels:
attachments: unknown
metadata: unknown`,
		},
	}
	for _, c := range cases {
		input, err := os.ReadFile(c.inputfile)
		assert.Nil(t, err)
		r := bytes.NewReader(input)
		w := new(bytes.Buffer)

		t.Run(c.assertion, func(t *testing.T) {
			reader, err := mcap.NewReader(r)
			assert.Nil(t, err)
			defer reader.Close()
			info, err := reader.Info()
			assert.Nil(t, err)
			err = printInfo(w, info)
			assert.Nil(t, err)

			// for each line, strip leading/trailing whitespace
			// this prevents test failures from formatting changes
			actualLines := strings.Split(strings.TrimSpace(w.String()), "\n")
			expectedLines := strings.Split(strings.TrimSpace(c.expected), "\n")

			for i, line := range actualLines {
				actualLines[i] = strings.TrimSpace(line)
			}
			for i, line := range expectedLines {
				expectedLines[i] = strings.TrimSpace(line)
			}

			actualStripped := strings.Join(actualLines, "\n")
			expectedStripped := strings.Join(expectedLines, "\n")

			assert.Equal(t, expectedStripped, actualStripped)
		})
	}
}
