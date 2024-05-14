package mcap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlicemapLength(t *testing.T) {
	var s slicemap[string]
	val := "hello"
	assert.Empty(t, s.Slice())

	// setting the first value expands the slice enough to fit it
	s.Set(0, &val)
	assert.Equal(t, &val, s.Get(0))
	assert.Len(t, s.Slice(), 1)

	// setting another higher expands the slice enough to fit it
	s.Set(5, &val)
	assert.Equal(t, &val, s.Get(5))
	assert.Len(t, s.Slice(), 6)

	// setting a value <= len does not expand the slice
	s.Set(1, &val)
	assert.Equal(t, &val, s.Get(1))
	assert.Len(t, s.Slice(), 6)

	// getting a value > len does not expand the slice
	var nilString *string
	assert.Equal(t, nilString, s.Get(10))
	assert.Len(t, s.Slice(), 6)
}
