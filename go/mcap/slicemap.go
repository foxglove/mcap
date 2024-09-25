package mcap

import "math"

// slicemap is an arraymap implementation with uint16 keys. This is useful for associating a set of
// Schema or Channel records with their IDs.
type slicemap[T any] struct {
	items []*T
}

func (s *slicemap[T]) Set(key uint16, val *T) {
	if int(key) >= len(s.items) {
		// extend the len() of s.items up to key + 1
		toAdd := int(key) + 1 - len(s.items)
		// let append decide how much to expand the capacity of the slice
		s.items = append(s.items, make([]*T, toAdd)...)
	}
	s.items[key] = val
}

func (s *slicemap[T]) Get(key uint16) *T {
	if int(key) >= len(s.items) {
		return nil
	}
	return s.items[key]
}

func (s *slicemap[T]) Slice() []*T {
	return s.items
}

func (s *slicemap[T]) ToMap() map[uint16]*T {
	out := make(map[uint16]*T)
	for key, val := range s.items {
		if key > math.MaxUint16 {
			break
		}
		if val == nil {
			continue
		}
		out[uint16(key)] = val
	}
	return out
}
