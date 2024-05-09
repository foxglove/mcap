package mcap

import "math"

// slicemap implements an arraymap with uint16 keys. this can be used to store and quickly
// look up pointers to Schema and Channel structs by ID.
type slicemap[T any] struct {
	items []*T
}

// gets the item at idx, returning nil if not found.
func (s *slicemap[T]) get(idx uint16) *T {
	if int(idx) >= len(s.items) {
		return nil
	}
	return s.items[idx]
}

func (s *slicemap[T]) set(idx uint16, item *T) {
	if int(idx) >= len(s.items) {
		// extend the len() of s.items up to idx + 1
		toAdd := int(idx) + 1 - len(s.items)
		// let append decide how much to expand the capacity of the slice
		s.items = append(s.items, make([]*T, toAdd)...)
	}
	s.items[idx] = item
}

func (s *slicemap[T]) toMap() map[uint16]*T {
	out := make(map[uint16]*T)
	for idx, item := range s.items {
		if idx > math.MaxUint16 {
			break
		}
		if item == nil {
			continue
		}
		out[uint16(idx)] = item
	}
	return out
}
