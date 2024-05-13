package mcap

import "math"

type slicemap[T any] struct {
	items []*T
}

func (s *slicemap[T]) Set(idx uint16, item *T) {
	if int(idx) >= len(s.items) {
		// extend the len() of s.items up to idx + 1
		toAdd := int(idx) + 1 - len(s.items)
		// let append decide how much to expand the capacity of the slice
		s.items = append(s.items, make([]*T, toAdd)...)
	}
	s.items[idx] = item
}

func (s *slicemap[T]) Get(idx uint16) *T {
	if int(idx) >= len(s.items) {
		return nil
	}
	return s.items[idx]
}

func (s *slicemap[T]) Slice() []*T {
	return s.items
}

func (s *slicemap[T]) ToMap() map[uint16]*T {
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
