package slicemap

import "math"

// gets the item at idx, returning nil if not found.
func GetAt[T any](items []*T, idx uint16) *T {
	if int(idx) >= len(items) {
		return nil
	}
	return items[idx]
}

// inserts item into items at idx, extending items to fit if neccessary.
func SetAt[T any](items []*T, idx uint16, item *T) []*T {
	if int(idx) >= len(items) {
		// extend the len() of s.items up to idx + 1
		toAdd := int(idx) + 1 - len(items)
		// let append decide how much to expand the capacity of the slice
		items = append(items, make([]*T, toAdd)...)
	}
	items[idx] = item
	return items
}

func ToMap[T any](items []*T) map[uint16]*T {
	out := make(map[uint16]*T)
	for idx, item := range items {
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
