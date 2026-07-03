package indexstore

import "time"

// ObjectRowChanged reports whether an incoming LIST row changes the persisted
// current-state fields that define object identity for delta reporting.
func ObjectRowChanged(existing *ObjectRow, candidate ObjectRow) bool {
	if existing == nil {
		return true
	}
	if existing.SizeBytes != candidate.SizeBytes || existing.ETag != candidate.ETag {
		return true
	}
	if !stringPtrEqual(existing.StorageClass, candidate.StorageClass) {
		return true
	}
	return !timePtrEqual(existing.LastModified, candidate.LastModified)
}

func stringPtrEqual(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func timePtrEqual(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}
