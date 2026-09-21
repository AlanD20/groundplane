package keyvalue

func RevisionOf(value *KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}
