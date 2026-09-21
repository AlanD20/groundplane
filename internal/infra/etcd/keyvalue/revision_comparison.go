package keyvalue

func RevisionChanged(value *KeyValue, expected int64) bool {
	return (expected == 0 && value != nil) ||
		(expected > 0 && (value == nil || value.ModRevision != expected))
}
