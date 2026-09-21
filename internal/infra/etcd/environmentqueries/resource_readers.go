package environmentqueries

// ServiceReader joins desired Services with runtime state and sealed inputs.
type ServiceReader struct {
	store projectionStore
}

func NewServiceReader(store projectionStore) *ServiceReader {
	return &ServiceReader{store: store}
}

// ZoneReader reads Zones from the selected immutable Environment projection.
type ZoneReader struct {
	store snapshotReader
}

func NewZoneReader(store snapshotReader) *ZoneReader {
	return &ZoneReader{store: store}
}
