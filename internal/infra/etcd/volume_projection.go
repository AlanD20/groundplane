package etcd

// VolumeRecord is a projection-only read model. It has no primary key, codec,
// owner index, or independently mutable persistence authority.
type VolumeRecord struct {
	ID            string
	EnvironmentID string
	Slug          string
	Key           string
}
