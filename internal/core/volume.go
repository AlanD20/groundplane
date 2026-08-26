package core

// Volume is persistent storage owned by an Environment. Slug is a mutable
// operator label and Key is the immutable Compose key and direct directory
// child. Durable service mounts reference ID rather than either label.
type Volume struct {
	ID   string `yaml:"id"   json:"id"` // vol_<ulid>
	Slug string `yaml:"slug" json:"slug"`
	Key  string `yaml:"key"  json:"key"`
}
