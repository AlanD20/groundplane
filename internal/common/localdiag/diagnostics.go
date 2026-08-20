// Package localdiag defines the dependency-light results shared by the
// application wiring and the local CLI diagnostic commands.
package localdiag

// ControllerKey is the safe projection of the Controller's local age key.
// The identity and recipient are intentionally absent.
type ControllerKey struct {
	Path        string `json:"path" yaml:"path"`
	Fingerprint string `json:"fingerprint" yaml:"fingerprint"`
}

// EtcdEndpoint is one configured etcd endpoint's independently observed
// health. A failed endpoint does not suppress successful peers.
type EtcdEndpoint struct {
	Endpoint    string `json:"endpoint" yaml:"endpoint"`
	Healthy     bool   `json:"healthy" yaml:"healthy"`
	Version     string `json:"version,omitempty" yaml:"version,omitempty"`
	MemberID    string `json:"member_id,omitempty" yaml:"member_id,omitempty"`
	LeaderID    string `json:"leader_id,omitempty" yaml:"leader_id,omitempty"`
	Revision    string `json:"revision,omitempty" yaml:"revision,omitempty"`
	DBSizeBytes *int64 `json:"db_size_bytes,omitempty" yaml:"db_size_bytes,omitempty"`
	LatencyMS   *int64 `json:"latency_ms,omitempty" yaml:"latency_ms,omitempty"`
	Error       string `json:"error,omitempty" yaml:"error,omitempty"`
}
