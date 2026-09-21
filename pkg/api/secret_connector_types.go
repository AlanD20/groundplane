package api

// MaximumSecretValueBytes is the decoded UTF-8 input ceiling for one reusable
// Secret. The durable ciphertext ceiling remains 256 KiB; this leaves a full
// KiB for the locked single-recipient age envelope.
const MaximumSecretValueBytes = 255 << 10

type Secret struct {
	ID        string `json:"id"`
	Scope     string `json:"scope"` // "project" | "platform"
	ProjectID string `json:"project_id,omitempty"`
	Key       string `json:"key"`
	Kind      string `json:"kind"` // "env_var" | "file"
	Ref       string `json:"ref"`
	UpdatedAt string `json:"updated_at"`
}

// SecretCreateRequest keeps the value write-only. ProjectID and Platform are
// mutually exclusive; Path is required only for file Secrets.
type SecretCreateRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Platform  bool   `json:"platform,omitempty"`
	Key       string `json:"key"`
	Kind      string `json:"kind"`
	Path      string `json:"path,omitempty"`
	Value     string `json:"value"`
}

type SecretValue struct {
	Value string `json:"value"`
}

type ConnectorCredentialKind string

const (
	ConnectorCredentialSecretRef ConnectorCredentialKind = "secret_ref"
	ConnectorCredentialDirect    ConnectorCredentialKind = "direct"
)

// ConnectorCredential is the redacted response projection. Direct values
// are represented only by their source kind and are never returned.
type ConnectorCredential struct {
	Kind      ConnectorCredentialKind `json:"kind"`
	SecretRef string                  `json:"secret_ref,omitempty"`
}

// ConnectorCredentialInput is accepted only on connector creation. Exactly
// one of SecretRef or Value is supplied and direct Value is write-only.
type ConnectorCredentialInput struct {
	SecretRef string `json:"secret_ref,omitempty"`
	Value     string `json:"value,omitempty"`
}

// Connector is owned by exactly one environment and never falls back to a
// project or platform connector.
type Connector struct {
	ID            string                         `json:"id"`
	EnvironmentID string                         `json:"environment_id"`
	Name          string                         `json:"name"`
	Kind          string                         `json:"kind"`
	Endpoint      string                         `json:"endpoint"`
	Bucket        string                         `json:"bucket"`
	Prefix        string                         `json:"prefix,omitempty"`
	Region        string                         `json:"region"`
	PathStyle     bool                           `json:"path_style"`
	Credentials   map[string]ConnectorCredential `json:"credentials"`
}

type ConnectorCreateRequest struct {
	Name        string                              `json:"name"`
	Kind        string                              `json:"kind"`
	Endpoint    string                              `json:"endpoint"`
	Bucket      string                              `json:"bucket"`
	Prefix      string                              `json:"prefix,omitempty"`
	Region      string                              `json:"region"`
	PathStyle   *bool                               `json:"path_style" nullable:"false"`
	Credentials map[string]ConnectorCredentialInput `json:"credentials"`
}
