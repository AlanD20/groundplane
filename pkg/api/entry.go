package api

// EntrySource is the discriminated union api-cli.md section 4 shows:
// literal, secret_ref, and fact are mutually exclusive.
type EntrySource struct {
	Kind          string `json:"kind"` // "literal" | "secret_ref" | "fact"
	Literal       string `json:"literal,omitempty"`
	SecretRef     string `json:"secret_ref,omitempty"`
	AttachID      string `json:"attach_id,omitempty"`
	GrantAttachID string `json:"grant_attach_id,omitempty"`
	Fact          string `json:"fact,omitempty"` // e.g. "pg16_URL"
}

type Entry struct {
	ID                string      `json:"id"`
	Type              string      `json:"type"`           // "env" | "file"
	Key               string      `json:"key,omitempty"`  // Type=env
	Path              string      `json:"path,omitempty"` // Type=file
	UID               *int64      `json:"uid,omitempty"`  // Type=file; required even when zero
	GID               *int64      `json:"gid,omitempty"`  // Type=file; required even when zero
	Source            EntrySource `json:"source"`
	Exposure          []string    `json:"exposure"` // service names, or ["all"]
	Secret            bool        `json:"secret"`
	EmptySecretValue  bool        `json:"empty_secret_value,omitempty"` // True only for an empty selected secret generation.
	ReconciliationKey string      `json:"reconciliation_key,omitempty"` // Immutable Blueprint key on metadata reads.
}

const MaximumEntryValueBytes = 256 << 10

type EntryCreateRequest struct {
	EnvironmentID string      `json:"environment_id"`
	Type          string      `json:"type"`
	Key           string      `json:"key,omitempty"`
	Path          string      `json:"path,omitempty"`
	UID           *int64      `json:"uid,omitempty"`
	GID           *int64      `json:"gid,omitempty"`
	Source        EntrySource `json:"source"`
	Exposure      []string    `json:"exposure"`
	Secret        bool        `json:"secret"`
}

type EntryValue struct {
	Value string `json:"value"`
}
