package api

// MaximumBackupPolicySources is the public replacement bound. It mirrors the
// persistence transaction budget and is intentionally available to clients.
const (
	MaximumBackupPolicySources       = 12
	MaximumBackupPolicyKeep    int64 = 9_007_199_254_740_991
)

// ValidBackupPolicyKeep reports whether keep is within the public integer
// range that every supported JSON consumer can represent exactly.
func ValidBackupPolicyKeep(keep int64) bool {
	return keep >= 1 && keep <= MaximumBackupPolicyKeep
}

// BackupPolicyReplacementRequest is one complete desired policy document.
type BackupPolicyReplacementRequest struct {
	Enabled     bool                `json:"enabled"`
	Frequency   string              `json:"frequency,omitempty" pattern:"^(\\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9])$"`
	Keep        int64               `json:"keep,omitempty" minimum:"1" maximum:"9007199254740991"`
	Encryption  BackupEncryption    `json:"encryption,omitempty" enum:"age,none"`
	ConnectorID string              `json:"connector_id,omitempty" pattern:"^con_[0-9A-HJKMNP-TV-Z]{26}$"`
	Sources     []BackupSourceInput `json:"sources" maxItems:"12" nullable:"false"`
}

type BackupEncryption string

const (
	BackupEncryptionAge  BackupEncryption = "age"
	BackupEncryptionNone BackupEncryption = "none"
)

// BackupPolicy is the effective Environment policy projection.
type BackupPolicy struct {
	Enabled      bool             `json:"enabled"`
	Frequency    string           `json:"frequency,omitempty" pattern:"^(\\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \\*-\\*-\\* (?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9])$"`
	Keep         int64            `json:"keep,omitempty" minimum:"1" maximum:"9007199254740991"`
	Encryption   BackupEncryption `json:"encryption,omitempty" enum:"age,none"`
	ConnectorID  string           `json:"connector_id,omitempty"`
	Sources      []BackupSource   `json:"sources" nullable:"false"`
	AgeRecipient string           `json:"age_recipient,omitempty"`
	KeyEra       int              `json:"key_era,omitempty"`
	KeyCreatedAt string           `json:"key_created_at,omitempty" format:"date-time"`
	KeyRotatedAt string           `json:"key_rotated_at,omitempty" format:"date-time"`
	NextRunAt    *string          `json:"next_run_at" format:"date-time"`
}

// BackupPolicyMutationResult carries the typed public value and the exact
// protected JSON representation committed for semantic replay.
type BackupPolicyMutationResult struct {
	Policy         BackupPolicy
	Representation []byte
}

type BackupSourceKind string

const (
	BackupSourceAttach BackupSourceKind = "attach"
	BackupSourceVolume BackupSourceKind = "volume"
	BackupSourceConfig BackupSourceKind = "config"
)

type BackupSourceInput struct {
	Kind     BackupSourceKind `json:"kind" enum:"attach,volume,config"`
	TargetID string           `json:"target_id"`
}

type BackupSource struct {
	ID       string           `json:"id"`
	Kind     BackupSourceKind `json:"kind" enum:"attach,volume,config"`
	TargetID string           `json:"target_id"`
}

type RecoveryPointStatus string

const RecoveryPointVerified RecoveryPointStatus = "verified"

type RecoveryPoint struct {
	ID         string              `json:"id"`
	SourceID   string              `json:"source_id"`
	SourceKind BackupSourceKind    `json:"source_kind" enum:"attach,volume,config"`
	TargetID   string              `json:"target_id"`
	CreatedAt  string              `json:"created_at" format:"date-time"`
	SizeBytes  int64               `json:"size_bytes"`
	Encrypted  bool                `json:"encrypted"`
	KeyEra     int                 `json:"key_era,omitempty"`
	Status     RecoveryPointStatus `json:"status" enum:"verified"`
}

type RecoveryPointPage struct {
	Items      []RecoveryPoint `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type RestoreRequest struct {
	SourceID        string `json:"source_id"`
	RecoveryPointID string `json:"recovery_point_id,omitempty"` // empty = latest
	AgeIdentity     string `json:"age_identity,omitempty"`
}
