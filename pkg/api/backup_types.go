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
	Frequency   string              `json:"frequency,omitempty" maxLength:"256" doc:"Five-field cron expression in UTC: minute hour day-of-month month weekday. No seconds, macros or timezone prefixes."`
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
	Frequency    string           `json:"frequency,omitempty" maxLength:"256" doc:"Five-field UTC cron expression."`
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
	Database          *RecoveryPointDatabase `json:"database,omitempty"`
	Capture           *RecoveryPointCapture  `json:"capture,omitempty"`
	ID                string                 `json:"id"`
	ConnectorID       string                 `json:"connector_id"`
	ConnectorEndpoint string                 `json:"connector_endpoint"`
	ConnectorBucket   string                 `json:"connector_bucket"`
	ConnectorPrefix   string                 `json:"connector_prefix"`
	SourceID          string                 `json:"source_id"`
	SourceKind        BackupSourceKind       `json:"source_kind" enum:"attach,volume,config"`
	TargetID          string                 `json:"target_id"`
	CreatedAt         string                 `json:"created_at" format:"date-time"`
	SizeBytes         int64                  `json:"size_bytes"`
	Encrypted         bool                   `json:"encrypted"`
	KeyEra            int                    `json:"key_era,omitempty"`
	Status            RecoveryPointStatus    `json:"status" enum:"verified"`
}

// RecoveryPointDatabase describes the authenticated archive, not an image tag.
type RecoveryPointDatabase struct {
	Family              string `json:"family" enum:"postgres,mysql"`
	SourceServerVersion string `json:"source_server_version"`
	BackupToolVersion   string `json:"backup_tool_version"`
	ArtifactFormat      string `json:"artifact_format"`
}

// RecoveryPointCapture remains available after its producing Task expires.
type RecoveryPointCapture struct {
	TaskID      string `json:"task_id" pattern:"^task_[0-9A-HJKMNP-TV-Z]{26}$"`
	CreatedAt   string `json:"created_at" format:"date-time"`
	SourceCount int    `json:"source_count" minimum:"1" maximum:"12"`
}

type RecoveryPointPage struct {
	Items      []RecoveryPoint `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type RestoreRequest struct {
	VersionReviewSHA256          string `json:"version_review_sha256,omitempty" pattern:"^[a-f0-9]{64}$"`
	AcknowledgeVersionDifference bool   `json:"acknowledge_version_difference,omitempty"`
	SourceID                     string `json:"source_id" pattern:"^spt_[0-9A-HJKMNP-TV-Z]{26}$"`
	RecoveryPointID              string `json:"recovery_point_id,omitempty" pattern:"^rp_[0-9A-HJKMNP-TV-Z]{26}$"` // empty = latest
	AgeIdentity                  string `json:"age_identity,omitempty" maxLength:"4096" writeOnly:"true"`
}

type RestorePreview struct {
	RecoveryPointID string                 `json:"recovery_point_id"`
	Database        *RestoreDatabaseReview `json:"database,omitempty"`
}

type RestoreDatabaseReview struct {
	Family              string `json:"family" enum:"postgres,mysql"`
	SourceServerVersion string `json:"source_server_version"`
	BackupToolVersion   string `json:"backup_tool_version"`
	TargetServerVersion string `json:"target_server_version"`
	RestoreToolVersion  string `json:"restore_tool_version"`
	ArtifactFormat      string `json:"artifact_format"`
	VersionDifference   bool   `json:"version_difference"`
	Compatibility       string `json:"compatibility" enum:"same-version,unverified"`
	ReviewSHA256        string `json:"review_sha256"`
}
