package api

type Volume struct {
	ID            string  `json:"id"`
	EnvironmentID string  `json:"environment_id"`
	Slug          string  `json:"slug"`
	Key           string  `json:"key"`
	Path          string  `json:"path,omitempty"`
	State         string  `json:"state,omitempty"`
	CreateTaskID  *string `json:"create_task_id,omitempty"`
	OriginTaskID  *string `json:"origin_task_id,omitempty"`
	CurrentTaskID *string `json:"current_task_id,omitempty"`
}

// VolumeCreate is the complete operator-authored Volume identity. Key is
// optional at the public boundary; the Controller defaults it to Slug only
// when the slug satisfies the immutable Compose-key grammar.
type VolumeCreate struct {
	EnvironmentID string `json:"environment_id"`
	Slug          string `json:"slug"`
	Key           string `json:"key,omitempty"`
}

// VolumeEdit contains the only mutable Volume field. The immutable key and
// derived path never enter an edit request.
type VolumeEdit struct {
	Slug string `json:"slug"`
}

type VolumeMutationResponse struct {
	Volume Volume `json:"volume"`
	TaskID string `json:"task_id"`
}

type VolumeDeletionImpactItemKind string

const (
	VolumeImpactMount         VolumeDeletionImpactItemKind = "mount"
	VolumeImpactBackupSource  VolumeDeletionImpactItemKind = "backup_source"
	VolumeImpactBackupPolicy  VolumeDeletionImpactItemKind = "backup_policy"
	VolumeImpactRecoveryPoint VolumeDeletionImpactItemKind = "recovery_point"
)

// VolumeDeletionImpactItem is one bounded, stable-id consequence in the
// fixed-revision removal stream. Kind determines which optional identity and
// consequence fields are populated.
type VolumeDeletionImpactItem struct {
	ID                   string                       `json:"id"`
	Kind                 VolumeDeletionImpactItemKind `json:"kind"`
	ServiceID            string                       `json:"service_id,omitempty"`
	ServiceName          string                       `json:"service_name,omitempty"`
	MountTarget          string                       `json:"mount_target,omitempty"`
	ReadOnly             bool                         `json:"read_only,omitempty"`
	SourceID             string                       `json:"source_id,omitempty"`
	RecoveryPointCount   int64                        `json:"recovery_point_count,omitempty"`
	HistoricalDigest     string                       `json:"historical_digest,omitempty"`
	PolicyDisables       bool                         `json:"policy_disables,omitempty"`
	RetainsConfiguration bool                         `json:"retains_configuration,omitempty"`
}

// VolumeDeletionImpactPage carries the common pinned identity and the
// ordered page prefix. Only the final page has Complete=true, no NextCursor,
// and ImpactToken set.
type VolumeDeletionImpactPage struct {
	VolumeID        string                     `json:"volume_id"`
	Slug            string                     `json:"slug"`
	Key             string                     `json:"key"`
	EnvironmentID   string                     `json:"environment_id"`
	Revision        int64                      `json:"revision"`
	EnvironmentHead string                     `json:"environment_head"`
	Items           []VolumeDeletionImpactItem `json:"items"`
	Complete        bool                       `json:"complete"`
	NextCursor      string                     `json:"next_cursor,omitempty"`
	ItemCount       int64                      `json:"item_count"`
	RollingDigest   string                     `json:"rolling_digest"`
	ImpactToken     string                     `json:"impact_token,omitempty"`
	DataHandling    string                     `json:"data_handling"`
}
