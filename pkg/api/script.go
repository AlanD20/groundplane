package api

// Script is the durable operator-facing Script projection.
type Script struct {
	ID                string          `json:"id"`
	EnvironmentID     string          `json:"environment_id"`
	Slug              string          `json:"slug"`
	ServiceID         string          `json:"service_id"`
	ServiceName       string          `json:"service"`
	Body              string          `json:"script"`
	When              string          `json:"when"`
	Order             uint16          `json:"order" minimum:"0" maximum:"65535"`
	Execution         ScriptExecution `json:"execution"`
	Origin            string          `json:"origin" enum:"api,blueprint"`
	ReconciliationKey string          `json:"reconciliation_key,omitempty"`
	ActiveGeneration  uint64          `json:"active_generation"`
}

// ScriptCreate is the complete operator-authored durable Script input.
type ScriptCreate struct {
	EnvironmentID string           `json:"environment_id"`
	Slug          string           `json:"slug"`
	ServiceID     string           `json:"service_id"`
	Body          string           `json:"script"`
	When          string           `json:"when"`
	Order         uint16           `json:"order,omitempty" minimum:"0" maximum:"65535"`
	Execution     *ScriptExecution `json:"execution,omitempty"`
}

// ScriptEdit contains the mutable Script desired-state fields.
type ScriptEdit struct {
	Slug      *string          `json:"slug,omitempty"`
	Body      *string          `json:"script,omitempty"`
	When      *string          `json:"when,omitempty"`
	Order     *uint16          `json:"order,omitempty" minimum:"0" maximum:"65535"`
	Execution *ScriptExecution `json:"execution,omitempty"`
}

// ScriptExecution is a complete execution choice; grants never merge or inherit.
type ScriptExecution struct {
	Mode     string              `json:"mode" enum:"inherited,explicit"`
	Image    string              `json:"image,omitempty"`
	User     string              `json:"user,omitempty"`
	Volumes  []ScriptVolumeGrant `json:"volumes,omitempty" maxItems:"32"`
	EntryIDs []string            `json:"entry_ids,omitempty" maxItems:"64"`
}

// ScriptVolumeGrant binds one managed Volume with an explicit access decision.
type ScriptVolumeGrant struct {
	VolumeID string `json:"volume_id"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}
