package api

// Script is the durable operator-facing Script projection.
type Script struct {
	ID                string `json:"id"`
	EnvironmentID     string `json:"environment_id"`
	Slug              string `json:"slug"`
	ServiceID         string `json:"service_id"`
	ServiceName       string `json:"service"`
	Body              string `json:"script"`
	When              string `json:"when"`
	Order             uint16 `json:"order" minimum:"0" maximum:"65535"`
	Origin            string `json:"origin" enum:"api,blueprint"`
	ReconciliationKey string `json:"reconciliation_key,omitempty"`
	ActiveGeneration  uint64 `json:"active_generation"`
}

// ScriptCreate is the complete operator-authored durable Script input.
type ScriptCreate struct {
	EnvironmentID string `json:"environment_id"`
	Slug          string `json:"slug"`
	ServiceID     string `json:"service_id"`
	Body          string `json:"script"`
	When          string `json:"when"`
	Order         uint16 `json:"order,omitempty" minimum:"0" maximum:"65535"`
}

// ScriptEdit contains the mutable Script desired-state fields.
type ScriptEdit struct {
	Slug  *string `json:"slug,omitempty"`
	Body  *string `json:"script,omitempty"`
	When  *string `json:"when,omitempty"`
	Order *uint16 `json:"order,omitempty" minimum:"0" maximum:"65535"`
}
