package api

// BackingServiceCreate is the complete operator input for one managed backing
// service. Workload image, command, volume, and bootstrap details come only
// from the compiled adapter catalog.
type BackingServiceCreate struct {
	Slug           string                   `json:"slug"`
	Name           string                   `json:"name"`
	Description    string                   `json:"description,omitempty"`
	Adapter        string                   `json:"adapter"`
	Authentication string                   `json:"authentication,omitempty" enum:"username_password,password,none" doc:"Immutable Valkey authentication mode; omitted selects username_password. Unsupported for other adapters."`
	NetworkPool    string                   `json:"network_pool"`
	Zone           BackingServiceZoneCreate `json:"zone"`
}

type BackingServiceZoneCreate struct {
	Name     string `json:"name"`
	Subnet   string `json:"subnet"`
	Internal bool   `json:"internal"`
}

type BackingServiceCreated struct {
	BackingService BackingService `json:"backing_service"`
	TaskID         string         `json:"task_id"`
}
