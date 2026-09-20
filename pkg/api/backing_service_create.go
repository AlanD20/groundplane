package api

// BackingServiceCreate is the complete operator input for one backing service.
// Managed adapter workload details come from the compiled catalog; custom
// creation requires only an operator-selected image.
type BackingServiceCreate struct {
	Slug           string                    `json:"slug"`
	Name           string                    `json:"name"`
	Description    string                    `json:"description,omitempty"`
	Adapter        string                    `json:"adapter"`
	Image          string                    `json:"image,omitempty" doc:"Required for the custom adapter and rejected for managed adapters."`
	Authentication string                    `json:"authentication,omitempty" enum:"username_password,password,none" doc:"Required explicit choice for Valkey: username_password, password, or none. No default. Immutable after creation; omitted for other adapters."`
	NetworkPool    string                    `json:"network_pool"`
	Zone           BackingServiceZoneCreate  `json:"zone"`
	Hooks          *BackingHookConfiguration `json:"hooks,omitempty"`
}

type BackingHookConfiguration struct {
	Attach     *BackingHookDefinition      `json:"attach,omitempty"`
	Detach     *BackingHookDefinition      `json:"detach,omitempty"`
	BeforeStop *BackingHookDefinition      `json:"before_stop,omitempty"`
	AfterStart *BackingHookDefinition      `json:"after_start,omitempty"`
	Facts      []BackingHookFactDefinition `json:"facts,omitempty"`
	Inputs     []BackingHookInput          `json:"inputs,omitempty"`
}

type BackingHookDefinition struct {
	Command        []string `json:"command" minItems:"1" maxItems:"64"`
	TimeoutSeconds uint32   `json:"timeout_seconds" minimum:"1" maximum:"900"`
}

type BackingHookFactDefinition struct {
	Key    string `json:"key"`
	Secret bool   `json:"secret"`
}

type BackingHookInput struct {
	Key       string  `json:"key"`
	Value     *string `json:"value,omitempty"`
	SecretRef string  `json:"secret_ref,omitempty"`
	Generate  string  `json:"generate,omitempty" enum:"password"`
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
