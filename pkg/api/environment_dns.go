package api

// EnvironmentDNSWrite replaces one authored DNS target. An omitted scope keeps
// an existing record's captured namespace; creation defaults to Environment.
type EnvironmentDNSWrite struct {
	Label   string  `json:"label" minLength:"1" maxLength:"63"`
	Scope   *string `json:"scope,omitempty" enum:"environment,project,tenant"`
	Address string  `json:"address,omitempty"`
	Service string  `json:"service,omitempty"`
	Zone    string  `json:"zone,omitempty"`
}

type EnvironmentDNSRecord struct {
	ID              string `json:"id"`
	Label           string `json:"label"`
	Hostname        string `json:"hostname"`
	Scope           string `json:"scope" enum:"environment,project,tenant"`
	Address         string `json:"address,omitempty"`
	Service         string `json:"service,omitempty"`
	Zone            string `json:"zone,omitempty"`
	BoundServiceID  string `json:"bound_service_id,omitempty"`
	ResolvedAddress string `json:"resolved_address,omitempty"`
	State           string `json:"state" enum:"pending,applied,waiting_for_service,failed,resolver_disabled"`
	TaskID          string `json:"task_id,omitempty"`
}

type EnvironmentDNSWithdrawal struct {
	RecordID string `json:"record_id"`
	Hostname string `json:"hostname"`
}

type EnvironmentDNS struct {
	EnvironmentID   string                     `json:"environment_id"`
	Revision        string                     `json:"revision"`
	ResolverEnabled bool                       `json:"resolver_enabled"`
	State           string                     `json:"state" enum:"pending,applied,failed,resolver_disabled"`
	TaskID          string                     `json:"task_id,omitempty"`
	Records         []EnvironmentDNSRecord     `json:"records"`
	Withdrawals     []EnvironmentDNSWithdrawal `json:"withdrawals"`
}

// Saving changes intention only; EnvironmentDNS reports resolver activation.
type EnvironmentDNSSaved struct {
	EnvironmentID string `json:"environment_id"`
	RecordID      string `json:"record_id"`
}
