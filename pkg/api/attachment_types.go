package api

type AttachFact struct {
	Key    string `json:"key"`
	Secret bool   `json:"secret"`
}

type AttachFactSet struct {
	GrantAttachID string       `json:"grant_attach_id,omitempty"`
	Facts         []AttachFact `json:"facts"`
}

type AttachFactValue struct {
	Value string `json:"value"`
}

type AttachCredentialMode string

const (
	AttachCredentialNew      AttachCredentialMode = "new"
	AttachCredentialExisting AttachCredentialMode = "existing"
)

// AttachCredential is a closed credential-source choice. AttachID is present
// only for existing credentials and always names the direct credential owner.
type AttachCredential struct {
	Mode     AttachCredentialMode `json:"mode" enum:"new,existing"`
	AttachID string               `json:"attach_id,omitempty" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
}

// AttachStatus values mirror internal/core's lifecycle exactly:
// pending -> provisioning -> ready, with terminal failed and
// detaching -> detached paths.
type Attach struct {
	ID                   string           `json:"id"`
	Name                 string           `json:"name"`
	ServiceID            string           `json:"service_id"`
	Credential           AttachCredential `json:"credential"`
	BackingProjectID     string           `json:"backing_project_id"`
	BackingServiceID     string           `json:"backing_service_id"`
	BackingEnvironmentID string           `json:"backing_environment_id,omitempty"`
	BackingNetworkID     string           `json:"backing_network_id"`
	GrantAttachIDs       []string         `json:"grant_attach_ids,omitempty"`
	FactSets             []AttachFactSet  `json:"fact_sets"`
	Status               string           `json:"status"`
}

type AttachRequest struct {
	ServiceID        string           `json:"service_id"`
	BackingServiceID string           `json:"backing_service_id"`
	Name             string           `json:"name,omitempty"` // omitted => Controller-suggested, made unique within the environment
	Credential       AttachCredential `json:"credential"`
	GrantAttachIDs   []string         `json:"grant_attach_ids,omitempty"`
}

type AttachRenameRequest struct {
	Name string `json:"name"`
}
