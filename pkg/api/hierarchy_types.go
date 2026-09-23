package api

// --- Core entity DTOs (mirror internal/core's fields at the API
// surface; see mvp.md's "Desired state" and blueprint.md's extension
// grammar for the full internal shape) ---

type Tenant struct {
	ID             string  `json:"id"`
	Slug           string  `json:"slug"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	DeletionTaskID *string `json:"deletion_task_id"`
}
type TenantCreate struct {
	Slug        string  `json:"slug"`
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type TenantEdit struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type TenantRename struct {
	Slug string `json:"slug"`
}

type TenantPage struct {
	Items      []Tenant `json:"items"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

type Project struct {
	ID             string  `json:"id"`
	TenantID       string  `json:"tenant_id,omitempty"`
	Slug           string  `json:"slug"`
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Kind           string  `json:"kind"` // "tenant" | "backing"
	DeletionTaskID *string `json:"deletion_task_id"`
}
type ProjectCreate struct {
	TenantID    string  `json:"tenant_id"`
	Slug        string  `json:"slug"`
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type ProjectEdit struct {
	Name *string `json:"name,omitempty"`
}

type ProjectRename struct {
	Slug string `json:"slug"`
}

type ProjectPage struct {
	Items      []Project `json:"items"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// BackingService is the public facade over one backing Project, its sole
// main Environment, and its sole adapter-backed Service. ProjectID is the
// facade's stable public identity; the other ids address shared operations.
type BackingService struct {
	Authentication   string `json:"authentication,omitempty"`
	ProjectID        string `json:"project_id"`
	EnvironmentID    string `json:"environment_id"`
	ServiceID        string `json:"service_id"`
	BackingNetworkID string `json:"backing_network_id"`
}

type Environment struct {
	ID                string                       `json:"id"`
	ProjectID         string                       `json:"project_id"`
	Name              string                       `json:"name"`
	NetworkPool       string                       `json:"network_pool"`
	VolumeDir         string                       `json:"volume_dir"`
	ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
	CreateTaskID      *string                      `json:"create_task_id"`
	DeletionTaskID    *string                      `json:"deletion_task_id"`
	NetworkCapacity   EnvironmentNetworkCapacity   `json:"network_capacity"`
}
type EnvironmentCreate struct {
	ProjectID   string `json:"project_id"`
	Name        string `json:"name"`
	NetworkPool string `json:"network_pool"`
}
type EnvironmentEdit struct {
	NetworkPool string `json:"network_pool"`
}

const EnvironmentBlueprintInitialRevision = "0"

type EnvironmentBlueprintDocument struct {
	EnvironmentID string `json:"environment_id"`
	Revision      string `json:"revision"`
	Document      string `json:"document"`
}

type BlueprintChangeAction string

const (
	BlueprintChangeCreate BlueprintChangeAction = "create"
	BlueprintChangeUpdate BlueprintChangeAction = "update"
	BlueprintChangeRetain BlueprintChangeAction = "retain"
	BlueprintChangeRemove BlueprintChangeAction = "remove"
)

type EnvironmentBlueprintChange struct {
	Resource         string                `json:"resource"`
	Key              string                `json:"key"`
	Action           BlueprintChangeAction `json:"action" enum:"create,update,retain,remove"`
	EmptySecretValue bool                  `json:"empty_secret_value,omitempty"`
}

type EnvironmentBlueprintValidation struct {
	Revision string                       `json:"revision"`
	Changes  []EnvironmentBlueprintChange `json:"changes"`
}

type EnvironmentRename struct {
	Name string `json:"name"`
}

type EnvironmentPage struct {
	Items      []Environment `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type EnvironmentProvisioningState string

const (
	EnvironmentProvisioning EnvironmentProvisioningState = "provisioning"
	EnvironmentReady        EnvironmentProvisioningState = "ready"
	EnvironmentFailed       EnvironmentProvisioningState = "failed"
)
