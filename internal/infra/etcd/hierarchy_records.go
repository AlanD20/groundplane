package etcd

import "time"

// TenantRecord is the versioned persistence DTO. It is intentionally not a
// core aggregate or a public API DTO.
type TenantRecord struct {
	ID             string `json:"id"`
	Slug           string `json:"slug"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	DeletionTaskID string `json:"deletion_task_id,omitempty"`
}

// ProjectRecord keeps ownership in the flat primary. Backing projects have no
// tenant owner and use the platform owner index.
type ProjectRecord struct {
	ID             string      `json:"id"`
	TenantID       string      `json:"tenant_id,omitempty"`
	Slug           string      `json:"slug"`
	Name           string      `json:"name"`
	Description    string      `json:"description"`
	Kind           ProjectKind `json:"kind"`
	DeletionTaskID string      `json:"deletion_task_id,omitempty"`
}

// EnvironmentRecord persists only the environment record itself. Desired
// resources are separate flat records and never embedded here.
type EnvironmentRecord struct {
	ID                string                       `json:"id"`
	ProjectID         string                       `json:"project_id"`
	Name              string                       `json:"name"`
	NetworkPool       string                       `json:"network_pool"`
	VolumeDir         string                       `json:"volume_dir"`
	ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
	CreateTaskID      string                       `json:"create_task_id"`
	CreatedAt         time.Time                    `json:"created_at"`
	DeletionTaskID    string                       `json:"deletion_task_id,omitempty"`
}
