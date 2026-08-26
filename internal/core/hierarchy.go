package core

// ProjectKind distinguishes a tenant project (an application) from a
// backing project (shared infra: datastore/cache/queue). Both follow the
// SAME hierarchy - project -> environment(s) -> service(s) - so the
// Controller runs identical logic for either; only adapter fields differ
// on a backing project's single service.
type ProjectKind string

const (
	ProjectKindTenant  ProjectKind = "tenant"
	ProjectKindBacking ProjectKind = "backing"
)

// Tenant is a strict isolation boundary (mvp.md, "Model").
type Tenant struct {
	ID             string  `yaml:"id"          json:"id"`   // tnt_<ulid>
	Slug           string  `yaml:"slug"        json:"slug"` // globally unique, renamable
	Name           string  `yaml:"name"        json:"name"`
	Description    string  `yaml:"description" json:"description"` // optional operator-authored summary
	DeletionTaskID *string `yaml:"deletion_task_id,omitempty" json:"deletion_task_id,omitempty"`
}

// Project is either a tenant application or a backing service.
type Project struct {
	ID             string      `yaml:"id"                  json:"id"`                  // prj_<ulid>
	TenantID       string      `yaml:"tenant_id,omitempty" json:"tenant_id,omitempty"` // empty for backing
	Slug           string      `yaml:"slug"                json:"slug"`                // unique within tenant
	Name           string      `yaml:"name"                json:"name"`
	Description    string      `yaml:"description"         json:"description"` // optional create-time summary
	Kind           ProjectKind `yaml:"kind"                json:"kind"`
	DeletionTaskID *string     `yaml:"deletion_task_id,omitempty" json:"deletion_task_id,omitempty"`
}
