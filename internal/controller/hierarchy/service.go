// Package hierarchy implements the Controller use cases for tenants and
// ordinary tenant-owned projects. Backing projects, environments, deletion,
// and transport concerns intentionally live outside this module.
package hierarchy

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Versioned carries the durable CAS revision separately from the domain
// record. ReadRevision is the fixed MVCC view used to read the record.
type Versioned[T any] struct {
	Record       T
	Revision     int64
	ReadRevision int64
}

// PageRequest carries the durable repository's opaque fixed-revision cursor.
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page contains records from one fixed MVCC revision. NextCursor is opaque to
// this module and must be returned unchanged by callers.
type Page[T any] struct {
	Items      []Versioned[T]
	NextCursor string
	Revision   int64
}

type ProjectFilter struct {
	TenantID string
	Kind     core.ProjectKind
}

// Repository is the persistence port for this closed hierarchy subset. It
// owns atomic slug uniqueness, owner-at-commit validation, CAS, and pinned
// pagination. GetProject returns either Project kind; tenant-only creation,
// resolution, listing, and rename methods preserve the facade boundary.
type Repository interface {
	ready() bool

	CreateTenant(context.Context, core.Tenant) (Versioned[core.Tenant], error)
	GetTenant(context.Context, string) (Versioned[core.Tenant], error)
	ResolveTenant(context.Context, string) (Versioned[core.Tenant], error)
	ListTenants(context.Context, PageRequest) (Page[core.Tenant], error)
	RenameTenant(context.Context, string, int64, string) (Versioned[core.Tenant], error)

	CreateProject(context.Context, core.Project) (Versioned[core.Project], error)
	GetProject(context.Context, string) (Versioned[core.Project], error)
	ResolveTenantProject(context.Context, string, string) (Versioned[core.Project], error)
	ListTenantProjects(context.Context, string, PageRequest) (Page[core.Project], error)
	ListProjects(context.Context, ProjectFilter, PageRequest) (Page[core.Project], error)
	RenameProject(context.Context, string, int64, string) (Versioned[core.Project], error)
}

const maximumRenameAttempts = 3

type CreateTenantInput struct {
	Slug        string
	Name        *string
	Description string
}

type EditTenantInput struct {
	Name        *string
	Description *string
}

type RenameTenantInput struct {
	Slug string
}

type CreateProjectInput struct {
	TenantID    string
	Slug        string
	Name        *string
	Description string
}

// Service owns stable-id generation, the normal-project restriction, output
// invariants, and dependency error normalization for hierarchy use cases.
type Service struct {
	repository Repository
}

func NewService(repository Repository) (*Service, error) {
	if repository == nil || !repository.ready() {
		return nil, errs.New(errs.KindInternal, "hierarchy repository is required")
	}
	return &Service{repository: repository}, nil
}
