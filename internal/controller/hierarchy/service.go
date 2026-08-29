// Package hierarchy implements the Controller use cases for tenants and
// ordinary tenant-owned projects. Backing projects, environments, deletion,
// and transport concerns intentionally live outside this module.
package hierarchy

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	sluggrammar "github.com/AlanD20/groundplane/internal/common/slug"
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

func (service *Service) CreateTenant(
	ctx context.Context,
	input CreateTenantInput,
) (Versioned[core.Tenant], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	record, err := PrepareTenant(input)
	if err != nil {
		return Versioned[core.Tenant]{}, err
	}
	stored, err := service.repository.CreateTenant(ctx, record)
	if err != nil {
		return Versioned[core.Tenant]{}, repositoryError(ctx, err)
	}
	if err := validateTenantVersion(stored); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if err := validateWriteRevision(0, stored); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if stored.Record != record {
		return Versioned[core.Tenant]{}, internalInvariant("repository changed the created tenant")
	}
	return stored, nil
}

// PrepareTenant validates the complete effective create input and generates
// its stable identity without persistence. Atomic idempotent callers use this
// exact constructor before building their same-transaction marker plan.
func PrepareTenant(input CreateTenantInput) (core.Tenant, error) {
	if err := sluggrammar.Validate("tenant slug", input.Slug); err != nil {
		return core.Tenant{}, err
	}
	name := input.Slug
	if input.Name != nil {
		name = *input.Name
	}
	if err := validateText("tenant name", name); err != nil {
		return core.Tenant{}, err
	}
	if err := validateOptionalText("tenant description", input.Description); err != nil {
		return core.Tenant{}, err
	}
	return core.Tenant{
		ID: ids.New(ids.KindTenant), Slug: input.Slug, Name: name, Description: input.Description,
	}, nil
}

func ValidateTenantEditInput(input EditTenantInput) error {
	if input.Name == nil && input.Description == nil {
		return errs.New(errs.KindValidationFailed, "Tenant edit requires at least one field")
	}
	if input.Name != nil {
		if err := validateText("tenant name", *input.Name); err != nil {
			return err
		}
	}
	if input.Description != nil {
		if err := validateOptionalText("tenant description", *input.Description); err != nil {
			return err
		}
	}
	return nil
}

func PrepareTenantEdit(current core.Tenant, input EditTenantInput) (core.Tenant, error) {
	if err := ValidateTenantEditInput(input); err != nil {
		return core.Tenant{}, err
	}
	replacement := current
	if input.Name != nil {
		replacement.Name = *input.Name
	}
	if input.Description != nil {
		replacement.Description = *input.Description
	}
	if err := validateStableID(ids.KindTenant, replacement.ID); err != nil {
		return core.Tenant{}, err
	}
	if err := sluggrammar.Validate("tenant slug", replacement.Slug); err != nil {
		return core.Tenant{}, err
	}
	if err := validateText("tenant name", replacement.Name); err != nil {
		return core.Tenant{}, err
	}
	if err := validateOptionalText("tenant description", replacement.Description); err != nil {
		return core.Tenant{}, err
	}
	return replacement, nil
}

func ValidateTenantRenameInput(input RenameTenantInput) error {
	return sluggrammar.Validate("tenant slug", input.Slug)
}

func PrepareTenantRename(current core.Tenant, input RenameTenantInput) (core.Tenant, error) {
	if err := ValidateTenantRenameInput(input); err != nil {
		return core.Tenant{}, err
	}
	replacement := current
	replacement.Slug = input.Slug
	if err := validateStableID(ids.KindTenant, replacement.ID); err != nil {
		return core.Tenant{}, err
	}
	if err := validateText("tenant name", replacement.Name); err != nil {
		return core.Tenant{}, err
	}
	if err := validateOptionalText("tenant description", replacement.Description); err != nil {
		return core.Tenant{}, err
	}
	return replacement, nil
}

func (service *Service) GetTenant(
	ctx context.Context,
	id string,
) (Versioned[core.Tenant], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if err := validateStableID(ids.KindTenant, id); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	stored, err := service.repository.GetTenant(ctx, id)
	if err != nil {
		return Versioned[core.Tenant]{}, repositoryError(ctx, err)
	}
	if err := validateTenantVersion(stored); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if stored.Record.ID != id {
		return Versioned[core.Tenant]{}, internalInvariant("repository returned the wrong tenant")
	}
	return stored, nil
}

func (service *Service) ResolveTenant(
	ctx context.Context,
	slug string,
) (Versioned[core.Tenant], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if err := sluggrammar.Validate("tenant slug", slug); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	stored, err := service.repository.ResolveTenant(ctx, slug)
	if err != nil {
		return Versioned[core.Tenant]{}, repositoryError(ctx, err)
	}
	if err := validateTenantVersion(stored); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if stored.Record.Slug != slug {
		return Versioned[core.Tenant]{}, internalInvariant("repository returned the wrong tenant slug")
	}
	return stored, nil
}

func (service *Service) ListTenants(
	ctx context.Context,
	request PageRequest,
) (Page[core.Tenant], error) {
	if err := requireContext(ctx); err != nil {
		return Page[core.Tenant]{}, err
	}
	limit, err := validatePageRequest(request)
	if err != nil {
		return Page[core.Tenant]{}, err
	}
	page, err := service.repository.ListTenants(ctx, request)
	if err != nil {
		return Page[core.Tenant]{}, repositoryError(ctx, err)
	}
	if err := validateTenantPage(page, limit); err != nil {
		return Page[core.Tenant]{}, err
	}
	return page, nil
}

// RenameTenant changes only the tenant's URL label. The repository owns the
// atomic record/index transaction; this use case owns bounded CAS retries.
func (service *Service) RenameTenant(
	ctx context.Context,
	id string,
	slug string,
) (Versioned[core.Tenant], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if err := validateStableID(ids.KindTenant, id); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	if err := sluggrammar.Validate("tenant slug", slug); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	for attempt := range maximumRenameAttempts {
		current, err := service.GetTenant(ctx, id)
		if err != nil {
			return Versioned[core.Tenant]{}, err
		}
		if err := requireContext(ctx); err != nil {
			return Versioned[core.Tenant]{}, err
		}
		renamed, err := service.repository.RenameTenant(ctx, id, current.Revision, slug)
		if err != nil {
			err = repositoryError(ctx, err)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || attempt == maximumRenameAttempts-1 {
				return Versioned[core.Tenant]{}, err
			}
			continue
		}
		if err := validateTenantVersion(renamed); err != nil {
			return Versioned[core.Tenant]{}, err
		}
		if renamed.Record.ID != current.Record.ID || renamed.Record.Name != current.Record.Name ||
			renamed.Record.Description != current.Record.Description ||
			renamed.Record.Slug != slug {
			return Versioned[core.Tenant]{}, internalInvariant("repository changed tenant identity during rename")
		}
		if current.Record.Slug == slug {
			if renamed.Record != current.Record || renamed.Revision != current.Revision {
				return Versioned[core.Tenant]{}, internalInvariant("repository wrote a tenant rename-to-current")
			}
		} else if err := validateWriteRevision(current.Revision, renamed); err != nil {
			return Versioned[core.Tenant]{}, err
		}
		return renamed, nil
	}
	return Versioned[core.Tenant]{}, internalInvariant("tenant rename attempt bound was not enforced")
}

func (service *Service) CreateProject(
	ctx context.Context,
	input CreateProjectInput,
) (Versioned[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Project]{}, err
	}
	record, err := PrepareProject(input)
	if err != nil {
		return Versioned[core.Project]{}, err
	}
	stored, err := service.repository.CreateProject(ctx, record)
	if err != nil {
		return Versioned[core.Project]{}, repositoryError(ctx, err)
	}
	if err := validateProjectVersion(stored); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateWriteRevision(0, stored); err != nil {
		return Versioned[core.Project]{}, err
	}
	if stored.Record != record {
		return Versioned[core.Project]{}, internalInvariant("repository changed the created project")
	}
	return stored, nil
}

// PrepareProject validates effective public input and generates the stable
// identity without persisting it. Protected idempotent creation uses this
// exact preparation path before its atomic durable transaction.
func PrepareProject(input CreateProjectInput) (core.Project, error) {
	if err := validateStableID(ids.KindTenant, input.TenantID); err != nil {
		return core.Project{}, err
	}
	if err := sluggrammar.Validate("project slug", input.Slug); err != nil {
		return core.Project{}, err
	}
	name := input.Slug
	if input.Name != nil {
		name = *input.Name
	}
	if err := validateText("project name", name); err != nil {
		return core.Project{}, err
	}
	if err := validateOptionalText("project description", input.Description); err != nil {
		return core.Project{}, err
	}
	return core.Project{
		ID: ids.New(ids.KindProject), TenantID: input.TenantID,
		Slug: input.Slug, Name: name, Description: input.Description,
		Kind: core.ProjectKindTenant,
	}, nil
}

func (service *Service) GetProject(
	ctx context.Context,
	id string,
) (Versioned[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateStableID(ids.KindProject, id); err != nil {
		return Versioned[core.Project]{}, err
	}
	stored, err := service.repository.GetProject(ctx, id)
	if err != nil {
		return Versioned[core.Project]{}, repositoryError(ctx, err)
	}
	if err := validateAnyProjectVersion(stored); err != nil {
		return Versioned[core.Project]{}, err
	}
	if stored.Record.ID != id {
		return Versioned[core.Project]{}, internalInvariant("repository returned the wrong project")
	}
	return stored, nil
}

func (service *Service) ListAllProjects(
	ctx context.Context,
	filter ProjectFilter,
	request PageRequest,
) (Page[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Page[core.Project]{}, err
	}
	if filter.Kind != "" && filter.Kind != core.ProjectKindTenant && filter.Kind != core.ProjectKindBacking {
		return Page[core.Project]{}, errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
	if filter.TenantID != "" {
		if err := validateStableID(ids.KindTenant, filter.TenantID); err != nil {
			return Page[core.Project]{}, err
		}
		if filter.Kind == core.ProjectKindBacking {
			return Page[core.Project]{}, errs.New(
				errs.KindValidationFailed,
				"backing projects cannot have a tenant filter",
			)
		}
	}
	limit, err := validatePageRequest(request)
	if err != nil {
		return Page[core.Project]{}, err
	}
	page, err := service.repository.ListProjects(ctx, filter, request)
	if err != nil {
		return Page[core.Project]{}, repositoryError(ctx, err)
	}
	if err := validateAllProjectPage(page, filter, limit); err != nil {
		return Page[core.Project]{}, err
	}
	return page, nil
}

func (service *Service) ResolveProject(
	ctx context.Context,
	tenantID string,
	slug string,
) (Versioned[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateStableID(ids.KindTenant, tenantID); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := sluggrammar.Validate("project slug", slug); err != nil {
		return Versioned[core.Project]{}, err
	}
	stored, err := service.repository.ResolveTenantProject(ctx, tenantID, slug)
	if err != nil {
		return Versioned[core.Project]{}, repositoryError(ctx, err)
	}
	if err := validateProjectVersion(stored); err != nil {
		return Versioned[core.Project]{}, err
	}
	if stored.Record.TenantID != tenantID || stored.Record.Slug != slug {
		return Versioned[core.Project]{}, internalInvariant("repository returned a project outside its slug scope")
	}
	return stored, nil
}

func (service *Service) ListProjects(
	ctx context.Context,
	tenantID string,
	request PageRequest,
) (Page[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Page[core.Project]{}, err
	}
	if err := validateStableID(ids.KindTenant, tenantID); err != nil {
		return Page[core.Project]{}, err
	}
	limit, err := validatePageRequest(request)
	if err != nil {
		return Page[core.Project]{}, err
	}
	page, err := service.repository.ListTenantProjects(ctx, tenantID, request)
	if err != nil {
		return Page[core.Project]{}, repositoryError(ctx, err)
	}
	if err := validateProjectPage(page, tenantID, limit); err != nil {
		return Page[core.Project]{}, err
	}
	return page, nil
}

// RenameProject changes only an ordinary tenant-owned Project's URL label.
func (service *Service) RenameProject(
	ctx context.Context,
	id string,
	slug string,
) (Versioned[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateStableID(ids.KindProject, id); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := sluggrammar.Validate("project slug", slug); err != nil {
		return Versioned[core.Project]{}, err
	}
	for attempt := range maximumRenameAttempts {
		current, err := service.GetProject(ctx, id)
		if err != nil {
			return Versioned[core.Project]{}, err
		}
		if current.Record.Kind != core.ProjectKindTenant {
			return Versioned[core.Project]{}, errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if err := requireContext(ctx); err != nil {
			return Versioned[core.Project]{}, err
		}
		renamed, err := service.repository.RenameProject(ctx, id, current.Revision, slug)
		if err != nil {
			err = repositoryError(ctx, err)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || attempt == maximumRenameAttempts-1 {
				return Versioned[core.Project]{}, err
			}
			continue
		}
		if err := validateProjectVersion(renamed); err != nil {
			return Versioned[core.Project]{}, err
		}
		if renamed.Record.ID != current.Record.ID || renamed.Record.TenantID != current.Record.TenantID ||
			renamed.Record.Name != current.Record.Name || renamed.Record.Description != current.Record.Description ||
			renamed.Record.Kind != current.Record.Kind ||
			renamed.Record.Slug != slug {
			return Versioned[core.Project]{}, internalInvariant("repository changed project identity during rename")
		}
		if current.Record.Slug == slug {
			if renamed.Record != current.Record || renamed.Revision != current.Revision {
				return Versioned[core.Project]{}, internalInvariant("repository wrote a project rename-to-current")
			}
		} else if err := validateWriteRevision(current.Revision, renamed); err != nil {
			return Versioned[core.Project]{}, err
		}
		return renamed, nil
	}
	return Versioned[core.Project]{}, internalInvariant("project rename attempt bound was not enforced")
}

func requireContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy context is required")
	}
	return ctx.Err()
}

func validateText(field string, value string) error {
	if value == "" || !utf8.ValidString(value) {
		return errs.Newf(errs.KindValidationFailed, "%s is required and must be valid UTF-8", field)
	}
	return nil
}

func validateOptionalText(field string, value string) error {
	if value == "" {
		return nil
	}
	return validateText(field, value)
}

func validateStableID(kind ids.Kind, id string) error {
	if err := ids.Validate(kind, id); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func repositoryError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}

func validateWriteRevision(previous int64, stored interface{ revisions() (int64, int64) }) error {
	revision, readRevision := stored.revisions()
	if revision <= previous || readRevision != revision {
		return internalInvariant("repository returned invalid write revision metadata")
	}
	return nil
}

func (stored Versioned[T]) revisions() (int64, int64) {
	return stored.Revision, stored.ReadRevision
}

const (
	defaultPageLimit = 50
	maximumPageLimit = 200
)

func validatePageRequest(request PageRequest) (int, error) {
	if request.Limit < 0 || request.Limit > maximumPageLimit {
		return 0, errs.New(errs.KindValidationFailed, "page limit must be between 1 and 200")
	}
	if request.Limit == 0 {
		return defaultPageLimit, nil
	}
	return request.Limit, nil
}

func validateTenantVersion(stored Versioned[core.Tenant]) error {
	if err := validateVersion(stored.Revision, stored.ReadRevision); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindTenant, stored.Record.ID); err != nil {
		return internalInvariant("repository returned an invalid tenant id")
	}
	if err := stored.Record.Validate(); err != nil {
		return internalInvariant("repository returned an invalid tenant")
	}
	if err := sluggrammar.Validate("tenant slug", stored.Record.Slug); err != nil {
		return internalInvariant("repository returned a noncanonical tenant slug")
	}
	if err := validateOptionalText("tenant description", stored.Record.Description); err != nil {
		return internalInvariant("repository returned an invalid tenant description")
	}
	return nil
}

func validateProjectVersion(stored Versioned[core.Project]) error {
	if err := validateAnyProjectVersion(stored); err != nil {
		return err
	}
	if stored.Record.Kind != core.ProjectKindTenant || stored.Record.TenantID == "" {
		return internalInvariant("repository returned a non-tenant project")
	}
	return nil
}

func validateAnyProjectVersion(stored Versioned[core.Project]) error {
	if err := validateVersion(stored.Revision, stored.ReadRevision); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindProject, stored.Record.ID); err != nil {
		return internalInvariant("repository returned an invalid project id")
	}
	switch stored.Record.Kind {
	case core.ProjectKindTenant:
		if err := ids.Validate(ids.KindTenant, stored.Record.TenantID); err != nil {
			return internalInvariant("repository returned an invalid project owner")
		}
	case core.ProjectKindBacking:
		if stored.Record.TenantID != "" {
			return internalInvariant("repository returned an owned backing project")
		}
	default:
		return internalInvariant("repository returned an invalid project kind")
	}
	if err := stored.Record.Validate(); err != nil {
		return internalInvariant("repository returned an invalid project")
	}
	if err := sluggrammar.Validate("project slug", stored.Record.Slug); err != nil {
		return internalInvariant("repository returned a noncanonical project slug")
	}
	if err := validateOptionalText("project description", stored.Record.Description); err != nil {
		return internalInvariant("repository returned an invalid project description")
	}
	return nil
}

func validateVersion(revision int64, readRevision int64) error {
	if revision <= 0 || readRevision < revision {
		return internalInvariant("repository returned invalid revision metadata")
	}
	return nil
}

func validateTenantPage(page Page[core.Tenant], limit int) error {
	if page.Revision <= 0 {
		return internalInvariant("repository returned an invalid tenant page revision")
	}
	if len(page.Items) > limit {
		return internalInvariant("repository returned too many tenants")
	}
	previousID := ""
	for _, item := range page.Items {
		if err := validateTenantVersion(item); err != nil {
			return err
		}
		if item.ReadRevision != page.Revision || item.Record.ID <= previousID {
			return internalInvariant("repository returned an inconsistent tenant page")
		}
		previousID = item.Record.ID
	}
	return nil
}

func validateProjectPage(page Page[core.Project], tenantID string, limit int) error {
	if page.Revision <= 0 {
		return internalInvariant("repository returned an invalid project page revision")
	}
	if len(page.Items) > limit {
		return internalInvariant("repository returned too many projects")
	}
	previousID := ""
	for _, item := range page.Items {
		if err := validateProjectVersion(item); err != nil {
			return err
		}
		if item.ReadRevision != page.Revision || item.Record.TenantID != tenantID || item.Record.ID <= previousID {
			return internalInvariant("repository returned an inconsistent project page")
		}
		previousID = item.Record.ID
	}
	return nil
}

func validateAllProjectPage(page Page[core.Project], filter ProjectFilter, limit int) error {
	if page.Revision <= 0 || len(page.Items) > limit {
		return internalInvariant("repository returned an invalid global project page")
	}
	previousID := ""
	for _, item := range page.Items {
		if err := validateAnyProjectVersion(item); err != nil {
			return err
		}
		if item.ReadRevision != page.Revision || item.Record.ID <= previousID ||
			(filter.Kind != "" && item.Record.Kind != filter.Kind) ||
			(filter.TenantID != "" && item.Record.TenantID != filter.TenantID) {
			return internalInvariant("repository returned an inconsistent global project page")
		}
		previousID = item.Record.ID
	}
	return nil
}

func internalInvariant(message string) error {
	return errs.New(errs.KindInternal, "hierarchy: "+message)
}
