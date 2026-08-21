// Package hierarchy implements the Controller use cases for tenants and
// ordinary tenant-owned projects. Backing projects, environments, deletion,
// and transport concerns intentionally live outside this module.
package hierarchy

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
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

// Repository is the persistence port for this closed hierarchy subset. It
// owns atomic slug uniqueness, owner-at-commit validation, CAS, and pinned
// pagination. Project methods accept and return ordinary tenant projects only.
type Repository interface {
	ready() bool

	CreateTenant(context.Context, core.Tenant) (Versioned[core.Tenant], error)
	GetTenant(context.Context, string) (Versioned[core.Tenant], error)
	ResolveTenant(context.Context, string) (Versioned[core.Tenant], error)
	ListTenants(context.Context, PageRequest) (Page[core.Tenant], error)

	CreateProject(context.Context, core.Project) (Versioned[core.Project], error)
	GetProject(context.Context, string) (Versioned[core.Project], error)
	ResolveTenantProject(context.Context, string, string) (Versioned[core.Project], error)
	ListTenantProjects(context.Context, string, PageRequest) (Page[core.Project], error)
}

type CreateTenantInput struct {
	Slug string
	Name *string
}

type CreateProjectInput struct {
	TenantID string
	Slug     string
	Name     *string
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
	if err := validateSlug("tenant slug", input.Slug); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	name := input.Slug
	if input.Name != nil {
		name = *input.Name
	}
	if err := validateText("tenant name", name); err != nil {
		return Versioned[core.Tenant]{}, err
	}
	record := core.Tenant{ID: ids.New(ids.KindTenant), Slug: input.Slug, Name: name}
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
	if err := validateSlug("tenant slug", slug); err != nil {
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

func (service *Service) CreateProject(
	ctx context.Context,
	input CreateProjectInput,
) (Versioned[core.Project], error) {
	if err := requireContext(ctx); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateStableID(ids.KindTenant, input.TenantID); err != nil {
		return Versioned[core.Project]{}, err
	}
	if err := validateSlug("project slug", input.Slug); err != nil {
		return Versioned[core.Project]{}, err
	}
	name := input.Slug
	if input.Name != nil {
		name = *input.Name
	}
	if err := validateText("project name", name); err != nil {
		return Versioned[core.Project]{}, err
	}
	record := core.Project{
		ID:       ids.New(ids.KindProject),
		TenantID: input.TenantID,
		Slug:     input.Slug,
		Name:     name,
		Kind:     core.ProjectKindTenant,
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
	if stored.Record.Kind == core.ProjectKindBacking {
		return Versioned[core.Project]{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if err := validateProjectVersion(stored); err != nil {
		return Versioned[core.Project]{}, err
	}
	if stored.Record.ID != id {
		return Versioned[core.Project]{}, internalInvariant("repository returned the wrong project")
	}
	return stored, nil
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
	if err := validateSlug("project slug", slug); err != nil {
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

func validateSlug(field string, value string) error {
	if len(value) == 0 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return errs.Newf(errs.KindValidationFailed, "%s must be a lowercase ASCII label of 1-63 bytes", field)
	}
	previousHyphen := false
	for index := range len(value) {
		character := value[index]
		isLetter := character >= 'a' && character <= 'z'
		isDigit := character >= '0' && character <= '9'
		if !isLetter && !isDigit && character != '-' || character == '-' && previousHyphen {
			return errs.Newf(errs.KindValidationFailed, "%s must be a lowercase ASCII hyphen label", field)
		}
		previousHyphen = character == '-'
	}
	return nil
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
	if err := validateSlug("tenant slug", stored.Record.Slug); err != nil {
		return internalInvariant("repository returned a noncanonical tenant slug")
	}
	return nil
}

func validateProjectVersion(stored Versioned[core.Project]) error {
	if err := validateVersion(stored.Revision, stored.ReadRevision); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindProject, stored.Record.ID); err != nil {
		return internalInvariant("repository returned an invalid project id")
	}
	if stored.Record.Kind != core.ProjectKindTenant || stored.Record.TenantID == "" {
		return internalInvariant("repository returned a non-tenant project")
	}
	if err := ids.Validate(ids.KindTenant, stored.Record.TenantID); err != nil {
		return internalInvariant("repository returned an invalid project owner")
	}
	if err := stored.Record.Validate(); err != nil {
		return internalInvariant("repository returned an invalid project")
	}
	if err := validateSlug("project slug", stored.Record.Slug); err != nil {
		return internalInvariant("repository returned a noncanonical project slug")
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

func internalInvariant(message string) error {
	return errs.New(errs.KindInternal, "hierarchy: "+message)
}
