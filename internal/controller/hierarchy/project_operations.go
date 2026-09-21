package hierarchy

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	sluggrammar "github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
