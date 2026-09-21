package hierarchy

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	sluggrammar "github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
