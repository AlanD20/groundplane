package hierarchy

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	sluggrammar "github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"unicode/utf8"
)

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
