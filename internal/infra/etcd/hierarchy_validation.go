package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
	"unicode/utf8"
)

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy context is required")
	}
	return ctx.Err()
}

func validateID(kind ids.Kind, id string) error {
	if err := ids.Validate(kind, id); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func validateLabel(field string, value string) error {
	if value == "" || !utf8.ValidString(value) {
		return errs.Newf(errs.KindValidationFailed, "%s is required and must be valid UTF-8", field)
	}
	return nil
}

func validateTenant(record TenantRecord) error {
	if err := validateID(ids.KindTenant, record.ID); err != nil {
		return err
	}
	if err := validateLabel("tenant slug", record.Slug); err != nil {
		return err
	}
	if err := validateLabel("tenant name", record.Name); err != nil {
		return err
	}
	if record.Description != "" && !utf8.ValidString(record.Description) {
		return errs.New(errs.KindValidationFailed, "tenant description must be valid UTF-8")
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "tenant deletion_task_id is invalid")
	}
	return nil
}

func validateProject(record ProjectRecord) error {
	if err := validateID(ids.KindProject, record.ID); err != nil {
		return err
	}
	if err := validateLabel("project slug", record.Slug); err != nil {
		return err
	}
	if err := validateLabel("project name", record.Name); err != nil {
		return err
	}
	if record.Description != "" && !utf8.ValidString(record.Description) {
		return errs.New(errs.KindValidationFailed, "project description must be valid UTF-8")
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "project deletion_task_id is invalid")
	}
	switch record.Kind {
	case ProjectKindTenant:
		return validateID(ids.KindTenant, record.TenantID)
	case ProjectKindBacking:
		if record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "backing projects must not have a tenant id")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
}

func validateEnvironment(record EnvironmentRecord) error {
	if err := validateID(ids.KindEnvironment, record.ID); err != nil {
		return err
	}
	if err := validateID(ids.KindProject, record.ProjectID); err != nil {
		return err
	}
	if err := validateLabel("environment name", record.Name); err != nil {
		return err
	}
	pool, err := ipam.ParseIPv4Prefix(record.NetworkPool)
	if err != nil || pool.String() != record.NetworkPool {
		return errs.New(errs.KindValidationFailed, "environment network_pool must be a canonical IPv4 CIDR")
	}
	if err := validateLabel("environment volume directory", record.VolumeDir); err != nil {
		return err
	}
	if err := validateEnvironmentRecordPath(record); err != nil {
		return err
	}
	if err := validateEnvironmentProvisioning(record); err != nil {
		return err
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "environment deletion_task_id is invalid")
	}
	_, offset := record.CreatedAt.Zone()
	if record.CreatedAt.IsZero() || offset != 0 {
		return errs.New(errs.KindValidationFailed, "environment created_at must be a non-zero UTC timestamp")
	}
	return nil
}
