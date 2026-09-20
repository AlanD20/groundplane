package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"unicode/utf8"
)

func ValidateTenant(record TenantRecord) error {
	if err := recordcodec.ValidateID(ids.KindTenant, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("tenant slug", record.Slug); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("tenant name", record.Name); err != nil {
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

func ValidateProject(record ProjectRecord) error {
	if err := recordcodec.ValidateID(ids.KindProject, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("project slug", record.Slug); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("project name", record.Name); err != nil {
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
		return recordcodec.ValidateID(ids.KindTenant, record.TenantID)
	case ProjectKindBacking:
		if record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "backing projects must not have a tenant id")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
}

func ValidateEnvironment(record EnvironmentRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindProject, record.ProjectID); err != nil {
		return err
	}
	if err := recordcodec.ValidateLabel("environment name", record.Name); err != nil {
		return err
	}
	pool, err := ipam.ParseIPv4Prefix(record.NetworkPool)
	if err != nil || pool.String() != record.NetworkPool {
		return errs.New(errs.KindValidationFailed, "environment network_pool must be a canonical IPv4 CIDR")
	}
	if err := recordcodec.ValidateLabel("environment volume directory", record.VolumeDir); err != nil {
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
