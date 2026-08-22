package etcd

import (
	"path/filepath"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewProvisioningEnvironment is the only write-side constructor for a new
// Environment. The caller supplies decisions; this function derives the
// immutable host path from trusted configuration and stable ownership IDs.
func NewProvisioningEnvironment(
	volumeRoot string,
	project ProjectRecord,
	environmentID string,
	name string,
	networkPool string,
	createTaskID string,
	createdAt time.Time,
) (EnvironmentRecord, error) {
	if err := validateProject(project); err != nil {
		return EnvironmentRecord{}, err
	}
	volumeDir, err := environmentpath.Derive(volumeRoot, environmentpath.Scope{
		ProjectKind:   environmentpath.ProjectKind(project.Kind),
		TenantID:      project.TenantID,
		ProjectID:     project.ID,
		EnvironmentID: environmentID,
	})
	if err != nil {
		return EnvironmentRecord{}, err
	}
	record := EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.ID,
		Name:              name,
		NetworkPool:       networkPool,
		VolumeDir:         volumeDir,
		ProvisioningState: EnvironmentProvisioningProvisioning,
		CreateTaskID:      createTaskID,
		CreatedAt:         createdAt,
	}
	if err := validateEnvironment(record); err != nil {
		return EnvironmentRecord{}, err
	}
	return record, nil
}

// ValidateEnvironmentVolumeDir cross-checks a persisted redundant path with
// the configured root and its durable Project owner.
func ValidateEnvironmentVolumeDir(
	volumeRoot string,
	project ProjectRecord,
	record EnvironmentRecord,
) error {
	if record.ProjectID != project.ID {
		return errs.New(errs.KindValidationFailed, "environment project ownership is invalid")
	}
	expected, err := environmentpath.Derive(volumeRoot, environmentpath.Scope{
		ProjectKind:   environmentpath.ProjectKind(project.Kind),
		TenantID:      project.TenantID,
		ProjectID:     project.ID,
		EnvironmentID: record.ID,
	})
	if err != nil {
		return err
	}
	if record.VolumeDir != expected {
		return errs.New(errs.KindValidationFailed, "environment volume_dir does not match its stable ownership path")
	}
	return nil
}

func validateEnvironmentRecordPath(record EnvironmentRecord) error {
	if !filepath.IsAbs(record.VolumeDir) || filepath.Clean(record.VolumeDir) != record.VolumeDir {
		return errs.New(errs.KindValidationFailed, "environment volume_dir must be a clean absolute path")
	}
	projectDir := filepath.Dir(record.VolumeDir)
	ownerDir := filepath.Dir(projectDir)
	if filepath.Base(record.VolumeDir) != record.ID || filepath.Base(projectDir) != record.ProjectID {
		return errs.New(
			errs.KindValidationFailed,
			"environment volume_dir must end with its stable project and environment ids",
		)
	}
	owner := filepath.Base(ownerDir)
	if owner != "platform" {
		if err := ids.Validate(ids.KindTenant, owner); err != nil {
			return errs.New(
				errs.KindValidationFailed,
				"environment volume_dir owner must be platform or a stable tenant id",
			)
		}
	}
	return nil
}

// CompleteEnvironmentProvisioning applies a helper result only to the Task
// that currently owns an in-flight create operation.
func CompleteEnvironmentProvisioning(
	record EnvironmentRecord,
	createTaskID string,
	succeeded bool,
) (EnvironmentRecord, error) {
	if record.ProvisioningState != EnvironmentProvisioningProvisioning {
		return EnvironmentRecord{}, errs.New(errs.KindStateConflict, "environment is not provisioning")
	}
	if record.CreateTaskID != createTaskID {
		return EnvironmentRecord{}, errs.New(
			errs.KindStateConflict,
			"environment create Task no longer owns provisioning",
		)
	}
	if succeeded {
		record.ProvisioningState = EnvironmentProvisioningReady
	} else {
		record.ProvisioningState = EnvironmentProvisioningFailed
	}
	return record, nil
}

// RetryEnvironmentProvisioning transfers a failed create operation to the
// new retry Task without changing Environment identity or volume_dir.
func RetryEnvironmentProvisioning(record EnvironmentRecord, createTaskID string) (EnvironmentRecord, error) {
	if record.ProvisioningState != EnvironmentProvisioningFailed {
		return EnvironmentRecord{}, errs.New(
			errs.KindStateConflict,
			"only failed environment provisioning can be retried",
		)
	}
	if err := ids.Validate(ids.KindTask, createTaskID); err != nil {
		return EnvironmentRecord{}, errs.New(errs.KindValidationFailed, "environment create_task_id is invalid")
	}
	record.ProvisioningState = EnvironmentProvisioningProvisioning
	record.CreateTaskID = createTaskID
	return record, nil
}
