package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentComposeProjectionAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if err := validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, "",
	); err != nil {
		return err
	}
	return preserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}

func validateEnvironmentComposeProjectionPublicationAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
	task TaskRecord,
) error {
	removedVolumeID := ""
	if task.Type == TaskRemove && task.Params[TaskResourceKindParam] == TaskResourceVolume {
		removedVolumeID = task.Target
		if recordcodec.ValidateID(ids.KindVolume, removedVolumeID) != nil {
			return errs.New(errs.KindValidationFailed, "Volume removal Task target is invalid")
		}
	}
	if err := validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, removedVolumeID,
	); err != nil {
		return err
	}
	return preserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}
