package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateEnvironmentComposeProjectionAdvance(
	previous projectionrecord.EnvironmentComposeProjection,
	hasPrevious bool,
	next projectionrecord.EnvironmentComposeProjection,
) error {
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, "",
	); err != nil {
		return err
	}
	return projectionrecord.PreserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}

func validateEnvironmentComposeProjectionPublicationAdvance(
	previous projectionrecord.EnvironmentComposeProjection,
	hasPrevious bool,
	next projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
) error {
	removedVolumeID := ""
	if task.Type == taskjournal.TaskRemove && task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceVolume {
		removedVolumeID = task.Target
		if recordcodec.ValidateID(ids.KindVolume, removedVolumeID) != nil {
			return errs.New(errs.KindValidationFailed, "Volume removal Task target is invalid")
		}
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, removedVolumeID,
	); err != nil {
		return err
	}
	return projectionrecord.PreserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}
