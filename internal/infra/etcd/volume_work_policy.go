package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareVolumeWorkPolicy preserves fixed-revision Backup source selection and
// schedule replacement in the same transaction as desired removal and work.
func prepareVolumeWorkPolicy(prepared VolumeRemovalBackupPolicyPreparation,
	claim blueprints.EnvironmentBlueprintStageClaim, projection projectionrecord.EnvironmentComposeProjection,
	task workexecution.Task, marker idempotency.IdempotencyMarker, at time.Time,
) (workqueue.Change, error) {
	if task.Action != workexecution.ActionRemove {
		if prepared.state != nil {
			return workqueue.Change{}, errs.New(errs.KindValidationFailed, "only Volume removal may replace Backup sources")
		}
		return workqueue.Change{}, nil
	}
	if task.ID != claim.TaskID || task.Scope.EnvironmentID != claim.EnvironmentID || marker.TaskID != task.ID {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Volume Backup policy does not belong to this action")
	}
	if err := prepared.validateDesiredPublication(claim, projection, marker, task.TargetID); err != nil {
		return workqueue.Change{}, err
	}
	publication, err := prepareVolumeRemovalBackupPolicyPublication(prepared, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: publication.conditions, Mutations: publication.mutations}, nil
}
