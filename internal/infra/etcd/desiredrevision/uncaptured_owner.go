package desiredrevision

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// uncapturedOwnerConditions distinguishes internal pre-publication staging from
// an operator Task whose pure intent exists but has never gained execution input.
// It never grants cleanup over a promoted Task or a different request marker.
func uncapturedOwnerConditions(
	marker, task *keyvalue.KeyValue,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
) ([]keyvalue.Condition, bool, error) {
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(
		blueprints.EnvironmentBlueprintTaskLocator(descriptor.Claim),
	)
	if err != nil {
		return nil, false, err
	}
	conditions := []keyvalue.Condition{{Key: markerKey}, {Key: workqueue.TaskKey(descriptor.Claim.TaskID)}}
	if marker == nil && task == nil && descriptor.Claim.TaskLocator == nil {
		return conditions, true, nil
	}
	if marker == nil || task == nil || descriptor.Claim.TaskLocator == nil {
		return nil, false, nil
	}
	owner, err := workqueue.DecodeTask(task.Value)
	if err != nil {
		return nil, false, err
	}
	if owner.ID != descriptor.Claim.TaskID || owner.AcceptedIntent == nil || owner.Intent != (workexecution.Input{}) ||
		owner.Scope.EnvironmentID != descriptor.Claim.EnvironmentID || !owner.CreatedAt.Equal(descriptor.Claim.CreatedAt) {
		return nil, false, nil
	}
	matched, err := environmentBlueprintMarkerOwnedBy(marker.Value, descriptor)
	if err != nil || !matched {
		return nil, false, err
	}
	conditions[0].ModRevision = marker.ModRevision
	conditions[1].ModRevision = task.ModRevision
	return conditions, true, nil
}
