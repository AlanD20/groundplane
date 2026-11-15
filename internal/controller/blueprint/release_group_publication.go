package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleaseGroupPublication advances only this accepted head's metadata.
// The caller commits the returned fragment with the Task revision guard before
// opening executable graph membership. Superseded/stopped Tasks publish only
// their captured membership so normal cancellation can settle them.
func (service *Service) PrepareReleaseGroupPublication(ctx context.Context,
	task workqueue.TaskSnapshot, revision int64,
) (workqueue.Change, bool, error) {
	if task.Task.Stop != nil || task.Task.FinishedAt != nil {
		return workqueue.Change{}, false, nil
	}
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, task.Task.Scope.EnvironmentID)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	if !found || head.Record.RevisionID != task.Task.ID {
		return workqueue.Change{}, false, nil
	}
	if head.Revision > revision {
		return workqueue.Change{}, false, errs.New(errs.KindStateConflict, "Blueprint head changed during publication")
	}
	input, err := service.loadAuthoredParentInput(ctx, task.Task)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	services := make([]core.Service, len(input.identities.Services))
	for index, identity := range input.identities.Services {
		services[index] = core.Service{ID: identity.ID, Name: identity.Name}
	}
	prepared, pending, err := service.releaseGroups.PrepareNext(
		ctx, task.Task.Scope.EnvironmentID, input.desired.Input.ReleaseGroups,
		services, func() string { return ids.New(ids.KindReleaseGroup) },
	)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{
		Key: blueprints.EnvironmentBlueprintHeadKey(task.Task.Scope.EnvironmentID), ModRevision: head.Revision,
	}}}
	change.Conditions, change.Mutations = prepared.AppendTo(change.Conditions, nil)
	return change, pending, nil
}
