package runnerwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareUnstartedRemoval releases only the deletion fence. No runtime or
// allocation is removed when cancellation wins before the first assignment.
func (repository *Repository) PrepareUnstartedRemoval(ctx context.Context,
	input keyvalue.Versioned[Input], item workexecution.Item,
) (workqueue.Change, error) {
	if input.Revision <= 0 || input.Record.MatchWork(item) != nil ||
		input.Record.Operation != runnerallocation.RuntimeOperationRemove || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		(item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner removal has execution effects to resolve")
	}
	conditions, err := repository.RemovalAuthority(ctx, input.Record, input.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions = append(conditions, keyvalue.Condition{Key: InputKey(item.ID), ModRevision: input.Revision})
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{{
		Type: keyvalue.MutationDelete,
		Key:  deletions.TombstoneKey(string(deletions.DeletionTargetRunner), input.Record.Desired.ID),
	}}}, nil
}
