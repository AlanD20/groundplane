package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RejectInvalidInput settles an immutable, never-dispatched Component input.
// A concurrent desired-state replacement or dispatch must win over this read.
func (repository *InputRepository) RejectInvalidInput(ctx context.Context,
	view workqueue.View, headRevision int64, reason string,
) error {
	if headRevision <= 0 || headRevision > view.ReadRevision {
		return errs.New(errs.KindStateConflict, "Component input rejection requires its captured head")
	}
	change, err := workqueue.PrepareInputRejection(view, reason, time.Now().UTC())
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(change.Conditions, keyvalue.Condition{
		Key:         blueprints.EnvironmentBlueprintHeadKey(view.Task.Task.Scope.EnvironmentID),
		ModRevision: headRevision,
	})
	result, err := repository.transactions.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Component input or desired head changed before rejection")
	}
	return nil
}
