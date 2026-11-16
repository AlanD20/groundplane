package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackingCreationStore is the existing, closed backing-facade transaction port.
// It is injected explicitly; no ordinary Store assertion or quota override exists.
type BackingCreationStore interface {
	MeasureTransaction(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionBudget, error)
	TransactBackingService(
		context.Context,
		[]keyvalue.Condition,
		[]keyvalue.Mutation,
	) (keyvalue.TransactionResult, error)
}

type BackingCreationPublisher struct{ store BackingCreationStore }

func NewBackingCreationPublisher(store BackingCreationStore) (*BackingCreationPublisher, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Backing creation publication storage is required")
	}
	return &BackingCreationPublisher{store: store}, nil
}

func (publisher *BackingCreationPublisher) Commit(ctx context.Context, task workexecution.Task, kind string,
	changes ...Change,
) (keyvalue.TransactionResult, error) {
	if publisher == nil ||
		publisher.store == nil ||
		kind != "backing-create" ||
		task.Action != workexecution.ActionCreate ||
		ids.Validate(ids.KindService, task.TargetID) != nil ||
		task.AcceptedIntent == nil ||
		task.Intent.Validate() != nil ||
		task.Scope.ProjectID == "" ||
		task.Scope.EnvironmentID == "" ||
		task.Actor != workexecution.OperatorActor {
		return keyvalue.TransactionResult{}, invalid(
			"only accepted backing creation may use the backing-facade publication port",
		)
	}
	joined, err := JoinChanges(changes...)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	guarded, published := false, false
	for _, condition := range joined.Conditions {
		if condition.Key == TaskKey(task.ID) && condition.ModRevision > 0 && !condition.Prefix {
			guarded = true
		}
	}
	for _, mutation := range joined.Mutations {
		if mutation.Key == TaskKey(task.ID) && mutation.Type == keyvalue.MutationPut {
			next, err := DecodeTask(mutation.Value)
			if err != nil || next.ID != task.ID || next.Action != task.Action || next.TargetID != task.TargetID ||
				next.Intent != task.Intent {
				return keyvalue.TransactionResult{}, invalid(
					"backing publication differs from its accepted Task promotion",
				)
			}
			published = true
		}
	}
	if !guarded || !published {
		return keyvalue.TransactionResult{}, invalid(
			"backing publication requires atomic promotion of the existing Task",
		)
	}
	budget, err := publisher.store.MeasureTransaction(ctx, joined.Conditions, joined.Mutations)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if budget.Operations < 0 || budget.Operations > 128 || budget.Bytes < 0 || budget.Bytes > keyvalue.MaximumBytes {
		return keyvalue.TransactionResult{}, invalid(
			"backing creation exceeds its bounded 128-operation publication or encoded request limit",
		)
	}
	return publisher.store.TransactBackingService(ctx, joined.Conditions, joined.Mutations)
}
