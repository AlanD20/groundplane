package environmentprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareResourceRetry(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}

	current, change, err := repository.resourceSnapshot(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	next, err := hierarchy.RetryEnvironmentProvisioning(current, input.TaskID)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	raw, err := hierarchy.EncodeEnvironment(next)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append([]keyvalue.Condition{capture}, change.Conditions...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: hierarchy.EnvironmentKey(current.ID), Value: raw})
	return change, nil
}

// PrepareCleanupSource pins the first unresolved forward result. Repeated
// cleanup retries cannot substitute a later input or replace the original error.
func (repository *Repository) PrepareCleanupSource(ctx context.Context, view workqueue.View, previous workqueue.AttemptSnapshot) (workqueue.Change, error) {
	attempt := previous.Attempt
	if attempt.Direction == workexecution.Recovery {
		_, capture, err := repository.LoadRecovery(ctx, view)
		return workqueue.Change{Conditions: []keyvalue.Condition{capture}}, err
	}
	receipt, err := repository.ReadResult(ctx, attempt, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if receipt.Record.Report.HelpersAbsent || attempt.FinishedAt == nil || attempt.Result == nil ||
		view.Item.Item.Result == nil || *attempt.Result != *view.Item.Item.Result || attempt.Outcome == workexecution.Succeeded {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "environment provisioning has no captured unresolved helpers")
	}
	key := RecoveryKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment cleanup source snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	condition := keyvalue.Condition{Key: key}
	if read.Values[0] != nil {
		condition.ModRevision = read.Values[0].ModRevision
	}
	raw, err := recordcodec.Encode("environment_provision_recovery", attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{condition, {Key: ResultKey(attempt.ID), ModRevision: receipt.Revision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}
