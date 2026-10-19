package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleCompletedBlueprintRelease commits the proven runtime, configuration,
// Release history and work outcome together. A failed comparison writes none
// of them. An uncertain transaction result requires rereading this attempt;
// it never authorizes another deployment.
func (ledger *ReleaseLedger) SettleCompletedBlueprintRelease(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.Input,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	if !view.CurrentExecutor {
		return errs.New(errs.KindStateConflict, "Release completion requires its current executor")
	}
	return ledger.settleProvenBlueprintRelease(ctx, view, input, authorization, at)
}

// SettleInterruptedBlueprintRelease records a fully proven completion after
// replacement. No effect is replayed and no old identity is adopted.
func (ledger *ReleaseLedger) SettleInterruptedBlueprintRelease(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.Input,
	authorization agentregistration.WorkAuthorization,
	at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	identity := authorization.Identity()
	if view.CurrentExecutor || identity.Executor != workexecution.Agent ||
		identity.HostID != input.Attempt.Executor.HostID || identity.Generation <= input.Attempt.Executor.Generation {
		return errs.New(errs.KindStateConflict, "interrupted completion requires a replacement Agent on the same host")
	}
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	current, err := queue.ReadExecutor(ctx, view.Work.Item.Item.Placement)
	if err != nil {
		return err
	}
	if current.Revision != view.ExecutorRevision || current.Record.Identity != identity {
		return errs.New(errs.KindStateConflict, "replacement Agent changed before completion reconciliation")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ExecutorKey(identity), ModRevision: current.Revision})
	return ledger.settleProvenBlueprintRelease(ctx, view, input, guards, at)
}

func (ledger *ReleaseLedger) settleProvenBlueprintRelease(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.Input,
	authorization []keyvalue.Condition,
	at time.Time,
) error {
	if len(authorization) == 0 {
		return errs.New(errs.KindStateConflict, "Release completion requires authenticated executor comparisons")
	}
	completed, err := ledger.LoadBlueprintReleaseCompletion(ctx, view, input)
	if err != nil {
		return err
	}
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedReleaseAt(ctx, view)
	}
	if err != nil {
		return err
	}
	runtime, err := prepareBlueprintReleaseRuntime(input, completed, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(runtime.Mutations)
	history, err := ledger.prepareCompletedBlueprintReleaseHistory(ctx, view, input, completed, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(history.Mutations)
	configuration, err := ledger.ReleaseWorkConfigurationConditions(ctx, view.Work, completed.Configuration)
	if err != nil {
		return err
	}
	files := workqueue.Change{Conditions: configuration}
	if len(completed.Configuration.Destinations) != 0 &&
		(completed.Configuration.Prior == nil || completed.Configuration.Current != *completed.Configuration.Prior) {
		configurationValue, err := runtimeconfiguration.EncodeReference(completed.Configuration.Current)
		if err != nil {
			return err
		}
		defer clear(configurationValue)
		files.Mutations = append(files.Mutations, keyvalue.Mutation{
			Type: keyvalue.MutationPut, Key: runtimeconfiguration.HeadKey(input.Release.EnvironmentId), Value: configurationValue,
		})
	}
	// Recording actual serving state does not turn superseded/cancelled work
	// back into a successful Apply. Already-authorized effects remain visible.
	outcome := workexecution.Succeeded
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	request.At = at
	request.Result = workexecution.Completion{
		Outcome:        outcome,
		EffectsSettled: true,
		Evidence:       view.Checkpoint.Record.Evidence,
	}
	execution, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: authorization}, runtime, history, files, execution)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(
			errs.KindStateConflict,
			"Release completion changed before it could be saved; reload the same attempt",
		)
	}
	return nil
}
