package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleRecoveredBlueprintRelease releases claims only after all captured
// restoration proofs and unchanged applied-state heads are compared atomically.
// Recovery records the failed Release without overwriting the original action
// outcome. A proved leave_active switch updates serving authority, not success.
func (ledger *ReleaseLedger) SettleRecoveredBlueprintRelease(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization []keyvalue.Condition, at time.Time,
) error {
	if !view.CurrentExecutor {
		return errs.New(errs.KindStateConflict, "Recovery completion requires its current executor")
	}
	return ledger.settleProvenBlueprintRecovery(ctx, view, input, authorization, at)
}

func (ledger *ReleaseLedger) settleProvenBlueprintRecovery(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization []keyvalue.Condition, at time.Time,
) error {
	if ledger == nil || ledger.store == nil || len(authorization) == 0 || input.Validate() != nil ||
		!workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) {
		return errs.New(
			errs.KindStateConflict,
			"Release recovery completion requires its authenticated captured attempt",
		)
	}
	sources, err := ledger.LoadReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	release := input.Input.Release
	configuration := sources.Input.Configuration.Current
	selected := release.Configuration
	if len(sources.Predecessors) != 1 || sources.Predecessors[0].ServiceID != release.ServiceId ||
		release.ServiceId != sources.Rollout.Record.Render.ServiceID ||
		release.ReleaseId != sources.Rollout.Record.Render.ReleaseID ||
		release.EnvironmentId != sources.Rollout.Record.Scope.EnvironmentID ||
		release.OperationId != sources.Input.OperationID ||
		release.RenderGeneration != sources.Rollout.Record.Render.Projection.RenderGeneration ||
		selected == nil || selected.ConfigurationId != configuration.ID ||
		selected.EnvironmentId != configuration.EnvironmentID || selected.Generation != configuration.Generation ||
		hex.EncodeToString(selected.Sha256) != configuration.SHA256 {
		return errs.New(errs.KindStateConflict, "Release recovery differs from its published input")
	}
	proof, err := releasework.LoadRecoveryCompletionProof(ctx, ledger.store, view, input)
	if err != nil {
		return err
	}
	if at.Before(proof.Progress.Record.Report.Progress.UpdatedAt) {
		return errs.New(errs.KindStateConflict, "Release recovery completion precedes its restoration proof")
	}
	scripts, err := NewScriptRepository(ledger.store)
	if err != nil {
		return err
	}
	hooks, err := scripts.PrepareReleaseHookRecovery(ctx, view.Work, input, proof.Evidence[0].Result.Quiescence,
		*proof.Progress.Record.Proofs[0], at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(hooks.Mutations)
	files, err := ledger.ReleaseWorkConfigurationConditions(ctx, view.Work, sources.Input.Configuration)
	if err != nil {
		return err
	}
	predecessor := sources.Predecessors[0]
	conditions := append(sources.Conditions, proof.Conditions...)
	conditions = append(conditions, files...)
	conditions = append(
		conditions,
		keyvalue.Condition{
			Key:         releases.ReleaseProjectionKey(predecessor.ServiceID),
			ModRevision: predecessor.ProjectionRevision,
		},
		keyvalue.Condition{
			Key:         serviceruntimerecord.Key(predecessor.ServiceID),
			ModRevision: predecessor.RuntimeRevision,
		},
	)
	runtime, history, err := ledger.prepareRecoveredBlueprintRuntime(ctx, view, input, sources, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(runtime.Mutations)
	defer keyvalue.ZeroMutationBytes(history.Mutations)
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedRecoveryAt(ctx, view)
	}
	if err != nil {
		return err
	}
	request.At = at
	request.Result = workexecution.Completion{
		Outcome: workexecution.Succeeded, EffectsSettled: true, Evidence: view.Checkpoint.Record.Evidence,
	}
	execution, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	committed, err := queue.Commit(
		ctx,
		workqueue.Change{Conditions: authorization},
		workqueue.Change{Conditions: conditions},
		hooks,
		runtime,
		history,
		execution,
	)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Release recovery state changed; reload the same attempt")
	}
	return nil
}
