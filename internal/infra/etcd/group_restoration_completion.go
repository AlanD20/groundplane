package etcd

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleCompletedGroupRestoration joins actual restoration proof, resource
// state and native work completion. The successful source Release remains
// immutable history: it really served before a later member failed.
func (ledger *ReleaseLedger) SettleCompletedGroupRestoration(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization []keyvalue.Condition, at time.Time,
) error {
	if ledger == nil || ledger.store == nil || len(authorization) == 0 || !view.CanSettleResult() ||
		input.Group == nil || input.Validate() != nil || !workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) {
		return errs.New(errs.KindStateConflict, "Group restoration completion requires its authenticated captured attempt")
	}
	return ledger.settleProvenGroupRestoration(ctx, view, input, authorization, at)
}

func (ledger *ReleaseLedger) settleProvenGroupRestoration(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, authorization []keyvalue.Condition, at time.Time,
) error {
	sources, err := ledger.LoadGroupRestorationSources(ctx, view.Work)
	if err != nil {
		return err
	}
	defer sources.Clear()
	if input.Group == nil || *input.Group != sources.Restoration.Input.Authority ||
		!workexecution.SameAssignment(input.Forward, sources.Restoration.Input.Source) ||
		input.Input.Release == nil || input.Input.Release.ServiceId != sources.Release.Intent.ServiceID ||
		input.Input.Release.ReleaseId != sources.Release.Intent.ID || input.Input.Release.OperationId != sources.Release.Intent.OperationID {
		return errs.New(errs.KindStateConflict, "Group restoration completion differs from its captured successful member")
	}
	state, err := ledger.groupRestorationState(ctx, sources, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	proof, err := releasework.LoadRecoveryCompletionProof(ctx, ledger.store, view, input)
	if err != nil {
		return err
	}
	if at.Before(proof.Progress.Record.Report.Progress.UpdatedAt) {
		return errs.New(errs.KindStateConflict, "Group restoration completion precedes its verified effects")
	}
	scripts, err := NewScriptRepository(ledger.store)
	if err != nil {
		return err
	}
	hooks, err := scripts.PrepareReleaseHookRecovery(ctx, sources.Source.Source, input,
		proof.Evidence[0].Result.Quiescence, *proof.Progress.Record.Proofs[0], at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(hooks.Mutations)
	resources, err := prepareGroupRestoredResources(view, input, sources, state, at)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(resources.Mutations)
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return err
	}
	var request workqueue.Completion
	if view.CanSettleResult() {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedGroupRestorationAt(ctx, view)
	}
	if err != nil {
		return err
	}
	request.At, request.Result = at, workexecution.Completion{Outcome: workexecution.Succeeded,
		Evidence: view.Checkpoint.Record.Evidence, EffectsSettled: true}
	execution, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(execution.Mutations)
	conditions, err := resourcePublicationConditions(authorization, sources.Conditions, proof.Conditions, state.conditions)
	if err != nil {
		return err
	}
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: conditions}, resources, hooks, execution)
	keyvalue.ClearValues(result.FailureReads)
	if err == nil && !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Group restoration authority changed before atomic completion")
	}
	return err
}

func prepareGroupRestoredResources(view workqueue.ResumeView, input releaseexecution.RecoveryInput,
	sources GroupRestorationSources, state groupRestorationState, at time.Time,
) (workqueue.Change, error) {
	if len(sources.Release.Predecessors) != 1 || view.Checkpoint.Record == nil ||
		state.projection.Revision == math.MaxUint64 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group restoration has no captured predecessor or available resource revision")
	}
	predecessor := sources.Release.Predecessors[0]
	projection := state.projection
	projection.ServingReleaseID, projection.ServingSlot = "", ""
	projection.CurrentSuccessfulReleaseID = sources.Release.Intent.PriorSuccessfulReleaseID
	projection.ActiveOperationID = ""
	projection.Revision++
	if predecessor.Serving != nil {
		projection.ServingReleaseID = predecessor.Serving.ServingReleaseID
		projection.ServingSlot = sources.Release.Rollout.Record.Render.PriorSlot
	}
	value, err := releases.EncodeReleaseRecord("service-release-projection", projection)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: releases.ReleaseProjectionKey(projection.ServiceID), Value: value},
	}}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	if predecessor.Serving == nil {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: serviceruntimerecord.Key(projection.ServiceID)})
	} else {
		attempt := input.Input.Attempt
		record, err := serviceruntimerecord.Restore(projection.EnvironmentID, projection.ServiceID, projection.ServingReleaseID,
			string(predecessor.Serving.Target), predecessor.CurrentArtifact, predecessor.RetainedPriorArtifact,
			serviceruntimerecord.Acknowledgement{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
				Executor: attempt.Executor, Input: attempt.Input, Checkpoint: *view.Checkpoint.Record,
				RenderGeneration: input.Input.Release.RenderGeneration, AcknowledgedAt: at})
		if err != nil {
			return workqueue.Change{}, err
		}
		record.Configuration = sources.Release.Input.Configuration.Prior
		if err := serviceruntimerecord.Validate(record); err != nil {
			return workqueue.Change{}, err
		}
		value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: serviceruntimerecord.Key(projection.ServiceID), Value: value})
	}
	configuration := sources.Release.Input.Configuration
	if configuration.Prior == nil && len(configuration.Destinations) != 0 {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: runtimeconfiguration.HeadKey(projection.EnvironmentID)})
	} else if configuration.Prior != nil && *configuration.Prior != configuration.Current {
		value, err := runtimeconfiguration.EncodeReference(*configuration.Prior)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runtimeconfiguration.HeadKey(projection.EnvironmentID), Value: value})
	}
	keep = true
	return change, nil
}
