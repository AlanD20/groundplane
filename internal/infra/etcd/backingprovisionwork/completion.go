package backingprovisionwork

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingruntimepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"google.golang.org/protobuf/proto"
)

// PrepareCompleted joins the verified actual runtime, configuration and native
// attempt completion. New desired input cannot be substituted here. A racing
// cancellation can change the action outcome, not erase an acknowledged effect.
func (repository *Repository) PrepareCompleted(ctx context.Context, view workqueue.ResumeView,
	observation backingruntimefact.Observation, at time.Time,
) (prepared workqueue.Change, result workexecution.Input, returnErr error) {
	if !view.ActiveAttempt || !view.CanSettleResult() || view.Attempt.Attempt.Direction != workexecution.Forward ||
		view.Checkpoint.Record == nil || observation.ObservedAt.After(at) {
		return prepared, result, invalid("backing creation completion requires its current captured Agent execution")
	}
	input, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return prepared, result, err
	}
	defer clear(input.Wire)
	defer func() {
		if returnErr != nil {
			keyvalue.ClearMutationValues(prepared.Mutations)
		}
	}()
	procedure, err := input.Procedure()
	if err != nil {
		return prepared, result, err
	}
	progress, err := repository.LoadProgress(ctx, view)
	if err != nil {
		return prepared, result, err
	}
	if progress == nil || progress.Stage != backingprovision.Complete || progress.InFlight || progress.Last == nil ||
		progress.Last.ContainerID != observation.ContainerID || observation.ObservedAt.Before(progress.Last.At) {
		return prepared, result, invalid("backing creation has not proved every captured execution stage")
	}
	workload, err := backingruntimefact.Workload(
		procedure.Runtime,
		procedure.ServiceId,
		input.PlanID,
		procedure.RenderGeneration,
	)
	if err != nil {
		return prepared, result, err
	}
	if !backingruntimefact.MatchesObservation(observation, procedure.Runtime, workload) {
		return prepared, result, invalid("backing creation observed runtime differs from its captured workload")
	}
	projection, found, err := blueprints.ReadEffectiveProjectionRevision(ctx, repository.store,
		procedure.EnvironmentId, procedure.TaskId, view.Work.ReadRevision)
	if err != nil {
		return prepared, result, err
	}
	runtimeBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(procedure.Runtime)
	if err != nil {
		return prepared, result, err
	}
	defer clear(runtimeBytes)
	if !found || projection.Record.EnvironmentID != procedure.EnvironmentId ||
		projection.Record.RevisionID != procedure.TaskId ||
		projection.Record.RenderGeneration != procedure.RenderGeneration ||
		!bytes.Equal(projection.Record.ComposeArtifact, runtimeBytes) {
		return prepared, result, invalid("backing creation applied projection differs from its captured runtime")
	}
	if _, err := repository.LoadConfiguration(ctx, view.Work); err != nil {
		return prepared, result, err
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return prepared, result, err
	}
	configuration := procedure.Configuration
	reference := runtimeconfiguration.Reference{
		ID:            configuration.ConfigurationId,
		EnvironmentID: configuration.EnvironmentId,
		Generation:    configuration.Generation,
		SHA256:        hex.EncodeToString(configuration.Sha256),
	}
	fileConditions, fileMutation, err := sources.PrepareInitialAcknowledgement(ctx, reference, view.Work.ReadRevision)
	if err != nil {
		return prepared, result, err
	}
	prepared.Conditions = append(prepared.Conditions, guard)
	for _, condition := range fileConditions {
		prepared.Conditions = append(
			prepared.Conditions,
			keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision},
		)
	}
	prepared.Mutations = append(
		prepared.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: fileMutation.Key, Value: fileMutation.Value},
	)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return prepared, result, err
	}
	completion, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return prepared, result, err
	}
	evidence, err := backingprovision.CompletionEvidence(view.Attempt.Attempt, *view.Checkpoint.Record, observation)
	if err != nil {
		return prepared, result, err
	}
	outcome := workexecution.Succeeded
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	completion.At, completion.Result = at, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       evidence,
		EffectsSettled: true,
	}
	_, terminal, err := workexecution.Finish(view.Work.Item.Item, view.Attempt.Attempt, completion.Result, at)
	if err != nil {
		return prepared, result, err
	}
	receipt, err := recordcodec.Encode("backing-provision-completion", CompletionRecord{Attempt: terminal,
		Checkpoint: *view.Checkpoint.Record, Observation: observation})
	if err != nil {
		return prepared, result, err
	}
	prepared.Conditions = append(prepared.Conditions, keyvalue.Condition{Key: ResultKey(terminal.ID)})
	prepared.Mutations = append(
		prepared.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ResultKey(terminal.ID), Value: receipt},
	)
	applied := environmentprojection.CloneEnvironmentComposeProjection(projection.Record)
	if backingruntimepublication.IsManagedDatabase(applied, procedure.ServiceId) {
		conditions, err := backingruntimepublication.Prepare(ctx, repository.store, backingruntimepublication.Input{
			Source: environmentprojection.BackingRuntimeReceipt{ServiceID: procedure.ServiceId, Attempt: terminal,
				Checkpoint: *view.Checkpoint.Record, Observation: observation, LocalImageID: observation.LocalImageID,
				RenderGeneration: procedure.RenderGeneration, AcknowledgedAt: at}, CreatedAt: procedure.CreatedAt.AsTime(),
		}, &applied, view.Work.ReadRevision)
		if err != nil {
			return prepared, result, err
		}
		prepared.Conditions = append(prepared.Conditions, conditions...)
	}
	value, err := environmentprojection.EncodeEnvironmentComposeProjectionStorage(applied)
	if err != nil {
		return prepared, result, err
	}
	projectionKey := environmentprojection.EnvironmentComposeProjectionStorageKey(procedure.EnvironmentId)
	prepared.Conditions = append(prepared.Conditions, keyvalue.Condition{Key: projectionKey})
	prepared.Mutations = append(
		prepared.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: projectionKey, Value: value},
	)
	if applied.BackingRuntime != nil && applied.BackingRuntime.Adapter == "postgres" {
		backing, err := backingpostgresruntime.FromProvisioning(applied)
		if err != nil {
			return prepared, result, err
		}
		value, err := backingpostgresruntime.Encode(backing)
		if err != nil {
			return prepared, result, err
		}
		key := backingpostgresruntime.Key(procedure.EnvironmentId, procedure.ServiceId)
		prepared.Conditions = append(prepared.Conditions, keyvalue.Condition{Key: key})
		prepared.Mutations = append(
			prepared.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
		)
	}
	directories, err := repository.prepareDirectoryState(ctx, view.Work, input, true)
	if err != nil {
		return prepared, result, err
	}
	settled, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		keyvalue.ClearMutationValues(directories.Mutations)
		return prepared, result, err
	}
	prepared.Conditions = append(prepared.Conditions, directories.Conditions...)
	prepared.Conditions = append(prepared.Conditions, settled.Conditions...)
	prepared.Mutations = append(prepared.Mutations, directories.Mutations...)
	prepared.Mutations = append(prepared.Mutations, settled.Mutations...)
	return prepared, evidence, nil
}

func (repository *Repository) prepareDirectoryState(
	ctx context.Context,
	view workqueue.View,
	input Input,
	ready bool,
) (workqueue.Change, error) {
	key := hierarchy.EnvironmentKey(view.Task.Task.Scope.EnvironmentID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, invalid("backing creation directory acknowledgement lost its Environment")
	}
	defer keyvalue.ClearValues(read.Values)
	stored := read.Values[0]
	if stored == nil && !ready {
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}}}, nil
	}
	if stored == nil {
		return workqueue.Change{}, invalid("backing creation cannot acknowledge a removed Environment")
	}
	if stored.Key != key || stored.ModRevision <= 0 || stored.ModRevision > view.ReadRevision {
		return workqueue.Change{}, invalid("backing creation Environment revision is invalid")
	}
	current, err := hierarchy.DecodeEnvironment(stored.Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !ready && current.CreateTaskID != view.Task.Task.ID {
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: stored.ModRevision}}}, nil
	}
	if current.ID != input.Environment.ID || current.CreateTaskID != view.Task.Task.ID ||
		current.ProjectID != input.Environment.ProjectID ||
		current.VolumeDir != input.Environment.VolumeDir ||
		current.DeletionTaskID != "" {
		return workqueue.Change{}, invalid("backing creation directory no longer belongs to this action")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: stored.ModRevision}}}
	if current.ProvisioningState == hierarchy.EnvironmentProvisioningReady ||
		!ready && current.ProvisioningState == hierarchy.EnvironmentProvisioningFailed {
		return change, nil
	}
	if current.ProvisioningState == hierarchy.EnvironmentProvisioningFailed {
		current, err = hierarchy.RetryEnvironmentProvisioning(current, view.Task.Task.ID)
		if err != nil {
			return workqueue.Change{}, err
		}
	}
	next, err := hierarchy.CompleteEnvironmentProvisioning(current, view.Task.Task.ID, ready)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := hierarchy.EncodeEnvironment(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
