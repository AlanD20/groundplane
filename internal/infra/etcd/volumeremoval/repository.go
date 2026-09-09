package volumeremoval

import (
	"context"
	"time"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*etcd.GetResult, error)
	GetMany(context.Context, etcd.GetManyRequest) (*etcd.GetManyResult, error)
	Transact(context.Context, []etcd.Condition, []etcd.Mutation) (etcd.TransactionResult, error)
}

type EnvironmentVolumeRemovalRuntimeRepository struct {
	store store
}

func NewEnvironmentVolumeRemovalRuntimeRepository(
	backend etcd.Store,
) (*EnvironmentVolumeRemovalRuntimeRepository, error) {
	return newEnvironmentVolumeRemovalRuntimeRepository(backend)
}

func newEnvironmentVolumeRemovalRuntimeRepository(
	backend store,
) (*EnvironmentVolumeRemovalRuntimeRepository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Environment Volume removal runtime store is required")
	}
	return &EnvironmentVolumeRemovalRuntimeRepository{store: backend}, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) Create(
	ctx context.Context,
	runtime removalrecord.Runtime,
	task etcd.TaskRecord,
) (EnvironmentVolumeRemovalResumeState, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if runtime.Checkpoint != removalrecord.IntentSealed || runtime.AttemptOrdinal != 1 ||
		runtime.CurrentTaskID != task.ID || task.Status != etcd.TaskStatusPending {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal root state is invalid",
		)
	}
	attempt := removalrecord.Attempt{
		OperationID: runtime.OperationID, OriginTaskID: runtime.OriginTaskID,
		TaskID: runtime.CurrentTaskID, Ordinal: 1, CreatedAt: runtime.CreatedAt,
	}
	if err := validateEnvironmentVolumeRemovalTask(task, runtime, attempt); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	progress := removalrecord.Progress{
		OperationID: runtime.OperationID, NextRequestOrdinal: 1, UpdatedAt: runtime.CreatedAt,
	}
	runtimeValue, err := removalrecord.EncodeRuntime(runtime)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		clear(runtimeValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		clear(runtimeValue)
		clear(attemptValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	defer clear(runtimeValue)
	defer clear(attemptValue)
	defer clear(progressValue)
	conditions := []etcd.Condition{
		{Key: removalrecord.RuntimeKey(runtime.OperationID)},
		{Key: removalrecord.AttemptKey(runtime.OperationID, 1)},
		{Key: removalrecord.ProgressKey(runtime.OperationID)},
		{Key: removalrecord.PendingPathKey(runtime.OperationID)},
	}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: conditions[0].Key, Value: runtimeValue},
		{Type: etcd.MutationPut, Key: conditions[1].Key, Value: attemptValue},
		{Type: etcd.MutationPut, Key: conditions[2].Key, Value: progressValue},
	}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 48); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if !result.Succeeded {
		existing, resumeErr := repository.Resume(ctx, runtime.OperationID)
		if resumeErr != nil {
			return EnvironmentVolumeRemovalResumeState{}, resumeErr
		}
		if !sameEnvironmentVolumeRemovalRuntime(existing.Runtime.Record, runtime) ||
			existing.Attempt != attempt || !sameEnvironmentVolumeRemovalProgress(existing.Progress.Record, progress) {
			return EnvironmentVolumeRemovalResumeState{}, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal operation already exists with different evidence",
			)
		}
		return existing, nil
	}
	return EnvironmentVolumeRemovalResumeState{
		Runtime: etcd.Versioned[removalrecord.Runtime]{
			Record: runtime, Revision: result.Revision, ReadRevision: result.Revision,
		},
		Attempt: attempt,
		Progress: etcd.Versioned[removalrecord.Progress]{
			Record: progress, Revision: result.Revision, ReadRevision: result.Revision,
		},
	}, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) Resume(
	ctx context.Context,
	operationID string,
) (EnvironmentVolumeRemovalResumeState, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if ids.Validate(ids.KindOperation, operationID) != nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal operation id is invalid",
		)
	}
	keys := []string{
		removalrecord.RuntimeKey(operationID),
		removalrecord.ProgressKey(operationID),
		removalrecord.PendingPathKey(operationID),
	}
	state, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if state == nil || state.ReadRevision <= 0 || len(state.Values) != len(keys) ||
		state.Values[0] == nil || state.Values[1] == nil {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal runtime is incomplete",
		)
	}
	defer clearKeyValues(state.Values)
	runtime, err := removalrecord.DecodeRuntime(state.Values[0].Value)
	if err != nil || runtime.OperationID != operationID {
		return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
	}
	progress, err := removalrecord.DecodeProgress(state.Values[1].Value)
	if err != nil || progress.OperationID != operationID {
		return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
	}
	attemptRead, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys:     []string{removalrecord.AttemptKey(operationID, runtime.AttemptOrdinal)},
		Revision: state.ReadRevision,
	})
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if attemptRead == nil || attemptRead.ReadRevision != state.ReadRevision ||
		len(attemptRead.Values) != 1 || attemptRead.Values[0] == nil {
		return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
	}
	defer clearKeyValues(attemptRead.Values)
	attempt, err := removalrecord.DecodeAttempt(attemptRead.Values[0].Value)
	if err != nil || attempt.OperationID != operationID || attempt.Ordinal != runtime.AttemptOrdinal ||
		attempt.TaskID != runtime.CurrentTaskID || attempt.OriginTaskID != runtime.OriginTaskID ||
		attempt.PredecessorTaskID != runtime.PredecessorTaskID {
		return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
	}
	result := EnvironmentVolumeRemovalResumeState{
		Runtime: etcd.Versioned[removalrecord.Runtime]{
			Record: runtime, Revision: state.Values[0].ModRevision, ReadRevision: state.ReadRevision,
		},
		Attempt: attempt,
		Progress: etcd.Versioned[removalrecord.Progress]{
			Record: progress, Revision: state.Values[1].ModRevision, ReadRevision: state.ReadRevision,
		},
	}
	if state.Values[2] != nil {
		pending, decodeErr := removalrecord.DecodePendingPath(state.Values[2].Value)
		if decodeErr != nil || pending.OperationID != operationID ||
			pending.RequestOrdinal != progress.NextRequestOrdinal || progress.DirectoryAbsent {
			return EnvironmentVolumeRemovalResumeState{}, removalrecord.Corrupt()
		}
		result.Pending = &etcd.Versioned[removalrecord.PendingPath]{
			Record: pending, Revision: state.Values[2].ModRevision, ReadRevision: state.ReadRevision,
		}
	}
	return result, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) AdvanceCheckpoint(
	ctx context.Context,
	operationID string,
	next removalrecord.Checkpoint,
	at time.Time,
) (etcd.Versioned[removalrecord.Runtime], error) {
	state, err := repository.Resume(ctx, operationID)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	if next != state.Runtime.Record.Checkpoint+1 ||
		next > removalrecord.DesiredPublished {
		return etcd.Versioned[removalrecord.Runtime]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal checkpoint transition is invalid",
		)
	}
	return repository.advanceCheckpoint(ctx, state, next, at, nil)
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) MarkConsumersDetached(
	ctx context.Context,
	assignment EnvironmentVolumeRemovalAssignment,
	at time.Time,
) (etcd.Versioned[removalrecord.Runtime], error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	if state.Runtime.Record.Checkpoint != removalrecord.DesiredPublished {
		return etcd.Versioned[removalrecord.Runtime]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal consumers cannot be detached yet",
		)
	}
	return repository.advanceCheckpoint(
		ctx, state, removalrecord.ConsumersDetached, at, &assignment,
	)
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) advanceCheckpoint(
	ctx context.Context,
	state EnvironmentVolumeRemovalResumeState,
	next removalrecord.Checkpoint,
	at time.Time,
	assignment *EnvironmentVolumeRemovalAssignment,
) (etcd.Versioned[removalrecord.Runtime], error) {
	updated := state.Runtime.Record
	updated.Checkpoint = next
	updated.UpdatedAt = at
	value, err := removalrecord.EncodeRuntime(updated)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	defer clear(value)
	conditions := []etcd.Condition{{
		Key:         removalrecord.RuntimeKey(updated.OperationID),
		ModRevision: state.Runtime.Revision,
	}}
	if assignment != nil {
		fence, fenceErr := repository.loadAssignmentFence(
			ctx, *assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
		)
		if fenceErr != nil {
			return etcd.Versioned[removalrecord.Runtime]{}, fenceErr
		}
		conditions = append(conditions, fence...)
	}
	mutations := []etcd.Mutation{{
		Type: etcd.MutationPut, Key: removalrecord.RuntimeKey(updated.OperationID), Value: value,
	}}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	if !result.Succeeded {
		return etcd.Versioned[removalrecord.Runtime]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal checkpoint raced",
		)
	}
	return etcd.Versioned[removalrecord.Runtime]{
		Record: updated, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) BeginPathCall(
	ctx context.Context,
	assignment EnvironmentVolumeRemovalAssignment,
	at time.Time,
) (etcd.Versioned[removalrecord.PendingPath], bool, error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	if state.Runtime.Record.Checkpoint != removalrecord.ConsumersDetached ||
		state.Progress.Record.DirectoryAbsent {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal path is not mutable",
		)
	}
	if state.Pending != nil {
		pending := state.Pending.Record
		if pending.TaskID != assignment.TaskID || pending.AssignmentID != assignment.AssignmentID ||
			pending.AgentID != assignment.AgentID || pending.AgentGeneration != assignment.AgentGeneration {
			return etcd.Versioned[removalrecord.PendingPath]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal has a pending call for another assignment",
			)
		}
		return *state.Pending, true, nil
	}
	if _, err := repository.loadAssignmentFence(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	); err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	pending := removalrecord.PendingPath{
		OperationID:     state.Runtime.Record.OperationID,
		VolumeID:        state.Runtime.Record.VolumeID,
		Key:             state.Runtime.Record.Key,
		IntentSHA256:    state.Runtime.Record.IntentSHA256,
		RequestOrdinal:  state.Progress.Record.NextRequestOrdinal,
		MutationBudget:  removalrecord.MutationBudget,
		ComponentStack:  append([]string(nil), state.Progress.Record.ComponentStack...),
		Cursor:          append([]byte(nil), state.Progress.Record.Cursor...),
		TaskID:          assignment.TaskID,
		AssignmentID:    assignment.AssignmentID,
		AgentID:         assignment.AgentID,
		AgentGeneration: assignment.AgentGeneration,
		CreatedAt:       at,
	}
	pending.RequestSHA256 = removalrecord.PathRequestDigest(pending)
	value, err := removalrecord.EncodePendingPath(pending)
	if err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	defer clear(value)
	conditions := []etcd.Condition{
		{Key: removalrecord.RuntimeKey(assignment.OperationID), ModRevision: state.Runtime.Revision},
		{Key: removalrecord.ProgressKey(assignment.OperationID), ModRevision: state.Progress.Revision},
		{Key: removalrecord.PendingPathKey(assignment.OperationID)},
	}
	fence, err := repository.loadAssignmentFence(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	conditions = append(conditions, fence...)
	mutations := []etcd.Mutation{{
		Type: etcd.MutationPut, Key: removalrecord.PendingPathKey(assignment.OperationID), Value: value,
	}}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[removalrecord.PendingPath]{}, false, err
	}
	if !result.Succeeded {
		reloaded, reloadErr := repository.Resume(ctx, assignment.OperationID)
		if reloadErr != nil || reloaded.Pending == nil ||
			!sameEnvironmentVolumeRemovalPendingPath(reloaded.Pending.Record, pending) {
			return etcd.Versioned[removalrecord.PendingPath]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal path request raced",
			)
		}
		return *reloaded.Pending, true, nil
	}
	return etcd.Versioned[removalrecord.PendingPath]{
		Record: pending, Revision: result.Revision, ReadRevision: result.Revision,
	}, false, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) CompletePathCall(
	ctx context.Context,
	input EnvironmentVolumeRemovalPathResult,
) (etcd.Versioned[removalrecord.Progress], bool, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	state, err := repository.Resume(ctx, input.Assignment.OperationID)
	if err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	completion := removalrecord.Completion{
		OperationID:        input.Assignment.OperationID,
		RequestOrdinal:     input.RequestOrdinal,
		RequestSHA256:      input.RequestSHA256,
		ResponseSHA256:     input.ResponseSHA256,
		ResponseBytes:      input.ResponseBytes,
		MutationCount:      input.MutationCount,
		NextComponentStack: append([]string(nil), input.NextComponentStack...),
		NextCursor:         append([]byte(nil), input.NextCursor...),
		DirectoryAbsent:    input.DirectoryAbsent,
		CompletedAt:        input.CompletedAt,
	}
	if err := removalrecord.ValidateCompletion(completion); err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	if state.Pending == nil {
		return repository.replayPathCompletion(ctx, state, completion)
	}
	pending := state.Pending.Record
	if pending.OperationID != completion.OperationID || pending.RequestOrdinal != completion.RequestOrdinal ||
		pending.RequestSHA256 != completion.RequestSHA256 ||
		pending.TaskID != input.Assignment.TaskID || pending.AssignmentID != input.Assignment.AssignmentID ||
		pending.AgentID != input.Assignment.AgentID ||
		pending.AgentGeneration != input.Assignment.AgentGeneration {
		return etcd.Versioned[removalrecord.Progress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion does not match its pending call",
		)
	}
	if _, err := repository.loadAssignmentFence(
		ctx, input.Assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	); err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	progress := removalrecord.Progress{
		OperationID:        completion.OperationID,
		NextRequestOrdinal: completion.RequestOrdinal + 1,
		ComponentStack:     append([]string(nil), completion.NextComponentStack...),
		Cursor:             append([]byte(nil), completion.NextCursor...),
		DirectoryAbsent:    completion.DirectoryAbsent,
		UpdatedAt:          completion.CompletedAt,
	}
	if completion.RequestOrdinal == ^uint64(0) || removalrecord.ValidateProgress(progress) != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal progress is invalid",
		)
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	completionValue, err := removalrecord.EncodeCompletion(completion)
	if err != nil {
		clear(progressValue)
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	defer clear(progressValue)
	defer clear(completionValue)
	conditions := []etcd.Condition{
		{Key: removalrecord.RuntimeKey(completion.OperationID), ModRevision: state.Runtime.Revision},
		{Key: removalrecord.ProgressKey(completion.OperationID), ModRevision: state.Progress.Revision},
		{Key: removalrecord.PendingPathKey(completion.OperationID), ModRevision: state.Pending.Revision},
		{Key: removalrecord.CompletionKey(completion.OperationID, completion.RequestOrdinal)},
	}
	fence, err := repository.loadAssignmentFence(
		ctx, input.Assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	conditions = append(conditions, fence...)
	mutations := []etcd.Mutation{
		{
			Type:  etcd.MutationPut,
			Key:   removalrecord.ProgressKey(completion.OperationID),
			Value: progressValue,
		},
		{
			Type:  etcd.MutationPut,
			Key:   removalrecord.CompletionKey(completion.OperationID, completion.RequestOrdinal),
			Value: completionValue,
		},
		{Type: etcd.MutationDelete, Key: removalrecord.PendingPathKey(completion.OperationID)},
	}
	if completion.DirectoryAbsent {
		if state.Runtime.Record.Checkpoint != removalrecord.ConsumersDetached {
			return etcd.Versioned[removalrecord.Progress]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal directory checkpoint is invalid",
			)
		}
		updatedRuntime := state.Runtime.Record
		updatedRuntime.Checkpoint = removalrecord.DirectoryAbsent
		updatedRuntime.UpdatedAt = completion.CompletedAt
		runtimeValue, encodeErr := removalrecord.EncodeRuntime(updatedRuntime)
		if encodeErr != nil {
			return etcd.Versioned[removalrecord.Progress]{}, false, encodeErr
		}
		defer clear(runtimeValue)
		mutations = append(mutations, etcd.Mutation{
			Type: etcd.MutationPut, Key: removalrecord.RuntimeKey(completion.OperationID), Value: runtimeValue,
		})
	}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	if !result.Succeeded {
		return repository.replayPathCompletion(ctx, state, completion)
	}
	return etcd.Versioned[removalrecord.Progress]{
		Record: progress, Revision: result.Revision, ReadRevision: result.Revision,
	}, false, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) replayPathCompletion(
	ctx context.Context,
	state EnvironmentVolumeRemovalResumeState,
	completion removalrecord.Completion,
) (etcd.Versioned[removalrecord.Progress], bool, error) {
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{
		removalrecord.CompletionKey(completion.OperationID, completion.RequestOrdinal),
		removalrecord.ProgressKey(completion.OperationID),
	}})
	if err != nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return etcd.Versioned[removalrecord.Progress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion evidence changed",
		)
	}
	defer clearKeyValues(read.Values)
	existing, err := removalrecord.DecodeCompletion(read.Values[0].Value)
	if err != nil || !sameEnvironmentVolumeRemovalCompletion(existing, completion) {
		return etcd.Versioned[removalrecord.Progress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion digest changed",
		)
	}
	progress, err := removalrecord.DecodeProgress(read.Values[1].Value)
	if err != nil || progress.NextRequestOrdinal != completion.RequestOrdinal+1 ||
		progress.DirectoryAbsent != completion.DirectoryAbsent {
		return etcd.Versioned[removalrecord.Progress]{}, false, removalrecord.Corrupt()
	}
	return etcd.Versioned[removalrecord.Progress]{
		Record: progress, Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision,
	}, true, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) FinalizeCheckpoint(
	ctx context.Context,
	assignment EnvironmentVolumeRemovalAssignment,
	status etcd.TaskStatus,
	result etcd.TaskResultRecord,
	at time.Time,
) (etcd.Versioned[removalrecord.Runtime], error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	if state.Runtime.Record.Checkpoint != removalrecord.DirectoryAbsent ||
		!state.Progress.Record.DirectoryAbsent || state.Pending != nil || status != etcd.TaskStatusCompleted {
		return etcd.Versioned[removalrecord.Runtime]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal cannot be finalized",
		)
	}
	task, _, err := repository.loadAssignedTask(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	if err := validateEnvironmentVolumeRemovalTaskResult(result, task.Steps, status); err != nil {
		return etcd.Versioned[removalrecord.Runtime]{}, err
	}
	return repository.advanceCheckpoint(
		ctx, state, removalrecord.RuntimeFinalized, at, &assignment,
	)
}
