package volumeremoval

import (
	"context"
	"time"

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
	runtime EnvironmentVolumeRemovalRuntimeRecord,
	task etcd.TaskRecord,
) (EnvironmentVolumeRemovalResumeState, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if runtime.Checkpoint != EnvironmentVolumeRemovalIntentSealed || runtime.AttemptOrdinal != 1 ||
		runtime.CurrentTaskID != task.ID || task.Status != etcd.TaskStatusPending {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal root state is invalid",
		)
	}
	attempt := EnvironmentVolumeRemovalAttemptRecord{
		OperationID: runtime.OperationID, OriginTaskID: runtime.OriginTaskID,
		TaskID: runtime.CurrentTaskID, Ordinal: 1, CreatedAt: runtime.CreatedAt,
	}
	if err := validateEnvironmentVolumeRemovalTask(task, runtime, attempt); err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	progress := EnvironmentVolumeRemovalPathProgress{
		OperationID: runtime.OperationID, NextRequestOrdinal: 1, UpdatedAt: runtime.CreatedAt,
	}
	runtimeValue, err := encodeEnvironmentVolumeRemovalRuntime(runtime)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	attemptValue, err := encodeEnvironmentVolumeRemovalAttempt(attempt)
	if err != nil {
		clear(runtimeValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	progressValue, err := encodeEnvironmentVolumeRemovalProgress(progress)
	if err != nil {
		clear(runtimeValue)
		clear(attemptValue)
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	defer clear(runtimeValue)
	defer clear(attemptValue)
	defer clear(progressValue)
	conditions := []etcd.Condition{
		{Key: environmentVolumeRemovalRuntimeKey(runtime.OperationID)},
		{Key: environmentVolumeRemovalAttemptKey(runtime.OperationID, 1)},
		{Key: environmentVolumeRemovalProgressKey(runtime.OperationID)},
		{Key: environmentVolumeRemovalPendingPathKey(runtime.OperationID)},
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
		Runtime: etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{
			Record: runtime, Revision: result.Revision, ReadRevision: result.Revision,
		},
		Attempt: attempt,
		Progress: etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{
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
		environmentVolumeRemovalRuntimeKey(operationID),
		environmentVolumeRemovalProgressKey(operationID),
		environmentVolumeRemovalPendingPathKey(operationID),
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
	runtime, err := decodeEnvironmentVolumeRemovalRuntime(state.Values[0].Value)
	if err != nil || runtime.OperationID != operationID {
		return EnvironmentVolumeRemovalResumeState{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	progress, err := decodeEnvironmentVolumeRemovalProgress(state.Values[1].Value)
	if err != nil || progress.OperationID != operationID {
		return EnvironmentVolumeRemovalResumeState{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	attemptRead, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys:     []string{environmentVolumeRemovalAttemptKey(operationID, runtime.AttemptOrdinal)},
		Revision: state.ReadRevision,
	})
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if attemptRead == nil || attemptRead.ReadRevision != state.ReadRevision ||
		len(attemptRead.Values) != 1 || attemptRead.Values[0] == nil {
		return EnvironmentVolumeRemovalResumeState{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	defer clearKeyValues(attemptRead.Values)
	attempt, err := decodeEnvironmentVolumeRemovalAttempt(attemptRead.Values[0].Value)
	if err != nil || attempt.OperationID != operationID || attempt.Ordinal != runtime.AttemptOrdinal ||
		attempt.TaskID != runtime.CurrentTaskID || attempt.OriginTaskID != runtime.OriginTaskID ||
		attempt.PredecessorTaskID != runtime.PredecessorTaskID {
		return EnvironmentVolumeRemovalResumeState{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	result := EnvironmentVolumeRemovalResumeState{
		Runtime: etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{
			Record: runtime, Revision: state.Values[0].ModRevision, ReadRevision: state.ReadRevision,
		},
		Attempt: attempt,
		Progress: etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{
			Record: progress, Revision: state.Values[1].ModRevision, ReadRevision: state.ReadRevision,
		},
	}
	if state.Values[2] != nil {
		pending, decodeErr := decodeEnvironmentVolumeRemovalPendingPath(state.Values[2].Value)
		if decodeErr != nil || pending.OperationID != operationID ||
			pending.RequestOrdinal != progress.NextRequestOrdinal || progress.DirectoryAbsent {
			return EnvironmentVolumeRemovalResumeState{}, corruptEnvironmentVolumeRemovalRuntime()
		}
		result.Pending = &etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{
			Record: pending, Revision: state.Values[2].ModRevision, ReadRevision: state.ReadRevision,
		}
	}
	return result, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) AdvanceCheckpoint(
	ctx context.Context,
	operationID string,
	next EnvironmentVolumeRemovalCheckpoint,
	at time.Time,
) (etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord], error) {
	state, err := repository.Resume(ctx, operationID)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	if next != state.Runtime.Record.Checkpoint+1 ||
		next > EnvironmentVolumeRemovalDesiredPublished {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, errs.New(
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
) (etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord], error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	if state.Runtime.Record.Checkpoint != EnvironmentVolumeRemovalDesiredPublished {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal consumers cannot be detached yet",
		)
	}
	return repository.advanceCheckpoint(
		ctx, state, EnvironmentVolumeRemovalConsumersDetached, at, &assignment,
	)
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) advanceCheckpoint(
	ctx context.Context,
	state EnvironmentVolumeRemovalResumeState,
	next EnvironmentVolumeRemovalCheckpoint,
	at time.Time,
	assignment *EnvironmentVolumeRemovalAssignment,
) (etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord], error) {
	updated := state.Runtime.Record
	updated.Checkpoint = next
	updated.UpdatedAt = at
	value, err := encodeEnvironmentVolumeRemovalRuntime(updated)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	defer clear(value)
	conditions := []etcd.Condition{{
		Key:         environmentVolumeRemovalRuntimeKey(updated.OperationID),
		ModRevision: state.Runtime.Revision,
	}}
	if assignment != nil {
		fence, fenceErr := repository.loadAssignmentFence(
			ctx, *assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
		)
		if fenceErr != nil {
			return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, fenceErr
		}
		conditions = append(conditions, fence...)
	}
	mutations := []etcd.Mutation{{
		Type: etcd.MutationPut, Key: environmentVolumeRemovalRuntimeKey(updated.OperationID), Value: value,
	}}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	if !result.Succeeded {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal checkpoint raced",
		)
	}
	return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{
		Record: updated, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) BeginPathCall(
	ctx context.Context,
	assignment EnvironmentVolumeRemovalAssignment,
	at time.Time,
) (etcd.Versioned[EnvironmentVolumeRemovalPendingPath], bool, error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	if state.Runtime.Record.Checkpoint != EnvironmentVolumeRemovalConsumersDetached ||
		state.Progress.Record.DirectoryAbsent {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal path is not mutable",
		)
	}
	if state.Pending != nil {
		pending := state.Pending.Record
		if pending.TaskID != assignment.TaskID || pending.AssignmentID != assignment.AssignmentID ||
			pending.AgentID != assignment.AgentID || pending.AgentGeneration != assignment.AgentGeneration {
			return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal has a pending call for another assignment",
			)
		}
		return *state.Pending, true, nil
	}
	if _, err := repository.loadAssignmentFence(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	pending := EnvironmentVolumeRemovalPendingPath{
		OperationID:     state.Runtime.Record.OperationID,
		VolumeID:        state.Runtime.Record.VolumeID,
		Key:             state.Runtime.Record.Key,
		IntentSHA256:    state.Runtime.Record.IntentSHA256,
		RequestOrdinal:  state.Progress.Record.NextRequestOrdinal,
		MutationBudget:  environmentVolumeRemovalMutationBudget,
		ComponentStack:  append([]string(nil), state.Progress.Record.ComponentStack...),
		Cursor:          append([]byte(nil), state.Progress.Record.Cursor...),
		TaskID:          assignment.TaskID,
		AssignmentID:    assignment.AssignmentID,
		AgentID:         assignment.AgentID,
		AgentGeneration: assignment.AgentGeneration,
		CreatedAt:       at,
	}
	pending.RequestSHA256 = environmentVolumeRemovalPathRequestDigest(pending)
	value, err := encodeEnvironmentVolumeRemovalPendingPath(pending)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	defer clear(value)
	conditions := []etcd.Condition{
		{Key: environmentVolumeRemovalRuntimeKey(assignment.OperationID), ModRevision: state.Runtime.Revision},
		{Key: environmentVolumeRemovalProgressKey(assignment.OperationID), ModRevision: state.Progress.Revision},
		{Key: environmentVolumeRemovalPendingPathKey(assignment.OperationID)},
	}
	fence, err := repository.loadAssignmentFence(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	conditions = append(conditions, fence...)
	mutations := []etcd.Mutation{{
		Type: etcd.MutationPut, Key: environmentVolumeRemovalPendingPathKey(assignment.OperationID), Value: value,
	}}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, err
	}
	if !result.Succeeded {
		reloaded, reloadErr := repository.Resume(ctx, assignment.OperationID)
		if reloadErr != nil || reloaded.Pending == nil ||
			!sameEnvironmentVolumeRemovalPendingPath(reloaded.Pending.Record, pending) {
			return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal path request raced",
			)
		}
		return *reloaded.Pending, true, nil
	}
	return etcd.Versioned[EnvironmentVolumeRemovalPendingPath]{
		Record: pending, Revision: result.Revision, ReadRevision: result.Revision,
	}, false, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) CompletePathCall(
	ctx context.Context,
	input EnvironmentVolumeRemovalPathResult,
) (etcd.Versioned[EnvironmentVolumeRemovalPathProgress], bool, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	state, err := repository.Resume(ctx, input.Assignment.OperationID)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	completion := EnvironmentVolumeRemovalPathCompletion{
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
	if err := validateEnvironmentVolumeRemovalCompletion(completion); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
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
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion does not match its pending call",
		)
	}
	if _, err := repository.loadAssignmentFence(
		ctx, input.Assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	progress := EnvironmentVolumeRemovalPathProgress{
		OperationID:        completion.OperationID,
		NextRequestOrdinal: completion.RequestOrdinal + 1,
		ComponentStack:     append([]string(nil), completion.NextComponentStack...),
		Cursor:             append([]byte(nil), completion.NextCursor...),
		DirectoryAbsent:    completion.DirectoryAbsent,
		UpdatedAt:          completion.CompletedAt,
	}
	if completion.RequestOrdinal == ^uint64(0) || validateEnvironmentVolumeRemovalProgress(progress) != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal progress is invalid",
		)
	}
	progressValue, err := encodeEnvironmentVolumeRemovalProgress(progress)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	completionValue, err := encodeEnvironmentVolumeRemovalCompletion(completion)
	if err != nil {
		clear(progressValue)
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	defer clear(progressValue)
	defer clear(completionValue)
	conditions := []etcd.Condition{
		{Key: environmentVolumeRemovalRuntimeKey(completion.OperationID), ModRevision: state.Runtime.Revision},
		{Key: environmentVolumeRemovalProgressKey(completion.OperationID), ModRevision: state.Progress.Revision},
		{Key: environmentVolumeRemovalPendingPathKey(completion.OperationID), ModRevision: state.Pending.Revision},
		{Key: environmentVolumeRemovalCompletionKey(completion.OperationID, completion.RequestOrdinal)},
	}
	fence, err := repository.loadAssignmentFence(
		ctx, input.Assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	conditions = append(conditions, fence...)
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: environmentVolumeRemovalProgressKey(completion.OperationID), Value: progressValue},
		{Type: etcd.MutationPut, Key: environmentVolumeRemovalCompletionKey(completion.OperationID, completion.RequestOrdinal), Value: completionValue},
		{Type: etcd.MutationDelete, Key: environmentVolumeRemovalPendingPathKey(completion.OperationID)},
	}
	if completion.DirectoryAbsent {
		if state.Runtime.Record.Checkpoint != EnvironmentVolumeRemovalConsumersDetached {
			return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, errs.New(
				errs.KindStateConflict,
				"Environment Volume removal directory checkpoint is invalid",
			)
		}
		updatedRuntime := state.Runtime.Record
		updatedRuntime.Checkpoint = EnvironmentVolumeRemovalDirectoryAbsent
		updatedRuntime.UpdatedAt = completion.CompletedAt
		runtimeValue, encodeErr := encodeEnvironmentVolumeRemovalRuntime(updatedRuntime)
		if encodeErr != nil {
			return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, encodeErr
		}
		defer clear(runtimeValue)
		mutations = append(mutations, etcd.Mutation{
			Type: etcd.MutationPut, Key: environmentVolumeRemovalRuntimeKey(completion.OperationID), Value: runtimeValue,
		})
	}
	if err := validateEnvironmentVolumeRemovalTransaction(repository.store, conditions, mutations, 32); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	if !result.Succeeded {
		return repository.replayPathCompletion(ctx, state, completion)
	}
	return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{
		Record: progress, Revision: result.Revision, ReadRevision: result.Revision,
	}, false, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) replayPathCompletion(
	ctx context.Context,
	state EnvironmentVolumeRemovalResumeState,
	completion EnvironmentVolumeRemovalPathCompletion,
) (etcd.Versioned[EnvironmentVolumeRemovalPathProgress], bool, error) {
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{
		environmentVolumeRemovalCompletionKey(completion.OperationID, completion.RequestOrdinal),
		environmentVolumeRemovalProgressKey(completion.OperationID),
	}})
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion evidence changed",
		)
	}
	defer clearKeyValues(read.Values)
	existing, err := decodeEnvironmentVolumeRemovalCompletion(read.Values[0].Value)
	if err != nil || !sameEnvironmentVolumeRemovalCompletion(existing, completion) {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal completion digest changed",
		)
	}
	progress, err := decodeEnvironmentVolumeRemovalProgress(read.Values[1].Value)
	if err != nil || progress.NextRequestOrdinal != completion.RequestOrdinal+1 ||
		progress.DirectoryAbsent != completion.DirectoryAbsent {
		return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{}, false, corruptEnvironmentVolumeRemovalRuntime()
	}
	return etcd.Versioned[EnvironmentVolumeRemovalPathProgress]{
		Record: progress, Revision: read.Values[1].ModRevision, ReadRevision: read.ReadRevision,
	}, true, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) FinalizeCheckpoint(
	ctx context.Context,
	assignment EnvironmentVolumeRemovalAssignment,
	status etcd.TaskStatus,
	result etcd.TaskResultRecord,
	at time.Time,
) (etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord], error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	if state.Runtime.Record.Checkpoint != EnvironmentVolumeRemovalDirectoryAbsent ||
		!state.Progress.Record.DirectoryAbsent || state.Pending != nil || status != etcd.TaskStatusCompleted {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal cannot be finalized",
		)
	}
	task, _, err := repository.loadAssignedTask(
		ctx, assignment, state.Runtime.ReadRevision, state.Runtime.Record, state.Attempt,
	)
	if err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	if err := validateEnvironmentVolumeRemovalTaskResult(result, task.Steps, status); err != nil {
		return etcd.Versioned[EnvironmentVolumeRemovalRuntimeRecord]{}, err
	}
	return repository.advanceCheckpoint(
		ctx, state, EnvironmentVolumeRemovalRuntimeFinalized, at, &assignment,
	)
}
