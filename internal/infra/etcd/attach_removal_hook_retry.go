package etcd

import (
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backinghooks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type InterruptedDetachRetry struct {
	TaskID   string
	AttachID string
	StepID   string
}

type interruptedDetachRetrySource struct {
	parent     keyvalue.Versioned[TaskRecord]
	current    TaskAssignment
	checkpoint *keyvalue.KeyValue
	record     backinghooks.CheckpointRecord
}

// The visible Apply owns the operator action. Its hidden child stays hidden,
// keeps its original Task/plan/inputs and resumes only its unfinished suffix.
func (repository *TaskRepository) GetInterruptedDetachRetry(
	ctx context.Context,
	parentID string,
) (*InterruptedDetachRetry, error) {
	source, err := repository.interruptedDetachRetrySource(ctx, parentID)
	if err != nil || source == nil {
		return nil, err
	}
	defer clear(source.checkpoint.Value)
	return &InterruptedDetachRetry{TaskID: source.current.Task.Record.ID, AttachID: source.current.Task.Record.Target,
		StepID: source.record.StepID}, nil
}

func (repository *TaskRepository) interruptedDetachRetrySource(
	ctx context.Context,
	parentID string,
) (*interruptedDetachRetrySource, error) {
	parent, err := repository.GetTask(ctx, parentID)
	if err != nil {
		return nil, err
	}
	if parent.Record.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Record.Status != taskjournal.TaskStatusRunning {
		return nil, nil
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, parent.Record.Owner.EnvironmentID, parent.ReadRevision)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(snapshot.Executions, func(a, b keyvalue.Versioned[blueprintunits.ExecutionRecord]) int {
		if a.Record.TaskID < b.Record.TaskID {
			return -1
		}
		if a.Record.TaskID > b.Record.TaskID {
			return 1
		}
		return 0
	})
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID != parentID || !execution.Record.Unit.Removal ||
			execution.Record.Unit.Target.Kind != ids.KindAttach {
			continue
		}
		current, err := repository.GetTaskAssignment(ctx, execution.Record.TaskID)
		if err != nil {
			return nil, err
		}
		assignment := current.Assignment.Record
		if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
			assignment.AttachRemovalAuthoritySHA256 == "" {
			continue
		}
		recovery, value, err := repository.readAttachRemovalRecovery(
			ctx,
			current.Task.Record,
			assignment,
			current.Task.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		clear(value.Value)
		if int(recovery.Cursor) >= len(recovery.Authority.StepIDs) {
			continue
		}
		stepID := recovery.Authority.StepIDs[recovery.Cursor]
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{
				Keys:     []string{backinghooks.CheckpointKey(assignment.TaskID, stepID)},
				Revision: current.Task.ReadRevision,
			},
		)
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != current.Task.ReadRevision || len(read.Values) != 1 {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if read.Values[0] == nil {
			continue
		}
		checkpoint := read.Values[0]
		record, err := recordcodec.Decode[backinghooks.CheckpointRecord](checkpoint.Value, "backing-hook-checkpoint")
		if err != nil || backinghooks.ValidateCheckpointRecord(record) != nil {
			clear(checkpoint.Value)
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if record.State != backinghooks.CheckpointStarted || record.Event != "detach" {
			clear(checkpoint.Value)
			continue
		}
		if record.TaskID != assignment.TaskID || record.StepID != stepID ||
			record.AssignmentID != assignment.AssignmentID ||
			record.PlanHash != current.Task.Record.PlanHash ||
			record.OperationID != current.Task.Record.OperationID ||
			record.AttachID != current.Task.Record.Target ||
			record.ExecutionEpoch > assignment.ExecutionEpoch {
			clear(checkpoint.Value)
			return nil, taskassignments.CorruptTaskAssignment()
		}
		// A durable recovery outcome, not a stale heartbeat or an absent RESULT,
		// proves the current attempt has stopped before another is authorized.
		journal, err := repository.ListTaskEvents(ctx, assignment.TaskID, current.Task.ReadRevision)
		if err != nil {
			clear(checkpoint.Value)
			return nil, err
		}
		stopped := func(identity taskjournal.TaskEventIdentity, state taskjournal.TaskEventState) bool {
			return identity.AssignmentID == assignment.AssignmentID && identity.AgentID == assignment.AgentID &&
				identity.AgentGeneration == assignment.AgentGeneration && identity.Attempt == assignment.ExecutionEpoch &&
				identity.StepID == stepID && identity.Ordinal == 2 && (state == taskjournal.TaskEventStateFailed ||
				state == taskjournal.TaskEventStateTimedOut || state == taskjournal.TaskEventStateAborted)
		}
		found := false
		for _, event := range journal.Events {
			found = found || stopped(event.Identity, event.State)
		}
		for _, event := range journal.Task.EventCheckpoints {
			found = found || stopped(event.Identity, event.State)
		}
		if !found {
			clear(checkpoint.Value)
			continue
		}
		return &interruptedDetachRetrySource{
			parent:     parent,
			current:    current,
			checkpoint: checkpoint,
			record:     record,
		}, nil
	}
	return nil, nil
}

func (repository *TaskRepository) RetryInterruptedDetach(
	ctx context.Context,
	parentID string,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	bounded, release, err := repository.lockAgentDispatch(ctx)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer release()
	ctx = bounded
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	source, err := repository.interruptedDetachRetrySource(ctx, parentID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if source == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"Apply has no stopped Custom Detach hook awaiting explicit Retry",
		)
	}
	defer clear(source.checkpoint.Value)
	current := source.current
	if err := repository.ensureRecoveryDispatchCapacity(ctx, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	assignment := current.Assignment.Record
	if assignment.ExecutionEpoch == math.MaxUint32 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Detach execution epoch is exhausted")
	}
	next := assignment
	next.ExecutionEpoch++
	// A Retry receives a bounded recovery execution budget, not renewed forward
	// work or a modified original recovery deadline/failure record.
	if current.RecoveryProofRequired {
		next.RecoveryExecutionDeadline = marker.CreatedAt.Add(releaseRecoveryProofExecutionBudget)
	}
	encoded, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(encoded)
	old, err := taskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(old)
	lifecycleKey, lifecycle, proof, err := repository.assignmentLifecycleIndexAtRevision(ctx, assignment,
		&keyvalue.KeyValue{Value: old, ModRevision: current.Assignment.Revision}, current.Task.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if proof != current.RecoveryProofRequired || !proof && !assignment.RecoveryDeadline.After(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Detach recovery deadline changed; wait for its recovery fence",
		)
	}
	permit := backinghooks.RetryAuthorization{TaskID: assignment.TaskID, StepID: source.record.StepID,
		AssignmentID: assignment.AssignmentID, PlanHash: current.Task.Record.PlanHash,
		CheckpointRevision: source.checkpoint.ModRevision, ExecutionEpoch: next.ExecutionEpoch, AuthorizedAt: marker.CreatedAt}
	permitBytes, err := backinghooks.EncodeRetryAuthorization(permit)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(permitBytes)
	permitKey := backinghooks.RetryAuthorizationKey(assignment.TaskID, permit.StepID)
	historyKey := backinghooks.RetryHistoryKey(permit)
	permitRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{permitKey}, Revision: current.Task.ReadRevision},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if permitRead == nil || len(permitRead.Values) != 1 {
		return IdempotencyTransactionResult{}, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(permitRead.Values)
	claim, err := repository.prepareBlueprintChildAssignmentEpoch(
		ctx,
		current.Task.Record,
		assignment.ExecutionEpoch,
		true,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer claim.Clear()
	conditions := []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(parentID), ModRevision: source.parent.Revision},
		{Key: taskjournal.TaskStorageKey(assignment.TaskID), ModRevision: current.Task.Revision},
		{Key: taskClosingReportKey(current.Task.Record)},
		{Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), ModRevision: current.Assignment.Revision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID),
			ModRevision: current.Assignment.Revision,
		},
		{Key: lifecycleKey, ModRevision: lifecycle.ModRevision},
		{Key: source.checkpoint.Key, ModRevision: source.checkpoint.ModRevision},
		{Key: permitKey, ModRevision: keyvalue.RevisionOf(permitRead.Values[0])},
		{Key: historyKey},
	}
	conditions = append(conditions, claim.Conditions()...)
	_, retryRevision, err := repository.readRecoveryRetryControl(ctx, assignment.TaskID, current.Task.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: taskassignments.RecoveryRetryControlKey(assignment.TaskID), ModRevision: retryRevision},
	)
	taskBytes, err := EncodeTaskRecord(current.Task.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskBytes)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(assignment.TaskID), Value: taskBytes},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), Value: encoded},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID),
			Value: encoded,
		},
		{Type: keyvalue.MutationPut, Key: lifecycleKey, Value: encoded},
		{Type: keyvalue.MutationPut, Key: permitKey, Value: permitBytes},
		{Type: keyvalue.MutationPut, Key: historyKey, Value: permitBytes},
	}
	mutations = append(mutations, claim.Mutations()...)
	mutations = append(
		mutations,
		keyvalue.Mutation{
			Type: keyvalue.MutationDelete,
			Key:  taskassignments.RecoveryRetryControlKey(assignment.TaskID),
		},
	)
	plan, err := NewIdempotencyMutationPlan(conditions, mutations, func(int64, []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Custom Detach Retry raced its captured execution; refresh the Task")
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotencyRepository, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotencyRepository.Apply(ctx, marker, plan)
}
