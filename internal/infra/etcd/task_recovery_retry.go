package etcd

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type stoppedRecoverySource struct {
	visible         keyvalue.Versioned[TaskRecord]
	current         TaskAssignment
	control         *taskassignments.RecoveryRetryControl
	controlRevision int64
}

func (repository *TaskRepository) GetStoppedRecoveryRetry(ctx context.Context, taskID string) (bool, error) {
	source, err := repository.stoppedRecoverySource(ctx, taskID)
	return source != nil, err
}

func (repository *TaskRepository) stoppedRecoverySource(
	ctx context.Context,
	taskID string,
) (*stoppedRecoverySource, error) {
	visible, err := repository.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if visible.Record.Status != taskjournal.TaskStatusRunning {
		return nil, nil
	}
	candidates := []keyvalue.Versioned[TaskRecord]{visible}
	if visible.Record.Executor == taskjournal.TaskExecutorBlueprint {
		hook, err := repository.interruptedDetachRetrySource(ctx, taskID)
		if err != nil {
			return nil, err
		}
		if hook != nil {
			clear(hook.checkpoint.Value)
			return nil, nil // Custom STARTED hooks require their captured one-use permit.
		}
		ledger, err := blueprintunits.NewRepository(repository.store)
		if err != nil {
			return nil, err
		}
		state, err := ledger.LoadAtRevision(ctx, visible.Record.Owner.EnvironmentID, visible.ReadRevision)
		if err != nil {
			return nil, err
		}
		candidates = nil
		for _, execution := range state.Executions {
			if execution.Record.ParentTaskID != taskID {
				continue
			}
			child, err := repository.GetTask(ctx, execution.Record.TaskID)
			if err != nil {
				return nil, err
			}
			if child.Record.Params[taskjournal.TaskBlueprintParentParam] != taskID {
				return nil, taskassignments.CorruptTaskAssignment()
			}
			candidates = append(candidates, child)
		}
		slices.SortFunc(candidates, func(a, b keyvalue.Versioned[TaskRecord]) int {
			if a.Record.ID < b.Record.ID {
				return -1
			}
			if a.Record.ID > b.Record.ID {
				return 1
			}
			return 0
		})
	}
	for _, task := range candidates {
		if task.Record.Executor != taskjournal.TaskExecutorAgent ||
			task.Record.Status != taskjournal.TaskStatusRunning {
			continue
		}
		current, err := repository.GetTaskAssignment(ctx, task.Record.ID)
		if err != nil {
			return nil, err
		}
		if current.Assignment.Record.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
			continue
		}
		control, revision, err := repository.readRecoveryRetryControl(ctx, task.Record.ID, current.Task.ReadRevision)
		if err != nil {
			return nil, err
		}
		if control != nil && (control.AssignmentID != current.Assignment.Record.AssignmentID ||
			control.ExecutionEpoch > current.Assignment.Record.ExecutionEpoch) {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if control != nil && control.Stopped || current.RecoveryProofRequired &&
			!current.Assignment.Record.RecoveryExecutionDeadline.After(time.Now().UTC()) {
			return &stoppedRecoverySource{
				visible:         visible,
				current:         current,
				control:         control,
				controlRevision: revision,
			}, nil
		}
	}
	return nil, nil
}

// RetryStoppedRecovery grants one operator-selected bounded recovery attempt.
// It neither reruns forward work nor changes the captured plan or first failure.
func (repository *TaskRepository) RetryStoppedRecovery(
	ctx context.Context, taskID string, marker idempotency.IdempotencyMarker,
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
	if marker.Kind != idempotency.IdempotencyMarkerDirect || marker.State != idempotency.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"recovery retry requires completed request authority",
		)
	}
	source, err := repository.stoppedRecoverySource(ctx, taskID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if source == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable,
			"no stopped recovery is awaiting operator Retry",
		)
	}
	current := source.current
	if err := repository.ensureRecoveryDispatchCapacity(ctx, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	assignment := current.Assignment.Record
	if assignment.ExecutionEpoch == math.MaxUint32 ||
		!current.RecoveryProofRequired && !marker.CreatedAt.Before(assignment.RecoveryDeadline) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"recovery deadline settlement is pending; refresh the Task",
		)
	}
	next := assignment
	next.ExecutionEpoch++
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
	if proof != current.RecoveryProofRequired {
		return IdempotencyTransactionResult{}, taskassignments.CorruptTaskAssignment()
	}
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
	taskBytes, err := EncodeTaskRecord(current.Task.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskBytes)
	conditions := []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(assignment.TaskID), ModRevision: current.Task.Revision},
		{Key: taskClosingReportKey(current.Task.Record)},
		{Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), ModRevision: current.Assignment.Revision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID),
			ModRevision: current.Assignment.Revision,
		},
		{Key: lifecycleKey, ModRevision: lifecycle.ModRevision},
		{Key: taskassignments.RecoveryRetryControlKey(assignment.TaskID), ModRevision: source.controlRevision},
	}
	if taskID != assignment.TaskID {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: taskjournal.TaskStorageKey(taskID), ModRevision: source.visible.Revision},
		)
	}
	conditions = append(conditions, claim.Conditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(assignment.TaskID), Value: taskBytes},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), Value: encoded},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID),
			Value: encoded,
		},
		{Type: keyvalue.MutationPut, Key: lifecycleKey, Value: encoded},
		{Type: keyvalue.MutationDelete, Key: taskassignments.RecoveryRetryControlKey(assignment.TaskID)},
	}
	mutations = append(mutations, claim.Mutations()...)
	plan, err := NewIdempotencyMutationPlan(conditions, mutations, func(int64, []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "recovery Retry raced its captured assignment; refresh the Task")
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	requests, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return requests.Apply(ctx, marker, plan)
}
