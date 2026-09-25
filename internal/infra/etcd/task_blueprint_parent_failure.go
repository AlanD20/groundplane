package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintParentFailurePrefix = "/v1/runtime/blueprint-parent-failures/"

type blueprintParentFailureRecord struct {
	Schema       int       `json:"schema"`
	ParentTaskID string    `json:"parent_task_id"`
	ChildTaskID  string    `json:"child_task_id"`
	ChildPlanID  string    `json:"child_plan_id"`
	FailedAt     time.Time `json:"failed_at"`
}

func blueprintParentFailureKey(taskID string) string {
	return blueprintParentFailurePrefix + taskID
}

func validateBlueprintParentFailureRecord(record blueprintParentFailureRecord) error {
	if record.Schema != 1 || ids.Validate(ids.KindTask, record.ParentTaskID) != nil ||
		ids.Validate(ids.KindTask, record.ChildTaskID) != nil ||
		ids.Validate(ids.KindPlan, record.ChildPlanID) != nil ||
		recordcodec.ValidateTimestamp("Blueprint parent failure", record.FailedAt) != nil {
		return errs.New(errs.KindInternal, "Blueprint parent failure record is corrupt")
	}
	return nil
}

func encodeBlueprintParentFailureRecord(record blueprintParentFailureRecord) ([]byte, error) {
	if err := validateBlueprintParentFailureRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint-parent-failure", record)
}

func decodeBlueprintParentFailureRecord(value []byte) (blueprintParentFailureRecord, error) {
	record, err := recordcodec.Decode[blueprintParentFailureRecord](value, "blueprint-parent-failure")
	if err != nil || validateBlueprintParentFailureRecord(record) != nil {
		return blueprintParentFailureRecord{}, errs.New(
			errs.KindInternal,
			"Blueprint parent failure record is corrupt",
		)
	}
	return record, nil
}

// prepareBlueprintParentFailure durably closes the parent's existing stop
// gate and records which exactly restored child failed. Desired-plan and child
// publication already compare the stop gate absent, so no new work can race
// the terminal receipt.
func (repository *TaskRepository) prepareBlueprintParentFailure(
	ctx context.Context,
	parentTaskID string,
	child TaskRecord,
	failedAt time.Time,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if ids.Validate(ids.KindTask, parentTaskID) != nil || child.ID == parentTaskID ||
		ids.Validate(ids.KindTask, child.ID) != nil || ids.Validate(ids.KindPlan, child.PlanID) != nil ||
		recordcodec.ValidateTimestamp("Blueprint child failure", failedAt) != nil {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint child failure authority is invalid")
	}
	stopKey := taskjournal.BlueprintParentAbortKey(parentTaskID)
	failureKey := blueprintParentFailureKey(parentTaskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{stopKey, failureKey}, Revision: revision,
	})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint parent failure read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	stop, failure := read.Values[0], read.Values[1]
	conditions := []keyvalue.Condition{
		{Key: stopKey, ModRevision: keyvalue.RevisionOf(stop)},
		{Key: failureKey, ModRevision: keyvalue.RevisionOf(failure)},
	}
	if stop != nil {
		request, decodeErr := decodeBlueprintParentAbortRequest(stop.Value)
		if decodeErr != nil || request.TaskID != parentTaskID {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint parent stop gate is corrupt")
		}
		if failure == nil {
			// An operator abort won the race. The child may settle after exact
			// restoration, but it must not replace that requested outcome.
			return conditions, nil, nil
		}
		record, decodeErr := decodeBlueprintParentFailureRecord(failure.Value)
		if decodeErr != nil || record.ParentTaskID != parentTaskID {
			return nil, nil, errs.New(errs.KindInternal, "Blueprint parent failure record changed")
		}
		return conditions, nil, nil
	}
	if failure != nil {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint parent failure lacks its stop gate")
	}
	stopValue, err := encodeBlueprintParentAbortRequest(blueprintParentAbortRequest{
		TaskID: parentTaskID, RequestedAt: failedAt,
	})
	if err != nil {
		return nil, nil, err
	}
	failureValue, err := encodeBlueprintParentFailureRecord(blueprintParentFailureRecord{
		Schema: 1, ParentTaskID: parentTaskID, ChildTaskID: child.ID,
		ChildPlanID: child.PlanID, FailedAt: failedAt,
	})
	if err != nil {
		clear(stopValue)
		return nil, nil, err
	}
	return conditions, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: stopKey, Value: stopValue},
		{Type: keyvalue.MutationPut, Key: failureKey, Value: failureValue},
	}, nil
}

// BlueprintParentFailureRequested reports a proved child failure. The paired
// stop gate is validated by the final parent transaction.
func (repository *TaskRepository) BlueprintParentFailureRequested(
	ctx context.Context,
	taskID string,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return false, errs.New(errs.KindValidationFailed, "Blueprint parent id is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{blueprintParentFailureKey(taskID)},
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Blueprint parent failure read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	record, err := decodeBlueprintParentFailureRecord(read.Values[0].Value)
	if err != nil || record.ParentTaskID != taskID {
		return false, errs.New(errs.KindInternal, "Blueprint parent failure record is corrupt")
	}
	return true, nil
}

// FailBlueprintParent terminalizes a failed Apply only after every owned child
// claim is settled and no unknown effects remain.
func (repository *TaskRepository) FailBlueprintParent(
	ctx context.Context,
	environmentID string,
	taskID string,
	at time.Time,
) (keyvalue.Versioned[TaskRecord], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint parent identity is invalid",
		)
	}
	if err := recordcodec.ValidateTimestamp("Blueprint parent failure", at); err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	snapshot, err := ledger.Load(ctx, environmentID)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, err
	}
	for _, execution := range snapshot.Executions {
		if execution.Record.ParentTaskID == taskID {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindResourceInUse,
				"Blueprint parent has unsettled child effects",
			)
		}
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.ParentTaskID == taskID && applied.Record.State == blueprintunits.Uncertain {
			return keyvalue.Versioned[TaskRecord]{}, errs.New(
				errs.KindResourceInUse,
				"Blueprint parent has unknown child effects",
			)
		}
	}
	return repository.terminalizeBlueprintParent(ctx, snapshot, taskID, taskjournal.TaskStatusFailed, at)
}
