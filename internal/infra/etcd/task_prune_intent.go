package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumTaskPruneBatchRecords = (etcdstore.MaximumOperations - 2) / 2
	maximumTaskPruneCASAttempts  = 8
)

type taskPruneIntent struct {
	TaskID                                 string `json:"task_id"`
	TaskRevision                           int64  `json:"task_revision"`
	BackupCheckpointCursorsComplete        bool   `json:"backup_checkpoint_cursors_complete"`
	BackupCheckpointDeduplicationsComplete bool   `json:"backup_checkpoint_deduplications_complete"`
	TaskPrimaryDeleted                     bool   `json:"task_primary_deleted"`
	BackupTerminalReceiptRevision          int64  `json:"backup_terminal_receipt_revision,omitempty"`
	AttachPlanID                           string `json:"attach_plan_id,omitempty"`
	RemainingEvents                        uint32 `json:"remaining_events"`
	RemainingDeduplications                uint32 `json:"remaining_deduplications"`
}

func encodeTaskPruneIntent(intent taskPruneIntent) ([]byte, error) {
	if err := validateTaskPruneIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task_prune_intent", intent)
}

func decodeTaskPruneIntent(value []byte) (taskPruneIntent, error) {
	intent, err := recordcodec.Decode[taskPruneIntent](value, "task_prune_intent")
	if err != nil {
		return taskPruneIntent{}, err
	}
	if err := validateTaskPruneIntent(intent); err != nil {
		return taskPruneIntent{}, corruptTaskPruneIntent()
	}
	return intent, nil
}

func validateTaskPruneIntent(intent taskPruneIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil || intent.TaskRevision <= 0 ||
		(intent.BackupCheckpointDeduplicationsComplete &&
			!intent.BackupCheckpointCursorsComplete) ||
		(intent.TaskPrimaryDeleted && !intent.BackupCheckpointDeduplicationsComplete) ||
		intent.BackupTerminalReceiptRevision < 0 ||
		intent.RemainingEvents > taskjournal.MaximumTaskEvents ||
		intent.RemainingDeduplications > taskjournal.MaximumTaskEvents {
		return errs.New(errs.KindValidationFailed, "task prune intent is invalid")
	}
	if intent.AttachPlanID != "" && ids.Validate(ids.KindPlan, intent.AttachPlanID) != nil {
		return errs.New(errs.KindValidationFailed, "task prune Attach plan is invalid")
	}
	return nil
}

func (repository *TaskRepository) advanceTaskPruneIntent(
	ctx context.Context,
	current etcdstore.Versioned[taskPruneIntent],
	next taskPruneIntent,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.Versioned[taskPruneIntent], error) {
	intentValue, err := encodeTaskPruneIntent(next)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	defer clear(intentValue)
	conditions = append([]etcdstore.Condition{{
		Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}, conditions...)
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), Value: intentValue,
	})
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return etcdstore.Versioned[taskPruneIntent]{}, errs.New(
			errs.KindInternal,
			"task prune batch exceeds transaction limit",
		)
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[taskPruneIntent]{}, errs.New(
			errs.KindStateConflict,
			"task prune batch changed",
		)
	}
	return etcdstore.Versioned[taskPruneIntent]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func (repository *TaskRepository) finishTaskPruneIntent(
	ctx context.Context,
	current etcdstore.Versioned[taskPruneIntent],
) error {
	conditions := []etcdstore.Condition{{
		Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID), ModRevision: current.Revision,
	}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: taskjournal.TaskPruneIntentKey(current.Record.TaskID)}}
	if current.Record.AttachPlanID != "" {
		references, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: attachTaskPlanReferenceScopePrefix(current.Record.AttachPlanID), Limit: 1,
		})
		if err != nil {
			return err
		}
		if references == nil || references.ReadRevision <= 0 || len(references.Values) > 1 {
			return corruptTaskPruneIntent()
		}
		defer clearKeyValueSlice(references.Values)
		if len(references.Values) != 0 {
			if _, err := decodeAttachTaskPlanReference(
				references.Values[0].Key,
				references.Values[0].Value,
				current.Record.AttachPlanID,
			); err != nil {
				return corruptTaskPruneIntent()
			}
		} else {
			inputKey := attachTaskRenderInputKey(current.Record.AttachPlanID)
			inputResult, err := repository.store.GetMany(
				ctx,
				etcdstore.GetManyRequest{Keys: []string{inputKey}},
			)
			if err != nil {
				return err
			}
			if inputResult == nil || len(inputResult.Values) != 1 || inputResult.Values[0] == nil {
				return corruptTaskPruneIntent()
			}
			defer etcdstore.ClearValues(inputResult.Values)
			input, err := decodeAttachTaskRenderInput(inputResult.Values[0].Value)
			if err != nil || input.PlanID != current.Record.AttachPlanID {
				return corruptTaskPruneIntent()
			}
			conditions = append(conditions, etcdstore.Condition{
				Key: inputKey, ModRevision: inputResult.Values[0].ModRevision,
			})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: inputKey})
		}
	}
	conditions, mutations = appendBackupTerminalReceiptPruneFinalization(
		current.Record,
		conditions,
		mutations,
	)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return errs.New(errs.KindStateConflict, "task prune completion changed")
	}
	return nil
}
