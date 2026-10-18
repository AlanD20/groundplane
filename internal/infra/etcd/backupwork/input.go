// Package backupwork owns captured inputs and execution state for native Backup work.
package backupwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type InputReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

type InputRepository struct{ store InputReader }

func NewInputRepository(store InputReader) (*InputRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Backup work input storage is required")
	}
	return &InputRepository{store: store}, nil
}

func InputKey(workID string) string { return "/v1/runtime/backup-work-inputs/" + workID }

// PrepareInput must be committed with Task admission, source revision checks,
// exclusion claims and secret pins. An input record alone grants no execution.
func PrepareInput(item workexecution.Item, input *agentpb.BackupWorkInput) (workqueue.Change, error) {
	if item.Validate() != nil || item.Input == nil || item.Placement.Executor != workexecution.Agent ||
		input == nil || operation(input.Operation) != item.Operation {
		return workqueue.Change{}, conflict("Backup work input does not match its item")
	}
	_, raw, reference, err := executionplan.CaptureBackupWorkInput(item.Input.ID, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if reference != *item.Input {
		clear(raw)
		return workqueue.Change{}, conflict("Backup captured input digest changed")
	}
	key := InputKey(item.ID)
	change, err := prepareSourceStagingIndexes(item, input)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	return change, nil
}

func (repository *InputRepository) Load(ctx context.Context, view workqueue.View) (*agentpb.BackupWorkInput, keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision <= 0 || task.Validate() != nil || item.Validate() != nil ||
		item.TaskID != task.ID || item.Input == nil || item.Placement.Executor != workexecution.Agent {
		return nil, keyvalue.Condition{}, conflict("Backup work has no captured input")
	}
	key := InputKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if read == nil {
		return nil, keyvalue.Condition{}, conflict("Backup work input read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return nil, keyvalue.Condition{}, conflict("Backup work input is unavailable")
	}
	input, err := executionplan.DecodeBackupWorkInput(read.Values[0].Value)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	_, raw, reference, err := executionplan.CaptureBackupWorkInput(item.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if reference != *item.Input || operation(input.Operation) != item.Operation ||
		input.EnvironmentId != task.Scope.EnvironmentID || input.ProjectId != task.Scope.ProjectID ||
		!actionMatches(task.Action, input.Operation) ||
		!slices.Equal(item.Claims, claims(input, item.Placement.HostID)) {
		return nil, keyvalue.Condition{}, conflict("Backup input differs from its accepted work")
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func operation(value agentpb.BackupWorkOperation) workexecution.Operation {
	switch value {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		return workexecution.Backup
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		return workexecution.Restore
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		return workexecution.BackupPrune
	default:
		return ""
	}
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
