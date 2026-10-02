package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Acceptance rereads and compares this assignment in its mutation transaction.
// This read selects the durable generation; it is not mutation authority alone.
func (repository *BackupRuntimeRepository) GetBackupCheckpointAssignment(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[taskassignments.TaskAssignmentRecord], error) {
	var zero etcdstore.Versioned[taskassignments.TaskAssignmentRecord]
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return zero, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return zero, errs.New(errs.KindValidationFailed, "Backup checkpoint Task is invalid")
	}
	result, err := repository.store.Get(ctx, taskjournal.TaskAssignmentIndexKey(taskID))
	if err != nil {
		return zero, err
	}
	if result == nil || result.Entry == nil {
		return zero, errs.New(errs.KindStateConflict, "Backup checkpoint assignment is unavailable")
	}
	defer clear(result.Entry.Value)
	assignment, err := taskassignments.DecodeTaskAssignment(result.Entry.Value)
	if err != nil || assignment.TaskID != taskID {
		return zero, taskassignments.CorruptTaskAssignment()
	}
	return etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
		Record:       assignment,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}
