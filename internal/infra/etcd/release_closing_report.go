package etcd

import (
	"context"
	"time"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Persist the received report before any member projection can commit. A
// reconnect resumes Controller bookkeeping instead of undoing accepted effects.
func (repository *TaskRepository) prepareOrdinaryReleaseClosingReport(
	ctx context.Context, current TaskAssignment, status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord, observedAt *time.Time, proof []etcdstore.Condition,
) (scriptTerminalSourceRelease, bool, error) {
	report, value, err := repository.readTaskClosingReport(ctx, current)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if value != nil {
		if !report.matches(status, result) {
			return scriptTerminalSourceRelease{}, false, errs.New(
				errs.KindStateConflict,
				"release closing report changed",
			)
		}
		*observedAt = report.ObservedAt
		return scriptTerminalSourceRelease{
			conditions: []etcdstore.Condition{{Key: value.Key, ModRevision: value.ModRevision}},
			mutations:  []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: value.Key}},
		}, false, nil
	}
	_, condition, mutation, err := repository.prepareTaskClosingReport(ctx, current, status, result, *observedAt, true)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	defer clear(mutation.Value)
	task, assignment := current.Task.Record, current.Assignment.Record
	conditions := []etcdstore.Condition{
		condition,
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: current.Task.Revision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			ModRevision: current.Assignment.Revision,
		},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: current.Assignment.Revision},
	}
	transaction, err := repository.store.Transact(ctx, append(conditions, proof...), []etcdstore.Mutation{mutation})
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return scriptTerminalSourceRelease{}, false, errs.New(
			errs.KindStateConflict,
			"release closing authority changed",
		)
	}
	return scriptTerminalSourceRelease{}, true, nil
}
