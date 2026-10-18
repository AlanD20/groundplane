package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingZoneTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareBackingZoneTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
	assignment taskassignments.TaskAssignmentRecord,
) (backingZoneTaskChange, error) {
	if task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		conditions, mutations, err := repository.prepareZoneRemovalAcknowledgement(
			ctx, task, terminalStatus, terminalAt, revision, assignment,
		)
		values := make([][]byte, 0, len(mutations))
		for _, mutation := range mutations {
			if mutation.Type == etcdstore.MutationPut {
				values = append(values, mutation.Value)
			}
		}
		return backingZoneTaskChange{
			applies: true, conditions: conditions, mutations: mutations, values: values,
		}, err
	}
	_, err := taskOwnsBackingZoneCascade(task)
	return backingZoneTaskChange{}, err
}

func (repository *TaskRepository) validateBackingZoneTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	if task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		if retainedAuthoredZoneAbsence(task) {
			terminalStatus = taskjournal.TaskStatusCompleted
		}
		return repository.validateZoneRemovalReplay(ctx, task, terminalStatus, revision)
	}
	_, err := taskOwnsBackingZoneCascade(task)
	return err
}

func taskOwnsBackingZoneCascade(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceBackingZone {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil ||
		len(task.Params) != 5 ||
		ids.Validate(ids.KindEnvironment, task.Params[taskjournal.TaskZoneEnvironmentParam]) != nil ||
		ids.Validate(ids.KindOperation, task.Params[taskjournal.TaskZoneRemovalOperationParam]) != nil ||
		ids.Validate(ids.KindTask, task.Params[blueprints.EnvironmentDesiredRevisionParam]) != nil ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskZoneImpactTokenParam]) {
		return false, errs.New(errs.KindInternal, "backing Zone cascade Task has invalid durable input")
	}
	return true, nil
}

func clearBackingZoneTaskChange(change backingZoneTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
