package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func firstTaskEventSequence(task TaskRecord) uint64 {
	return task.NextEventSequence - uint64(task.EventCount)
}

func nextTaskControllerTimestamp(previous time.Time, supplied time.Time) (time.Time, error) {
	if err := recordcodec.ValidateTimestamp("task controller timestamp", supplied); err != nil {
		return time.Time{}, err
	}
	if supplied.After(previous) {
		return supplied, nil
	}
	return previous.Add(time.Nanosecond), nil
}

func taskCheckpointAssignmentMatches(
	checkpoint taskjournal.TaskEventCheckpoint,
	assignment taskassignments.TaskAssignmentRecord,
) bool {
	identity := checkpoint.Identity
	return identity.AssignmentID == assignment.AssignmentID && identity.AgentID == assignment.AgentID &&
		identity.AgentGeneration == assignment.AgentGeneration && identity.Attempt > 0 && identity.Attempt <= assignment.ExecutionEpoch
}

func validateTaskEventCheckpoints(task TaskRecord) error {
	limit := len(task.Steps)
	if task.Executor == taskjournal.TaskExecutorController {
		limit = 1
	}
	if len(task.EventCheckpoints) > limit {
		return recordcodec.CorruptRecord()
	}
	previous := ""
	for index, checkpoint := range task.EventCheckpoints {
		if taskjournal.ValidateTaskEventIdentity(checkpoint.Identity) != nil || checkpoint.Identity.TaskID != task.ID ||
			!taskEventBelongsToTask(task, checkpoint.Identity) || index > 0 && checkpoint.Identity.StepID <= previous ||
			checkpoint.Sequence == 0 || checkpoint.Sequence >= firstTaskEventSequence(task) ||
			!recordcodec.ValidSHA256(checkpoint.PayloadSHA256) || !taskjournal.ValidTaskEventState(checkpoint.State) ||
			checkpoint.Completed && !checkpoint.Running || checkpoint.Running && !checkpoint.EffectPossible ||
			checkpoint.State == taskjournal.TaskEventStateRunning && !checkpoint.Running ||
			checkpoint.State == taskjournal.TaskEventStateCompleted && !checkpoint.Completed ||
			checkpoint.State != taskjournal.TaskEventStatePending && !checkpoint.EffectPossible {
			return recordcodec.CorruptRecord()
		}
		previous = checkpoint.Identity.StepID
		if checkpoint.ControllerOperation.Validate() != nil ||
			checkpoint.Identity.ControllerOperation() != (checkpoint.ControllerOperation != nil) ||
			checkpoint.ControllerOperation != nil && checkpoint.HealthWait != nil {
			return recordcodec.CorruptRecord()
		}
		if checkpoint.Failure.Validate() != nil || checkpoint.Failure != nil &&
			checkpoint.State != taskjournal.TaskEventStateFailed && checkpoint.State != taskjournal.TaskEventStateTimedOut && checkpoint.State != taskjournal.TaskEventStateAborted {
			return recordcodec.CorruptRecord()
		}
		if checkpoint.HealthWait.Validate() != nil ||
			checkpoint.HealthWait == nil &&
				(checkpoint.HealthWaitOrdinal != 0 || checkpoint.HealthWaitReceivedAt != nil) ||
			checkpoint.HealthWait != nil &&
				(checkpoint.HealthWaitOrdinal < 2 || checkpoint.HealthWaitOrdinal > checkpoint.Identity.Ordinal ||
					checkpoint.HealthWaitReceivedAt == nil || recordcodec.ValidateTimestamp("health wait received_at", *checkpoint.HealthWaitReceivedAt) != nil || checkpoint.HealthWaitReceivedAt.After(task.UpdatedAt)) {
			return recordcodec.CorruptRecord()
		}
	}
	return nil
}

func trimmedTaskEventReplay(
	task TaskRecord,
	input taskjournal.TaskEventInput,
	hash string,
) (*taskjournal.TaskEventDedupRecord, error) {
	for _, checkpoint := range task.EventCheckpoints {
		if checkpoint.Identity.StepID != input.Identity.StepID {
			continue
		}
		if checkpoint.Identity == input.Identity {
			if checkpoint.PayloadSHA256 != hash {
				return nil, errs.New(errs.KindInternal, "trimmed event replay changed payload")
			}
			return &taskjournal.TaskEventDedupRecord{Identity: checkpoint.Identity, Sequence: checkpoint.Sequence,
				PayloadSHA256: checkpoint.PayloadSHA256}, nil
		}
		if input.Identity.Attempt < checkpoint.Identity.Attempt ||
			input.Identity.Attempt == checkpoint.Identity.Attempt &&
				input.Identity.Ordinal <= checkpoint.Identity.Ordinal {
			return nil, errs.New(errs.KindStateConflict, "task event is older than retained replay authority")
		}
	}
	return nil, nil
}

// Trimming and appending share the Task/assignment CAS. The old replay record
// must match the oldest event exactly; partial eviction is never committed.
func (repository *TaskRepository) prepareTaskEventTrim(
	ctx context.Context, task TaskRecord, prepared *PreparedTaskEvent, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if task.EventCount < taskjournal.MaximumTaskEvents {
		return nil, nil, nil
	}
	oldest := firstTaskEventSequence(task)
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskEventKey(task.ID, oldest)}, Revision: revision},
	)
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, recordcodec.CorruptRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	event, err := taskjournal.DecodeTaskEventRecord(read.Values[0].Value)
	if err != nil || event.Sequence != oldest || event.Identity.TaskID != task.ID {
		return nil, nil, recordcodec.CorruptRecord()
	}
	dedupKey := taskjournal.TaskEventDedupKey(event.Identity)
	dedupRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{dedupKey}, Revision: revision},
	)
	if err != nil {
		return nil, nil, err
	}
	if dedupRead == nil || dedupRead.ReadRevision != revision || len(dedupRead.Values) != 1 ||
		dedupRead.Values[0] == nil {
		return nil, nil, recordcodec.CorruptRecord()
	}
	defer etcdstore.ClearValues(dedupRead.Values)
	dedup, err := taskjournal.DecodeTaskEventDedupRecord(dedupRead.Values[0].Value)
	if err != nil || dedup.Identity != event.Identity || dedup.Sequence != event.Sequence ||
		dedup.PayloadSHA256 != event.PayloadSHA256 {
		return nil, nil, recordcodec.CorruptRecord()
	}
	payload, err := taskprogress.DecodePayload(event.Payload)
	if err != nil {
		return nil, nil, err
	}
	checkpoint := taskjournal.TaskEventCheckpoint{Identity: event.Identity, Sequence: event.Sequence,
		PayloadSHA256: event.PayloadSHA256, State: event.State,
		Running: event.State == taskjournal.TaskEventStateRunning ||
			event.State == taskjournal.TaskEventStateCompleted,
		Completed: event.State == taskjournal.TaskEventStateCompleted, EffectPossible: event.State != taskjournal.TaskEventStatePending}
	checkpoint.HealthWait = payload.HealthWait
	checkpoint.Failure = taskprogress.CloneFailure(payload.Failure)
	checkpoint.ControllerOperation = taskprogress.CloneControllerOperation(payload.ControllerOperation)
	if payload.HealthWait != nil {
		checkpoint.HealthWaitOrdinal = event.Identity.Ordinal
		checkpoint.HealthWaitReceivedAt = &event.ReceivedAt
	}
	index := slices.IndexFunc(
		prepared.Task.EventCheckpoints,
		func(value taskjournal.TaskEventCheckpoint) bool {
			return value.Identity.StepID == event.Identity.StepID
		},
	)
	if index >= 0 {
		prior := prepared.Task.EventCheckpoints[index]
		checkpoint.Running = checkpoint.Running || prior.Running
		checkpoint.Completed = checkpoint.Completed || prior.Completed
		checkpoint.EffectPossible = checkpoint.EffectPossible || prior.EffectPossible
		if prior.Identity.Attempt > checkpoint.Identity.Attempt ||
			prior.Identity.Attempt == checkpoint.Identity.Attempt &&
				prior.HealthWaitOrdinal > checkpoint.HealthWaitOrdinal {
			checkpoint.HealthWait, checkpoint.HealthWaitOrdinal = prior.HealthWait, prior.HealthWaitOrdinal
			checkpoint.HealthWaitReceivedAt = prior.HealthWaitReceivedAt
		}
		// Delivery order may differ from ordinal order. Never lower the replay
		// watermark when a later-evicted event has an older protocol identity.
		if prior.Identity.Attempt > checkpoint.Identity.Attempt ||
			prior.Identity.Attempt == checkpoint.Identity.Attempt &&
				prior.Identity.Ordinal > checkpoint.Identity.Ordinal {
			checkpoint.Identity, checkpoint.Sequence, checkpoint.PayloadSHA256 = prior.Identity, prior.Sequence, prior.PayloadSHA256
			checkpoint.State = prior.State
			checkpoint.Failure = taskprogress.CloneFailure(prior.Failure)
			checkpoint.ControllerOperation = taskprogress.CloneControllerOperation(prior.ControllerOperation)
		}
		prepared.Task.EventCheckpoints[index] = checkpoint
	} else {
		prepared.Task.EventCheckpoints = append(prepared.Task.EventCheckpoints, checkpoint)
		slices.SortFunc(prepared.Task.EventCheckpoints, func(a, b taskjournal.TaskEventCheckpoint) int {
			if a.Identity.StepID < b.Identity.StepID {
				return -1
			}
			if a.Identity.StepID > b.Identity.StepID {
				return 1
			}
			return 0
		})
	}
	historyConditions, historyMutations, err := repository.prepareTrimmedAttemptHistory(ctx, task, event, revision)
	if err != nil {
		return nil, nil, err
	}
	conditions := []etcdstore.Condition{{Key: read.Values[0].Key, ModRevision: read.Values[0].ModRevision},
		{Key: dedupKey, ModRevision: dedupRead.Values[0].ModRevision}}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: read.Values[0].Key},
		{Type: etcdstore.MutationDelete, Key: dedupKey},
	}
	return append(conditions, historyConditions...), append(mutations, historyMutations...), nil
}
