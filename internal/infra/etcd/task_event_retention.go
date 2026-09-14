package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskEventCheckpoint retains bounded step progress and mutation facts, not a
// second history. Its identity is the replay watermark of evicted events.
type TaskEventCheckpoint struct {
	Identity       TaskEventIdentity `json:"identity"`
	Sequence       uint64            `json:"sequence"`
	PayloadSHA256  string            `json:"payload_sha256"`
	State          TaskEventState    `json:"state"`
	Running        bool              `json:"running"`
	Completed      bool              `json:"completed"`
	EffectPossible bool              `json:"effect_possible"`
}

func firstTaskEventSequence(task TaskRecord) uint64 {
	return task.NextEventSequence - uint64(task.EventCount)
}

func nextTaskControllerTimestamp(previous time.Time, supplied time.Time) (time.Time, error) {
	if err := validateTimestamp("task controller timestamp", supplied); err != nil {
		return time.Time{}, err
	}
	if supplied.After(previous) {
		return supplied, nil
	}
	return previous.Add(time.Nanosecond), nil
}

func taskCheckpointAssignmentMatches(checkpoint TaskEventCheckpoint, assignment TaskAssignmentRecord) bool {
	identity := checkpoint.Identity
	return identity.AssignmentID == assignment.AssignmentID && identity.AgentID == assignment.AgentID &&
		identity.AgentGeneration == assignment.AgentGeneration && identity.Attempt > 0 && identity.Attempt <= assignment.ExecutionEpoch
}

func validateTaskEventCheckpoints(task TaskRecord) error {
	if len(task.EventCheckpoints) > len(task.Steps) {
		return corruptRecord()
	}
	previous := ""
	for _, checkpoint := range task.EventCheckpoints {
		if validateTaskEventIdentity(checkpoint.Identity) != nil || checkpoint.Identity.TaskID != task.ID ||
			!taskContainsStep(task, checkpoint.Identity.StepID) || checkpoint.Identity.StepID <= previous ||
			checkpoint.Sequence == 0 || checkpoint.Sequence >= firstTaskEventSequence(task) ||
			!validSHA256(checkpoint.PayloadSHA256) || !validTaskEventState(checkpoint.State) ||
			checkpoint.Completed && !checkpoint.Running || checkpoint.Running && !checkpoint.EffectPossible ||
			checkpoint.State == TaskEventStateRunning && !checkpoint.Running ||
			checkpoint.State == TaskEventStateCompleted && !checkpoint.Completed ||
			checkpoint.State != TaskEventStatePending && !checkpoint.EffectPossible {
			return corruptRecord()
		}
		previous = checkpoint.Identity.StepID
	}
	return nil
}

func trimmedTaskEventReplay(task TaskRecord, input TaskEventInput, hash string) (*TaskEventDedupRecord, error) {
	for _, checkpoint := range task.EventCheckpoints {
		if checkpoint.Identity.StepID != input.Identity.StepID {
			continue
		}
		if checkpoint.Identity == input.Identity {
			if checkpoint.PayloadSHA256 != hash {
				return nil, errs.New(errs.KindInternal, "trimmed event replay changed payload")
			}
			return &TaskEventDedupRecord{Identity: checkpoint.Identity, Sequence: checkpoint.Sequence,
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
) ([]Condition, []Mutation, error) {
	if task.EventCount < MaximumTaskEvents {
		return nil, nil, nil
	}
	oldest := firstTaskEventSequence(task)
	read, err := repository.store.GetMany(
		ctx,
		GetManyRequest{Keys: []string{taskEventKey(task.ID, oldest)}, Revision: revision},
	)
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, corruptRecord()
	}
	defer clearKeyValues(read.Values)
	event, err := decodeTaskEventRecord(read.Values[0].Value)
	if err != nil || event.Sequence != oldest || event.Identity.TaskID != task.ID {
		return nil, nil, corruptRecord()
	}
	dedupKey := taskEventDedupKey(event.Identity)
	dedupRead, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{dedupKey}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if dedupRead == nil || dedupRead.ReadRevision != revision || len(dedupRead.Values) != 1 ||
		dedupRead.Values[0] == nil {
		return nil, nil, corruptRecord()
	}
	defer clearKeyValues(dedupRead.Values)
	dedup, err := decodeTaskEventDedupRecord(dedupRead.Values[0].Value)
	if err != nil || dedup.Identity != event.Identity || dedup.Sequence != event.Sequence ||
		dedup.PayloadSHA256 != event.PayloadSHA256 {
		return nil, nil, corruptRecord()
	}
	checkpoint := TaskEventCheckpoint{Identity: event.Identity, Sequence: event.Sequence,
		PayloadSHA256: event.PayloadSHA256, State: event.State,
		Running:   event.State == TaskEventStateRunning || event.State == TaskEventStateCompleted,
		Completed: event.State == TaskEventStateCompleted, EffectPossible: event.State != TaskEventStatePending}
	index := slices.IndexFunc(
		prepared.Task.EventCheckpoints,
		func(value TaskEventCheckpoint) bool { return value.Identity.StepID == event.Identity.StepID },
	)
	if index >= 0 {
		prior := prepared.Task.EventCheckpoints[index]
		checkpoint.Running = checkpoint.Running || prior.Running
		checkpoint.Completed = checkpoint.Completed || prior.Completed
		checkpoint.EffectPossible = checkpoint.EffectPossible || prior.EffectPossible
		// Delivery order may differ from ordinal order. Never lower the replay
		// watermark when a later-evicted event has an older protocol identity.
		if prior.Identity.Attempt > checkpoint.Identity.Attempt ||
			prior.Identity.Attempt == checkpoint.Identity.Attempt &&
				prior.Identity.Ordinal > checkpoint.Identity.Ordinal {
			checkpoint.Identity, checkpoint.Sequence, checkpoint.PayloadSHA256 = prior.Identity, prior.Sequence, prior.PayloadSHA256
		}
		prepared.Task.EventCheckpoints[index] = checkpoint
	} else {
		prepared.Task.EventCheckpoints = append(prepared.Task.EventCheckpoints, checkpoint)
		slices.SortFunc(prepared.Task.EventCheckpoints, func(a, b TaskEventCheckpoint) int {
			if a.Identity.StepID < b.Identity.StepID {
				return -1
			}
			if a.Identity.StepID > b.Identity.StepID {
				return 1
			}
			return 0
		})
	}
	return []Condition{{Key: read.Values[0].Key, ModRevision: read.Values[0].ModRevision},
			{Key: dedupKey, ModRevision: dedupRead.Values[0].ModRevision}},
		[]Mutation{{Type: MutationDelete, Key: read.Values[0].Key}, {Type: MutationDelete, Key: dedupKey}}, nil
}
