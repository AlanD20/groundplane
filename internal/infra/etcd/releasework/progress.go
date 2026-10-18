// Package releasework owns native Release execution state independently of Task
// diagnostics. Cursor publication is joined to the native attempt checkpoint.
package releasework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumProgressBytes = 16 * 1024

type Record struct {
	Serving            *ServingProofReference    `json:"serving,omitempty"`
	TaskID             string                    `json:"task_id"`
	WorkItemID         string                    `json:"work_item_id"`
	Input              workexecution.Input       `json:"input"`
	Progress           releaseexecution.Progress `json:"progress"`
	StageID            string                    `json:"stage_id"`
	CheckpointSequence uint64                    `json:"checkpoint_sequence"`
	ActiveHookID       string                    `json:"active_hook_id,omitempty"`
}

// ServingProofReference survives post-deploy hook checkpoints. Settlement must
// still consume the exact final serving proof, not whichever hook completed last.
type ServingProofReference struct {
	Boundary releaseexecution.CheckpointReport `json:"boundary"`
	Evidence workexecution.Input               `json:"evidence"`
}

type Snapshot struct {
	Record   *Record
	Revision int64
}

func ProgressKey(attemptID string) string { return "/v1/execution/release-progress/" + attemptID }

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func Load(ctx context.Context, store Reader, input releaseexecution.Input, revision int64) (Snapshot, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if store == nil || revision <= 0 || input.Attempt.Validate() != nil {
		return Snapshot{}, invalid()
	}
	key := ProgressKey(input.Attempt.ID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Snapshot{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return Snapshot{}, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || len(value.Value) > maximumProgressBytes {
		return Snapshot{}, invalid()
	}
	record, err := recordcodec.Decode[Record](value.Value, "release-work-progress")
	if err != nil {
		return Snapshot{}, err
	}
	if err := validate(record, input); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Record: &record, Revision: value.ModRevision}, nil
}

// Prepare publishes no effect by itself. The operation owner must join its
// current effect authority for Begin, or verified result evidence for Confirm.
func Prepare(view workqueue.ResumeView, input releaseexecution.Input, previous Snapshot, report releaseexecution.CheckpointReport) (workqueue.Change, error) {
	if report.Ack != nil || report.ValidateEnvelope(input.Attempt) != nil {
		return workqueue.Change{}, invalid()
	}
	next := report.Progress
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) ||
		view.Attempt.Attempt.StartedAt == nil || input.Attempt.StartedAt == nil || !view.Attempt.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) ||
		previous.Revision < 0 || (previous.Record == nil) != (previous.Revision == 0) ||
		(previous.Record == nil) != (view.Checkpoint.Record == nil) {
		return workqueue.Change{}, invalid()
	}
	var prior *releaseexecution.Progress
	if previous.Record != nil {
		if err := validate(*previous.Record, input); err != nil {
			return workqueue.Change{}, err
		}
		prior = &previous.Record.Progress
		if previous.Record.CheckpointSequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, invalid()
		}
		if previous.Record.CheckpointSequence == view.Checkpoint.Record.Sequence {
			expected, err := (releaseexecution.CheckpointReport{Sequence: previous.Record.CheckpointSequence, StageID: previous.Record.StageID, Progress: previous.Record.Progress}).Checkpoint(input.Attempt)
			if err != nil {
				return workqueue.Change{}, err
			}
			if !releaseexecution.EqualCheckpoint(*view.Checkpoint.Record, expected) || previous.Revision != view.Checkpoint.Revision {
				return workqueue.Change{}, invalid()
			}
		} else if !prior.InFlight || prior.Phase != releaseexecution.PreHooks && prior.Phase != releaseexecution.PostHooks {
			// Script hook checkpoints share the attempt cursor. Other Release
			// phases cannot be silently overtaken by another checkpoint owner.
			return workqueue.Change{}, invalid()
		}
	}
	if err := releaseexecution.ValidateTransition(input, prior, next); err != nil {
		return workqueue.Change{}, err
	}
	replay := prior != nil && releaseexecution.EqualProgress(*prior, next)
	if !replay && next.InFlight && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked) {
		return workqueue.Change{}, invalid()
	}
	stage := next
	if prior != nil && !next.InFlight {
		stage = *prior
	}
	stageID, err := stage.StageID(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	sequence := uint64(1)
	if view.Checkpoint.Record != nil {
		if view.Checkpoint.Record.Sequence == ^uint64(0) {
			return workqueue.Change{}, invalid()
		}
		sequence = view.Checkpoint.Record.Sequence + 1
	}
	if replay {
		if previous.Record.CheckpointSequence != view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, invalid()
		}
		stageID, sequence = previous.Record.StageID, previous.Record.CheckpointSequence
	}
	if report.Sequence != sequence || report.StageID != stageID {
		return workqueue.Change{}, invalid()
	}
	record := Record{TaskID: input.Attempt.TaskID, WorkItemID: input.Attempt.WorkItemID, Input: input.Attempt.Input, Progress: next,
		StageID: stageID, CheckpointSequence: sequence}
	if previous.Record != nil {
		record.Serving = previous.Record.Serving
	}
	if !replay && prior != nil && prior.InFlight && prior.Phase == releaseexecution.Rollout && prior.Action == releaseexecution.VerifyServing && !next.InFlight {
		boundary := releaseexecution.CheckpointReport{Sequence: previous.Record.CheckpointSequence, StageID: previous.Record.StageID, Progress: *prior}
		ack, err := boundary.Checkpoint(input.Attempt)
		if err != nil || next.Evidence == nil {
			return workqueue.Change{}, invalid()
		}
		boundary.Ack = &ack
		record.Serving = &ServingProofReference{Boundary: boundary, Evidence: *next.Evidence}
	}
	if next.InFlight && (next.Phase == releaseexecution.PreHooks || next.Phase == releaseexecution.PostHooks) {
		hook, err := next.ActiveHook(input)
		if err != nil {
			return workqueue.Change{}, err
		}
		record.ActiveHookID = hook.ExecutionId
	}
	if err := validate(record, input); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("release-work-progress", record)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(value) > maximumProgressBytes {
		clear(value)
		return workqueue.Change{}, invalid()
	}
	checkpoint, err := (releaseexecution.CheckpointReport{Sequence: sequence, StageID: stageID, Progress: next}).Checkpoint(input.Attempt)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ProgressKey(input.Attempt.ID), ModRevision: previous.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ProgressKey(input.Attempt.ID), Value: value})
	return change, nil
}

func validate(record Record, input releaseexecution.Input) error {
	if record.TaskID != input.Attempt.TaskID || record.WorkItemID != input.Attempt.WorkItemID || record.Input != input.Attempt.Input || record.Progress.Sequence == 0 ||
		record.CheckpointSequence == 0 || ids.Validate(ids.KindStep, record.StageID) != nil {
		return invalid()
	}
	expectedHook := ""
	if record.Progress.InFlight && (record.Progress.Phase == releaseexecution.PreHooks || record.Progress.Phase == releaseexecution.PostHooks) {
		hook, err := record.Progress.ActiveHook(input)
		if err != nil {
			return err
		}
		expectedHook = hook.ExecutionId
	}
	if record.ActiveHookID != expectedHook {
		return invalid()
	}
	afterServing := record.Progress.Phase == releaseexecution.PostHooks || record.Progress.Phase == releaseexecution.Complete
	if afterServing != (record.Serving != nil) {
		return invalid()
	}
	if serving := record.Serving; serving != nil {
		boundary := serving.Boundary
		if boundary.Ack == nil || boundary.ValidateEnvelope(input.Attempt) != nil || boundary.Progress.Validate(input) != nil ||
			!boundary.Progress.InFlight || boundary.Progress.Phase != releaseexecution.Rollout || boundary.Progress.Action != releaseexecution.VerifyServing ||
			boundary.Sequence >= record.CheckpointSequence || boundary.Progress.Sequence >= record.Progress.Sequence ||
			boundary.Progress.UpdatedAt.After(record.Progress.UpdatedAt) || serving.Evidence.Validate() != nil {
			return invalid()
		}
		stage, err := boundary.Progress.StageID(input)
		if err != nil || stage != boundary.StageID {
			return invalid()
		}
	}
	return record.Progress.Validate(input)
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "Release progress does not match its active attempt and checkpoint")
}
