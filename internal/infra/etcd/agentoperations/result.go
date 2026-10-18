package agentoperations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Observation string

const (
	NotStarted    Observation = "not_started"
	Absent        Observation = "absent"
	ReadyDesired  Observation = "ready_desired"
	ReadyPrevious Observation = "ready_previous"
	Unresolved    Observation = "unresolved"
)

type Result struct {
	Outcome    workexecution.Outcome `json:"outcome"`
	Observed   Observation           `json:"observed"`
	Generation uint64                `json:"generation,omitempty"`
	Failure    *taskprogress.Failure `json:"failure,omitempty"`
}

func (result Result) Settled() bool { return result.Observed != Unresolved }

func (result Result) validate(capture Capture) error {
	if result.Outcome != workexecution.Succeeded && result.Outcome != workexecution.Failed && result.Outcome != workexecution.Cancelled && result.Outcome != workexecution.Superseded {
		return errs.New(errs.KindValidationFailed, "Agent lifecycle result outcome is invalid")
	}
	if err := result.Failure.Validate(); err != nil {
		return err
	}
	switch result.Observed {
	case NotStarted, Absent, Unresolved:
		if result.Generation != 0 {
			return errs.New(errs.KindValidationFailed, "Agent lifecycle observation has an unexpected generation")
		}
	case ReadyDesired:
		generation := uint64(1)
		if capture.Action == workexecution.ActionUpdate {
			generation = capture.StartingGeneration + 1
		}
		if capture.Action == workexecution.ActionRemove || result.Generation != generation {
			return errs.New(errs.KindValidationFailed, "Agent lifecycle desired-generation evidence is invalid")
		}
	case ReadyPrevious:
		if capture.Action != workexecution.ActionUpdate && capture.Action != workexecution.ActionRemove ||
			result.Generation != capture.StartingGeneration && (capture.Action != workexecution.ActionUpdate || result.Generation != capture.StartingGeneration+2) {
			return errs.New(errs.KindValidationFailed, "Agent lifecycle predecessor evidence is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Agent lifecycle result observation is invalid")
	}
	if result.Outcome == workexecution.Succeeded && (result.Failure != nil || capture.Action == workexecution.ActionRemove && result.Observed != Absent ||
		capture.Action != workexecution.ActionRemove && result.Observed != ReadyDesired) {
		return errs.New(errs.KindValidationFailed, "Agent lifecycle success requires its exact requested effect")
	}
	return nil
}

type Receipt struct {
	TaskID     string              `json:"task_id"`
	WorkItemID string              `json:"work_item_id"`
	AttemptID  string              `json:"attempt_id"`
	Input      workexecution.Input `json:"input"`
	Result     Result              `json:"result"`
	At         time.Time           `json:"at"`
}

func encodeReceipt(receipt Receipt) ([]byte, error) {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil || ids.Validate(ids.KindWorkItem, receipt.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, receipt.AttemptID) != nil || receipt.Input.Validate() != nil || !recordcodec.IsCanonicalUTC(receipt.At) {
		return nil, errs.New(errs.KindValidationFailed, "Agent lifecycle receipt identity is invalid")
	}
	return recordcodec.Encode("agent_lifecycle_result", receipt)
}

func (repository *Repository) ReadResult(ctx context.Context, attempt workexecution.Attempt, revision int64) (keyvalue.Versioned[Receipt], bool, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(errs.KindValidationFailed, "Agent lifecycle receipt requires its Controller attempt")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{ResultKey(attempt.ID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, false, err
	}
	if read == nil {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(errs.KindStorageUnavailable, "Agent lifecycle result read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && revision != read.ReadRevision || len(read.Values) != 1 {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(errs.KindStateConflict, "Agent lifecycle result snapshot is unavailable")
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Receipt]{ReadRevision: read.ReadRevision}, false, nil
	}
	receipt, err := recordcodec.Decode[Receipt](read.Values[0].Value, "agent_lifecycle_result")
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, false, err
	}
	raw, err := encodeReceipt(receipt)
	clear(raw)
	if err != nil || receipt.TaskID != attempt.TaskID || receipt.WorkItemID != attempt.WorkItemID || receipt.AttemptID != attempt.ID || receipt.Input != attempt.Input {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(errs.KindInternal, "Agent lifecycle receipt owner is corrupt")
	}
	return keyvalue.Versioned[Receipt]{Record: receipt, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, true, nil
}

func (repository *Repository) SaveResult(ctx context.Context, queue *workqueue.Repository, attempt workexecution.Attempt, result Result, at time.Time) error {
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return errs.New(errs.KindStateConflict, "Agent lifecycle result no longer owns its attempt")
	}
	input, err := repository.Load(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if err := result.validate(input.Record); err != nil {
		return err
	}
	previous, found, err := repository.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && previous.Record.Result.Settled() {
		return nil
	}
	// An earlier observed failure cannot become success when later reconciliation
	// merely proves that the captured effects are safe.
	if found && previous.Record.Result.Failure != nil {
		result.Failure = taskprogress.CloneFailure(previous.Record.Result.Failure)
		result.Outcome = previous.Record.Result.Outcome
	}
	raw, err := encodeReceipt(Receipt{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID, Input: attempt.Input, Result: result, At: at})
	if err != nil {
		return err
	}
	defer clear(raw)
	conditions := []keyvalue.Condition{
		{Key: InputKey(attempt.WorkItemID), ModRevision: input.Revision}, {Key: ResultKey(attempt.ID), ModRevision: previous.Revision},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision}, {Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision}, {Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}
	committed, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: raw}})
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent lifecycle ownership changed before result publication")
	}
	return nil
}

func PrepareResult(input keyvalue.Versioned[Capture], saved keyvalue.Versioned[Receipt], attempt workexecution.Attempt) (workqueue.Change, workexecution.Completion, error) {
	reference, err := input.Record.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	receipt := saved.Record
	if input.Revision <= 0 || saved.Revision <= 0 || reference != attempt.Input || receipt.Input != reference || receipt.AttemptID != attempt.ID ||
		receipt.TaskID != attempt.TaskID || receipt.WorkItemID != attempt.WorkItemID {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "Agent lifecycle result differs from captured work")
	}
	if err := receipt.Result.validate(input.Record); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := encodeReceipt(receipt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	outcome := receipt.Result.Outcome
	if attempt.Direction == workexecution.Recovery && receipt.Result.Settled() {
		outcome = workexecution.Succeeded
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: InputKey(attempt.WorkItemID), ModRevision: input.Revision}, {Key: ResultKey(attempt.ID), ModRevision: saved.Revision}}},
		workexecution.Completion{Outcome: outcome, EffectsSettled: receipt.Result.Settled(), Evidence: workexecution.Input{
			ID: ids.DeriveAt(ids.KindPlan, input.Record.CreatedAt, attempt.ID, "agent-lifecycle-result"), SHA256: hex.EncodeToString(digest[:])}}, nil
}
