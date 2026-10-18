package etcdconfiguration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Result struct {
	TaskID     string                 `json:"task_id"`
	WorkItemID string                 `json:"work_item_id"`
	AttemptID  string                 `json:"attempt_id"`
	Input      workexecution.Input    `json:"input"`
	Observed   etcdactivation.Outcome `json:"observed"`
	Failed     bool                   `json:"failed"`
	At         time.Time              `json:"at"`
}

func encodeResult(result Result) ([]byte, error) {
	if ids.Validate(ids.KindTask, result.TaskID) != nil || ids.Validate(ids.KindWorkItem, result.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, result.AttemptID) != nil || result.Input.Validate() != nil ||
		!result.Observed.Valid() || !recordcodec.IsCanonicalUTC(result.At) ||
		(result.Observed != etcdactivation.Applied && !result.Failed) {
		return nil, errs.New(errs.KindValidationFailed, "etcd activation result is invalid")
	}
	return recordcodec.Encode("etcd_configuration_result", result)
}

func (repository *Repository) ReadResult(ctx context.Context, attempt workexecution.Attempt, revision int64) (keyvalue.Versioned[Result], bool, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[Result]{}, false, errs.New(errs.KindValidationFailed, "etcd activation result requires a Controller attempt")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{ResultKey(attempt.ID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Result]{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || (revision > 0 && read.ReadRevision != revision) || len(read.Values) != 1 {
		return keyvalue.Versioned[Result]{}, false, errs.New(errs.KindStateConflict, "etcd activation result read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Result]{ReadRevision: read.ReadRevision}, false, nil
	}
	result, err := recordcodec.Decode[Result](read.Values[0].Value, "etcd_configuration_result")
	if err != nil {
		return keyvalue.Versioned[Result]{}, false, err
	}
	raw, err := encodeResult(result)
	clear(raw)
	if err != nil || result.TaskID != attempt.TaskID || result.WorkItemID != attempt.WorkItemID || result.AttemptID != attempt.ID || result.Input != attempt.Input {
		return keyvalue.Versioned[Result]{}, false, errs.New(errs.KindInternal, "etcd activation result owner is corrupt")
	}
	return keyvalue.Versioned[Result]{Record: result, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, true, nil
}

// SaveResult records an observation under the exact active attempt. Replaced
// process reconciliation may account for effects, but cannot grant new Apply.
func (repository *Repository) SaveResult(ctx context.Context, queue *workqueue.Repository, attempt workexecution.Attempt,
	observed etcdactivation.Outcome, failed bool,
) error {
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) || view.Attempt.Attempt.FinishedAt != nil {
		return errs.New(errs.KindStateConflict, "etcd activation result no longer owns execution")
	}
	input, err := repository.Load(ctx, attempt.WorkItemID, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if err := input.Record.Match(view.Work.Item.Item); err != nil {
		return err
	}
	previous, found, err := repository.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && previous.Record.Observed != etcdactivation.Unresolved {
		return nil
	}
	if found {
		failed = failed || previous.Record.Failed
	}
	result := Result{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
		Input: attempt.Input, Observed: observed, Failed: failed || observed != etcdactivation.Applied, At: time.Now().UTC()}
	raw, err := encodeResult(result)
	if err != nil {
		return err
	}
	defer clear(raw)
	conditions := []keyvalue.Condition{{Key: InputKey(attempt.WorkItemID), ModRevision: input.Revision},
		{Key: ResultKey(attempt.ID), ModRevision: previous.Revision},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision}}
	committed, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: raw}})
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "etcd activation result changed before publication")
	}
	return nil
}

func PrepareResult(input keyvalue.Versioned[Input], saved keyvalue.Versioned[Result], attempt workexecution.Attempt) (workqueue.Change, workexecution.Completion, error) {
	result := saved.Record
	reference, err := input.Record.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if input.Revision <= 0 || saved.Revision <= 0 || reference != attempt.Input || result.Input != reference ||
		result.AttemptID != attempt.ID || result.TaskID != attempt.TaskID || result.WorkItemID != attempt.WorkItemID {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "etcd activation has no captured result")
	}
	raw, err := encodeResult(result)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	evidence := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, input.Record.CreatedAt, attempt.ID, "etcd-configuration-result"), SHA256: hex.EncodeToString(digest[:])}
	settled := result.Observed != etcdactivation.Unresolved
	outcome := workexecution.Failed
	if settled && (!result.Failed || attempt.Direction == workexecution.Recovery) {
		outcome = workexecution.Succeeded
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: InputKey(attempt.WorkItemID), ModRevision: input.Revision},
			{Key: ResultKey(attempt.ID), ModRevision: saved.Revision}}},
		workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}, nil
}
