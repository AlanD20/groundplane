package controllersoftware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Receipt struct {
	TaskID           string                  `json:"task_id"`
	WorkItemID       string                  `json:"work_item_id"`
	AttemptID        string                  `json:"attempt_id"`
	Input            workexecution.Input     `json:"input"`
	OriginAssignedAt time.Time               `json:"origin_assigned_at"`
	OriginDeadline   time.Time               `json:"origin_deadline"`
	Result           upgrade.ExecutionResult `json:"result"`
	At               time.Time               `json:"at"`
}

func encodeReceipt(receipt Receipt) ([]byte, error) {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil || ids.Validate(ids.KindWorkItem, receipt.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, receipt.AttemptID) != nil || receipt.Input.Validate() != nil ||
		!recordcodec.IsCanonicalUTC(receipt.OriginAssignedAt) || !recordcodec.IsCanonicalUTC(receipt.OriginDeadline) ||
		!recordcodec.IsCanonicalUTC(receipt.At) || !receipt.OriginDeadline.After(receipt.OriginAssignedAt) {
		return nil, errs.New(errs.KindValidationFailed, "Controller activation receipt is invalid")
	}
	return recordcodec.Encode("controller_software_result", receipt)
}

func (repository *Repository) ReadResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[Receipt], bool, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(
			errs.KindValidationFailed,
			"Controller activation receipt requires its Controller attempt",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ResultKey(attempt.ID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, false, err
	}
	if read == nil {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(
			errs.KindStorageUnavailable,
			"Controller activation receipt read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(
			errs.KindStateConflict,
			"Controller activation receipt snapshot is unavailable",
		)
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Receipt]{ReadRevision: read.ReadRevision}, false, nil
	}
	receipt, err := recordcodec.Decode[Receipt](read.Values[0].Value, "controller_software_result")
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, false, err
	}
	raw, err := encodeReceipt(receipt)
	clear(raw)
	if err != nil || receipt.TaskID != attempt.TaskID || receipt.WorkItemID != attempt.WorkItemID ||
		receipt.AttemptID != attempt.ID ||
		receipt.Input != attempt.Input {
		return keyvalue.Versioned[Receipt]{}, false, errs.New(
			errs.KindInternal,
			"Controller activation receipt owner is corrupt",
		)
	}
	return keyvalue.Versioned[Receipt]{
		Record:       receipt,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, nil
}

// SaveResult can account for a replaced executor's effects, but never authorizes
// a new release, launch or host mutation. Exact native ownership fences writes.
func (repository *Repository) SaveResult(
	ctx context.Context,
	queue *workqueue.Repository,
	attempt workexecution.Attempt,
	expected upgrade.Journal,
	result upgrade.ExecutionResult,
	at time.Time,
) error {
	if err := result.Validate(expected); err != nil {
		return err
	}
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil ||
		!workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return errs.New(errs.KindStateConflict, "Controller activation result no longer owns execution")
	}
	captured, err := repository.Load(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	actual, err := captured.Record.Journal(expected.StartedAt, expected.Deadline)
	if err != nil || !actual.SameOperation(expected) {
		return errs.New(errs.KindStateConflict, "Controller activation result changed captured predecessor")
	}
	previous, found, err := repository.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && previous.Record.Result.Settled {
		return nil
	}
	receipt := Receipt{
		TaskID:           attempt.TaskID,
		WorkItemID:       attempt.WorkItemID,
		AttemptID:        attempt.ID,
		Input:            attempt.Input,
		OriginAssignedAt: expected.StartedAt,
		OriginDeadline:   expected.Deadline,
		Result:           result,
		At:               at,
	}
	raw, err := encodeReceipt(receipt)
	if err != nil {
		return err
	}
	defer clear(raw)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: InputKey(attempt.WorkItemID), ModRevision: captured.Revision},
		{Key: ResultKey(attempt.ID), ModRevision: previous.Revision},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: raw}}}
	projectionOutcome := result.Outcome
	if attempt.Direction == workexecution.Recovery {
		projectionOutcome = view.Work.Item.Item.Outcome
	} else if result.Settled && (result.Journal == nil || result.Journal.Phase == upgrade.PhaseCancelled) && view.Work.Task.Task.Stop != nil {
		projectionOutcome = view.Work.Task.Task.Stop.Outcome
	}
	projection, err := repository.PrepareCurrent(
		ctx,
		captured,
		view.Work.ReadRevision,
		view.Attempt.Attempt.StartedAt != nil,
		projectionOutcome,
		result.Settled,
	)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(projection.Mutations)
	change.Conditions = append(change.Conditions, projection.Conditions...)
	change.Mutations = append(change.Mutations, projection.Mutations...)
	committed, err := repository.store.Transact(ctx, change.Conditions, change.Mutations)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Controller activation changed before result publication")
	}
	return nil
}

func PrepareResult(
	capture keyvalue.Versioned[Capture],
	saved keyvalue.Versioned[Receipt],
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	receipt := saved.Record
	expected, err := capture.Record.Journal(receipt.OriginAssignedAt, receipt.OriginDeadline)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if err := receipt.Result.Validate(expected); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	reference, err := capture.Record.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if capture.Revision <= 0 || saved.Revision <= 0 || reference != attempt.Input || receipt.Input != reference ||
		receipt.AttemptID != attempt.ID ||
		receipt.TaskID != attempt.TaskID ||
		receipt.WorkItemID != attempt.WorkItemID {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"Controller activation result differs from its captured input",
		)
	}
	raw, err := encodeReceipt(receipt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	outcome := receipt.Result.Outcome
	if attempt.Direction == workexecution.Recovery && receipt.Result.Settled {
		outcome = workexecution.Succeeded
	}
	return workqueue.Change{
			Conditions: []keyvalue.Condition{
				{Key: InputKey(attempt.WorkItemID), ModRevision: capture.Revision},
				{Key: ResultKey(attempt.ID), ModRevision: saved.Revision},
			},
		},
		workexecution.Completion{
			Outcome:        outcome,
			EffectsSettled: receipt.Result.Settled,
			Evidence: workexecution.Input{
				ID: ids.DeriveAt(
					ids.KindPlan,
					capture.Record.CreatedAt,
					attempt.ID,
					"controller-activation-result",
				), SHA256: hex.EncodeToString(digest[:])},
		}, nil
}
