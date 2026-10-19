package softwareactivation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type StagingResult struct {
	Controller     *upgrade.Release      `json:"controller,omitempty"`
	Agent          bool                  `json:"agent"`
	Settled        bool                  `json:"settled"`
	Failure        *taskprogress.Failure `json:"failure,omitempty"`
	CleanupFailure *taskprogress.Failure `json:"cleanup_failure,omitempty"`
}

type StagingReceipt struct {
	TaskID     string              `json:"task_id"`
	WorkItemID string              `json:"work_item_id"`
	AttemptID  string              `json:"attempt_id"`
	Input      workexecution.Input `json:"input"`
	Result     StagingResult       `json:"result"`
	At         time.Time           `json:"at"`
}

func StagingResultKey(attemptID string) string {
	return "/v1/execution/software-staging/results/" + attemptID
}

func (result StagingResult) validate(input Input) error {
	if err := result.Failure.Validate(); err != nil {
		return err
	}
	if err := result.CleanupFailure.Validate(); err != nil {
		return err
	}
	if result.Settled && result.CleanupFailure != nil {
		return errs.New(
			errs.KindValidationFailed,
			"settled software staging cannot retain an unresolved cleanup failure",
		)
	}
	selection := input.Preparation.Source.Selection
	if result.Controller != nil && (!selection.IncludesController() || result.Controller.ValidateRecord() != nil ||
		string(result.Controller.Manifest.ControllerSHA256) != input.Preparation.Progress.Result.Controller.BinarySHA256 ||
		result.Controller.Manifest.AgentImage != input.StagingAgentImage) || result.Agent && !selection.IncludesAgent() {
		return errs.New(errs.KindStateConflict, "software staging output differs from its captured artifacts")
	}
	if result.Failure == nil &&
		(!result.Settled || selection.IncludesController() != (result.Controller != nil) || selection.IncludesAgent() != result.Agent) {
		return errs.New(errs.KindStateConflict, "software staging success requires every selected artifact and cleanup")
	}
	return nil
}

func encodeStagingReceipt(receipt StagingReceipt) ([]byte, error) {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil || ids.Validate(ids.KindWorkItem, receipt.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, receipt.AttemptID) != nil ||
		receipt.Input.Validate() != nil ||
		!recordcodec.IsCanonicalUTC(receipt.At) {
		return nil, errs.New(errs.KindValidationFailed, "software staging receipt identity is invalid")
	}
	return recordcodec.Encode("software_staging_result", receipt)
}

func (repository *Repository) StagingResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[StagingReceipt], bool, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[StagingReceipt]{}, false, errs.New(
			errs.KindValidationFailed,
			"software staging requires its Controller attempt",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{StagingResultKey(attempt.ID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[StagingReceipt]{}, false, err
	}
	if read == nil {
		return keyvalue.Versioned[StagingReceipt]{}, false, errs.New(
			errs.KindStorageUnavailable,
			"software staging result read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[StagingReceipt]{}, false, errs.New(
			errs.KindStateConflict,
			"software staging result snapshot is unavailable",
		)
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[StagingReceipt]{ReadRevision: read.ReadRevision}, false, nil
	}
	receipt, err := recordcodec.Decode[StagingReceipt](read.Values[0].Value, "software_staging_result")
	if err != nil {
		return keyvalue.Versioned[StagingReceipt]{}, false, err
	}
	raw, err := encodeStagingReceipt(receipt)
	clear(raw)
	if err != nil || receipt.TaskID != attempt.TaskID || receipt.WorkItemID != attempt.WorkItemID ||
		receipt.AttemptID != attempt.ID ||
		receipt.Input != attempt.Input {
		return keyvalue.Versioned[StagingReceipt]{}, false, errs.New(
			errs.KindInternal,
			"software staging receipt ownership is corrupt",
		)
	}
	return keyvalue.Versioned[StagingReceipt]{
		Record:       receipt,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, nil
}

func (repository *Repository) SaveStagingResult(
	ctx context.Context,
	queue *workqueue.Repository,
	attempt workexecution.Attempt,
	result StagingResult,
	at time.Time,
) error {
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || view.Attempt.Attempt.FinishedAt != nil ||
		!workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
		return errs.New(errs.KindStateConflict, "software staging no longer owns execution")
	}
	record, err := repository.At(ctx, attempt.TaskID, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if err := record.Record.MatchStage(view.Work.Item.Item); err != nil {
		return err
	}
	if err := result.validate(record.Record.Input); err != nil {
		return err
	}
	previous, found, err := repository.StagingResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && previous.Record.Result.Settled {
		return nil
	}
	if found && previous.Record.Result.Failure != nil {
		result.Failure = taskprogress.CloneFailure(previous.Record.Result.Failure)
	}
	raw, err := encodeStagingReceipt(
		StagingReceipt{
			TaskID:     attempt.TaskID,
			WorkItemID: attempt.WorkItemID,
			AttemptID:  attempt.ID,
			Input:      attempt.Input,
			Result:     result,
			At:         at,
		},
	)
	if err != nil {
		return err
	}
	defer clear(raw)
	conditions := []keyvalue.Condition{
		{Key: Key(attempt.TaskID), ModRevision: record.Revision},
		{Key: StagingResultKey(attempt.ID), ModRevision: previous.Revision},
		{
			Key:         workqueue.TaskKey(attempt.TaskID),
			ModRevision: view.Work.Task.Revision,
		},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{
			Key:         workqueue.AttemptKey(attempt.ID),
			ModRevision: view.Attempt.Revision,
		},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: StagingResultKey(attempt.ID), Value: raw}}
	if result.Settled && result.Failure == nil {
		updated := record.Record
		updated.Progress = Progress{Phase: PhaseStaged, StagedController: result.Controller, AgentStaged: result.Agent}
		updated.UpdatedAt = at
		value, err := Encode(updated)
		if err != nil {
			return err
		}
		defer clear(value)
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(attempt.TaskID), Value: value},
		)
	}
	committed, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "software staging changed before result publication")
	}
	return nil
}

func PrepareStagingResult(
	record keyvalue.Versioned[Record],
	saved keyvalue.Versioned[StagingReceipt],
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	reference, err := record.Record.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	receipt := saved.Record
	if record.Revision <= 0 || saved.Revision <= 0 || reference != attempt.Input || receipt.Input != reference ||
		receipt.AttemptID != attempt.ID ||
		receipt.TaskID != attempt.TaskID ||
		receipt.WorkItemID != attempt.WorkItemID {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"software staging result differs from captured work",
		)
	}
	if err := receipt.Result.validate(record.Record.Input); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := encodeStagingReceipt(receipt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	outcome := workexecution.Succeeded
	if receipt.Result.Failure != nil {
		outcome = workexecution.Failed
	}
	if attempt.Direction == workexecution.Recovery && receipt.Result.Settled {
		outcome = workexecution.Succeeded
	}
	return workqueue.Change{
			Conditions: []keyvalue.Condition{
				{Key: Key(attempt.TaskID), ModRevision: record.Revision},
				{Key: StagingResultKey(attempt.ID), ModRevision: saved.Revision},
			},
		},
		workexecution.Completion{
			Outcome:        outcome,
			EffectsSettled: receipt.Result.Settled,
			Evidence: workexecution.Input{
				ID:     ids.DeriveAt(ids.KindPlan, record.Record.CreatedAt, attempt.ID, "software-staging-result"),
				SHA256: hex.EncodeToString(digest[:]),
			},
		}, nil
}
