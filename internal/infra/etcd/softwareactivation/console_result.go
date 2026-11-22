package softwareactivation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ConsoleReceipt struct {
	TaskID     string                  `json:"task_id"`
	WorkItemID string                  `json:"work_item_id"`
	AttemptID  string                  `json:"attempt_id"`
	Input      workexecution.Input     `json:"input"`
	Desired    consolerelease.Artifact `json:"desired"`
	Current    consolerelease.Artifact `json:"current"`
	Found      bool                    `json:"found"`
	At         time.Time               `json:"at"`
}

func ConsoleResultKey(attemptID string) string {
	return "/v1/execution/console-software/results/" + attemptID
}

func encodeConsoleReceipt(receipt ConsoleReceipt) ([]byte, error) {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, receipt.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, receipt.AttemptID) != nil || receipt.Input.Validate() != nil ||
		!receipt.Desired.Valid() || receipt.Found && !receipt.Current.Valid() ||
		!receipt.Found && receipt.Current != (consolerelease.Artifact{}) ||
		!recordcodec.IsCanonicalUTC(receipt.At) {
		return nil, errs.New(errs.KindValidationFailed, "Console activation receipt is invalid")
	}
	return recordcodec.Encode("console_software_result", receipt)
}

func validateConsoleReceipt(record Record, receipt ConsoleReceipt, attempt workexecution.Attempt) error {
	reference, err := record.ConsoleReference()
	if err != nil || attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller ||
		!record.Input.Preparation.Source.Selection.IncludesConsole() ||
		receipt.TaskID != attempt.TaskID || receipt.WorkItemID != attempt.WorkItemID ||
		receipt.AttemptID != attempt.ID || receipt.Input != attempt.Input || reference != attempt.Input ||
		record.TaskID != attempt.TaskID || record.ConsoleWorkItemID != attempt.WorkItemID ||
		record.Input.Preparation.Progress.Result.Console == nil ||
		receipt.Desired != record.Input.Preparation.Progress.Result.Console.Artifact ||
		attempt.FinishedAt != nil && !receipt.At.Equal(*attempt.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Console activation result differs from captured work")
	}
	return nil
}

func PrepareConsoleReceipt(
	record keyvalue.Versioned[Record],
	attempt workexecution.Attempt,
	current consolerelease.Artifact,
	found bool,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	if record.Revision <= 0 {
		return workqueue.Change{}, workexecution.Completion{},
			errs.New(errs.KindStateConflict, "Console activation record is unversioned")
	}
	if record.Record.Input.Preparation.Progress.Result.Console == nil {
		return workqueue.Change{}, workexecution.Completion{},
			errs.New(errs.KindStateConflict, "Console activation record has no captured artifact")
	}
	receipt := ConsoleReceipt{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID,
		AttemptID: attempt.ID, Input: attempt.Input,
		Desired: record.Record.Input.Preparation.Progress.Result.Console.Artifact,
		Current: current, Found: found, At: at}
	if err := validateConsoleReceipt(record.Record, receipt, attempt); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := encodeConsoleReceipt(receipt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	result := consoleReceiptCompletion(record.Record, receipt, attempt, raw)
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: ConsoleResultKey(attempt.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut,
			Key: ConsoleResultKey(attempt.ID), Value: raw}},
	}, result, nil
}

func (repository *Repository) ReadConsoleResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[ConsoleReceipt], bool, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Controller {
		return keyvalue.Versioned[ConsoleReceipt]{}, false,
			errs.New(errs.KindValidationFailed, "Console activation receipt requires its Controller attempt")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{ConsoleResultKey(attempt.ID)}, Revision: revision,
	})
	if err != nil {
		return keyvalue.Versioned[ConsoleReceipt]{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[ConsoleReceipt]{}, false,
			errs.New(errs.KindStateConflict, "Console activation receipt snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[ConsoleReceipt]{ReadRevision: read.ReadRevision}, false, nil
	}
	receipt, err := recordcodec.Decode[ConsoleReceipt](read.Values[0].Value, "console_software_result")
	if err != nil {
		return keyvalue.Versioned[ConsoleReceipt]{}, false, err
	}
	raw, encodeErr := encodeConsoleReceipt(receipt)
	clear(raw)
	if encodeErr != nil || receipt.TaskID != attempt.TaskID ||
		receipt.WorkItemID != attempt.WorkItemID || receipt.AttemptID != attempt.ID || receipt.Input != attempt.Input {
		return keyvalue.Versioned[ConsoleReceipt]{}, false,
			errs.New(errs.KindInternal, "Console activation receipt owner is corrupt")
	}
	return keyvalue.Versioned[ConsoleReceipt]{Record: receipt, Revision: read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision}, true, nil
}

func PrepareConsoleResult(
	record keyvalue.Versioned[Record],
	saved keyvalue.Versioned[ConsoleReceipt],
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	if record.Revision <= 0 || saved.Revision <= 0 {
		return workqueue.Change{}, workexecution.Completion{},
			errs.New(errs.KindStateConflict, "Console activation result is unversioned")
	}
	if err := validateConsoleReceipt(record.Record, saved.Record, attempt); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := encodeConsoleReceipt(saved.Record)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: ConsoleResultKey(attempt.ID),
		ModRevision: saved.Revision}}}, consoleReceiptCompletion(record.Record, saved.Record, attempt, raw), nil
}

func consoleReceiptCompletion(
	record Record,
	receipt ConsoleReceipt,
	attempt workexecution.Attempt,
	raw []byte,
) workexecution.Completion {
	digest := sha256.Sum256(raw)
	outcome := workexecution.Failed
	if attempt.StartedAt != nil && receipt.Found && receipt.Current == receipt.Desired {
		outcome = workexecution.Succeeded
	}
	return workexecution.Completion{Outcome: outcome, EffectsSettled: receipt.Found, Evidence: workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, record.CreatedAt, attempt.ID, "console-activation-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}}
}
