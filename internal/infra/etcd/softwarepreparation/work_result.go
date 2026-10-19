package softwarepreparation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Outcome struct {
	TaskID       string              `json:"task_id"`
	WorkItemID   string              `json:"work_item_id"`
	AttemptID    string              `json:"attempt_id"`
	Input        workexecution.Input `json:"input"`
	RecordSHA256 string              `json:"record_sha256"`
	Cleaned      bool                `json:"cleaned"`
	At           time.Time           `json:"at"`
}

func encodeOutcome(outcome Outcome) ([]byte, error) {
	if ids.Validate(ids.KindTask, outcome.TaskID) != nil || ids.Validate(ids.KindWorkItem, outcome.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, outcome.AttemptID) != nil || outcome.Input.Validate() != nil ||
		!recordcodec.ValidSHA256(outcome.RecordSHA256) || !recordcodec.IsCanonicalUTC(outcome.At) {
		return nil, errs.New(errs.KindValidationFailed, "software preparation outcome is invalid")
	}
	return recordcodec.Encode("software_preparation_outcome", outcome)
}

func (repository *Repository) ReadOutcome(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[Outcome], bool, error) {
	if attempt.Validate() != nil {
		return keyvalue.Versioned[Outcome]{}, false, errs.New(
			errs.KindValidationFailed,
			"software outcome attempt is invalid",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ResultKey(attempt.ID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[Outcome]{}, false, err
	}
	if read == nil {
		return keyvalue.Versioned[Outcome]{}, false, errs.New(
			errs.KindStorageUnavailable,
			"software outcome read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Outcome]{}, false, errs.New(
			errs.KindStateConflict,
			"software outcome snapshot is unavailable",
		)
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Outcome]{ReadRevision: read.ReadRevision}, false, nil
	}
	value, err := recordcodec.Decode[Outcome](read.Values[0].Value, "software_preparation_outcome")
	if err != nil {
		return keyvalue.Versioned[Outcome]{}, false, err
	}
	raw, err := encodeOutcome(value)
	clear(raw)
	if err != nil || value.TaskID != attempt.TaskID || value.WorkItemID != attempt.WorkItemID ||
		value.AttemptID != attempt.ID ||
		value.Input != attempt.Input {
		return keyvalue.Versioned[Outcome]{}, false, errs.New(errs.KindInternal, "software outcome owner is corrupt")
	}
	return keyvalue.Versioned[Outcome]{
		Record:       value,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, nil
}

// RecordOutcome joins terminal artifact progress with exact-attempt cleanup
// proof. Unknown cleanup can later be refined, never rewritten after settlement.
func (repository *Repository) RecordOutcome(
	ctx context.Context,
	queue *workqueue.Repository,
	attempt workexecution.Attempt,
	progress preparation.Progress,
	cleaned bool,
	at time.Time,
) error {
	if progress.Phase != preparation.PhaseFailed && progress.Phase != preparation.PhaseVerified ||
		progress.Phase == preparation.PhaseVerified && !cleaned {
		return errs.New(
			errs.KindValidationFailed,
			"software outcome requires a terminal preparation and explicit cleanup proof",
		)
	}
	view, snapshot, err := repository.activeWork(ctx, queue, attempt)
	if err != nil {
		return err
	}
	previous, found, err := repository.ReadOutcome(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if found && previous.Record.Cleaned {
		return nil
	}
	// The final verification checkpoint is written only after builder cleanup
	// and exact registry readback. A lost checkpoint response cannot undo it.
	if snapshot.Record.Record.Progress.Phase == preparation.PhaseVerified {
		progress, cleaned = snapshot.Record.Record.Progress, true
	}
	change, record, err := prepareProgress(snapshot, progress, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	rawRecord, err := Encode(record)
	if err != nil {
		return err
	}
	defer clear(rawRecord)
	digest := sha256.Sum256(rawRecord)
	outcome := Outcome{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
		Input: attempt.Input, RecordSHA256: hex.EncodeToString(digest[:]), Cleaned: cleaned, At: at}
	raw, err := encodeOutcome(outcome)
	if err != nil {
		return err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: ResultKey(attempt.ID), ModRevision: previous.Revision},
	)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: raw},
	)
	return repository.commitWork(ctx, view, change)
}

func PrepareResult(
	snapshot WorkSnapshot,
	saved keyvalue.Versioned[Outcome],
	attempt workexecution.Attempt,
) (workqueue.Change, workexecution.Completion, error) {
	outcome := saved.Record
	rawRecord, err := Encode(snapshot.Record.Record)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(rawRecord)
	digest := sha256.Sum256(rawRecord)
	reference, err := snapshot.Capture.Reference()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if saved.Revision <= 0 || outcome.RecordSHA256 != hex.EncodeToString(digest[:]) || reference != attempt.Input ||
		outcome.Input != reference || outcome.TaskID != attempt.TaskID || outcome.WorkItemID != attempt.WorkItemID || outcome.AttemptID != attempt.ID {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"software preparation result differs from its accepted evidence",
		)
	}
	raw, err := encodeOutcome(outcome)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer clear(raw)
	digest = sha256.Sum256(raw)
	result := workexecution.Completion{Outcome: workexecution.Failed, EffectsSettled: outcome.Cleaned,
		Evidence: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, snapshot.Capture.CreatedAt, attempt.ID, "software-preparation-result"),
			SHA256: hex.EncodeToString(digest[:]),
		}}
	if outcome.Cleaned &&
		(snapshot.Record.Record.Progress.Phase == preparation.PhaseVerified || attempt.Direction == workexecution.Recovery) {
		result.Outcome = workexecution.Succeeded
	}
	change := snapshot.Guard()
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: ResultKey(attempt.ID), ModRevision: saved.Revision},
	)
	return change, result, nil
}
