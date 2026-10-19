package attachprovision

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type Result struct {
	Attempt     workexecution.Attempt    `json:"attempt"`
	Checkpoint  workexecution.Checkpoint `json:"checkpoint"`
	AttachID    string                   `json:"attach_id"`
	FactsSHA256 string                   `json:"facts_sha256"`
	CompletedAt time.Time                `json:"completed_at"`
}

func ResultKey(workID string) string { return "/v1/attach-provision-results/" + workID }

// PrepareCompletion publishes only the proven binding and its final facts. The
// caller joins applied-state receipt and native attempt completion atomically.
func (repository *InputRepository) PrepareCompletion(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, at time.Time,
) (_ workqueue.Change, _ workexecution.Input, resultErr error) {
	completed, err := repository.LoadCompleted(ctx, view, input, at)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	defer completed.Clear()
	guards, err := repository.PendingBindingConditions(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change := workqueue.Change{Conditions: append(completed.Conditions, guards...)}
	defer func() {
		if resultErr != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	record, err := completed.Source.Generation.Record.PendingRecord(
		view.Work.Task.Task.ID,
		view.Work.Task.Task.CreatedAt,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	record.Status = core.AttachReady
	value, err := attachments.EncodeAttachRecord(record)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachments.AttachKey(record.ID), Value: value},
	)
	if record.OwnsCredential() && len(completed.Facts.Ciphertext) != 0 {
		facts, err := attachments.EncodeAttachEncryptedFacts(completed.Facts)
		if err != nil {
			return workqueue.Change{}, workexecution.Input{}, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachments.AttachFactsKey(record.ID), Value: facts},
		)
	}
	result := Result{Attempt: view.Attempt.Attempt, Checkpoint: *view.Checkpoint.Record, AttachID: record.ID,
		FactsSHA256: completed.Facts.CiphertextSHA256, CompletedAt: at}
	raw, evidence, err := encodeResult(result)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	key := ResultKey(view.Work.Item.Item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	var ownFacts *attachments.EncryptedFacts
	if record.OwnsCredential() && len(completed.Facts.Ciphertext) != 0 {
		ownFacts = &completed.Facts
	}
	applied, err := repository.prepareAppliedBinding(
		ctx,
		view.Work,
		completed.Source.Generation.Record,
		record,
		ownFacts,
	)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Conditions = append(change.Conditions, applied.Conditions...)
	change.Mutations = append(change.Mutations, applied.Mutations...)
	return change, evidence, nil
}

func encodeResult(result Result) ([]byte, workexecution.Input, error) {
	attempt := result.Attempt
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward ||
		attempt.Executor.Executor != workexecution.Agent ||
		attempt.StartedAt == nil ||
		attempt.FinishedAt != nil ||
		attempt.Result != nil ||
		ids.Validate(ids.KindAttach, result.AttachID) != nil ||
		result.FactsSHA256 != "" && !recordcodec.ValidSHA256(result.FactsSHA256) ||
		result.Checkpoint.AttemptID != attempt.ID ||
		result.Checkpoint.Sequence == 0 ||
		result.Checkpoint.Evidence.Validate() != nil ||
		ids.Validate(ids.KindStep, result.Checkpoint.StageID) != nil ||
		result.Checkpoint.RecordedAt.Before(*attempt.StartedAt) ||
		result.CompletedAt.Before(result.Checkpoint.RecordedAt) ||
		result.CompletedAt.IsZero() ||
		result.CompletedAt.Location() != time.UTC {
		return nil, workexecution.Input{}, checkpointConflict("Attach completion result has invalid execution evidence")
	}
	raw, err := recordcodec.Encode("attach-provision-result", result)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "attach-binding-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (repository *InputRepository) LoadResult(ctx context.Context, view workqueue.ResumeView) (Result, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || view.Work.Item.Item.Operation != workexecution.Attach ||
		view.Work.ReadRevision <= 0 {
		return Result{}, checkpointConflict("Attach has no retained binding settlement")
	}
	key := ResultKey(attempt.WorkItemID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return Result{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.Work.ReadRevision {
		return Result{}, checkpointConflict("Attach binding settlement evidence is unavailable")
	}
	result, err := recordcodec.Decode[Result](read.Values[0].Value, "attach-provision-result")
	if err != nil {
		return Result{}, err
	}
	raw, evidence, err := encodeResult(result)
	if err != nil {
		return Result{}, err
	}
	defer clear(raw)
	if !bytes.Equal(raw, read.Values[0].Value) || evidence != *attempt.Result ||
		!workexecution.SameAssignment(result.Attempt, attempt) || attempt.StartedAt == nil ||
		!attempt.StartedAt.Equal(*result.Attempt.StartedAt) || !attempt.FinishedAt.Equal(result.CompletedAt) ||
		result.AttachID != view.Work.Item.Item.TargetID || view.Checkpoint.Record == nil || !sameCheckpoint(result.Checkpoint, *view.Checkpoint.Record) {
		return Result{}, checkpointConflict("Attach binding settlement differs from its terminal execution")
	}
	return result, nil
}
