package backingprovisionwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// CompletionRecord authenticates replay and aggregate Task completion even
// after a later action replaces the resource's current applied runtime.
// Resource-owned receipts independently survive collection of this record.
type CompletionRecord struct {
	Attempt     workexecution.Attempt          `json:"attempt"`
	Checkpoint  workexecution.Checkpoint       `json:"checkpoint"`
	Observation backingruntimefact.Observation `json:"observation"`
}

func ResultKey(attemptID string) string {
	return "/v1/execution/backing-provision/results/" + attemptID
}

func (repository *Repository) ReadCompleted(ctx context.Context, attempt workexecution.Attempt,
	revision int64,
) (CompletionRecord, keyvalue.Condition, error) {
	if attempt.Validate() != nil || attempt.FinishedAt == nil || attempt.Result == nil || attempt.StartedAt == nil ||
		attempt.Direction != workexecution.Forward || revision <= 0 ||
		attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled &&
			attempt.Outcome != workexecution.Superseded {
		return CompletionRecord{}, keyvalue.Condition{}, invalid(
			"backing creation has no completed forward runtime receipt",
		)
	}
	key := ResultKey(attempt.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return CompletionRecord{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return CompletionRecord{}, keyvalue.Condition{}, invalid(
			"backing creation completed attempt lost its exact receipt",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return CompletionRecord{}, keyvalue.Condition{}, invalid(
			"backing creation completion receipt ownership is invalid",
		)
	}
	receipt, err := recordcodec.Decode[CompletionRecord](value.Value, "backing-provision-completion")
	if err != nil {
		return CompletionRecord{}, keyvalue.Condition{}, err
	}
	saved := receipt.Attempt
	if saved.Validate() != nil || !workexecution.SameAssignment(saved, attempt) || saved.StartedAt == nil ||
		!saved.StartedAt.Equal(
			*attempt.StartedAt,
		) || saved.FinishedAt == nil || !saved.FinishedAt.Equal(*attempt.FinishedAt) ||
		saved.Outcome != attempt.Outcome || saved.Result == nil || *saved.Result != *attempt.Result ||
		receipt.Observation.ObservedAt.After(*attempt.FinishedAt) {
		return CompletionRecord{}, keyvalue.Condition{}, invalid(
			"backing creation completion receipt differs from its actual attempt",
		)
	}
	evidence, err := backingprovision.CompletionEvidence(saved, receipt.Checkpoint, receipt.Observation)
	if err != nil || evidence != *attempt.Result {
		return CompletionRecord{}, keyvalue.Condition{}, invalid(
			"backing creation completion receipt has different runtime evidence",
		)
	}
	return receipt, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}
