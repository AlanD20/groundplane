package imageoperations

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// removalGuard is shared with workload image selection. Keeping an empty record
// after release advances its revision: pre-removal selections cannot commit later.
type removalGuard struct {
	Operation string `json:"operation"`
}

func (journal *Journal) readRemovalGuard(ctx context.Context, revision int64) (removalGuard, int64, error) {
	read, err := journal.repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{imagefence.Key}, Revision: revision},
	)
	if err != nil {
		return removalGuard{}, 0, err
	}
	if read == nil {
		return removalGuard{}, 0, conflict()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 {
		return removalGuard{}, 0, conflict()
	}
	if read.Values[0] == nil {
		return removalGuard{}, 0, nil
	}
	var guard removalGuard
	if json.Unmarshal(read.Values[0].Value, &guard) != nil {
		return removalGuard{}, 0, conflict()
	}
	return guard, read.Values[0].ModRevision, nil
}

// BeginRemoval reserves the shared deletion guard before the owner rereads
// container/recovery protections. No Docker deletion is authorized by this write
// alone: those protections must pass while the guard remains held.
func (journal *Journal) BeginRemoval(ctx context.Context, at time.Time) error {
	owned, err := journal.read(ctx, true)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Ready || record.Input.Action != workexecution.ActionRemove {
		return conflict()
	}
	guard, revision, err := journal.readRemovalGuard(ctx, owned.revision)
	if err != nil {
		return err
	}
	if guard.Operation != "" {
		return conflict()
	}
	value, err := json.Marshal(removalGuard{Operation: record.Input.OperationID})
	if err != nil {
		return err
	}
	defer clear(value)
	record.Phase, record.AttemptID, record.UpdatedAt = Removing, journal.attemptID, at
	return journal.commit(ctx, owned, record, workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: imagefence.Key, ModRevision: revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: value}},
	})
}

type RemovalOutcome uint8

const (
	RemovalUnknown RemovalOutcome = iota
	RemovalAbsent
	RemovalRejected
)

// RemovalRejected requires proof that Docker did not start deletion (for example
// an in-use refusal or failed preflight). Mere image presence after a timeout is
// not that proof: the outstanding Docker request may still remove it.
func (journal *Journal) RecordRemovalOutcome(ctx context.Context, outcome RemovalOutcome, at time.Time) error {
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Removing && !(record.Phase == Uncertain && record.UncertainPhase == Removing) {
		return conflict()
	}
	guard, revision, err := journal.readRemovalGuard(ctx, owned.revision)
	if err != nil {
		return err
	}
	if guard.Operation != record.Input.OperationID {
		return conflict()
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: imagefence.Key, ModRevision: revision}}}
	record.UpdatedAt = at
	switch outcome {
	case RemovalUnknown:
		record.Phase, record.UncertainPhase = Uncertain, Removing
	case RemovalAbsent, RemovalRejected:
		record.Phase, record.UncertainPhase, record.FinishedAt = Finished, "", &at
		record.Outcome = workexecution.Failed
		if outcome == RemovalAbsent {
			record.Outcome = workexecution.Succeeded
		}
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: []byte(`{"operation":""}`)},
		}
	default:
		return invalid()
	}
	return journal.commit(ctx, owned, record, change)
}
