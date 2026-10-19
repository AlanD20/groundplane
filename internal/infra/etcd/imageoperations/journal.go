package imageoperations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Journal struct {
	repository *Repository
	taskID     string
	workID     string
	attemptID  string
	identity   workexecution.Identity
}

func NewJournal(
	repository *Repository,
	input Input,
	attemptID string,
	identity workexecution.Identity,
) (*Journal, error) {
	if repository == nil || input.Validate() != nil || ids.Validate(ids.KindAttempt, attemptID) != nil ||
		identity.Validate() != nil ||
		input.Placement != (workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID}) {
		return nil, invalid()
	}
	return &Journal{
		repository: repository,
		taskID:     input.TaskID,
		workID:     input.WorkItemID,
		attemptID:  attemptID,
		identity:   identity,
	}, nil
}

type ownedRecord struct {
	record     Record
	attempt    workexecution.Attempt
	conditions []keyvalue.Condition
	revision   int64
}

// read binds every mutation to the current process generation and exact input.
// Revocation prevents new effects, but does not prevent recording an outcome.
func (journal *Journal) read(ctx context.Context, forward bool) (ownedRecord, error) {
	return journal.readState(ctx, forward, false)
}

func (journal *Journal) readState(ctx context.Context, forward, assignedOnly bool) (ownedRecord, error) {
	keys := []string{Key(journal.taskID), workqueue.TaskKey(journal.taskID), workqueue.ItemKey(journal.workID),
		workqueue.AttemptKey(journal.attemptID), workqueue.ExecutorKey(journal.identity)}
	read, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return ownedRecord{}, err
	}
	if read == nil {
		return ownedRecord{}, conflict()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return ownedRecord{}, conflict()
	}
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if value == nil {
			return ownedRecord{}, conflict()
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return ownedRecord{}, err
	}
	task, err := workqueue.DecodeTask(read.Values[1].Value)
	if err != nil {
		return ownedRecord{}, err
	}
	item, err := workqueue.DecodeItem(read.Values[2].Value)
	if err != nil {
		return ownedRecord{}, err
	}
	attempt, err := workqueue.DecodeAttempt(read.Values[3].Value)
	if err != nil {
		return ownedRecord{}, err
	}
	executor, err := workqueue.DecodeExecutor(read.Values[4].Value)
	if err != nil {
		return ownedRecord{}, err
	}
	input, err := record.Input.Reference()
	if err != nil {
		return ownedRecord{}, err
	}
	if record.Input.TaskID != journal.taskID || record.Input.WorkItemID != journal.workID ||
		task.ID != journal.taskID || task.Intent != input || item.ID != journal.workID || item.TaskID != task.ID ||
		item.Operation != workexecution.Image || item.TargetID != record.Input.Target() ||
		item.Placement != record.Input.Placement ||
		(!assignedOnly && item.Phase != workexecution.Running || assignedOnly && item.Phase != workexecution.Assigned) ||
		item.ActiveAttemptID != journal.attemptID || item.Input == nil || *item.Input != input ||
		attempt.ID != journal.attemptID || attempt.TaskID != task.ID || attempt.WorkItemID != item.ID ||
		attempt.Input != input || attempt.Executor != journal.identity || executor.Identity != journal.identity ||
		(attempt.StartedAt == nil) != assignedOnly || attempt.FinishedAt != nil || record.Phase == Finished ||
		record.AttemptID != "" && record.AttemptID != attempt.ID {
		return ownedRecord{}, conflict()
	}
	if forward &&
		(attempt.Direction != workexecution.Forward || task.Stop != nil || item.ForwardRevoked || !time.Now().UTC().Before(attempt.Deadline)) {
		return ownedRecord{}, conflict()
	}
	return ownedRecord{record: record, attempt: attempt, conditions: conditions, revision: read.ReadRevision}, nil
}

func (journal *Journal) commit(ctx context.Context, owned ownedRecord, record Record, extra workqueue.Change) error {
	if record.UpdatedAt.Before(owned.record.UpdatedAt) ||
		owned.attempt.StartedAt != nil && record.UpdatedAt.Before(*owned.attempt.StartedAt) {
		return conflict()
	}
	value, err := Encode(record)
	if err != nil {
		return err
	}
	defer clear(value)
	conditions := append(owned.conditions, extra.Conditions...)
	mutations := append(
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(journal.taskID), Value: value}},
		extra.Mutations...)
	result, err := journal.repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict()
	}
	return nil
}

func (journal *Journal) StartFetch(ctx context.Context, at time.Time) error {
	owned, err := journal.read(ctx, true)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Ready || record.Input.Fetch == nil {
		return conflict()
	}
	record.Phase, record.AttemptID, record.UpdatedAt = Fetching, journal.attemptID, at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}

func (journal *Journal) Fetched(ctx context.Context, imageID string, at time.Time) error {
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	record := owned.record
	if record.Phase != Fetching || record.Input.Fetch == nil || imageID != record.Input.Fetch.ConfigDigest {
		return conflict()
	}
	record.ImageID, record.UpdatedAt = imageID, at
	return journal.commit(ctx, owned, record, workqueue.Change{})
}

func conflict() error {
	return errs.New(errs.KindStateConflict, "image operation input, attempt or executor changed; reload its execution")
}
