package edgechecks

import (
	"bytes"
	"context"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Journal struct {
	repository   *Repository
	owner        Owner
	assignmentID string
}

func NewJournal(repository *Repository, owner Owner, assignmentID string) (*Journal, error) {
	if repository == nil || owner.Validate() != nil || ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "edge diagnostic journal ownership is required")
	}
	return &Journal{repository: repository, owner: owner, assignmentID: assignmentID}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, assignment taskassignments.TaskAssignmentRecord) error {
		return record.start(now, assignment.Deadline)
	})
}

func (journal *Journal) ObservePrivate(
	ctx context.Context,
	result *contract.PrivateResult,
	pending bool,
	failure string,
) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		return record.observePrivate(result, pending, failure)
	})
}
func (journal *Journal) StartPublic(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, assignment taskassignments.TaskAssignmentRecord) error {
		return record.startPublic(now, assignment.Deadline)
	})
}
func (journal *Journal) ObservePublic(ctx context.Context, result *contract.PublicResult, failure string) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		return record.observePublic(result, failure)
	})
}
func (journal *Journal) Finish(ctx context.Context, now time.Time) error {
	return journal.mutate(
		ctx,
		func(record *Record, _ taskassignments.TaskAssignmentRecord) error { return record.finish(now) },
	)
}
func (journal *Journal) Stop(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(
		ctx,
		func(record *Record, _ taskassignments.TaskAssignmentRecord) error { return record.Stop(failure, now) },
	)
}
func (journal *Journal) ResumeCleanup(ctx context.Context) error {
	return journal.mutate(
		ctx,
		func(record *Record, _ taskassignments.TaskAssignmentRecord) error { return record.resumeCleanup() },
	)
}
func (journal *Journal) FinishCleanup(ctx context.Context, now time.Time) error {
	return journal.mutate(
		ctx,
		func(record *Record, _ taskassignments.TaskAssignmentRecord) error { return record.finishCleanup(now) },
	)
}

func (journal *Journal) mutate(
	ctx context.Context,
	change func(*Record, taskassignments.TaskAssignmentRecord) error,
) error {
	keys := []string{Key(journal.owner.TaskID), taskjournal.ControllerTaskClaimKey(journal.owner.TaskID),
		taskjournal.TaskAssignmentIndexKey(journal.owner.TaskID), OperationKey(journal.owner.OperationID)}
	read, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return errs.New(errs.KindInternal, "edge diagnostic journal read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return errs.New(errs.KindStateConflict, "edge diagnostic no longer has its Controller claim")
		}
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	assignment, err := taskassignments.DecodeTaskAssignment(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.AssignmentID != journal.assignmentID || assignment.Executor != taskjournal.TaskExecutorController ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward || !bytes.Equal(read.Values[1].Value, read.Values[2].Value) ||
		string(read.Values[3].Value) != journal.owner.TaskID {
		return errs.New(errs.KindStateConflict, "edge diagnostic Controller ownership changed")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	references := ReferenceKeys(record)
	refs, err := journal.repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: references, Revision: read.ReadRevision},
	)
	if err != nil {
		return err
	}
	if refs == nil || refs.ReadRevision != read.ReadRevision || len(refs.Values) != len(references) {
		if refs != nil {
			keyvalue.ClearValues(refs.Values)
		}
		return errs.New(errs.KindInternal, "edge diagnostic cleanup reference read is incomplete")
	}
	defer keyvalue.ClearValues(refs.Values)
	for index, key := range references {
		if refs.Values[index] == nil || string(refs.Values[index].Value) != journal.owner.TaskID {
			return errs.New(errs.KindStateConflict, "edge diagnostic cleanup authority changed")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: refs.Values[index].ModRevision})
	}
	if err := change(&record, assignment); err != nil {
		return err
	}
	value, err := Encode(record)
	if err != nil {
		return err
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[0], Value: value}}
	if record.Phase == Finished {
		for _, key := range references {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[3]})
	}
	result, err := journal.repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "edge diagnostic journal write raced its owner")
	}
	return nil
}
