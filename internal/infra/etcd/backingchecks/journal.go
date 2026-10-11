package backingchecks

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
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
	if repository == nil || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil ||
		ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backing access check journal ownership is required")
	}
	return &Journal{repository: repository, owner: owner, assignmentID: assignmentID}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, assignment taskassignments.TaskAssignmentRecord) error {
		return record.start(now, assignment.Deadline)
	})
}

func (journal *Journal) Observe(ctx context.Context, result *backingaccess.Result, pending bool, failure string) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		return record.observe(result, pending, failure)
	})
}

func (journal *Journal) ResumeCleanup(ctx context.Context) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Checking && record.Phase != Cleaning || !record.CleanupPending {
			return errs.New(errs.KindStateConflict, "Backing access check has no interrupted helper to clean up")
		}
		record.Phase = Cleaning
		return nil
	})
}

func (journal *Journal) FinishCleanup(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Cleaning || !record.CleanupPending {
			return errs.New(errs.KindStateConflict, "Backing access cleanup ownership changed")
		}
		record.CleanupPending = false
		return record.finish(InterruptedFailure, now)
	})
}

func (journal *Journal) Finish(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(
		ctx,
		func(record *Record, _ taskassignments.TaskAssignmentRecord) error { return record.finish(failure, now) },
	)
}

func (journal *Journal) FinishUnstarted(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Ready {
			return errs.New(errs.KindStateConflict, "Backing access check already started")
		}
		return record.Stop(failure, now)
	})
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
		return errs.New(errs.KindInternal, "Backing access journal read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return errs.New(errs.KindStateConflict, "Backing access check no longer has its Controller claim")
		}
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	defer record.Clear()
	assignment, err := taskassignments.DecodeTaskAssignment(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.AssignmentID != journal.assignmentID || assignment.Executor != taskjournal.TaskExecutorController ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward || !bytes.Equal(read.Values[1].Value, read.Values[2].Value) ||
		string(read.Values[3].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "Backing access Controller ownership changed")
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
		return errs.New(errs.KindInternal, "Backing access cleanup reference read is incomplete")
	}
	defer keyvalue.ClearValues(refs.Values)
	for index, key := range references {
		if refs.Values[index] == nil || string(refs.Values[index].Value) != record.Owner.TaskID {
			return errs.New(errs.KindStateConflict, "Backing access cleanup authority changed")
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
	defer clear(value)
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
		return errs.New(errs.KindStateConflict, "Backing access journal write raced its owner")
	}
	return nil
}
