package backingchecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Journal struct {
	repository *Repository
	owner      Owner
	attemptID  string
	identity   workexecution.Identity
}

func NewJournal(
	repository *Repository,
	owner Owner,
	attemptID string,
	identity workexecution.Identity,
) (*Journal, error) {
	if repository == nil || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil ||
		ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil || ids.Validate(ids.KindAttempt, attemptID) != nil ||
		identity.Validate() != nil || identity.Executor != workexecution.Controller {
		return nil, errs.New(errs.KindValidationFailed, "Backing access check journal ownership is required")
	}
	return &Journal{repository: repository, owner: owner, attemptID: attemptID, identity: identity}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, true, func(record *Record, assignment workexecution.Attempt) error {
		return record.start(now, assignment.Deadline)
	})
}

func (journal *Journal) Stop(ctx context.Context, failure string, at time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		return record.Stop(failure, at)
	})
}

func (journal *Journal) Observe(ctx context.Context, result *backingaccess.Result, pending bool, failure string) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		return record.observe(result, pending, failure)
	})
}

func (journal *Journal) ResumeCleanup(ctx context.Context) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		if record.Phase != Checking && record.Phase != Cleaning && record.Phase != CleanupRequired ||
			!record.CleanupPending {
			return errs.New(errs.KindStateConflict, "Backing access check has no interrupted helper to clean up")
		}
		record.Phase = Cleaning
		record.Outcome, record.Failure, record.FinishedAt = "", "", nil
		return nil
	})
}

func (journal *Journal) FinishCleanup(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		if record.Phase != Cleaning || !record.CleanupPending {
			return errs.New(errs.KindStateConflict, "Backing access cleanup ownership changed")
		}
		record.CleanupPending = false
		return record.finish(InterruptedFailure, now)
	})
}

func (journal *Journal) Finish(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(
		ctx, false,
		func(record *Record, _ workexecution.Attempt) error { return record.finish(failure, now) },
	)
}

func (journal *Journal) FinishUnstarted(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		if record.Phase != Ready {
			return errs.New(errs.KindStateConflict, "Backing access check already started")
		}
		return record.Stop(failure, now)
	})
}

func (journal *Journal) mutate(
	ctx context.Context,
	forward bool,
	change func(*Record, workexecution.Attempt) error,
) error {
	return journal.mutateState(ctx, forward, false, change)
}

func (journal *Journal) RejectAssigned(ctx context.Context, failure string, at time.Time) error {
	return journal.mutateState(ctx, false, true, func(record *Record, attempt workexecution.Attempt) error {
		if attempt.StartedAt != nil || attempt.Direction != workexecution.Forward || record.Phase != Ready {
			return errs.New(errs.KindStateConflict, "Backing check already started; reconcile its helper")
		}
		return record.Stop(failure, at)
	})
}

func (journal *Journal) mutateState(ctx context.Context, forward, allowAssigned bool,
	change func(*Record, workexecution.Attempt) error,
) error {
	keys := []string{Key(journal.owner.TaskID), workqueue.AttemptKey(journal.attemptID),
		workqueue.ItemKey(journal.owner.WorkItemID), OperationKey(journal.owner.OperationID),
		workqueue.TaskKey(journal.owner.TaskID), workqueue.ExecutorKey(journal.identity)}
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
	assignment, err := workqueue.DecodeAttempt(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.ID != journal.attemptID || assignment.Executor != journal.identity ||
		assignment.WorkItemID != journal.owner.WorkItemID || assignment.StartedAt == nil && !allowAssigned || assignment.FinishedAt != nil ||
		string(read.Values[3].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "Backing access Controller ownership changed")
	}
	item, err := workqueue.DecodeItem(read.Values[2].Value)
	if err != nil || item.ID != journal.owner.WorkItemID || item.TaskID != journal.owner.TaskID ||
		(item.Phase != workexecution.Running && !(allowAssigned && item.Phase == workexecution.Assigned && assignment.StartedAt == nil && record.Phase == Ready)) ||
		item.ActiveAttemptID != assignment.ID || item.Input == nil || *item.Input != assignment.Input {
		return errs.New(errs.KindStateConflict, "Backing access work ownership changed")
	}
	digest, err := PlanHash(record)
	if err != nil || assignment.Input.ID != record.Owner.PlanID || assignment.Input.SHA256 != digest {
		return errs.New(errs.KindStateConflict, "Backing access captured input changed")
	}
	task, err := workqueue.DecodeTask(read.Values[4].Value)
	if err != nil || task.ID != journal.owner.TaskID {
		return errs.New(errs.KindStateConflict, "Backing access Task ownership changed")
	}
	executor, err := workqueue.DecodeExecutor(read.Values[5].Value)
	if err != nil || executor.Identity != journal.identity {
		return errs.New(errs.KindStateConflict, "Backing access executor was replaced")
	}
	if forward &&
		(task.Stop != nil || item.ForwardRevoked || assignment.Direction != workexecution.Forward || !time.Now().UTC().Before(assignment.Deadline)) {
		return errs.New(errs.KindStateConflict, "Backing access check no longer permits a new probe")
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
