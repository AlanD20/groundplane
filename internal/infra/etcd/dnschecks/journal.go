package dnschecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Journal struct {
	repository *Repository
	owner      Owner
	attemptID  string
	identity   workexecution.Identity
}

func NewJournal(repository *Repository, owner Owner, attemptID string, identity workexecution.Identity) (*Journal, error) {
	if repository == nil || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil ||
		ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil || ids.Validate(ids.KindAttempt, attemptID) != nil ||
		identity.Validate() != nil || identity.Executor != workexecution.Controller {
		return nil, errs.New(errs.KindValidationFailed, "DNS check journal requires its exact Controller assignment")
	}
	return &Journal{repository: repository, owner: owner, attemptID: attemptID, identity: identity}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, true, func(record *Record, deadline time.Time) error { return record.Start(now, deadline) })
}

func (journal *Journal) Finish(ctx context.Context, result *agentpb.DNSConnectionResult, now time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ time.Time) error { return record.Finish(result, now) })
}

func (journal *Journal) Stop(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ time.Time) error { return record.Stop(failure, now) })
}

func (journal *Journal) mutate(ctx context.Context, forward bool, change func(*Record, time.Time) error) error {
	return journal.mutateState(ctx, forward, false, change)
}

func (journal *Journal) RejectAssigned(ctx context.Context, failure string, at time.Time) error {
	return journal.mutateState(ctx, false, true, func(record *Record, _ time.Time) error {
		if record.Phase != Ready {
			return errs.New(errs.KindStateConflict, "DNS check already started")
		}
		return record.Stop(failure, at)
	})
}

func (journal *Journal) mutateState(ctx context.Context, forward, assignedOnly bool, change func(*Record, time.Time) error) error {
	keys := []string{RecordKey(journal.owner.TaskID), workqueue.AttemptKey(journal.attemptID),
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
		return errs.New(errs.KindInternal, "DNS check journal read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return errs.New(errs.KindStateConflict, "DNS check no longer has its Controller claim")
		}
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	assignment, err := workqueue.DecodeAttempt(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.ID != journal.attemptID || assignment.Executor != journal.identity ||
		assignment.WorkItemID != journal.owner.WorkItemID || assignment.Direction != workexecution.Forward ||
		(assignment.StartedAt == nil) != assignedOnly || assignment.FinishedAt != nil || string(read.Values[3].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "DNS check Controller ownership changed")
	}
	item, err := workqueue.DecodeItem(read.Values[2].Value)
	if err != nil || item.ID != journal.owner.WorkItemID || item.TaskID != journal.owner.TaskID ||
		(!assignedOnly && item.Phase != workexecution.Running || assignedOnly && item.Phase != workexecution.Assigned) ||
		item.ActiveAttemptID != assignment.ID || item.Input == nil || *item.Input != assignment.Input {
		return errs.New(errs.KindStateConflict, "DNS check work ownership changed")
	}
	digest, err := PlanHash(record)
	if err != nil || assignment.Input.ID != record.Owner.PlanID || digest != assignment.Input.SHA256 {
		return errs.New(errs.KindStateConflict, "DNS check captured input changed")
	}
	task, err := workqueue.DecodeTask(read.Values[4].Value)
	if err != nil || task.ID != journal.owner.TaskID {
		return errs.New(errs.KindStateConflict, "DNS check Task ownership changed")
	}
	executor, err := workqueue.DecodeExecutor(read.Values[5].Value)
	if err != nil || executor.Identity != journal.identity {
		return errs.New(errs.KindStateConflict, "DNS check executor was replaced")
	}
	if forward && (task.Stop != nil || item.ForwardRevoked || !time.Now().UTC().Before(assignment.Deadline)) {
		return errs.New(errs.KindStateConflict, "DNS check no longer permits queries")
	}
	activeKey := ActiveKey(record.Source.ComponentID)
	active, err := journal.repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{activeKey}, Revision: read.ReadRevision},
	)
	if err != nil {
		return err
	}
	if active == nil || active.ReadRevision != read.ReadRevision || len(active.Values) != 1 {
		if active != nil {
			keyvalue.ClearValues(active.Values)
		}
		return errs.New(errs.KindInternal, "DNS check active ownership read is incomplete")
	}
	defer keyvalue.ClearValues(active.Values)
	if active.Values[0] == nil || string(active.Values[0].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "DNS check active ownership changed")
	}
	conditions := make([]keyvalue.Condition, 0, len(keys)+1)
	for index, key := range keys {
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	conditions = append(conditions, keyvalue.Condition{Key: activeKey, ModRevision: active.Values[0].ModRevision})
	if err := change(&record, assignment.Deadline); err != nil {
		return err
	}
	value, err := Encode(record)
	if err != nil {
		return err
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[0], Value: value}}
	if record.Phase == Finished {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[3]},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: activeKey})
	}
	result, err := journal.repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "DNS check journal raced its Controller assignment")
	}
	return nil
}
