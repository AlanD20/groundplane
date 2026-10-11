package dnschecks

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
		ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS check journal requires its exact Controller assignment")
	}
	return &Journal{repository: repository, owner: owner, assignmentID: assignmentID}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, deadline time.Time) error { return record.Start(now, deadline) })
}

func (journal *Journal) Finish(ctx context.Context, result *agentpb.DNSConnectionResult, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, _ time.Time) error { return record.Finish(result, now) })
}

func (journal *Journal) Stop(ctx context.Context, failure string, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, _ time.Time) error { return record.Stop(failure, now) })
}

func (journal *Journal) mutate(ctx context.Context, change func(*Record, time.Time) error) error {
	keys := []string{RecordKey(journal.owner.TaskID), taskjournal.ControllerTaskClaimKey(journal.owner.TaskID),
		taskjournal.TaskAssignmentIndexKey(journal.owner.TaskID), OperationKey(journal.owner.OperationID)}
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
	assignment, err := taskassignments.DecodeTaskAssignment(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.AssignmentID != journal.assignmentID || assignment.Executor != taskjournal.TaskExecutorController ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		!bytes.Equal(
			read.Values[1].Value,
			read.Values[2].Value,
		) || string(read.Values[3].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "DNS check Controller ownership changed")
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
