package releasegroups

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	release "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ExecutionRepository struct {
	store workqueue.Store
	queue *workqueue.Repository
}

func NewExecutionRepository(store workqueue.Store) (*ExecutionRepository, error) {
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return &ExecutionRepository{store: store, queue: queue}, nil
}

func (repository *ExecutionRepository) Load(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
) (keyvalue.Versioned[ExecutionCapture], error) {
	if task.Revision <= 0 || revision < task.Revision || task.Task.Validate() != nil ||
		task.Task.Action != workexecution.ActionDeploy && task.Task.Action != workexecution.ActionRollback {
		return keyvalue.Versioned[ExecutionCapture]{}, errs.New(
			errs.KindStateConflict,
			"Release Group execution requires its accepted Task snapshot",
		)
	}
	key := ExecutionCaptureKey(task.Task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[ExecutionCapture]{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[ExecutionCapture]{}, errs.New(
			errs.KindStateConflict,
			"Release Group execution capture read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision ||
		len(value.Value) > keyvalue.MaximumBytes {
		return keyvalue.Versioned[ExecutionCapture]{}, errs.New(
			errs.KindStateConflict,
			"Release Group has no accepted execution capture",
		)
	}
	capture, err := recordcodec.Decode[ExecutionCapture](value.Value, "release_group_execution")
	if err != nil {
		return keyvalue.Versioned[ExecutionCapture]{}, err
	}
	expected, err := capture.Task()
	actual := task.Task
	if err != nil || expected.ID != actual.ID || expected.Scope != actual.Scope || expected.Action != actual.Action ||
		expected.TargetID != actual.TargetID || expected.TargetName != actual.TargetName || expected.Intent != actual.Intent ||
		expected.Actor != actual.Actor || expected.ExpectedItems != actual.ExpectedItems ||
		!expected.CreatedAt.Equal(actual.CreatedAt) || !expected.Deadline.Equal(actual.Deadline) ||
		capture.SourceReadRevision > value.ModRevision {
		return keyvalue.Versioned[ExecutionCapture]{}, errs.New(
			errs.KindStateConflict,
			"Release Group execution capture differs from its Task",
		)
	}
	return keyvalue.Versioned[ExecutionCapture]{
		Record:       capture,
		Revision:     value.ModRevision,
		ReadRevision: revision,
	}, nil
}

type ExecutionDecision struct {
	Capture  keyvalue.Versioned[ExecutionCapture]
	Work     map[string]workqueue.ItemSnapshot
	Decision release.GroupWorkDecision
}

// Decide reads all Service members at the Task snapshot. Network dependencies
// remain normal native work; this owner decides only the Group's declared order
// and conditional restoration policy. No current desired input is consulted.
func (repository *ExecutionRepository) Decide(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	revision int64,
	at time.Time,
) (ExecutionDecision, error) {
	capture, err := repository.Load(ctx, task, revision)
	if err != nil {
		return ExecutionDecision{}, err
	}
	items := make([]workexecution.Item, 0, len(capture.Record.Policy.Members)*2)
	result := ExecutionDecision{Capture: capture, Work: make(map[string]workqueue.ItemSnapshot)}
	for _, member := range capture.Record.Policy.Members {
		for _, id := range []string{member.ForwardID, member.RestoreID} {
			view, err := repository.queue.ReadWork(ctx, task.Task.ID, id, revision)
			if err != nil {
				return ExecutionDecision{}, err
			}
			items = append(items, view.Item.Item)
			result.Work[id] = view.Item
		}
	}
	result.Decision, err = release.DecideGroupWork(capture.Record.Policy, items, at)
	return result, err
}

// PrepareSkip joins the saved explanation, immutable policy and native work
// settlement. A concurrent prepared input/attempt wins or loses atomically;
// it can never be skipped after it acquired execution authority.
func (repository *ExecutionRepository) PrepareSkip(
	task workqueue.TaskSnapshot,
	decision ExecutionDecision,
	at time.Time,
) (workqueue.Change, error) {
	if decision.Decision.Action != release.GroupWorkSkip || task.Task.Stop != nil ||
		decision.Capture.Revision <= 0 || decision.Capture.ReadRevision < task.Revision ||
		decision.Capture.Record.Policy.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release Group no-effect settlement requires its current policy decision",
		)
	}
	accepted, err := decision.Capture.Record.Task()
	if err != nil || accepted.ID != task.Task.ID || accepted.Intent != task.Task.Intent ||
		accepted.Scope != task.Task.Scope || accepted.Action != task.Task.Action || accepted.TargetID != task.Task.TargetID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release Group no-effect policy differs from the accepted Task intent",
		)
	}
	item, found := decision.Work[decision.Decision.WorkID]
	if !found {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release Group decision has no captured work snapshot",
		)
	}
	// Re-evaluate the bounded snapshot, rather than trusting a caller-selected
	// work ID or outcome passed alongside a legitimate capture.
	items := make([]workexecution.Item, 0, len(decision.Work))
	for _, snapshot := range decision.Work {
		items = append(items, snapshot.Item)
	}
	expected, err := release.DecideGroupWork(decision.Capture.Record.Policy, items, at)
	if err != nil || expected.Action != decision.Decision.Action || expected.WorkID != decision.Decision.WorkID ||
		expected.Outcome != decision.Decision.Outcome || expected.Reason != decision.Decision.Reason {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Release Group no-effect settlement differs from its captured policy",
		)
	}
	change, err := workqueue.PrepareInputDecision(
		workqueue.View{Task: task, Item: item, ReadRevision: decision.Capture.ReadRevision},
		task.Task.Intent,
		expected.Outcome,
		expected.Reason,
		at,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: ExecutionCaptureKey(task.Task.ID), ModRevision: decision.Capture.Revision},
	)
	for _, id := range expected.Reads {
		snapshot := decision.Work[id]
		if snapshot.Revision <= 0 || snapshot.Revision > decision.Capture.ReadRevision {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Release Group policy read has no fixed work revision",
			)
		}
	}
	// Every member transition compares/advances the same Task revision. Its CAS
	// fences all policy reads without 64 extra transaction comparisons.
	return change, nil
}
