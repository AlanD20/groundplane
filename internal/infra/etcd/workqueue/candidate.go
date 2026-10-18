package workqueue

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type QueueEntry struct {
	TaskID     string
	WorkItemID string
	Revision   int64
}

type QueuePage struct {
	Entries []QueueEntry
	More    bool
	LastID  string
}

// ScanReady returns a bounded page of ready work and captured delayed retries,
// not just the oldest entry. The scheduler advances LastID while blocked or
// not-yet-due candidates remain queued, then wraps at the end.
func (repository *Repository) ScanReady(ctx context.Context, target workexecution.Placement, after string) (QueuePage, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return QueuePage{}, err
	}
	if err := target.Validate(); err != nil {
		return QueuePage{}, err
	}
	if after != "" && ids.Validate(ids.KindWorkItem, after) != nil {
		return QueuePage{}, invalid("execution queue cursor is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: QueuePrefix(target), Limit: 64}
	if after != "" {
		request.StartExclusive = request.Prefix + after
	}
	read, err := repository.store.Range(ctx, request)
	if err != nil {
		return QueuePage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) > 64 || read.More && len(read.Values) == 0 {
		return QueuePage{}, corrupt()
	}
	page := QueuePage{More: read.More, Entries: make([]QueueEntry, 0, len(read.Values))}
	for _, value := range read.Values {
		id, err := queueItemID(target, value.Key)
		if err != nil {
			return QueuePage{}, err
		}
		owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_owner")
		if err != nil || validateOwner(owner) != nil || owner.WorkItemID != id || value.ModRevision <= 0 ||
			id <= after || page.LastID != "" && id <= page.LastID {
			return QueuePage{}, corrupt()
		}
		page.Entries = append(page.Entries, QueueEntry{TaskID: owner.TaskID, WorkItemID: id, Revision: value.ModRevision})
		page.LastID = id
	}
	return page, nil
}

type Candidate struct {
	Assignment Assignment
	Wait       *workexecution.Wait
	Stopped    bool
	RetryDue   bool
}

// ReadCandidate uses one MVCC view for eligibility, prerequisites and resources.
// A wait is returned without claiming a worker slot. The prepared assignment
// still compares this evidence at commit, including the scanned queue revision.
func (repository *Repository) ReadCandidate(ctx context.Context, entry QueueEntry, identity workexecution.Identity, at time.Time, decisions []workexecution.Operation) (Candidate, error) {
	if err := identity.Validate(); err != nil {
		return Candidate{}, err
	}
	if entry.Revision <= 0 || at.IsZero() {
		return Candidate{}, invalid("execution candidate queue revision is invalid")
	}
	view, err := repository.ReadWork(ctx, entry.TaskID, entry.WorkItemID, 0)
	if err != nil {
		return Candidate{}, err
	}
	item := view.Item.Item
	decisionOnly := item.Placement.Executor == workexecution.Controller && slices.Contains(decisions, item.Operation)
	if decisionOnly && (identity.Executor != workexecution.Controller || item.AttemptNumber != 0) {
		return Candidate{}, invalid("Controller decision has invalid placement or an execution attempt")
	}
	direction := workexecution.Forward
	if item.Cleanup == workexecution.CleanupRequired || item.Cleanup == workexecution.CleanupStopped {
		direction = workexecution.Recovery
	}
	if !view.Task.Task.PublicationComplete || item.Input == nil {
		return waiting(workexecution.Wait{Reason: workexecution.WaitInput}), nil
	}
	if item.Phase != workexecution.Ready && !retryWaiting(item) || item.Placement.Executor != identity.Executor ||
		item.Placement.HostID != identity.HostID {
		return Candidate{}, invalid("queue entry does not identify ready work for this executor")
	}
	if view.Task.Task.Stop != nil && direction == workexecution.Forward {
		return Candidate{Stopped: true, Assignment: Assignment{Task: view.Task, Item: view.Item}}, nil
	}
	// Expiry is an operation decision, not worker admission. Let its owner
	// settle captured input even when no executor or free slot is available.
	if !item.Deadline.After(at) {
		return Candidate{Assignment: Assignment{Task: view.Task, Item: view.Item,
			ReadRevision: view.ReadRevision, QueueRevision: entry.Revision, Direction: direction, DecisionOnly: decisionOnly}}, nil
	}
	if retryWaiting(item) {
		if at.Before(*item.Wait.Until) {
			return waiting(*item.Wait), nil
		}
		return Candidate{RetryDue: true, Assignment: Assignment{Task: view.Task, Item: view.Item,
			QueueRevision: entry.Revision, Direction: direction}}, nil
	}
	executorRead, err := repository.read(ctx, []string{ExecutorKey(identity)}, view.ReadRevision)
	if err != nil {
		return Candidate{}, err
	}
	if executorRead.Values[0] == nil {
		return waiting(workexecution.Wait{Reason: workexecution.WaitExecutor}), nil
	}
	executor, err := DecodeExecutor(executorRead.Values[0].Value)
	if err != nil {
		return Candidate{}, err
	}
	if executor.Identity != identity || !executor.Ready || !slices.Contains(executor.Operations, item.Operation) {
		return waiting(workexecution.Wait{Reason: workexecution.WaitExecutor}), nil
	}
	if !executor.Allows(item.Operation) {
		return waiting(workexecution.Wait{Reason: workexecution.WaitOperation}), nil
	}
	request := Assignment{Task: view.Task, Item: view.Item, QueueRevision: entry.Revision, Direction: direction,
		ReadRevision: view.ReadRevision, DecisionOnly: decisionOnly,
		Executor: ExecutorSnapshot{Record: executor, Revision: executorRead.Values[0].ModRevision}}
	request.Reservations, err = repository.readTaskReservations(ctx, view.Task.Task, view.ReadRevision)
	if err != nil {
		return Candidate{}, err
	}
	for _, reservation := range request.Reservations {
		if owner := reservation.Owner; owner != nil && (owner.TaskID != item.TaskID || owner.Intent != view.Task.Task.Intent) {
			return waiting(workexecution.Wait{Reason: workexecution.WaitReservation, TaskID: owner.TaskID}), nil
		}
	}
	dependencies := item.Dependencies
	if direction == workexecution.Recovery {
		dependencies = nil
	}
	keys := make([]string, 0, len(dependencies)+len(item.Claims)+maximumExecutorCapacity)
	for _, dependency := range dependencies {
		keys = append(keys, ItemKey(dependency))
	}
	for _, claim := range item.Claims {
		keys = append(keys, ClaimKey(claim))
	}
	if !decisionOnly {
		for index := uint16(0); index < maximumExecutorCapacity; index++ {
			keys = append(keys, SlotKey(identity, index))
		}
	}
	if item.Placement.Executor == workexecution.Agent {
		keys = append(keys, HostEffectKey(item.Placement.HostID, item.ID), HostForwardFenceKey(item.Placement.HostID))
	}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return Candidate{}, err
	}
	for index, dependencyID := range dependencies {
		value := read.Values[index]
		if value == nil {
			return Candidate{}, corrupt()
		}
		dependency, err := DecodeItem(value.Value)
		if err != nil || dependency.ID != dependencyID || dependency.TaskID != item.TaskID {
			return Candidate{}, corrupt()
		}
		if dependency.Phase != workexecution.Settled || dependency.Outcome != workexecution.Succeeded {
			return waiting(workexecution.Wait{Reason: workexecution.WaitDependency, WorkItemID: dependency.ID, TaskID: item.TaskID}), nil
		}
		request.Dependencies = append(request.Dependencies, ItemSnapshot{Item: dependency, Revision: value.ModRevision})
	}
	for index, claim := range item.Claims {
		value := read.Values[len(dependencies)+index]
		snapshot := ClaimSnapshot{Claim: claim}
		if value != nil {
			owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_claim")
			if err != nil || validateOwner(owner) != nil {
				return Candidate{}, corrupt()
			}
			if owner.WorkItemID != item.ID || owner.TaskID != item.TaskID {
				return waiting(workexecution.Wait{Reason: workexecution.WaitConflict, WorkItemID: owner.WorkItemID, TaskID: owner.TaskID}), nil
			}
			snapshot.Owner, snapshot.Revision = &owner, value.ModRevision
		}
		request.Claims = append(request.Claims, snapshot)
	}
	if decisionOnly {
		return Candidate{Assignment: request}, nil
	}
	offset := len(dependencies) + len(item.Claims)
	if item.Placement.Executor == workexecution.Agent {
		if direction == workexecution.Forward && read.Values[offset+maximumExecutorCapacity+1] != nil {
			return waiting(workexecution.Wait{Reason: workexecution.WaitExecutor}), nil
		}
		request.HostEffect, err = readHostEffect(read.Values[offset+maximumExecutorCapacity], item, direction == workexecution.Recovery)
		if err != nil {
			return Candidate{}, err
		}
	}
	occupied := 0
	for _, value := range read.Values[offset : offset+maximumExecutorCapacity] {
		if value == nil {
			continue
		}
		slot, err := recordcodec.Decode[Slot](value.Value, "execution_slot")
		if err != nil || validateOwner(ClaimOwner{TaskID: slot.TaskID, WorkItemID: slot.WorkItemID}) != nil ||
			ids.Validate(ids.KindAttempt, slot.AttemptID) != nil {
			return Candidate{}, corrupt()
		}
		occupied++
	}
	if occupied >= int(executor.Capacity) {
		return waiting(workexecution.Wait{Reason: workexecution.WaitCapacity}), nil
	}
	for index := uint16(0); index < executor.Capacity; index++ {
		if read.Values[offset+int(index)] == nil {
			request.Slot = index
			return Candidate{Assignment: request}, nil
		}
	}
	return waiting(workexecution.Wait{Reason: workexecution.WaitCapacity}), nil
}

func waiting(reason workexecution.Wait) Candidate { return Candidate{Wait: &reason} }
