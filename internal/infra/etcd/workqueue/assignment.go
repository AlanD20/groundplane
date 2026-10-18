package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Assignment struct {
	ReadRevision  int64
	DecisionOnly  bool
	Task          TaskSnapshot
	Item          ItemSnapshot
	Executor      ExecutorSnapshot
	Dependencies  []ItemSnapshot
	Claims        []ClaimSnapshot
	Reservations  []TaskReservationSnapshot
	HostEffect    HostEffectSnapshot
	QueueRevision int64
	Slot          uint16
	AttemptID     string
	Direction     workexecution.Direction
	At            time.Time
	Deadline      time.Time
}

// PrepareAssignment compares every dependency and conflict scope together with
// capacity. Operation owners add their captured-input and desired-head checks
// before committing; this function neither reads feature state nor runs effects.
func PrepareAssignment(request Assignment) (Change, workexecution.Attempt, error) {
	if request.DecisionOnly {
		return Change{}, workexecution.Attempt{}, invalid("Controller decision cannot acquire an execution slot")
	}
	if request.Task.Task.Stop != nil && request.Direction == workexecution.Forward {
		return Change{}, workexecution.Attempt{}, errs.New(errs.KindStateConflict, "task no longer permits forward execution")
	}
	if request.Item.Revision <= 0 || request.Executor.Revision <= 0 || request.QueueRevision <= 0 {
		return Change{}, workexecution.Attempt{}, invalid("assignment requires versioned execution state")
	}
	executor := request.Executor.Record
	if err := executor.Validate(); err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	if !executor.Allows(request.Item.Item.Operation) || request.Slot >= executor.Capacity {
		return Change{}, workexecution.Attempt{}, errs.New(errs.KindStateConflict, "executor is not eligible for this work")
	}
	next, attempt, err := workexecution.Assign(request.Item.Item, executor.Identity,
		request.AttemptID, request.Direction, request.At, request.Deadline)
	if err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	itemBytes, err := EncodeItem(next)
	if err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	attemptBytes, err := EncodeAttempt(attempt)
	if err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	slotBytes, err := encode("execution_slot", Slot{TaskID: next.TaskID, WorkItemID: next.ID, AttemptID: attempt.ID})
	if err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{
			{Key: ItemKey(next.ID), ModRevision: request.Item.Revision},
			{Key: ExecutorKey(executor.Identity), ModRevision: request.Executor.Revision},
			{Key: QueueKey(next), ModRevision: request.QueueRevision},
			{Key: AttemptKey(attempt.ID)},
			{Key: AttemptIndexKey(next.ID, attempt.Number)},
			{Key: SlotKey(executor.Identity, request.Slot)},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: ItemKey(next.ID), Value: itemBytes},
			{Type: keyvalue.MutationPut, Key: AttemptKey(attempt.ID), Value: attemptBytes},
			{Type: keyvalue.MutationPut, Key: AttemptIndexKey(next.ID, attempt.Number), Value: slotBytes},
			{Type: keyvalue.MutationPut, Key: SlotKey(executor.Identity, request.Slot), Value: slotBytes},
			{Type: keyvalue.MutationDelete, Key: QueueKey(next)},
		},
	}
	if err := addDependencies(&change, request.Item.Item, request.Dependencies, request.Direction); err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	if err := addClaims(&change, next, request.Claims); err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	reservations, err := taskReservationConditions(request.Task.Task, next, request.Reservations)
	if err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	change.Conditions = append(change.Conditions, reservations...)
	if err := assignHostEffect(&change, next, request.HostEffect, request.Direction); err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	if err := countTransition(&change, request.Task, request.Item.Item, next, request.At); err != nil {
		return Change{}, workexecution.Attempt{}, err
	}
	return change, attempt, nil
}

func addDependencies(change *Change, item workexecution.Item, dependencies []ItemSnapshot,
	direction workexecution.Direction,
) error {
	// Cleanup may be necessary precisely because a prerequisite failed. It uses
	// the captured operation's own cleanup authority, not forward prerequisites.
	if direction == workexecution.Recovery {
		if len(dependencies) != 0 {
			return invalid("recovery assignment must not substitute forward dependencies")
		}
		return nil
	}
	conditions, err := DependencyConditions(item, dependencies)
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, conditions...)
	return nil
}

// DependencyConditions binds successful prerequisite outputs both when preparing
// an input and when assigning it. The operation owner supplies the exact
// snapshots used to derive that input, never a later replacement output.
func DependencyConditions(item workexecution.Item, dependencies []ItemSnapshot) ([]keyvalue.Condition, error) {
	if len(dependencies) != len(item.Dependencies) {
		return nil, invalid("execution dependency evidence is incomplete")
	}
	conditions := make([]keyvalue.Condition, 0, len(dependencies))
	for index, dependencyID := range item.Dependencies {
		dependency := dependencies[index]
		if err := dependency.Item.Validate(); err != nil {
			return nil, err
		}
		if dependency.Revision <= 0 || dependency.Item.ID != dependencyID ||
			dependency.Item.TaskID != item.TaskID || dependency.Item.Phase != workexecution.Settled ||
			dependency.Item.Outcome != workexecution.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "execution dependency is not satisfied")
		}
		conditions = append(conditions, keyvalue.Condition{
			Key: ItemKey(dependencyID), ModRevision: dependency.Revision,
		})
	}
	return conditions, nil
}

func addClaims(change *Change, item workexecution.Item, snapshots []ClaimSnapshot) error {
	if len(snapshots) != len(item.Claims) {
		return invalid("assignment conflict evidence is incomplete")
	}
	owner := ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID}
	ownerBytes, err := encode("execution_claim", owner)
	if err != nil {
		return err
	}
	for index, claim := range item.Claims {
		snapshot := snapshots[index]
		if snapshot.Claim != claim || snapshot.Revision < 0 || (snapshot.Owner == nil) != (snapshot.Revision == 0) {
			return invalid("execution conflict snapshot is invalid")
		}
		if snapshot.Owner != nil && *snapshot.Owner != owner {
			return errs.New(errs.KindResourceInUse, "another operation owns an execution resource")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ClaimKey(claim), ModRevision: snapshot.Revision})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{
			Type: keyvalue.MutationPut, Key: ClaimKey(claim), Value: ownerBytes,
		})
	}
	return nil
}
