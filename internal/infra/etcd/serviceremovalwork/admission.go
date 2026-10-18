package serviceremovalwork

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// PrepareAdmission constructs a direct Remove Task. The caller joins hierarchy,
// references, DNS withdrawal, staged desired revision and request publication in
// this same transaction. This does not delete the definition ahead of execution.
func PrepareAdmission(input Input, current keyvalue.Versioned[services.ServiceRecord], deadline time.Time, marker idempotency.IdempotencyMarker) (workexecution.Task, workqueue.Change, error) {
	ref, err := input.Reference()
	if err != nil {
		return workexecution.Task{}, workqueue.Change{}, err
	}
	if input.DesiredRevisionID != input.TaskID || !deadline.After(input.CreatedAt) {
		return workexecution.Task{}, workqueue.Change{}, conflict("Service removal action does not own its desired revision or deadline")
	}
	resource, err := PrepareResource(input, current)
	if err != nil {
		return workexecution.Task{}, workqueue.Change{}, err
	}
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor,
		Scope: input.Scope, Action: workexecution.ActionRemove, TargetID: input.ServiceID, TargetName: input.ServiceName,
		Intent: ref, ExpectedItems: 1, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	item, err := NewItem(input, deadline)
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workexecution.Task{}, workqueue.Change{}, err
	}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workexecution.Task{}, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return task, change, nil
}

// NewItem is shared by direct removal and Blueprint omission publication. Apply
// owns this item directly; it never creates an internal visible Task.
func NewItem(input Input, deadline time.Time) (workexecution.Item, error) {
	ref, err := input.Reference()
	if err != nil {
		return workexecution.Item{}, err
	}
	placement := workexecution.Placement{Executor: workexecution.Controller}
	if input.HostID != "" {
		placement = workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: input.TaskID, Operation: workexecution.ServiceRemoval,
		TargetID: input.ServiceID, Placement: placement, Claims: removalClaims(input),
		Input: &ref, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt, Deadline: deadline}
	if err := item.Validate(); err != nil {
		return workexecution.Item{}, err
	}
	return item, nil
}

// Recovery can recreate the captured runtime and its shared configuration.
// Reserve the same Environment and host-file scopes as other file writers from
// admission onward, not after a partial removal has already occurred.
func removalClaims(input Input) []workexecution.Claim {
	claims := []workexecution.Claim{{ScopeID: input.ServiceID}}
	if input.HostID != "" {
		claims = append(claims, workexecution.Claim{ScopeID: input.Scope.EnvironmentID},
			workexecution.Claim{ScopeID: input.Scope.EnvironmentID, HostID: input.HostID})
	}
	return claims
}

// PrepareResource captures removal ownership without changing the running state.
// Both entry points must compare their selected desired revision separately.
func PrepareResource(input Input, current keyvalue.Versioned[services.ServiceRecord]) (workqueue.Change, error) {
	if err := input.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	if services.ValidateServiceRecord(current.Record) != nil || current.Revision != input.ServiceRevision ||
		current.ReadRevision < current.Revision || current.Record.EnvironmentID != input.Scope.EnvironmentID ||
		current.Record.Desired.ID != input.ServiceID || current.Record.Desired.Name != input.ServiceName ||
		services.ServiceRuntimeRevision(current) != input.RuntimeRevision {
		return workqueue.Change{}, conflict("Service removal capture differs from its resource snapshot")
	}
	tombstone := deletions.DeletionTombstoneRecord{
		TargetKind: deletions.DeletionTargetService, TargetID: input.ServiceID, TargetRevision: input.ServiceRevision,
		TaskID: input.TaskID, Phase: deletions.DeletionPhaseHostEffects, CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt,
	}
	value, err := deletions.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := PreparePublication(input)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		services.ServiceDesiredCondition(current), services.ServiceRuntimeCondition(current),
		keyvalue.Condition{Key: serviceruntimerecord.Key(input.ServiceID), ModRevision: input.RuntimeSourceRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return change, nil
}
