package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskReservation preserves a captured action's resource exclusion across its
// member attempts, without reserving an executor slot or inventing a work item.
// Members of the same Task still acquire their ordinary exclusive work claims.
type TaskReservation struct {
	TaskID string              `json:"task_id"`
	Intent workexecution.Input `json:"intent"`
	Claim  workexecution.Claim `json:"claim"`
}

type TaskReservationSnapshot struct {
	Claim    workexecution.Claim
	Owner    *TaskReservation
	Revision int64
}

func TaskReservationKey(claim workexecution.Claim) string {
	return root + "task-reservations/" + strings.TrimPrefix(ClaimKey(claim), root+"claims/")
}

func taskReservationIndexKey(taskID string) string { return root + "task-reservation-owners/" + taskID }

func (reservation TaskReservation) validate() error {
	if ids.Validate(ids.KindTask, reservation.TaskID) != nil || reservation.Intent.Validate() != nil ||
		reservation.Claim.HostID != "" || ids.Validate(ids.KindEnvironment, reservation.Claim.ScopeID) != nil {
		return invalid("Task reservation identity or captured resource is invalid")
	}
	return nil
}

// PrepareTaskReservation joins initial Task acceptance and immutable source
// publication. An active work claim prevents reservation; a reservation created
// concurrently with another assignment wins or loses through the same keys.
// This MVP uses one reserved effect scope per action, not a host-wide lock.
func PrepareTaskReservation(task workexecution.Task, claim workexecution.Claim) (Change, error) {
	if task.Validate() != nil || task.PublicationComplete || task.Progress.Total() != 0 ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil || task.Stop != nil ||
		task.Scope.EnvironmentID == "" || claim != (workexecution.Claim{ScopeID: task.Scope.EnvironmentID}) {
		return Change{}, invalid("Task reservation must join unexecuted action acceptance")
	}
	reservation := TaskReservation{TaskID: task.ID, Intent: task.Intent, Claim: claim}
	if err := reservation.validate(); err != nil {
		return Change{}, err
	}
	value, err := encode("execution_task_reservation", reservation)
	if err != nil {
		return Change{}, err
	}
	key, index := TaskReservationKey(claim), taskReservationIndexKey(task.ID)
	return Change{Conditions: []keyvalue.Condition{{Key: TaskKey(task.ID)}, {Key: key}, {Key: index}, {Key: ClaimKey(claim)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value},
			{Type: keyvalue.MutationPut, Key: index, Value: append([]byte(nil), value...)}}}, nil
}

func (repository *Repository) readTaskReservations(ctx context.Context, task workexecution.Task, revision int64) ([]TaskReservationSnapshot, error) {
	if task.Scope.EnvironmentID == "" {
		return nil, nil
	}
	claim := workexecution.Claim{ScopeID: task.Scope.EnvironmentID}
	keys := []string{TaskReservationKey(claim)}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	snapshots := make([]TaskReservationSnapshot, len(keys))
	for index := range keys {
		snapshot := TaskReservationSnapshot{Claim: claim}
		if value := read.Values[index]; value != nil {
			reservation, err := recordcodec.Decode[TaskReservation](value.Value, "execution_task_reservation")
			if err != nil || reservation.validate() != nil || reservation.Claim != claim || value.Key != keys[index] || value.ModRevision <= 0 {
				return nil, corrupt()
			}
			snapshot.Owner, snapshot.Revision = &reservation, value.ModRevision
		}
		snapshots[index] = snapshot
	}
	return snapshots, nil
}

func taskReservationConditions(task workexecution.Task, item workexecution.Item, snapshots []TaskReservationSnapshot) ([]keyvalue.Condition, error) {
	count := 0
	if task.Scope.EnvironmentID != "" {
		count = 1
	}
	if len(snapshots) != count || item.TaskID != task.ID {
		return nil, invalid("assignment Task reservation evidence is incomplete")
	}
	conditions := make([]keyvalue.Condition, len(snapshots))
	for index, snapshot := range snapshots {
		if snapshot.Claim != (workexecution.Claim{ScopeID: task.Scope.EnvironmentID}) || snapshot.Revision < 0 || (snapshot.Owner == nil) != (snapshot.Revision == 0) {
			return nil, invalid("Task reservation snapshot is invalid")
		}
		if owner := snapshot.Owner; owner != nil {
			if owner.validate() != nil || owner.Claim != snapshot.Claim {
				return nil, corrupt()
			}
			if owner.TaskID != task.ID || owner.Intent != task.Intent {
				return nil, errs.New(errs.KindResourceInUse, "another Task reserves this execution resource")
			}
		}
		conditions[index] = keyvalue.Condition{Key: TaskReservationKey(snapshot.Claim), ModRevision: snapshot.Revision}
	}
	return conditions, nil
}

// Resource exclusion ends in the SAME transaction as settled Task completion.
// Timeout, disconnection or diagnostic retention cannot release it. A Task with
// unresolved restoration retains its reservation while unrelated scopes run.
func (repository *Repository) prepareTaskReservationRelease(ctx context.Context, task TaskSnapshot) (Change, error) {
	if task.Task.Validate() != nil || task.Revision <= 0 || !task.Task.PublicationComplete || task.Task.Progress.Unsettled() != 0 {
		return Change{}, invalid("Task reservation release requires fully settled published work")
	}
	index := taskReservationIndexKey(task.Task.ID)
	read, err := repository.read(ctx, []string{index}, 0)
	if err != nil {
		return Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return Change{Conditions: []keyvalue.Condition{{Key: index}}}, nil
	}
	reservation, err := recordcodec.Decode[TaskReservation](read.Values[0].Value, "execution_task_reservation")
	if err != nil || reservation.validate() != nil || reservation.TaskID != task.Task.ID || reservation.Intent != task.Task.Intent {
		return Change{}, corrupt()
	}
	key := TaskReservationKey(reservation.Claim)
	resource, err := repository.read(ctx, []string{key}, read.ReadRevision)
	if err != nil {
		return Change{}, err
	}
	defer keyvalue.ClearValues(resource.Values)
	if resource.Values[0] == nil {
		return Change{}, corrupt()
	}
	owner, err := recordcodec.Decode[TaskReservation](resource.Values[0].Value, "execution_task_reservation")
	if err != nil || owner != reservation {
		return Change{}, corrupt()
	}
	return Change{Conditions: []keyvalue.Condition{{Key: index, ModRevision: read.Values[0].ModRevision},
		{Key: key, ModRevision: resource.Values[0].ModRevision}, {Key: TaskKey(task.Task.ID), ModRevision: task.Revision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}, {Type: keyvalue.MutationDelete, Key: index}}}, nil
}
