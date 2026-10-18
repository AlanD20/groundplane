package hierarchydeletion

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// PlanningRejection retains the accepted removal context after its planning-only
// lock is released. It contains no host execution or generated hook values.
type PlanningRejectionReason string

const (
	PlanningDeadline           PlanningRejectionReason = "planning_deadline_exceeded"
	PlanningStopped            PlanningRejectionReason = "planning_stopped"
	PlanningInvalidInput       PlanningRejectionReason = "planning_invalid_input"
	PlanningDependencyConflict PlanningRejectionReason = "planning_dependency_conflict"
)

func (reason PlanningRejectionReason) Valid(task workexecution.Task, outcome workexecution.Outcome, at time.Time) bool {
	switch reason {
	case PlanningDeadline:
		return outcome == workexecution.Failed && task.Stop == nil && !at.Before(task.Deadline)
	case PlanningStopped:
		return task.Stop != nil && outcome == task.Stop.Outcome && !at.Before(task.Stop.RequestedAt) &&
			(outcome == workexecution.Cancelled || outcome == workexecution.Superseded)
	case PlanningInvalidInput, PlanningDependencyConflict:
		return outcome == workexecution.Failed && task.Stop == nil
	default:
		return false
	}
}

type PlanningRejection struct {
	Operation HierarchyDeletionOperation `json:"operation"`
	Reason    PlanningRejectionReason    `json:"reason"`
	Outcome   workexecution.Outcome      `json:"outcome"`
	At        time.Time                  `json:"at"`
}

func PlanningRejectionKey(taskID string) string {
	return "/v1/private/hierarchy-planning-rejections/" + taskID
}

func EncodePlanningRejection(value PlanningRejection, task workexecution.Task) ([]byte, error) {
	op := value.Operation
	if task.Action != workexecution.ActionRemove || task.ExpectedItems != 0 || task.Progress.Total() != 0 ||
		!value.Reason.Valid(task, value.Outcome, value.At) || value.At.Before(task.CreatedAt) ||
		op.Tombstone.Phase != HierarchyDeletionPlanning || op.Tombstone.CurrentTaskID != task.ID || op.RootTaskID != task.ID ||
		op.Tombstone.TargetID != task.TargetID || op.Scope != task.Scope || op.Tombstone.PlanCount != nil || op.Tombstone.PlanDigest != nil ||
		op.Fence.Dispatch != HierarchyDeletionDispatchClosed || op.Fence.CurrentTaskID != task.ID ||
		op.Fence.ParentOperationID != op.Tombstone.OperationID || !ValidHierarchyDeletionPrivateID(op.Tombstone.OperationID, "del") ||
		!ValidHierarchyDeletionTimestamp(value.At) {
		return nil, CorruptHierarchyDeletion()
	}
	return recordcodec.Encode("hierarchy-planning-rejection", value)
}

func DecodePlanningRejection(raw []byte, task workexecution.Task) (PlanningRejection, error) {
	value, err := recordcodec.Decode[PlanningRejection](raw, "hierarchy-planning-rejection")
	if err != nil {
		return PlanningRejection{}, err
	}
	encoded, err := EncodePlanningRejection(value, task)
	clear(encoded)
	if err != nil || task.FinishedAt == nil || task.Outcome != value.Outcome || !task.FinishedAt.Equal(value.At) {
		return PlanningRejection{}, CorruptHierarchyDeletion()
	}
	if value.Outcome == workexecution.Failed && (task.FirstFailure == nil || *task.FirstFailure != PlanningRejectionEvidence(task, raw)) {
		return PlanningRejection{}, CorruptHierarchyDeletion()
	}
	return value, nil
}

func PlanningRejectionEvidence(task workexecution.Task, raw []byte) workexecution.Input {
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "hierarchy-planning-rejection"),
		SHA256: HierarchyDeletionBytesDigest(raw)}
}
