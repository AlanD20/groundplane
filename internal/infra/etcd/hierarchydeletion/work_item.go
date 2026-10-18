package hierarchydeletion

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ActionWork gives a captured deletion action a work identity under the one
// visible removal Task. Retries change attempts, never this action identity.
// The caller publishes the returned exact bytes with the work and deletion fence.
func ActionWork(task workexecution.Task, action HierarchyDeletionAction, hostID string) (workexecution.Item, []byte, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionRemove {
		return workexecution.Item{}, nil, errs.New(errs.KindStateConflict, "hierarchy work requires its active removal Task")
	}
	raw, err := EncodeHierarchyDeletionAction(action)
	if err != nil {
		return workexecution.Item{}, nil, err
	}
	workID := actionWorkID(task, action.ParentOperationID, action.Ordinal)
	digest := sha256.Sum256(raw)
	input := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, workID, "hierarchy-action"), SHA256: hex.EncodeToString(digest[:])}
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: workexecution.Hierarchy,
		TargetID: action.TargetID, Input: &input, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: action.TargetID}}, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	if action.ActionKind == HierarchyDeletionRecoveryPointRemove || action.ActionKind == HierarchyDeletionOrphanObjectRemove {
		item.Operation = workexecution.RemovalBackupObject
	}
	if action.ProcedureKind == HierarchyDeletionProcedureAgent {
		if ids.Validate(ids.KindHost, hostID) != nil {
			return workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "hierarchy host cleanup requires a captured host")
		}
		item.Placement = workexecution.Placement{Executor: workexecution.Agent, HostID: hostID}
		item.Claims = append(item.Claims, workexecution.Claim{HostID: hostID, ScopeID: action.TargetID})
		// Admission applies this per-attempt budget when the action becomes ready;
		// time spent waiting for prerequisites must not consume its execution budget.
		if action.AgentProcedure.TimeoutSeconds <= 0 || action.AgentProcedure.TimeoutSeconds > 86400 {
			return workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "hierarchy action execution budget is invalid")
		}
	}
	for _, ordinal := range action.PrerequisiteOrdinals {
		item.Dependencies = append(item.Dependencies, actionWorkID(task, action.ParentOperationID, ordinal))
	}
	if err := item.Validate(); err != nil {
		return workexecution.Item{}, nil, err
	}
	return item, raw, nil
}

func actionWorkID(task workexecution.Task, operationID string, ordinal int64) string {
	return ActionWorkID(task.ID, task.CreatedAt, operationID, ordinal)
}

func ActionWorkID(taskID string, createdAt time.Time, operationID string, ordinal int64) string {
	return ids.DeriveAt(ids.KindWorkItem, createdAt, taskID, operationID+"/"+strconv.FormatInt(ordinal, 10))
}
