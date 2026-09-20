package etcd

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const attachTaskPlanReferencePrefix = "/v1/indexes/attach-task-render-inputs/by-plan/"

func attachTaskPlanReferenceScopePrefix(planID string) string {
	return attachTaskPlanReferencePrefix + planID + "/"
}

func attachTaskPlanReferenceKey(planID string, taskID string) string {
	return attachTaskPlanReferenceScopePrefix(planID) + taskID
}

func prepareAttachTaskPlanReference(task TaskRecord) (string, []byte, bool, error) {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return "", nil, false, err
	}
	if ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return "", nil, false, errs.New(errs.KindInternal, "Attach Task plan id is invalid")
	}
	value, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return "", nil, false, err
	}
	return attachTaskPlanReferenceKey(task.PlanID, task.ID), value, true, nil
}

func decodeAttachTaskPlanReference(key string, value []byte, planID string) (string, error) {
	prefix := attachTaskPlanReferenceScopePrefix(planID)
	if ids.Validate(ids.KindPlan, planID) != nil || !strings.HasPrefix(key, prefix) {
		return "", corruptAttachTaskRenderInput()
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || ids.Validate(ids.KindTask, taskID) != nil {
		return "", corruptAttachTaskRenderInput()
	}
	referencedTaskID, err := idempotencyrecord.DecodeTaskReference(value)
	if err != nil || referencedTaskID != taskID || key != attachTaskPlanReferenceKey(planID, taskID) {
		return "", corruptAttachTaskRenderInput()
	}
	return taskID, nil
}
