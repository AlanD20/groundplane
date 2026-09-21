package attachrender

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

const attachTaskPlanReferencePrefix = "/v1/indexes/attach-task-render-inputs/by-plan/"

func AttachTaskPlanReferenceScopePrefix(planID string) string {
	return attachTaskPlanReferencePrefix + planID + "/"
}

func AttachTaskPlanReferenceKey(planID string, taskID string) string {
	return AttachTaskPlanReferenceScopePrefix(planID) + taskID
}

func DecodeAttachTaskPlanReference(key string, value []byte, planID string) (string, error) {
	prefix := AttachTaskPlanReferenceScopePrefix(planID)
	if ids.Validate(ids.KindPlan, planID) != nil || !strings.HasPrefix(key, prefix) {
		return "", corruptAttachTaskRenderInput()
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || ids.Validate(ids.KindTask, taskID) != nil {
		return "", corruptAttachTaskRenderInput()
	}
	referencedTaskID, err := idempotencyrecord.DecodeTaskReference(value)
	if err != nil || referencedTaskID != taskID || key != AttachTaskPlanReferenceKey(planID, taskID) {
		return "", corruptAttachTaskRenderInput()
	}
	return taskID, nil
}
