package taskjournal

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	TaskJournalSchemaKey            = "/v1/meta/task-journal-schema"
	TaskJournalSchemaValue          = "task-journal/v1"
	TaskPrefix                      = "/v1/tasks/"
	taskOperationIndexPrefix        = "/v1/indexes/tasks/operation/"
	taskActiveOperationPrefix       = "/v1/indexes/tasks/active-operation/"
	taskEventRootPrefix             = "/v1/runtime/task-events/"
	taskEventDedupRootPrefix        = "/v1/runtime/task-event-dedup/"
	taskQueueRootPrefix             = "/v1/runtime/task-queue/"
	taskAssignmentRootPrefix        = "/v1/runtime/assignments/"
	ControllerTaskClaimPrefix       = "/v1/runtime/controller-task-claims/"
	taskAssignmentIndexPrefix       = "/v1/indexes/tasks/assignment/"
	TaskTimeoutIndexPrefix          = "/v1/indexes/tasks/timeout/"
	taskRecoveryProofRequiredPrefix = "/v1/indexes/tasks/recovery-proof-required/"
	TaskRetentionIndexPrefix        = "/v1/indexes/tasks/by-retain-until/"
	TaskWorkspacePlatformPrefix     = "/v1/indexes/tasks/by-workspace/platform/"
	TaskWorkspaceTenantPrefix       = "/v1/indexes/tasks/by-workspace/tenant/"
	TaskEnvironmentIndexPrefix      = "/v1/indexes/tasks/by-environment/"
	TaskPruneIntentPrefix           = "/v1/runtime/task-pruning/"
	taskMaterializationWriterPrefix = "/v1/runtime/task-materialization-writers/"
	taskEventSequenceWidth          = 20
)

func TaskStorageKey(taskID string) string {
	return TaskPrefix + taskID
}

func TaskOperationIndexKey(operationID string, taskID string) string {
	return taskOperationIndexPrefix + operationID + "/" + taskID
}

func taskOperationIndexScopePrefix(operationID string) string {
	return taskOperationIndexPrefix + operationID + "/"
}

func TaskWorkspacePlatformIndexKey(taskID string) string {
	return TaskWorkspacePlatformPrefix + taskID
}

func TaskWorkspaceTenantIndexKey(tenantID string, taskID string) string {
	return TaskWorkspaceTenantPrefix + tenantID + "/" + taskID
}

func TaskEnvironmentIndexKey(environmentID string, taskID string) string {
	return TaskEnvironmentIndexPrefix + environmentID + "/" + taskID
}

func TaskActiveOperationKey(operationID string) string {
	return taskActiveOperationPrefix + operationID
}

func TaskMaterializationWriterKey(environmentID string) string {
	return taskMaterializationWriterPrefix + environmentID
}

func TaskEventScopePrefix(taskID string) string {
	return taskEventRootPrefix + taskID + "/"
}

func TaskEventKey(taskID string, sequence uint64) string {
	return TaskEventScopePrefix(taskID) + fmt.Sprintf("%020d", sequence)
}

func TaskEventDedupKey(identity TaskEventIdentity) string {
	return taskEventDedupRootPrefix + identity.TaskID + "/" + identity.AssignmentID + "/" + identity.StepID + "/" +
		strconv.FormatUint(uint64(identity.Attempt), 10) + "/" + strconv.FormatUint(identity.Ordinal, 10)
}

func TaskQueueScopePrefix(executor TaskExecutor) string {
	return taskQueueRootPrefix + string(executor) + "/"
}

func TaskQueueKey(executor TaskExecutor, taskID string) string {
	return TaskQueueScopePrefix(executor) + taskID
}

func TaskIDFromQueueKey(executor TaskExecutor, key string) (string, error) {
	prefix := TaskQueueScopePrefix(executor)
	if !ValidExecutor(executor) || !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "task queue key is outside the queue")
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(errs.KindInternal, "task queue key has an invalid task id")
	}
	return taskID, nil
}

func TaskAssignmentKey(agentID string, taskID string) string {
	return TaskAssignmentScopePrefix(agentID) + taskID
}

func ControllerTaskClaimKey(taskID string) string {
	return ControllerTaskClaimPrefix + taskID
}

func TaskExecutionClaimKey(executor TaskExecutor, agentID string, taskID string) string {
	if executor == TaskExecutorController {
		return ControllerTaskClaimKey(taskID)
	}
	return TaskAssignmentKey(agentID, taskID)
}

func TaskAssignmentScopePrefix(agentID string) string {
	return taskAssignmentRootPrefix + agentID + "/"
}

func TaskAssignmentIndexKey(taskID string) string {
	return taskAssignmentIndexPrefix + taskID
}

func TaskTimeoutIndexKey(taskID string, deadline time.Time) string {
	return TaskTimeoutIndexPrefix + fmt.Sprintf("%020d", deadline.UnixNano()) + "/" + taskID
}

func TaskRecoveryProofRequiredKey(taskID string) string {
	return taskRecoveryProofRequiredPrefix + taskID
}

func TaskRetentionIndexKey(taskID string, retainUntil time.Time) string {
	return TaskRetentionIndexPrefix + fmt.Sprintf("%020d", retainUntil.UnixNano()) + "/" + taskID
}

func TaskPruneIntentKey(taskID string) string {
	return TaskPruneIntentPrefix + taskID
}

func TaskEventDedupScopePrefix(taskID string) string {
	return taskEventDedupRootPrefix + taskID + "/"
}

func ParseTaskRetentionIndexKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, TaskRetentionIndexPrefix) {
		return "", time.Time{}, errs.New(errs.KindInternal, "Task retention key is outside its index")
	}
	segments := strings.Split(strings.TrimPrefix(key, TaskRetentionIndexPrefix), "/")
	if len(segments) != 2 || len(segments[0]) != taskEventSequenceWidth ||
		recordcodec.ValidateID(ids.KindTask, segments[1]) != nil {
		return "", time.Time{}, errs.New(errs.KindInternal, "Task retention key is invalid")
	}
	nanoseconds, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil || nanoseconds <= 0 || fmt.Sprintf("%020d", nanoseconds) != segments[0] {
		return "", time.Time{}, errs.New(errs.KindInternal, "Task retention deadline is invalid")
	}
	return segments[1], time.Unix(0, nanoseconds).UTC(), nil
}

func ParseTaskTimeoutIndexKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, TaskTimeoutIndexPrefix) {
		return "", time.Time{}, errs.New(errs.KindInternal, "task timeout key is outside its index")
	}
	segments := strings.Split(strings.TrimPrefix(key, TaskTimeoutIndexPrefix), "/")
	if len(segments) != 2 || len(segments[0]) != taskEventSequenceWidth ||
		recordcodec.ValidateID(ids.KindTask, segments[1]) != nil {
		return "", time.Time{}, errs.New(errs.KindInternal, "task timeout key is invalid")
	}
	nanoseconds, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil || nanoseconds <= 0 || fmt.Sprintf("%020d", nanoseconds) != segments[0] {
		return "", time.Time{}, errs.New(errs.KindInternal, "task timeout deadline is invalid")
	}
	return segments[1], time.Unix(0, nanoseconds).UTC(), nil
}

func TaskIDFromAssignmentKey(agentID string, key string) (string, error) {
	prefix := TaskAssignmentScopePrefix(agentID)
	if !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "task assignment key is outside its Agent")
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(errs.KindInternal, "task assignment key has an invalid task id")
	}
	return taskID, nil
}

func validateTaskRepositoryIdentity(taskID string, operationID string) error {
	if err := recordcodec.ValidateID(ids.KindTask, taskID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindOperation, operationID); err != nil {
		return err
	}
	return nil
}

func TaskEventSequenceFromKey(taskID string, key string) (uint64, error) {
	prefix := TaskEventScopePrefix(taskID)
	if !strings.HasPrefix(key, prefix) {
		return 0, errs.New(errs.KindInternal, "task event key is outside its task")
	}
	encoded := strings.TrimPrefix(key, prefix)
	if len(encoded) != taskEventSequenceWidth || strings.Contains(encoded, "/") {
		return 0, errs.New(errs.KindInternal, "task event key has an invalid sequence")
	}
	sequence, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil || sequence == 0 || fmt.Sprintf("%020d", sequence) != encoded {
		return 0, errs.New(errs.KindInternal, "task event key has an invalid sequence")
	}
	return sequence, nil
}
