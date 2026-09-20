package etcd

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
	taskJournalSchemaKey            = "/v1/meta/task-journal-schema"
	taskJournalSchemaValue          = "task-journal/v1"
	taskPrefix                      = "/v1/tasks/"
	taskOperationIndexPrefix        = "/v1/indexes/tasks/operation/"
	taskActiveOperationPrefix       = "/v1/indexes/tasks/active-operation/"
	taskEventRootPrefix             = "/v1/runtime/task-events/"
	taskEventDedupRootPrefix        = "/v1/runtime/task-event-dedup/"
	taskQueueRootPrefix             = "/v1/runtime/task-queue/"
	taskAssignmentRootPrefix        = "/v1/runtime/assignments/"
	controllerTaskClaimPrefix       = "/v1/runtime/controller-task-claims/"
	taskAssignmentIndexPrefix       = "/v1/indexes/tasks/assignment/"
	taskTimeoutIndexPrefix          = "/v1/indexes/tasks/timeout/"
	taskRecoveryProofRequiredPrefix = "/v1/indexes/tasks/recovery-proof-required/"
	taskRetentionIndexPrefix        = "/v1/indexes/tasks/by-retain-until/"
	taskWorkspacePlatformPrefix     = "/v1/indexes/tasks/by-workspace/platform/"
	taskWorkspaceTenantPrefix       = "/v1/indexes/tasks/by-workspace/tenant/"
	taskEnvironmentIndexPrefix      = "/v1/indexes/tasks/by-environment/"
	taskPruneIntentPrefix           = "/v1/runtime/task-pruning/"
	taskMaterializationWriterPrefix = "/v1/runtime/task-materialization-writers/"
	deletionTombstoneRootPrefix     = "/v1/runtime/deletions/"
	taskEventSequenceWidth          = 20
)

func taskKey(taskID string) string {
	return taskPrefix + taskID
}

func TaskStorageKey(taskID string) string { return taskKey(taskID) }

func taskOperationIndexKey(operationID string, taskID string) string {
	return taskOperationIndexPrefix + operationID + "/" + taskID
}

func taskOperationIndexScopePrefix(operationID string) string {
	return taskOperationIndexPrefix + operationID + "/"
}

func taskWorkspacePlatformIndexKey(taskID string) string {
	return taskWorkspacePlatformPrefix + taskID
}

func taskWorkspaceTenantIndexKey(tenantID string, taskID string) string {
	return taskWorkspaceTenantPrefix + tenantID + "/" + taskID
}

func taskEnvironmentIndexKey(environmentID string, taskID string) string {
	return taskEnvironmentIndexPrefix + environmentID + "/" + taskID
}

func taskActiveOperationKey(operationID string) string {
	return taskActiveOperationPrefix + operationID
}

func taskMaterializationWriterKey(environmentID string) string {
	return taskMaterializationWriterPrefix + environmentID
}

func taskEventScopePrefix(taskID string) string {
	return taskEventRootPrefix + taskID + "/"
}

func taskEventKey(taskID string, sequence uint64) string {
	return taskEventScopePrefix(taskID) + fmt.Sprintf("%020d", sequence)
}

func taskEventDedupKey(identity TaskEventIdentity) string {
	return taskEventDedupRootPrefix + identity.TaskID + "/" + identity.AssignmentID + "/" + identity.StepID + "/" +
		strconv.FormatUint(uint64(identity.Attempt), 10) + "/" + strconv.FormatUint(identity.Ordinal, 10)
}

func taskQueueScopePrefix(executor TaskExecutor) string {
	return taskQueueRootPrefix + string(executor) + "/"
}

func taskQueueKey(executor TaskExecutor, taskID string) string {
	return taskQueueScopePrefix(executor) + taskID
}

func taskIDFromQueueKey(executor TaskExecutor, key string) (string, error) {
	prefix := taskQueueScopePrefix(executor)
	if !validTaskExecutor(executor) || !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "task queue key is outside the queue")
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(errs.KindInternal, "task queue key has an invalid task id")
	}
	return taskID, nil
}

func taskAssignmentKey(agentID string, taskID string) string {
	return taskAssignmentScopePrefix(agentID) + taskID
}

func controllerTaskClaimKey(taskID string) string {
	return controllerTaskClaimPrefix + taskID
}

func taskExecutionClaimKey(executor TaskExecutor, agentID string, taskID string) string {
	if executor == TaskExecutorController {
		return controllerTaskClaimKey(taskID)
	}
	return taskAssignmentKey(agentID, taskID)
}

func taskAssignmentScopePrefix(agentID string) string {
	return taskAssignmentRootPrefix + agentID + "/"
}

func taskAssignmentIndexKey(taskID string) string {
	return taskAssignmentIndexPrefix + taskID
}

func taskTimeoutIndexKey(taskID string, deadline time.Time) string {
	return taskTimeoutIndexPrefix + fmt.Sprintf("%020d", deadline.UnixNano()) + "/" + taskID
}

func taskRecoveryProofRequiredKey(taskID string) string {
	return taskRecoveryProofRequiredPrefix + taskID
}

func taskRetentionIndexKey(taskID string, retainUntil time.Time) string {
	return taskRetentionIndexPrefix + fmt.Sprintf("%020d", retainUntil.UnixNano()) + "/" + taskID
}

func taskPruneIntentKey(taskID string) string {
	return taskPruneIntentPrefix + taskID
}

func taskEventDedupScopePrefix(taskID string) string {
	return taskEventDedupRootPrefix + taskID + "/"
}

func parseTaskRetentionIndexKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, taskRetentionIndexPrefix) {
		return "", time.Time{}, errs.New(errs.KindInternal, "Task retention key is outside its index")
	}
	segments := strings.Split(strings.TrimPrefix(key, taskRetentionIndexPrefix), "/")
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

func parseTaskTimeoutIndexKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, taskTimeoutIndexPrefix) {
		return "", time.Time{}, errs.New(errs.KindInternal, "task timeout key is outside its index")
	}
	segments := strings.Split(strings.TrimPrefix(key, taskTimeoutIndexPrefix), "/")
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

func taskIDFromAssignmentKey(agentID string, key string) (string, error) {
	prefix := taskAssignmentScopePrefix(agentID)
	if !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "task assignment key is outside its Agent")
	}
	taskID := strings.TrimPrefix(key, prefix)
	if strings.Contains(taskID, "/") || recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(errs.KindInternal, "task assignment key has an invalid task id")
	}
	return taskID, nil
}

func deletionTombstoneKey(targetKind string, targetID string) string {
	return deletionTombstoneRootPrefix + targetKind + "/" + targetID
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

func taskEventSequenceFromKey(taskID string, key string) (uint64, error) {
	prefix := taskEventScopePrefix(taskID)
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
