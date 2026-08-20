package etcd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	taskPrefix                  = "/v1/tasks/"
	taskOperationIndexPrefix    = "/v1/indexes/tasks/operation/"
	taskActiveOperationPrefix   = "/v1/indexes/tasks/active-operation/"
	taskEventRootPrefix         = "/v1/runtime/task-events/"
	taskEventDedupRootPrefix    = "/v1/runtime/task-event-dedup/"
	deletionTombstoneRootPrefix = "/v1/runtime/deletions/"
	taskEventSequenceWidth      = 20
)

func taskKey(taskID string) string {
	return taskPrefix + taskID
}

func taskOperationIndexKey(operationID string, taskID string) string {
	return taskOperationIndexPrefix + operationID + "/" + taskID
}

func taskOperationIndexScopePrefix(operationID string) string {
	return taskOperationIndexPrefix + operationID + "/"
}

func taskActiveOperationKey(operationID string) string {
	return taskActiveOperationPrefix + operationID
}

func taskEventScopePrefix(taskID string) string {
	return taskEventRootPrefix + taskID + "/"
}

func taskEventKey(taskID string, sequence uint64) string {
	return taskEventScopePrefix(taskID) + fmt.Sprintf("%020d", sequence)
}

func taskEventDedupKey(identity TaskEventIdentity) string {
	return taskEventDedupRootPrefix + identity.TaskID + "/" + identity.StepID + "/" +
		strconv.FormatUint(uint64(identity.Attempt), 10) + "/" + strconv.FormatUint(identity.Ordinal, 10)
}

func deletionTombstoneKey(targetKind string, targetID string) string {
	return deletionTombstoneRootPrefix + targetKind + "/" + targetID
}

func validateTaskRepositoryIdentity(taskID string, operationID string) error {
	if err := validateStableID(ids.KindTask, taskID); err != nil {
		return err
	}
	if err := validateStableID(ids.KindOperation, operationID); err != nil {
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
