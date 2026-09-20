package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
)

// Capability Task mechanics keep aggregate repositories from duplicating the
// durable Task journal grammar while that journal remains in the flat etcd
// migration source package.
func ValidateCapabilityContext(ctx context.Context) error { return etcdstore.ValidateContext(ctx) }
func ValidateCapabilityTimestamp(field string, value time.Time) error {
	return recordcodec.ValidateTimestamp(field, value)
}
func ValidateCapabilityTaskRecord(record TaskRecord) error { return validateTaskRecord(record) }
func ValidateCapabilityTaskResult(result TaskResultRecord, steps []TaskStepRecord, status TaskStatus) error {
	return validateTaskResult(result, steps, status)
}
func IsCapabilityTerminalTaskStatus(status TaskStatus) bool        { return isTerminalTaskStatus(status) }
func ValidateCapabilityVolumeComposeKey(key string) error          { return volumeidentity.ValidateKey(key) }
func EncodeCapabilityTaskRecord(record TaskRecord) ([]byte, error) { return encodeTaskRecord(record) }
func DecodeCapabilityTaskRecord(value []byte) (TaskRecord, error)  { return decodeTaskRecord(value) }
func EncodeCapabilityTaskReference(taskID string) ([]byte, error)  { return encodeTaskReference(taskID) }
func DecodeCapabilityTaskReference(value []byte) (string, error)   { return decodeTaskReference(value) }
func EncodeCapabilityTaskAssignment(record TaskAssignmentRecord) ([]byte, error) {
	return encodeTaskAssignment(record)
}
func DecodeCapabilityTaskAssignment(value []byte) (TaskAssignmentRecord, error) {
	return decodeTaskAssignment(value)
}
func DecodeCapabilityIdempotencyMarker(value []byte, locator IdempotencyLocator) (IdempotencyMarker, error) {
	return decodeIdempotencyMarker(value, locator)
}
func CapabilityIdempotencyMarkerKey(locator IdempotencyLocator) (string, error) {
	return idempotencyMarkerKey(locator)
}
func CapabilityTaskKey(taskID string) string { return taskKey(taskID) }
func CapabilityTaskOperationIndexKey(operationID, taskID string) string {
	return taskOperationIndexKey(operationID, taskID)
}
func CapabilityTaskActiveOperationKey(operationID string) string {
	return taskActiveOperationKey(operationID)
}
func CapabilityTaskQueueKey(executor TaskExecutor, taskID string) string {
	return taskQueueKey(executor, taskID)
}
func CapabilityTaskExecutionClaimKey(executor TaskExecutor, agentID, taskID string) string {
	return taskExecutionClaimKey(executor, agentID, taskID)
}
func CapabilityTaskAssignmentIndexKey(taskID string) string { return taskAssignmentIndexKey(taskID) }
func CapabilityTaskTimeoutIndexKey(taskID string, deadline time.Time) string {
	return taskTimeoutIndexKey(taskID, deadline)
}

func (s *store) TransactionSize(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) (int, error) {
	return s.transactionSize(conditions, mutations)
}

func EncodeCapabilityIdempotencyMarker(marker IdempotencyMarker) ([]byte, error) {
	return encodeIdempotencyMarker(marker)
}

func CloneCapabilityRetryTask(source TaskRecord, id string, actor TaskActor, createdAt time.Time) (TaskRecord, error) {
	return cloneRetryTask(source, id, actor, createdAt)
}

func TransitionCapabilityTaskStatus(record TaskRecord, expected, next TaskStatus, at time.Time) (TaskRecord, error) {
	return transitionTaskStatus(record, expected, next, at)
}
