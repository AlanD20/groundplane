package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

// Capability Task mechanics keep aggregate repositories from duplicating the
// durable Task journal grammar while that journal remains in the flat etcd
// migration source package.
func ValidateCapabilityTaskRecord(record TaskRecord) error         { return validateTaskRecord(record) }
func EncodeCapabilityTaskRecord(record TaskRecord) ([]byte, error) { return encodeTaskRecord(record) }
func DecodeCapabilityTaskRecord(value []byte) (TaskRecord, error)  { return decodeTaskRecord(value) }
func (s *store) TransactionSize(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) (int, error) {
	return s.transactionSize(conditions, mutations)
}

func CloneCapabilityRetryTask(source TaskRecord, id string, actor taskjournal.TaskActor, createdAt time.Time) (TaskRecord, error) {
	return cloneRetryTask(source, id, actor, createdAt)
}

func TransitionCapabilityTaskStatus(record TaskRecord, expected, next taskjournal.TaskStatus, at time.Time) (TaskRecord, error) {
	return transitionTaskStatus(record, expected, next, at)
}
