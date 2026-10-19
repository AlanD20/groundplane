package hierarchydeletion

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type TaskBinding struct {
	HostID      string                         `json:"host_id,omitempty"`
	TaskID      string                         `json:"task_id"`
	OperationID string                         `json:"operation_id"`
	Marker      idempotency.IdempotencyLocator `json:"marker"`
}

func TaskBindingKey(taskID string) string { return "/v1/hierarchy-removal-tasks/" + taskID }

func EncodeTaskBinding(binding TaskBinding) ([]byte, error) {
	if binding.HostID != "" && ids.Validate(ids.KindHost, binding.HostID) != nil {
		return nil, CorruptHierarchyDeletion()
	}
	if ids.Validate(ids.KindTask, binding.TaskID) != nil ||
		!ValidHierarchyDeletionPrivateID(binding.OperationID, "del") ||
		idempotency.ValidateIdempotencyLocator(binding.Marker) != nil {
		return nil, CorruptHierarchyDeletion()
	}
	return recordcodec.Encode("hierarchy-work-task", binding)
}

func DecodeTaskBinding(raw []byte) (TaskBinding, error) {
	binding, err := recordcodec.Decode[TaskBinding](raw, "hierarchy-work-task")
	if err != nil {
		return TaskBinding{}, err
	}
	if _, err := EncodeTaskBinding(binding); err != nil {
		return TaskBinding{}, err
	}
	return binding, nil
}
