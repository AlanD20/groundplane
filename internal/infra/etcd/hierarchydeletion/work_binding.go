package hierarchydeletion

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type WorkBinding struct {
	TaskID      string `json:"task_id"`
	OperationID string `json:"operation_id"`
	Ordinal     int64  `json:"ordinal"`
}

func WorkBindingKey(workID string) string { return "/v1/hierarchy-removal-work/" + workID }

func WorkStopKey(workID string) string { return "/v1/hierarchy-removal-stops/" + workID }

func WorkStopEvidencePrefix(workID string) string {
	return "/v1/hierarchy-removal-stop-evidence/" + workID + "/"
}

func WorkStopEvidenceKey(workID, digest string) string {
	return WorkStopEvidencePrefix(workID) + digest
}

func EnvironmentFailureKey(attemptID string) string {
	return "/v1/hierarchy-removal-failures/" + attemptID
}

func EncodeWorkBinding(binding WorkBinding) ([]byte, error) {
	if ids.Validate(ids.KindTask, binding.TaskID) != nil ||
		!ValidHierarchyDeletionPrivateID(binding.OperationID, "del") ||
		binding.Ordinal < 0 {
		return nil, CorruptHierarchyDeletion()
	}
	return recordcodec.Encode("hierarchy-work-binding", binding)
}

func DecodeWorkBinding(raw []byte) (WorkBinding, error) {
	binding, err := recordcodec.Decode[WorkBinding](raw, "hierarchy-work-binding")
	if err != nil {
		return WorkBinding{}, err
	}
	if _, err := EncodeWorkBinding(binding); err != nil {
		return WorkBinding{}, err
	}
	return binding, nil
}
