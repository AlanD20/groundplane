package hierarchydeletion

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// WorkSummary is aggregate evidence, not another copy of every action or runtime
// input. Detailed results remain owned by the work items until safe retention.
type WorkSummary struct {
	TaskID       string                `json:"task_id"`
	OperationID  string                `json:"operation_id"`
	PlanDigest   string                `json:"plan_digest"`
	ResultDigest string                `json:"result_digest"`
	Count        uint32                `json:"count"`
	Outcome      workexecution.Outcome `json:"outcome"`
	CompletedAt  time.Time             `json:"completed_at"`
}

func WorkSummaryKey(taskID string) string { return "/v1/hierarchy-removal-results/" + taskID }

func EncodeWorkSummary(summary WorkSummary) ([]byte, error) {
	if err := validateWorkSummary(summary); err != nil {
		return nil, err
	}
	return recordcodec.Encode("hierarchy-work-summary", summary)
}

func DecodeWorkSummary(raw []byte) (WorkSummary, error) {
	summary, err := recordcodec.Decode[WorkSummary](raw, "hierarchy-work-summary")
	if err != nil {
		return WorkSummary{}, err
	}
	if err := validateWorkSummary(summary); err != nil {
		return WorkSummary{}, err
	}
	return summary, nil
}

func validateWorkSummary(summary WorkSummary) error {
	if ids.Validate(ids.KindTask, summary.TaskID) != nil ||
		!ValidHierarchyDeletionPrivateID(summary.OperationID, "del") ||
		!ValidHierarchyDeletionDigest(summary.PlanDigest) ||
		!ValidHierarchyDeletionDigest(summary.ResultDigest) ||
		summary.Count == 0 ||
		summary.Count > workexecution.MaximumTaskItems ||
		!ValidHierarchyDeletionTimestamp(summary.CompletedAt) {
		return CorruptHierarchyDeletion()
	}
	switch summary.Outcome {
	case workexecution.Succeeded, workexecution.Failed, workexecution.Cancelled, workexecution.Superseded:
		return nil
	default:
		return CorruptHierarchyDeletion()
	}
}
