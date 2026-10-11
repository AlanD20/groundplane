package backinghooks

import (
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Authorization is one operator-approved replay, fenced to a new assignment
// epoch and the exact interrupted checkpoint. It cannot authorize later inputs.
type RetryAuthorization struct {
	TaskID             string    `json:"task_id"`
	StepID             string    `json:"step_id"`
	AssignmentID       string    `json:"assignment_id"`
	PlanHash           string    `json:"plan_hash"`
	CheckpointRevision int64     `json:"checkpoint_revision"`
	ExecutionEpoch     uint32    `json:"execution_epoch"`
	AuthorizedAt       time.Time `json:"authorized_at"`
}

func RetryAuthorizationKey(taskID, stepID string) string {
	return CheckpointKey(taskID, stepID) + "/retry"
}

func RetryHistoryKey(record RetryAuthorization) string {
	return CheckpointKey(
		record.TaskID,
		record.StepID,
	) + "/authorizations/" + strconv.FormatUint(
		uint64(record.ExecutionEpoch),
		10,
	)
}

func AttemptKey(record CheckpointRecord) string {
	return CheckpointKey(
		record.TaskID,
		record.StepID,
	) + "/attempts/" + strconv.FormatUint(
		uint64(record.ExecutionEpoch),
		10,
	)
}

func EncodeRetryAuthorization(record RetryAuthorization) ([]byte, error) {
	if record.CheckpointRevision <= 0 || record.ExecutionEpoch == 0 || record.AuthorizedAt.IsZero() ||
		!recordcodec.ValidSHA256(
			record.PlanHash,
		) || record.TaskID == "" || record.StepID == "" || record.AssignmentID == "" {
		return nil, errs.New(errs.KindValidationFailed, "Detach hook retry authorization is invalid")
	}
	return recordcodec.Encode("backing-hook-retry", record)
}

func AllowsRetry(value []byte, current CheckpointRecord, revision int64, input CheckpointInput) bool {
	record, err := recordcodec.Decode[RetryAuthorization](value, "backing-hook-retry")
	return err == nil && current.State == CheckpointStarted && current.Event == "detach" && input.Event == "detach" &&
		input.State == CheckpointStarted && record.CheckpointRevision == revision &&
		record.TaskID == current.TaskID && record.TaskID == input.TaskID &&
		record.StepID == current.StepID && record.StepID == input.StepID &&
		record.AssignmentID == current.AssignmentID && record.AssignmentID == input.AssignmentID &&
		record.PlanHash == current.PlanHash && record.PlanHash == input.PlanHash &&
		current.OperationID == input.OperationID && current.AttachID == input.AttachID &&
		record.ExecutionEpoch == input.ExecutionEpoch && record.ExecutionEpoch > current.ExecutionEpoch &&
		!record.AuthorizedAt.IsZero()
}
