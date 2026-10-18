package scriptexecutions

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SourceRetirement proves that the operation-wide references were drained.
// Keep it until every owning work item has retired its captured hook records.
type SourceRetirement struct {
	TaskID           string    `json:"task_id"`
	OperationID      string    `json:"operation_id"`
	MembershipCount  uint64    `json:"membership_count"`
	MembershipSHA256 string    `json:"membership_sha256"`
	RetiredAt        time.Time `json:"retired_at"`
}

func SourceRetirementKey(taskID, operationID string) string {
	return SourceRetirementPrefix(taskID) + operationID
}

func SourceRetirementPrefix(taskID string) string {
	return "/v1/script-source-retirements/" + taskID + "/"
}

func (receipt SourceRetirement) Validate() error {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil || ids.Validate(ids.KindOperation, receipt.OperationID) != nil ||
		receipt.MembershipCount == 0 || !ValidLowerSHA256(receipt.MembershipSHA256) ||
		receipt.RetiredAt.IsZero() || receipt.RetiredAt.Location() != time.UTC {
		return errs.New(errs.KindValidationFailed, "Script source retirement receipt is invalid")
	}
	return nil
}
