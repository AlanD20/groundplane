package taskjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintFailure preserves the first stop cause after its transient stop gate
// is retired. It contains only closed stages, error codes and immutable IDs.
type BlueprintFailure struct {
	Phase       string    `json:"phase"`
	Code        errs.Code `json:"code,omitempty"`
	ChildTaskID string    `json:"child_task_id,omitempty"`
	ChildPlanID string    `json:"child_plan_id,omitempty"`
	FailedAt    time.Time `json:"failed_at"`
}

func ValidateBlueprintFailure(failure BlueprintFailure) error {
	if err := recordcodec.ValidateTimestamp("Blueprint failure", failure.FailedAt); err != nil {
		return err
	}
	if failure.Phase == "child_execution" {
		if failure.Code != "" || ids.Validate(ids.KindTask, failure.ChildTaskID) != nil ||
			ids.Validate(ids.KindPlan, failure.ChildPlanID) != nil {
			return errs.New(errs.KindInternal, "Blueprint child failure identity is corrupt")
		}
		return nil
	}
	switch failure.Phase {
	case "planning", "selection", "settlement", "child":
	default:
		return errs.New(errs.KindInternal, "Blueprint coordinator failure phase is corrupt")
	}
	if failure.Code == "" || failure.ChildTaskID != "" || failure.ChildPlanID != "" {
		return errs.New(errs.KindInternal, "Blueprint coordinator failure is corrupt")
	}
	return nil
}

func CloneBlueprintFailure(failure *BlueprintFailure) *BlueprintFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	return &cloned
}
