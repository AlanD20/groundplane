package taskassignments

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const MaximumAutomaticRecoveryFailures = 3

// RecoveryRetryControl stops execution, not restoration ownership. The exact
// assignment and recovery record remain retained until an operator authorizes
// another bounded attempt or restoration is proven.
type RecoveryRetryControl struct {
	TaskID         string               `json:"task_id"`
	AssignmentID   string               `json:"assignment_id"`
	ExecutionEpoch uint32               `json:"execution_epoch"`
	Failures       uint32               `json:"failures"`
	Stopped        bool                 `json:"stopped"`
	StopReason     string               `json:"stop_reason,omitempty"`
	Failure        taskprogress.Failure `json:"failure"`
	NextAttemptAt  time.Time            `json:"next_attempt_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
}

func RecoveryRetryControlKey(taskID string) string {
	return "/v1/runtime/recovery-retry-control/" + taskID
}

func (control RecoveryRetryControl) Validate() error {
	if ids.Validate(ids.KindTask, control.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, control.AssignmentID) != nil ||
		control.ExecutionEpoch == 0 {
		return recordcodec.CorruptRecord()
	}
	if control.Failures == 0 || control.Failures > MaximumAutomaticRecoveryFailures ||
		control.Failure.Validate() != nil ||
		recordcodec.ValidateTimestamp("recovery retry updated_at", control.UpdatedAt) != nil ||
		recordcodec.ValidateTimestamp("recovery retry next_attempt_at", control.NextAttemptAt) != nil ||
		control.NextAttemptAt.Before(control.UpdatedAt) {
		return recordcodec.CorruptRecord()
	}
	if control.Stopped {
		switch control.StopReason {
		case "non_transient_failure", "retry_budget_exhausted", "recovery_deadline", "unsafe_replay":
		default:
			return recordcodec.CorruptRecord()
		}
	} else if control.StopReason != "" {
		return recordcodec.CorruptRecord()
	}
	return nil
}

func EncodeRecoveryRetryControl(control RecoveryRetryControl) ([]byte, error) {
	if err := control.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("recovery-retry-control", control)
}

func DecodeRecoveryRetryControl(value []byte) (RecoveryRetryControl, error) {
	if len(value) > 4096 {
		return RecoveryRetryControl{}, recordcodec.CorruptRecord()
	}
	control, err := recordcodec.Decode[RecoveryRetryControl](value, "recovery-retry-control")
	if err != nil || control.Validate() != nil {
		return RecoveryRetryControl{}, recordcodec.CorruptRecord()
	}
	return control, nil
}

func ObserveRecoveryFailure(
	previous *RecoveryRetryControl,
	assignment TaskAssignmentRecord,
	failure *taskprogress.Failure,
	now time.Time,
	safeToReplay bool,
) (RecoveryRetryControl, bool, error) {
	if previous != nil {
		if previous.Validate() != nil || previous.TaskID != assignment.TaskID ||
			previous.AssignmentID != assignment.AssignmentID || previous.ExecutionEpoch > assignment.ExecutionEpoch {
			return RecoveryRetryControl{}, false, recordcodec.CorruptRecord()
		}
		if previous.ExecutionEpoch == assignment.ExecutionEpoch || previous.Stopped {
			return *previous, false, nil // Report redelivery is not another execution failure.
		}
	}
	observed := taskprogress.Failure{Code: "internal_execution_error"}
	if failure != nil {
		observed = *failure
	}
	control := RecoveryRetryControl{
		TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID,
		ExecutionEpoch: assignment.ExecutionEpoch, Failures: 1,
		Failure: observed, UpdatedAt: now.UTC(), NextAttemptAt: now.UTC(),
	}
	if previous != nil {
		control.Failures = previous.Failures + 1
	}
	// Forward effects are never replayed here. Only the sealed recovery suffix
	// may repeat for an explicitly transient dependency failure.
	switch {
	case !safeToReplay:
		control.StopReason = "unsafe_replay"
	case observed.Code != "dependency_unavailable":
		control.StopReason = "non_transient_failure"
	case control.Failures >= MaximumAutomaticRecoveryFailures:
		control.StopReason = "retry_budget_exhausted"
	case !now.Before(assignment.RecoveryDeadline):
		control.StopReason = "recovery_deadline"
	}
	control.Stopped = control.StopReason != ""
	if !control.Stopped {
		delay := time.Second
		if control.Failures > 1 {
			delay = 5 * time.Second
		}
		control.NextAttemptAt = now.UTC().Add(delay)
		if !control.NextAttemptAt.Before(assignment.RecoveryDeadline) {
			control.Stopped = true
			control.StopReason = "recovery_deadline"
			control.NextAttemptAt = now.UTC()
		}
	}
	return control, true, control.Validate()
}
