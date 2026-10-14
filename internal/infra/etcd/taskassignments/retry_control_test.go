package taskassignments

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
)

func TestRecoveryFailureBudgetSurvivesRestartAndReportRedelivery(t *testing.T) {
	// TASK-16: reconnect/redelivery must not renew a failed operation forever.
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	assignment := TaskAssignmentRecord{TaskID: ids.NewAt(ids.KindTask, now, 1),
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 2), ExecutionEpoch: 1, RecoveryDeadline: now.Add(time.Minute)}
	failure := &taskprogress.Failure{Code: "dependency_unavailable"}
	var previous *RecoveryRetryControl
	for attempt := uint32(1); attempt <= 3; attempt++ {
		assignment.ExecutionEpoch = attempt
		control, changed, err := ObserveRecoveryFailure(previous, assignment, failure, now, true)
		if err != nil || !changed || control.Failures != attempt || control.Stopped != (attempt == 3) {
			t.Fatalf("attempt %d: %+v %v", attempt, control, err)
		}
		if attempt < 3 {
			delay := time.Second
			if attempt == 2 {
				delay = 5 * time.Second
			}
			if !control.NextAttemptAt.Equal(now.Add(delay)) {
				t.Fatalf("backoff: %+v", control)
			}
		} else if control.StopReason != "retry_budget_exhausted" {
			t.Fatalf("stop: %+v", control)
		}
		encoded, err := EncodeRecoveryRetryControl(control)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := DecodeRecoveryRetryControl(encoded)
		if err != nil {
			t.Fatal(err)
		}
		duplicate, changed, err := ObserveRecoveryFailure(
			&restored,
			assignment,
			failure,
			now.Add(time.Millisecond),
			true,
		)
		if err != nil || changed || duplicate != restored {
			t.Fatalf("redelivery changed budget: %+v %v", duplicate, err)
		}
		previous = &restored
		now = now.Add(6 * time.Second)
	}
	assignment.ExecutionEpoch++
	stillStopped, changed, err := ObserveRecoveryFailure(previous, assignment, failure, now, true)
	if err != nil || changed || stillStopped != *previous {
		t.Fatal("restart/reconnect revived exhausted recovery")
	}
}

func TestDeterministicUnsafeAndLateRecoveriesDoNotAutomaticallyReplay(t *testing.T) {
	// TASK-16: a faulty recipe, ambiguous hook, or expired deadline cannot gain
	// another execution merely by returning another failure or reconnecting.
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, code, reason string
		safe               bool
		remaining          time.Duration
	}{
		{"deterministic", "internal_execution_error", "non_transient_failure", true, time.Minute},
		{"ambiguous", "dependency_unavailable", "unsafe_replay", false, time.Minute},
		{"deadline", "dependency_unavailable", "recovery_deadline", true, time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment := TaskAssignmentRecord{
				TaskID:           ids.NewAt(ids.KindTask, now, 1),
				AssignmentID:     ids.NewAt(ids.KindAssignment, now, 2),
				ExecutionEpoch:   1,
				RecoveryDeadline: now.Add(test.remaining),
			}
			control, changed, err := ObserveRecoveryFailure(
				nil,
				assignment,
				&taskprogress.Failure{Code: test.code},
				now,
				test.safe,
			)
			if err != nil || !changed || !control.Stopped || control.Failures != 1 ||
				control.StopReason != test.reason {
				t.Fatalf("unsafe replay accepted: %+v %v", control, err)
			}
		})
	}
}
