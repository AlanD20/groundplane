package lifecycleexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-183: a completion/replay must keep the acknowledged action start and
// physical observation time, not report delivery time or attempt assignment.
func TestLifecycleDiagnosticsRetainActionTimingAndRejectUnconfirmedCompletion(t *testing.T) {
	at := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("captured lifecycle input"))
	attempt := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		Number: 1, Direction: workexecution.Forward, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: hex.EncodeToString(digest[:])},
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
			AgentID: ids.New(ids.KindAgent), Generation: 1},
	}
	input := &agentpb.ServiceLifecycleWorkInput{
		Action:         agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START,
		CapturedSource: &agentpb.WorkInputReference{InputId: attempt.Input.ID, Sha256: digest[:]},
		Members: []*agentpb.ServiceLifecycleMember{{Ownership: &agentpb.ServiceLifecycleSource{
			StepId: ids.New(ids.KindStep), ComposeNames: []string{"worker--singleton"},
		}}},
	}
	progress, err := NewProgress(attempt, input, at)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := progress.Begin(attempt, input, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, active, err := DiagnosticStages(attempt, input, begin, begin)
	if err != nil || active.State != workexecution.StageRunning || active.FinishedAt != nil {
		t.Fatalf("in-flight action was presented as completed: %+v, %v", active, err)
	}
	observedAt := at.Add(5 * time.Second)
	confirmed, err := begin.Confirm(attempt, input, Receipt{
		StageID: input.Members[0].Ownership.StepId, Member: "worker--singleton", State: Running,
		Evidence: attempt.Input, ObservedAt: observedAt,
	}, at.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, result, err := DiagnosticStages(attempt, input, begin, confirmed)
		if err != nil || result.State != workexecution.StageSucceeded ||
			result.StartedAt == nil || !result.StartedAt.Equal(*begin.BeganAt) ||
			result.FinishedAt == nil || !result.FinishedAt.Equal(observedAt) {
			t.Fatalf("completion/replay changed physical action timing: %+v, %v", result, err)
		}
	}
	invalid := confirmed
	invalid.Completed = nil
	if _, _, err := DiagnosticStages(attempt, input, begin, invalid); err == nil {
		t.Fatal("missing receipt was displayed as successful execution")
	}
}
