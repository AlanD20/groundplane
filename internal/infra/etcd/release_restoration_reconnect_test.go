package etcd

import (
	context "context"
	json "encoding/json"
	errors "errors"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

func TestReleaseReconnectAndMutationEventRaceIsCASFenced(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	start := make(chan struct{})
	reconnectResult := make(chan error, 1)
	eventResult := make(chan error, 1)
	go func() {
		<-start
		_, err := fixture.repository.ReconnectAgentAssignment(context.Background(), fixture.claim)
		reconnectResult <- err
	}()
	go func() {
		<-start
		_, err := fixture.repository.AppendTaskEvent(context.Background(), testtaskjournal.TaskEventInput{
			Identity: testtaskjournal.TaskEventIdentity{
				AssignmentID: fixture.claim.Assignment.Record.AssignmentID,
				AgentID:      fixture.agentID, AgentGeneration: 1, TaskID: fixture.claim.Task.Record.ID,
				StepID: fixture.forwardStepID, Attempt: 1, Ordinal: 1,
			},
			State: testtaskjournal.TaskEventStateRunning, Payload: json.RawMessage(`{"message":"running"}`),
		}, fixture.now.Add(2*time.Second))
		eventResult <- err
	}()
	close(start)
	reconnectErr, eventErr := <-reconnectResult, <-eventResult
	if reconnectErr != nil || eventErr != nil && !errors.Is(eventErr, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("reconnect/event race errors = %v / %v", reconnectErr, eventErr)
	}
	current, err := fixture.repository.GetTaskAssignment(context.Background(), fixture.claim.Task.Record.ID)
	if err != nil || current.Assignment.Record.ExecutionEpoch != 2 {
		t.Fatalf("reconnect/event race assignment = %#v, %v", current, err)
	}
	if eventErr == nil &&
		current.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly ||
		eventErr != nil && current.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeForward {
		t.Fatalf("reconnect/event race mode = %s, event error %v", current.Assignment.Record.ExecutionMode, eventErr)
	}
}
