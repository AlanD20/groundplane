package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testLocalAgentTaskOne      = "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testLocalAgentTaskTwo      = "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	testLocalAgentAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: removal must subscribe before abort delivery, exclude assignments
// that completed during subscription setup, and prefer a committed terminal
// acknowledgement over a concurrent transport close.
func TestLocalAgentTasksAdapterClosesAbortAcknowledgementRaces(t *testing.T) {
	t.Parallel()

	assignments := &fakeLocalAgentTaskAssignments{snapshots: [][]etcd.TaskAssignment{
		{testLocalAgentAssignment(testLocalAgentTaskOne), testLocalAgentAssignment(testLocalAgentTaskTwo)},
		{testLocalAgentAssignment(testLocalAgentTaskTwo)},
	}}
	channel := newFakeLocalAgentTaskChannel()
	channel.abort = func(taskID string, terminal chan error) error {
		terminal <- nil
		return errs.New(errs.KindStateConflict, "session closed after acknowledgement")
	}
	adapter, err := NewTasks(assignments, channel)
	if err != nil {
		t.Fatalf("NewTasks() error = %v", err)
	}

	if err := adapter.AbortActive(
		context.Background(),
		runtimeAdapterAgentID,
		7,
		4,
		"agent_removed",
	); err != nil {
		t.Fatalf("AbortActive() error = %v", err)
	}
	if assignments.calls != 2 || assignments.agentID != runtimeAdapterAgentID ||
		assignments.generation != 7 || assignments.maximum != 4 {
		t.Fatalf("assignment queries = %#v", assignments)
	}
	if !reflect.DeepEqual(channel.aborted, []string{testLocalAgentTaskTwo}) {
		t.Fatalf("aborted Tasks = %v, want [%s]", channel.aborted, testLocalAgentTaskTwo)
	}
	if !channel.subscribedBeforeAbort {
		t.Fatal("Task abort was delivered before its terminal subscription")
	}
	select {
	case <-channel.contexts[testLocalAgentTaskOne].Done():
	default:
		t.Fatal("completed Task subscription was not cancelled after the second snapshot")
	}
}

// Rationale: an Agent disconnect or failed durable acknowledgement must stop
// removal before credential revocation instead of treating delivery as success.
func TestLocalAgentTasksAdapterReturnsTerminalFailure(t *testing.T) {
	t.Parallel()

	assignments := &fakeLocalAgentTaskAssignments{snapshots: [][]etcd.TaskAssignment{
		{testLocalAgentAssignment(testLocalAgentTaskOne)},
		{testLocalAgentAssignment(testLocalAgentTaskOne)},
	}}
	want := errs.New(errs.KindStateConflict, "session ended before acknowledgement")
	channel := newFakeLocalAgentTaskChannel()
	channel.abort = func(_ string, terminal chan error) error {
		terminal <- want
		return nil
	}
	adapter, err := NewTasks(assignments, channel)
	if err != nil {
		t.Fatalf("NewTasks() error = %v", err)
	}

	err = adapter.AbortActive(context.Background(), runtimeAdapterAgentID, 7, 4, "agent_removed")
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AbortActive() error = %v, want state.conflict", err)
	}
}

func TestLocalAgentTasksAdapterRejectsNonIdleGenerationWithoutAbort(t *testing.T) {
	// Rationale: update is non-destructive and must report resource.in_use
	// without subscribing to or aborting the active assignment.
	t.Parallel()

	assignments := &fakeLocalAgentTaskAssignments{snapshots: [][]etcd.TaskAssignment{{
		testLocalAgentAssignment(testLocalAgentTaskOne),
	}}}
	channel := newFakeLocalAgentTaskChannel()
	adapter, err := NewTasks(assignments, channel)
	if err != nil {
		t.Fatalf("NewTasks() error = %v", err)
	}
	err = adapter.RequireIdle(context.Background(), runtimeAdapterAgentID, 7, 4)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("RequireIdle() error = %v, want resource.in_use", err)
	}
	if len(channel.aborted) != 0 || len(channel.terminals) != 0 {
		t.Fatalf("RequireIdle() touched Task channel: %#v", channel)
	}
}

func testLocalAgentAssignment(taskID string) etcd.TaskAssignment {
	return etcd.TaskAssignment{
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: testLocalAgentAssignmentID, TaskID: taskID,
			},
		},
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{ID: taskID}},
	}
}

type fakeLocalAgentTaskAssignments struct {
	snapshots  [][]etcd.TaskAssignment
	calls      int
	agentID    string
	generation uint64
	maximum    int32
}

func (assignments *fakeLocalAgentTaskAssignments) ListAgentAssignments(
	_ context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) ([]etcd.TaskAssignment, error) {
	assignments.calls++
	assignments.agentID = agentID
	assignments.generation = generation
	assignments.maximum = maximum
	if len(assignments.snapshots) == 0 {
		return nil, errors.New("unexpected assignment query")
	}
	result := assignments.snapshots[0]
	assignments.snapshots = assignments.snapshots[1:]
	return result, nil
}

type fakeLocalAgentTaskChannel struct {
	terminals             map[string]chan error
	contexts              map[string]context.Context
	aborted               []string
	abort                 func(string, chan error) error
	subscribedBeforeAbort bool
}

func newFakeLocalAgentTaskChannel() *fakeLocalAgentTaskChannel {
	return &fakeLocalAgentTaskChannel{
		terminals: make(map[string]chan error),
		contexts:  make(map[string]context.Context),
	}
}

func (channel *fakeLocalAgentTaskChannel) TaskTerminal(
	ctx context.Context,
	_ string,
	_ uint64,
	taskID string,
	_ string,
) (<-chan error, error) {
	terminal := make(chan error, 1)
	channel.terminals[taskID] = terminal
	channel.contexts[taskID] = ctx
	return terminal, nil
}

func (channel *fakeLocalAgentTaskChannel) AbortTask(
	_ context.Context,
	_ string,
	_ uint64,
	taskID string,
	_ string,
	_ string,
) error {
	terminal, subscribed := channel.terminals[taskID]
	channel.subscribedBeforeAbort = channel.subscribedBeforeAbort || subscribed
	channel.aborted = append(channel.aborted, taskID)
	if channel.abort == nil {
		return nil
	}
	return channel.abort(taskID, terminal)
}
