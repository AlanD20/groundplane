package agentchannel

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type taskReportConflictStore struct {
	*fakeTaskStore
	conflictTaskID string
	conflicts      int
	conflictResult etcd.TaskResultRecord
	fresh          *etcd.TaskAssignment
}

func (store *taskReportConflictStore) GetTaskAssignment(
	_ context.Context,
	taskID string,
) (etcd.TaskAssignment, error) {
	for _, assignment := range store.assignments {
		if assignment.Task.Record.ID == taskID {
			return assignment, nil
		}
	}
	return etcd.TaskAssignment{}, errs.New(errs.KindStateConflict, "Task has no active assignment")
}

func (store *taskReportConflictStore) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	generation uint64,
	taskID string,
	assignmentID string,
	terminal etcd.TaskStatus,
	result etcd.TaskResultRecord,
	at time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	if taskID == store.conflictTaskID {
		store.conflicts++
		store.conflictResult = result
		if store.fresh != nil {
			store.claims = append(store.claims, *store.fresh)
			store.fresh = nil
		}
		return store.tasks[taskID], errs.New(errs.KindStateConflict, "Task recovery authority changed")
	}
	acknowledged, err := store.fakeTaskStore.AcknowledgeTask(
		ctx, agentID, generation, taskID, assignmentID, terminal, result, at,
	)
	if err != nil {
		return etcd.Versioned[etcd.TaskRecord]{}, err
	}
	for index, assignment := range store.assignments {
		if assignment.Task.Record.ID == taskID {
			store.assignments = append(store.assignments[:index], store.assignments[index+1:]...)
			break
		}
	}
	return acknowledged, nil
}

func (store *taskReportConflictStore) ClaimNextTask(
	ctx context.Context,
	agentID string,
	generation uint64,
	at time.Time,
) (etcd.TaskAssignment, bool, error) {
	assignment, found, err := store.fakeTaskStore.ClaimNextTask(ctx, agentID, generation, at)
	if err == nil && found {
		store.assignments = append(store.assignments, assignment)
	}
	return assignment, found, err
}

// Rationale: a Task-level terminal publication conflict must quarantine only
// that exact assignment. The authenticated Agent remains useful, keeps actual
// Ready capacity, and may finish unrelated work without claiming the rejected
// report was durably terminal.
func TestConnectContainsAppliedTaskReportConflict(t *testing.T) {
	t.Parallel()

	at := testTime()
	first := assignmentQuarantineFixture(at, testAgentID, 9301)
	second := assignmentQuarantineFixture(at, testAgentID, 9302)
	firstPlan := testExecutionPlan(t, first.Task.Record)
	secondPlan := testExecutionPlan(t, second.Task.Record)
	first.Task.Record.PlanHash = hex.EncodeToString(firstPlan.GetPlanHash())
	second.Task.Record.PlanHash = hex.EncodeToString(secondPlan.GetPlanHash())
	base := &fakeTaskStore{
		assignments: []etcd.TaskAssignment{first},
		tasks: map[string]etcd.Versioned[etcd.TaskRecord]{
			first.Task.Record.ID:  first.Task,
			second.Task.Record.ID: second.Task,
		},
	}
	store := &taskReportConflictStore{
		fakeTaskStore: base, conflictTaskID: first.Task.Record.ID, fresh: &second,
	}
	auth := authorizedAuthenticator()
	auth.authorization.Generation = 7
	auth.authorization.Config.MaxConcurrentTasks = 2
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(93)),
		readyMessage(2),
		taskReportMessage(
			first,
			firstPlan,
			agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		),
		readyMessage(2),
		taskReportMessage(second, secondPlan, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE),
		readyMessage(2),
	}}
	server := New(auth, NewRegistry(), store, &assignmentQuarantinePlanResolver{plans: map[string]*agentpb.ExecutionPlan{
		first.Task.Record.ID: firstPlan, second.Task.Record.ID: secondPlan,
	}})
	server.now = func() time.Time { return at }

	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if store.conflicts != 1 {
		t.Fatalf("conflicting report attempts = %d, want 1", store.conflicts)
	}
	if store.conflictResult.Diagnostic != etcd.TaskResultDiagnosticComposeFailed {
		t.Fatalf("conflicting durable diagnostic = %q, want compose_failed", store.conflictResult.Diagnostic)
	}
	if firstTask := store.tasks[first.Task.Record.ID].Record; firstTask.Status != etcd.TaskStatusRunning {
		t.Fatalf("conflicting Task status = %q, want running", firstTask.Status)
	}
	if store.ackTaskID != second.Task.Record.ID ||
		store.tasks[second.Task.Record.ID].Record.Status != etcd.TaskStatusCompleted {
		t.Fatalf("unrelated Task did not complete after conflict: acknowledged=%q", store.ackTaskID)
	}
	if len(store.assignments) != 1 || store.assignments[0].Task.Record.ID != first.Task.Record.ID {
		t.Fatalf("recoverable assignments = %#v, want only conflicting Task", store.assignments)
	}
	if len(stream.sent) != 3 || stream.sent[1].GetTaskAssignment().GetTaskId() != first.Task.Record.ID ||
		stream.sent[2].GetTaskAssignment().GetTaskId() != second.Task.Record.ID {
		t.Fatalf("Controller messages = %#v, want config and both bounded assignments", stream.sent)
	}
}

func taskReportMessage(
	assignment etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
	diagnostic agentpb.ComposeHelperDiagnostic,
) *agentpb.AgentMessage {
	exitCode := int32(0)
	terminal := agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED
	failedStepID := ""
	if diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		exitCode = 17
		terminal = agentpb.TaskTerminal_TASK_TERMINAL_FAILED
		failedStepID = assignment.Task.Record.Steps[0].ID
	}
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: &agentpb.TaskAck{
		TaskId: assignment.Task.Record.ID, AssignmentId: assignment.Assignment.Record.AssignmentID,
		PlanHash: plan.GetPlanHash(), ExecutionEpoch: assignment.Assignment.Record.ExecutionEpoch,
		Terminal: terminal, ExitCode: exitCode,
		Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
			FailedStepId: failedStepID, Diagnostic: diagnostic,
		}},
	}}}
}

// Rationale: the protobuf catalog reserves distinct Component failure codes,
// and a failed Compose Task may report either without being misclassified as
// a malformed Agent message.
func TestValidateComposeTaskResultAcceptsComponentFailureDiagnostics(t *testing.T) {
	t.Parallel()

	for _, diagnostic := range []agentpb.ComposeHelperDiagnostic{
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	} {
		diagnostic := diagnostic
		t.Run(diagnostic.String(), func(t *testing.T) {
			t.Parallel()
			ack := &agentpb.TaskAck{
				Terminal: agentpb.TaskTerminal_TASK_TERMINAL_FAILED,
				ExitCode: 17,
				Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
					FailedStepId: "step-component", Diagnostic: diagnostic,
				}},
			}
			if err := validateComposeTaskResult(ack); err != nil {
				t.Fatalf("validateComposeTaskResult() error = %v", err)
			}
		})
	}
}

// Rationale: the durable Task journal intentionally has coarser failure
// classes than the helper wire. Component failures must retain their matching
// failure class instead of being silently persisted as diagnostic none.
func TestDurableComposeTaskResultClassifiesComponentFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		diagnostic agentpb.ComposeHelperDiagnostic
		want       etcd.TaskResultDiagnostic
	}{
		{
			diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED,
			want:       etcd.TaskResultDiagnosticConfigRejected,
		},
		{
			diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
			want:       etcd.TaskResultDiagnosticComposeFailed,
		},
	} {
		test := test
		t.Run(test.diagnostic.String(), func(t *testing.T) {
			t.Parallel()
			ack := &agentpb.TaskAck{Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
				Diagnostic: test.diagnostic,
			}}}
			if got := durableComposeTaskResult(ack).Diagnostic; got != test.want {
				t.Fatalf("durable diagnostic = %q, want %q", got, test.want)
			}
		})
	}
}

// Rationale: a quarantined report releases the Agent's local worker slot but
// not its durable assignment. Dispatch must keep those two bounds distinct and
// must not claim beyond the configured durable assignment limit.
func TestDispatchReadyDoesNotClaimPastQuarantinedDurableAssignment(t *testing.T) {
	t.Parallel()

	at := testTime()
	first := assignmentQuarantineFixture(at, testAgentID, 9501)
	second := assignmentQuarantineFixture(at, testAgentID, 9502)
	secondPlan := testExecutionPlan(t, second.Task.Record)
	second.Task.Record.PlanHash = hex.EncodeToString(secondPlan.GetPlanHash())
	store := &fakeTaskStore{
		assignments: []etcd.TaskAssignment{first},
		claims:      []etcd.TaskAssignment{second},
	}
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 7)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	server := New(nil, registry, store, &assignmentQuarantinePlanResolver{
		rejectedTaskID: first.Task.Record.ID,
		plans:          map[string]*agentpb.ExecutionPlan{second.Task.Record.ID: secondPlan},
	})
	server.now = func() time.Time { return at }
	delivered := make(map[string]string)
	quarantined := make(map[string]string)
	stream := &scriptedStream{ctx: context.Background()}

	if err := server.dispatchReady(
		stream,
		session,
		testAgentID,
		Authorization{Generation: 7, Config: &agentpb.AgentConfig{MaxConcurrentTasks: 1}},
		1,
		delivered,
		quarantined,
	); err != nil {
		t.Fatalf("dispatchReady() error = %v", err)
	}
	if len(stream.sent) != 0 || len(store.claims) != 1 || len(delivered) != 0 ||
		quarantined[first.Task.Record.ID] != first.Assignment.Record.AssignmentID {
		t.Fatalf(
			"sent/claims/delivered/quarantined = %d/%d/%#v/%#v",
			len(stream.sent),
			len(store.claims),
			delivered,
			quarantined,
		)
	}
}

// Rationale: containment is reserved for a conflict from the final durable
// application of an exact delivered report. Pre-validation faults and repeated
// or unowned reports remain strict protocol/session failures.
func TestQuarantineTaskReportConflictRequiresAppliedExactDelivery(t *testing.T) {
	t.Parallel()

	taskID := ids.NewAt(ids.KindTask, testTime(), 9401)
	assignmentID := ids.NewAt(ids.KindAssignment, testTime(), 9402)
	ack := &agentpb.TaskAck{TaskId: taskID, AssignmentId: assignmentID}
	conflict := errs.New(errs.KindStateConflict, "changed")
	validation := errs.New(errs.KindValidationFailed, "invalid")

	for _, test := range []struct {
		name      string
		stage     taskReportStage
		err       error
		delivered map[string]string
	}{
		{name: "pre-validation conflict", stage: taskReportRejected, err: conflict,
			delivered: map[string]string{taskID: assignmentID}},
		{name: "application validation", stage: taskReportApplicationAttempted, err: validation,
			delivered: map[string]string{taskID: assignmentID}},
		{name: "unowned application conflict", stage: taskReportApplicationAttempted, err: conflict,
			delivered: map[string]string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			quarantined := make(map[string]string)
			if quarantineTaskReportConflict(test.stage, test.err, ack, test.delivered, quarantined) {
				t.Fatal("strict report rejection was quarantined")
			}
			if len(quarantined) != 0 {
				t.Fatalf("quarantined = %#v, want empty", quarantined)
			}
		})
	}

	delivered := map[string]string{taskID: assignmentID}
	quarantined := make(map[string]string)
	if !quarantineTaskReportConflict(taskReportApplicationAttempted, conflict, ack, delivered, quarantined) {
		t.Fatal("exact applied Task conflict was not quarantined")
	}
	if len(delivered) != 0 || quarantined[taskID] != assignmentID {
		t.Fatalf("delivered/quarantined = %#v/%#v", delivered, quarantined)
	}
	if quarantineTaskReportConflict(taskReportApplicationAttempted, conflict, ack, delivered, quarantined) {
		t.Fatal("repeated quarantined report was accepted again")
	}
}
