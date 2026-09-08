package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeAuthenticator struct {
	configurationMu sync.Mutex
	authorization   Authorization
	configuration   *agentpb.AgentConfig
	err             error
	calls           int
	seenID          string
	seenToken       Token
}

type pausedAuthenticator struct {
	authorization Authorization
	entered       chan struct{}
	release       chan struct{}
}

type fakeTaskStore struct {
	assignments     []etcd.TaskAssignment
	claims          []etcd.TaskAssignment
	tasks           map[string]etcd.Versioned[etcd.TaskRecord]
	ackAgentID      string
	ackGeneration   uint64
	ackTaskID       string
	ackAssignmentID string
	ackTerminal     etcd.TaskStatus
	ackResult       etcd.TaskResultRecord
	events          []etcd.TaskEventInput
	durableEvents   []etcd.TaskEventRecord
	eventRevisions  []int64
}

type terminalReplayTaskStore struct{ *fakeTaskStore }

func (store *terminalReplayTaskStore) GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error) {
	return etcd.TaskAssignment{}, errs.New(errs.KindTaskNotFound, "terminal assignment was cleaned")
}

type wakeTaskStore struct {
	fakeTaskStore
	mu    sync.Mutex
	claim *etcd.TaskAssignment
	calls int
}

func (store *wakeTaskStore) ListAgentAssignments(
	context.Context,
	string,
	uint64,
	int32,
) ([]etcd.TaskAssignment, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]etcd.TaskAssignment(nil), store.assignments...), nil
}

func (store *wakeTaskStore) ClaimNextTask(
	context.Context,
	string,
	uint64,
	time.Time,
) (etcd.TaskAssignment, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.claim == nil {
		return etcd.TaskAssignment{}, false, nil
	}
	store.calls++
	claim := *store.claim
	store.claim = nil
	return claim, true, nil
}

func (store *wakeTaskStore) enqueue(claim etcd.TaskAssignment) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.claim = &claim
}

func (store *wakeTaskStore) setAssignments(assignments []etcd.TaskAssignment) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.assignments = append([]etcd.TaskAssignment(nil), assignments...)
}

func (store *wakeTaskStore) claimCalls() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.calls
}

type fakePlanResolver struct {
	plan      *agentpb.ExecutionPlan
	err       error
	onResolve func()
}

type blockingPlanResolver struct {
	plan    *agentpb.ExecutionPlan
	entered chan struct{}
	release chan struct{}
}

func (resolver *blockingPlanResolver) ResolveExecutionPlan(
	ctx context.Context,
	_ etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	close(resolver.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-resolver.release:
		return resolver.plan, nil
	}
}

// Rationale: the Agent channel must preserve every closed Task/plan operation
// pairing while rejecting a plan sealed for another Task type.
func TestOperationMatchesTaskAcceptsClosedPairingsAndRejectsCrossPairs(t *testing.T) {
	pairs := []struct {
		taskType  etcd.TaskType
		operation agentpb.PlanOperation
	}{
		{taskType: etcd.TaskDeploy, operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY},
		{taskType: etcd.TaskRollback, operation: agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK},
		{taskType: etcd.TaskStart, operation: agentpb.PlanOperation_PLAN_OPERATION_START},
		{taskType: etcd.TaskStop, operation: agentpb.PlanOperation_PLAN_OPERATION_STOP},
		{taskType: etcd.TaskDestroy, operation: agentpb.PlanOperation_PLAN_OPERATION_DESTROY},
		{taskType: etcd.TaskRemove, operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE},
		{taskType: etcd.TaskCreate, operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE},
		{taskType: etcd.TaskAttach, operation: agentpb.PlanOperation_PLAN_OPERATION_ATTACH},
		{taskType: etcd.TaskDetach, operation: agentpb.PlanOperation_PLAN_OPERATION_DETACH},
		{taskType: etcd.TaskBackup, operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP},
		{taskType: etcd.TaskBackupPrune, operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE},
	}
	for _, pair := range pairs {
		if !operationMatchesTask(pair.operation, etcd.TaskRecord{Type: pair.taskType}) {
			t.Errorf("operationMatchesTask(%s, %q) = false, want true", pair.operation, pair.taskType)
		}
	}
	for _, pair := range pairs {
		for _, other := range pairs {
			if pair.taskType == other.taskType {
				continue
			}
			if operationMatchesTask(pair.operation, etcd.TaskRecord{Type: other.taskType}) {
				t.Errorf(
					"operationMatchesTask(%s, %q) = true for cross-pair with %q",
					pair.operation,
					other.taskType,
					pair.taskType,
				)
			}
		}
	}
	backingCreation := etcd.TaskRecord{
		Type: etcd.TaskUpdate,
		Params: map[string]string{
			etcd.TaskBackingServiceHealthParam: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, backingCreation) {
		t.Error("backing-service TaskUpdate did not accept its Environment-create plan")
	}
	componentUpdate := etcd.TaskRecord{
		Type: etcd.TaskUpdate,
		Params: map[string]string{
			etcd.TaskResourceKindParam: etcd.TaskResourceComponent,
		},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, componentUpdate) {
		t.Error("Component TaskUpdate did not accept its Component-apply plan")
	}
	if operationMatchesTask(
		agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		etcd.TaskRecord{Type: etcd.TaskUpdate},
	) {
		t.Error("ordinary TaskUpdate accepted a Component-apply plan")
	}
	volumeCreation := etcd.TaskRecord{
		Type: etcd.TaskCreate,
		Params: map[string]string{
			etcd.TaskResourceKindParam: etcd.TaskResourceVolume,
		},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, volumeCreation) {
		t.Error("Volume TaskCreate did not accept its reconciliation plan")
	}
	if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, etcd.TaskRecord{Type: etcd.TaskCreate}) {
		t.Error("ordinary TaskCreate accepted a reconciliation plan")
	}
}

// Rationale: an Environment TaskUpdate's durable Compose procedure is the
// assignment authority. An absent or different marker must not let a plan
// cross between Blueprint apply and ordinary reconciliation semantics.
func TestOperationMatchesTaskRequiresClosedEnvironmentComposeProcedure(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	tests := []struct {
		name      string
		operation agentpb.PlanOperation
		marker    string
		want      bool
	}{
		{
			name: "marked full reconcile", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: string(taskcontract.BlueprintComposeProcedureFullReconcile), want: true,
		},
		{
			name: "unmarked reconcile", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		},
		{
			name: "wrong reconcile marker", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: string(taskcontract.BlueprintComposeProcedureNone),
		},
		{
			name: "malformed reconcile marker", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: "legacy",
		},
		{
			name: "Blueprint none", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureNone), want: true,
		},
		{
			name: "Blueprint candidate Releases", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureCandidateReleases), want: true,
		},
		{
			name: "full reconcile cannot masquerade as Blueprint", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureFullReconcile),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := map[string]string{}
			if test.marker != "" {
				params[taskcontract.EnvironmentBlueprintProcedureParam] = test.marker
			}
			task := etcd.TaskRecord{Type: etcd.TaskUpdate, Target: environmentID, Params: params}
			if got := operationMatchesTask(test.operation, task); got != test.want {
				t.Fatalf(
					"operationMatchesTask(%s, marker %q) = %t, want %t",
					test.operation,
					test.marker,
					got,
					test.want,
				)
			}
		})
	}
}

func (resolver *fakePlanResolver) ResolveExecutionPlan(
	context.Context,
	etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.onResolve != nil {
		resolver.onResolve()
	}
	return resolver.plan, resolver.err
}

func (store *fakeTaskStore) ListAgentAssignments(
	context.Context,
	string,
	uint64,
	int32,
) ([]etcd.TaskAssignment, error) {
	return append([]etcd.TaskAssignment(nil), store.assignments...), nil
}

func (store *fakeTaskStore) ClaimNextTask(
	context.Context,
	string,
	uint64,
	time.Time,
) (etcd.TaskAssignment, bool, error) {
	if len(store.claims) == 0 {
		return etcd.TaskAssignment{}, false, nil
	}
	claim := store.claims[0]
	store.claims = store.claims[1:]
	return claim, true, nil
}

func (store *fakeTaskStore) GetTask(
	_ context.Context,
	taskID string,
) (etcd.Versioned[etcd.TaskRecord], error) {
	task, ok := store.tasks[taskID]
	if !ok {
		return etcd.Versioned[etcd.TaskRecord]{}, errs.New(errs.KindTaskNotFound, "missing")
	}
	return task, nil
}

func (store *fakeTaskStore) ListTaskEvents(
	_ context.Context,
	taskID string,
	revision int64,
) (etcd.TaskEventSnapshot, error) {
	store.eventRevisions = append(store.eventRevisions, revision)
	task := store.tasks[taskID]
	return etcd.TaskEventSnapshot{
		Task: task.Record, Events: append([]etcd.TaskEventRecord(nil), store.durableEvents...), Revision: revision,
	}, nil
}

func (store *fakeTaskStore) AppendTaskEvent(
	_ context.Context,
	input etcd.TaskEventInput,
	_ time.Time,
) (etcd.TaskEventAppend, error) {
	store.events = append(store.events, input)
	sequence := uint64(len(store.events))
	store.durableEvents = append(store.durableEvents, etcd.TaskEventRecord{
		Sequence: sequence, Identity: input.Identity, State: input.State, Payload: append([]byte(nil), input.Payload...),
	})
	return etcd.TaskEventAppend{Sequence: sequence, Revision: 6}, nil
}

func (store *fakeTaskStore) AcknowledgeTask(
	_ context.Context,
	agentID string,
	generation uint64,
	taskID string,
	assignmentID string,
	terminal etcd.TaskStatus,
	result etcd.TaskResultRecord,
	_ time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	store.ackAgentID = agentID
	store.ackGeneration = generation
	store.ackTaskID = taskID
	store.ackAssignmentID = assignmentID
	store.ackTerminal = terminal
	store.ackResult = result
	task := store.tasks[taskID]
	task.Record.Status = terminal
	store.tasks[taskID] = task
	return task, nil
}

// Rationale: Environment removal executes the same directory runtime as
// Environment creation, so its terminal acknowledgement must not be rejected
// as a missing Compose result after the directory step has completed.
func TestAcknowledgeEnvironmentRemovalAcceptsDirectoryResult(t *testing.T) {
	t.Parallel()
	now := testTime()
	taskID := ids.NewAt(ids.KindTask, now, 91)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 92)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 93)
	planHash := sha256.Sum256([]byte("environment-removal-plan"))
	store := &fakeTaskStore{tasks: map[string]etcd.Versioned[etcd.TaskRecord]{
		taskID: {Record: etcd.TaskRecord{
			ID: taskID, Type: etcd.TaskRemove, Target: environmentID,
			PlanHash: hex.EncodeToString(planHash[:]), Status: etcd.TaskStatusRunning,
		}},
	}}
	server := &Server{tasks: store, plans: &fakePlanResolver{plan: &agentpb.ExecutionPlan{
		Steps: []*agentpb.ExecutionStep{{Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
			EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{},
		}}},
	}}, now: func() time.Time { return now }}

	err := server.acknowledge(context.Background(), testAgentID, 1, &agentpb.TaskAck{
		TaskId: taskID, AssignmentId: assignmentID, PlanHash: planHash[:],
		ExecutionEpoch: 1,
		Terminal:       agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED,
		Result: &agentpb.TaskAck_EnvironmentDirectoryResult{
			EnvironmentDirectoryResult: &agentpb.EnvironmentDirectoryTaskResult{},
		},
	})
	if err != nil {
		t.Fatalf("acknowledge(Environment remove) error = %v", err)
	}
	if store.ackTaskID != taskID || store.ackAssignmentID != assignmentID ||
		store.ackTerminal != etcd.TaskStatusCompleted ||
		store.ackResult.Kind != etcd.TaskResultEnvironmentDirectory {
		t.Fatalf(
			"acknowledgement = %q/%q/%q/%#v",
			store.ackTaskID, store.ackAssignmentID, store.ackTerminal, store.ackResult,
		)
	}
}

func TestAcknowledgeTerminalRecoveryReplaySurvivesAssignmentCleanup(t *testing.T) {
	t.Parallel()
	now := testTime()
	taskID := ids.NewAt(ids.KindTask, now, 191)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 192)
	serviceID := ids.NewAt(ids.KindService, now, 193)
	planHash := sha256.Sum256([]byte("terminal-recovery-plan"))
	recoveryDigest := sha256.Sum256([]byte("terminal-recovery-record"))
	result := &etcd.TaskResultRecord{Kind: etcd.TaskResultCompose, Diagnostic: etcd.TaskResultDiagnosticNone,
		ExecutionEpoch: 2, ReleaseRecoveryRecordSHA256: hex.EncodeToString(recoveryDigest[:])}
	base := &fakeTaskStore{tasks: map[string]etcd.Versioned[etcd.TaskRecord]{taskID: {Record: etcd.TaskRecord{
		ID: taskID, Type: etcd.TaskDeploy, Target: serviceID, PlanHash: hex.EncodeToString(planHash[:]),
		Status: etcd.TaskStatusFailed, Result: result, TerminalAssignment: &etcd.TaskTerminalAssignmentRecord{
			AssignmentID: assignmentID, AgentID: testAgentID, AgentGeneration: 1,
		},
	}}}}
	server := &Server{
		tasks: &terminalReplayTaskStore{fakeTaskStore: base},
		plans: &fakePlanResolver{plan: &agentpb.ExecutionPlan{
			Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, PlanHash: planHash[:],
		}},
		now: func() time.Time { return now },
	}
	ack := &agentpb.TaskAck{TaskId: taskID, AssignmentId: assignmentID, PlanHash: planHash[:], ExecutionEpoch: 2,
		ReleaseRecoveryRecordSha256: recoveryDigest[:], Terminal: agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED,
		Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		}},
	}
	if err := server.acknowledge(context.Background(), testAgentID, 1, ack); err != nil {
		t.Fatalf("exact terminal replay error = %v", err)
	}
	changed := proto.Clone(ack).(*agentpb.TaskAck)
	changed.ReleaseRecoveryRecordSha256[0] ^= 0xff
	if err := server.acknowledge(context.Background(), testAgentID, 1, changed); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("changed terminal replay error = %v, want state conflict", err)
	}
}

func (a *pausedAuthenticator) Authenticate(ctx context.Context, _ string, _ Token) (Authorization, error) {
	close(a.entered)
	select {
	case <-ctx.Done():
		return Authorization{}, ctx.Err()
	case <-a.release:
		return a.authorization, nil
	}
}

func (a *pausedAuthenticator) Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error) {
	return proto.Clone(a.authorization.Config).(*agentpb.AgentConfig), nil
}

func (a *fakeAuthenticator) Authenticate(_ context.Context, id string, token Token) (Authorization, error) {
	a.calls++
	a.seenID = id
	a.seenToken = token
	return a.authorization, a.err
}

type scriptedStream struct {
	ctx      context.Context
	messages []*agentpb.AgentMessage
	recvErr  error
	sent     []*agentpb.ControllerMessage
}

func (s *scriptedStream) Send(message *agentpb.ControllerMessage) error {
	s.sent = append(s.sent, message)
	return nil
}

func (s *scriptedStream) Recv() (*agentpb.AgentMessage, error) {
	if len(s.messages) != 0 {
		message := s.messages[0]
		s.messages = s.messages[1:]
		return message, nil
	}
	if s.recvErr != nil {
		err := s.recvErr
		s.recvErr = nil
		return nil, err
	}
	return nil, io.EOF
}

func (s *scriptedStream) SetHeader(metadata.MD) error  { return nil }
func (s *scriptedStream) SendHeader(metadata.MD) error { return nil }
func (s *scriptedStream) SetTrailer(metadata.MD)       {}
func (s *scriptedStream) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}
func (s *scriptedStream) SendMsg(any) error { return errors.New("unexpected SendMsg") }
func (s *scriptedStream) RecvMsg(any) error { return errors.New("unexpected RecvMsg") }

type liveStream struct {
	ctx      context.Context
	received chan *agentpb.AgentMessage
	sent     chan *agentpb.ControllerMessage
}

func newLiveStream(ctx context.Context) *liveStream {
	return &liveStream{
		ctx: ctx, received: make(chan *agentpb.AgentMessage, 1), sent: make(chan *agentpb.ControllerMessage, 2),
	}
}

func (s *liveStream) Send(message *agentpb.ControllerMessage) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sent <- message:
		return nil
	}
}

func (s *liveStream) Recv() (*agentpb.AgentMessage, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case message := <-s.received:
		return message, nil
	}
}

func (s *liveStream) SetHeader(metadata.MD) error  { return nil }
func (s *liveStream) SendHeader(metadata.MD) error { return nil }
func (s *liveStream) SetTrailer(metadata.MD)       {}
func (s *liveStream) Context() context.Context     { return s.ctx }
func (s *liveStream) SendMsg(any) error            { return errors.New("unexpected SendMsg") }
func (s *liveStream) RecvMsg(any) error            { return errors.New("unexpected RecvMsg") }

// Rationale: no unauthenticated payload may reach Agent session handling.
func TestConnectRequiresAuthenticateFirst(t *testing.T) {
	authenticator := &fakeAuthenticator{}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{readyMessage(1)}}

	err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
	if status.Code(err).String() != "Unauthenticated" {
		t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
	}
	if authenticator.calls != 0 || len(stream.sent) != 0 {
		t.Fatalf("auth calls = %d, sent = %d", authenticator.calls, len(stream.sent))
	}
}

// Rationale: successful authentication without a runtime configuration is a
// Controller wiring failure and must fail closed before opening a session.
func TestConnectRejectsMissingAuthorizedConfig(t *testing.T) {
	authenticator := &fakeAuthenticator{authorization: Authorization{Generation: 1}}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage("agt_01J00000000000000000000000", testToken('m')),
	}}

	err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, want Internal", status.Code(err))
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent = %d messages, want zero", len(stream.sent))
	}
}

// Rationale: a zero generation can only come from a corrupt authenticator
// result and must be classified as Controller failure, not client validation.
func TestConnectTreatsImpossibleAuthorizationAsInternal(t *testing.T) {
	authenticator := authorizedAuthenticator()
	authenticator.authorization.Generation = 0
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken('z')),
	}}

	err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, want Internal", status.Code(err))
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent = %d messages, want zero", len(stream.sent))
	}
}

// Rationale: removal may revoke an authenticated identity while Authenticate
// is paused before Registry.Open; releasing Authenticate must not resurrect it.
func TestConnectCannotOpenGenerationRevokedDuringAuthentication(t *testing.T) {
	registry := NewRegistry()
	authenticator := &pausedAuthenticator{
		authorization: authorizedAuthenticator().authorization,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken('r')),
	}}
	result := make(chan error, 1)
	go func() {
		result <- New(authenticator, registry, nil, nil).Connect(stream)
	}()
	<-authenticator.entered
	if err := registry.Revoke(context.Background(), testAgentID, 1); err != nil {
		t.Fatalf("revoke during Authenticate: %v", err)
	}
	close(authenticator.release)

	if err := <-result; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Connect() status = %v, want FailedPrecondition", status.Code(err))
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent = %d messages, want zero", len(stream.sent))
	}
}

// Rationale: malformed identities, malformed credentials, and authenticator
// mismatch are indistinguishable at the transport boundary.
func TestConnectRejectsMalformedAndMismatchedAuthentication(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		token   []byte
		authErr error
	}{
		{name: "malformed id", id: "agent-1", token: testToken(1)},
		{name: "short token", id: testAgentID, token: []byte("short")},
		{
			name:    "mismatch",
			id:      testAgentID,
			token:   testToken(2),
			authErr: errs.New(errs.KindAgentNotFound, "credential mismatch"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authenticator := &fakeAuthenticator{err: tt.authErr}
			stream := &scriptedStream{messages: []*agentpb.AgentMessage{authenticateMessage(tt.id, tt.token)}}

			err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
			if status.Code(err).String() != "Unauthenticated" {
				t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
			}
			if len(stream.sent) != 0 {
				t.Fatalf("sent %d messages before authentication", len(stream.sent))
			}
		})
	}
}

// Rationale: Authenticate is a one-message handshake and cannot be replayed
// within an authorized stream.
func TestConnectRejectsDuplicateAuthenticate(t *testing.T) {
	authenticator := authorizedAuthenticator()
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(3)),
		authenticateMessage(testAgentID, testToken(3)),
	}}

	err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
	if status.Code(err).String() != "Unauthenticated" {
		t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent messages = %d, want initial config only", len(stream.sent))
	}
}

// Rationale: authorized configuration is never disclosed before successful
// credential verification, and it is the first Controller message afterward.
func TestConnectGatesInitialConfigOnAuthentication(t *testing.T) {
	failed := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(4)),
	}}
	failedAuth := authorizedAuthenticator()
	failedAuth.err = errs.New(errs.KindAgentNotFound, "mismatch")
	_ = New(failedAuth, NewRegistry(), nil, nil).Connect(failed)
	if len(failed.sent) != 0 {
		t.Fatalf("failed authentication sent %d messages", len(failed.sent))
	}

	success := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(4)),
	}}
	if err := New(authorizedAuthenticator(), NewRegistry(), nil, nil).Connect(success); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if len(success.sent) != 1 || success.sent[0].GetConfigUpdate() == nil {
		t.Fatalf("first message = %+v, want ConfigUpdate", success.sent)
	}
	if success.sent[0].GetConfigUpdate().GetAgentConfig().GetMaxConcurrentTasks() != 4 {
		t.Fatalf("config = %+v", success.sent[0].GetConfigUpdate().GetAgentConfig())
	}
}

// Rationale: scheduling and stale-readiness decisions need the exact capacity
// and receipt time from the latest Ready message even after disconnect.
func TestConnectTracksReadyFreshnessAndCapacity(t *testing.T) {
	registry := NewRegistry()
	server := New(authorizedAuthenticator(), registry, &fakeTaskStore{}, nil)
	wantTime := testTime()
	server.now = func() time.Time { return wantTime }
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(5)),
		readyMessage(3),
	}}

	if err := server.Connect(stream); err != nil {
		t.Fatalf("connect: %v", err)
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || snapshot.Online || snapshot.Capacity != 3 || snapshot.Version != "v0.4.2" ||
		!snapshot.LastReady.Equal(wantTime) {
		t.Fatalf("snapshot = %+v, found = %v", snapshot, ok)
	}
}

// Rationale: TaskAbort must share the sole Controller send loop with config
// and assignments so a lifecycle caller never invokes gRPC Send concurrently.
func TestConnectDeliversFencedTaskAbort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	registry := NewRegistry()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(7))
	taskID := ids.NewAt(ids.KindTask, testTime(), 19)
	assignmentID := ids.NewAt(ids.KindAssignment, testTime(), 18)
	planHash := bytes.Repeat([]byte{0x19}, 32)
	tasks := &fakeTaskStore{tasks: map[string]etcd.Versioned[etcd.TaskRecord]{taskID: {Record: etcd.TaskRecord{
		ID: taskID, PlanHash: hex.EncodeToString(planHash), Status: etcd.TaskStatusRunning,
	}}}}
	result := make(chan error, 1)
	go func() {
		result <- New(
			authorizedAuthenticator(), registry, tasks,
			&fakePlanResolver{plan: &agentpb.ExecutionPlan{}},
		).Connect(stream)
	}()

	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("first Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send initial config")
	}
	terminal, err := registry.TaskTerminal(ctx, testAgentID, 1, taskID, assignmentID)
	if err != nil {
		t.Fatalf("TaskTerminal() error = %v", err)
	}
	if err := registry.AbortTask(ctx, testAgentID, 1, taskID, assignmentID, "agent_removed"); err != nil {
		t.Fatalf("AbortTask() error = %v", err)
	}
	select {
	case message := <-stream.sent:
		abort := message.GetTaskAbort()
		if abort == nil || abort.TaskId != taskID || abort.AssignmentId != assignmentID ||
			abort.Reason != "agent_removed" {
			t.Fatalf("TaskAbort = %#v", abort)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not deliver TaskAbort")
	}
	stream.received <- &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: &agentpb.TaskAck{
		TaskId: taskID, AssignmentId: assignmentID, ExecutionEpoch: 1,
		PlanHash: planHash, Terminal: agentpb.TaskTerminal_TASK_TERMINAL_ABORTED,
		Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		}},
	}}}
	select {
	case terminalErr := <-terminal:
		if terminalErr != nil {
			t.Fatalf("Task terminal result = %v", terminalErr)
		}
	case <-time.After(time.Second):
		t.Fatal("durable Task acknowledgement did not notify terminal subscriber")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Connect() cancellation error = %v", err)
	}
}

// Rationale: rotation can cancel a stream after its Ready pull commits a real
// durable claim; once the prior-generation fence is active, that old stream
// must not receive the assignment returned by the racing repository call.
func TestConnectDoesNotDeliverAssignmentClaimedDuringPriorGenerationFence(t *testing.T) {
	now := testTime()
	taskID := ids.NewAt(ids.KindTask, now, 120)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 121),
		IdempotencyKey: "channel-fence-0001",
		Owner:          etcd.PlatformTaskOwner(), Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent,
		PlanID:   ids.NewAt(ids.KindPlan, now, 122), RenderGeneration: 7,
		Type: etcd.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 123),
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 124)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	repository := newChannelTaskRepository(t, task)
	claimEntered := make(chan struct{}, 1)
	claimRelease := make(chan struct{})
	tasks := &blockingClaimTaskStore{
		repository: repository,
		entered:    claimEntered,
		release:    claimRelease,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(8))
	connectResult := make(chan error, 1)
	go func() {
		connectResult <- New(
			authorizedAuthenticator(),
			registry,
			tasks,
			&fakePlanResolver{plan: plan},
		).Connect(stream)
	}()
	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("first Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send initial config")
	}
	stream.received <- readyMessage(1)
	select {
	case <-claimEntered:
	case <-time.After(time.Second):
		t.Fatal("old stream did not commit a durable claim")
	}

	fenceResult := make(chan error, 1)
	go func() {
		fenceResult <- fenceRegistryThrough(context.Background(), registry, testAgentID, 1)
	}()
	deadline := time.After(time.Second)
	for {
		snapshot, ok := registry.Snapshot(testAgentID)
		if ok && snapshot.Revoked {
			break
		}
		select {
		case err := <-fenceResult:
			close(claimRelease)
			t.Fatalf("FenceThrough() returned before revoking the old session: %v", err)
		case <-deadline:
			close(claimRelease)
			t.Fatal("FenceThrough() did not revoke the old session")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(claimRelease)
	if err := <-fenceResult; err != nil {
		t.Fatalf("FenceThrough() error = %v", err)
	}
	if err := <-connectResult; err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	select {
	case message := <-stream.sent:
		t.Fatalf("old stream received post-fence Controller message %#v", message)
	default:
	}
	assignments, err := repository.ListAgentAssignments(context.Background(), testAgentID, 1, 1)
	if err != nil {
		t.Fatalf("ListAgentAssignments() error = %v", err)
	}
	if len(assignments) != 1 || assignments[0].Task.Record.ID != taskID ||
		assignments[0].Assignment.Record.AgentGeneration != 1 {
		t.Fatalf("persisted racing assignment = %#v", assignments)
	}
}

// Rationale: Ready is both the pull and the capacity fence. The Controller
// must claim durable work, preserve every execution-identity field in the
// protobuf assignment, and terminalize only an acknowledgement with the same
// plan hash and authenticated Agent generation.
func TestConnectClaimsAssignmentAndPersistsAcknowledgement(t *testing.T) {
	now := testTime()
	startedAt := now
	taskID := ids.NewAt(ids.KindTask, now, 20)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 21),
		PlanID:           ids.NewAt(ids.KindPlan, now, 22),
		RenderGeneration: 7, Type: etcd.TaskDeploy,
		Target:         ids.NewAt(ids.KindService, now, 23),
		Params:         map[string]string{"strategy": "blue-green"},
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 24)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	planHash := append([]byte(nil), plan.PlanHash...)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 26)
	deadline := now.Add(37 * time.Second)
	versioned := etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5}
	tasks := &fakeTaskStore{
		claims: []etcd.TaskAssignment{{
			Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
				AssignmentID: assignmentID, TaskID: taskID, Executor: etcd.TaskExecutorAgent,
				AgentID: testAgentID, AgentGeneration: 1,
				AssignedAt: now, Deadline: deadline, ExecutionEpoch: 1,
				ExecutionMode: etcd.TaskExecutionModeForward, RecoveryDeadline: deadline.Add(time.Minute),
			}},
			Task: versioned,
		}},
		tasks: map[string]etcd.Versioned[etcd.TaskRecord]{taskID: versioned},
	}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(8)),
		readyMessage(1),
		{Payload: &agentpb.AgentMessage_TaskEvent{TaskEvent: &agentpb.TaskEvent{
			TaskId: taskID, AssignmentId: assignmentID,
			PlanHash: planHash, StepId: task.Steps[0].ID,
			ExecutionEpoch: 1, Ordinal: 2, State: agentpb.TaskState_TASK_STATE_COMPLETED,
		}}},
		{Payload: &agentpb.AgentMessage_TaskAck{TaskAck: &agentpb.TaskAck{
			TaskId: taskID, AssignmentId: assignmentID, PlanHash: planHash,
			ExecutionEpoch: 1,
			Terminal:       agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED,
			Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
				Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			}},
		}}},
	}}
	server := New(authorizedAuthenticator(), NewRegistry(), tasks, &fakePlanResolver{plan: plan})
	server.now = func() time.Time { return now }
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if len(stream.sent) != 3 || stream.sent[2].GetTaskEventAck() == nil {
		t.Fatalf("Controller messages = %#v, want config, assignment, and durable event acknowledgement", stream.sent)
	}
	assignment := stream.sent[1].GetTaskAssignment()
	if assignment == nil || assignment.TaskId != task.ID || assignment.AssignmentId != assignmentID ||
		assignment.OperationId != task.OperationID || assignment.Plan == nil ||
		assignment.ExecutionEpoch != 1 ||
		assignment.Plan.PlanId != task.PlanID ||
		assignment.Plan.RenderGeneration != uint64(task.RenderGeneration) ||
		!bytes.Equal(assignment.Plan.PlanHash, planHash) ||
		len(assignment.Plan.Steps) != 1 || assignment.Plan.Steps[0].StepId != task.Steps[0].ID ||
		assignment.ForwardDeadline == nil || !assignment.ForwardDeadline.AsTime().Equal(deadline) {
		t.Fatalf("TaskAssignment = %#v", assignment)
	}
	if tasks.ackAgentID != testAgentID || tasks.ackGeneration != 1 ||
		tasks.ackTaskID != task.ID || tasks.ackAssignmentID != assignmentID ||
		tasks.ackTerminal != etcd.TaskStatusCompleted {
		t.Fatalf(
			"ack = agent %q generation %d task %q terminal %q",
			tasks.ackAgentID,
			tasks.ackGeneration,
			tasks.ackTaskID,
			tasks.ackTerminal,
		)
	}
	if len(tasks.events) != 1 ||
		tasks.events[0].Identity.AssignmentID != assignmentID ||
		tasks.events[0].Identity.AgentID != testAgentID ||
		tasks.events[0].Identity.AgentGeneration != 1 ||
		tasks.events[0].Identity.TaskID != task.ID ||
		tasks.events[0].Identity.StepID != task.Steps[0].ID ||
		tasks.events[0].Identity.Attempt != 1 ||
		tasks.events[0].Identity.Ordinal != 2 ||
		tasks.events[0].State != etcd.TaskEventStateCompleted ||
		!bytes.Equal(tasks.events[0].Payload, []byte(`{}`)) {
		t.Fatalf("persisted Task events = %#v", tasks.events)
	}
}

// Rationale: task publication must wake an already-ready Agent session rather
// than waiting for the client's next pull tick. This uses a one-minute pull
// interval and sends exactly one Ready, so assignment delivery is bounded by
// the Registry wake path.
func TestConnectWakeDispatchesPublishedTaskWithoutAnotherReady(t *testing.T) {
	now := testTime()
	startedAt := now
	taskID := ids.NewAt(ids.KindTask, now, 270)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 271),
		PlanID:           ids.NewAt(ids.KindPlan, now, 272),
		RenderGeneration: 7, Type: etcd.TaskDeploy,
		Target:         ids.NewAt(ids.KindService, now, 273),
		Steps:          []etcd.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 274)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 275)
	claim := etcd.TaskAssignment{
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: assignmentID, TaskID: taskID, Executor: etcd.TaskExecutorAgent,
			AgentID: testAgentID, AgentGeneration: 1, AssignedAt: now,
			Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
			ExecutionEpoch: 1, ExecutionMode: etcd.TaskExecutionModeForward,
		}},
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
	}
	authenticator := authorizedAuthenticator()
	authenticator.authorization.Config.PullIntervalSeconds = 60
	registry := NewRegistry()
	tasks := &wakeTaskStore{fakeTaskStore: fakeTaskStore{
		tasks: map[string]etcd.Versioned[etcd.TaskRecord]{taskID: claim.Task},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(10))
	server := New(authenticator, registry, tasks, &fakePlanResolver{plan: plan})
	server.now = func() time.Time { return now }
	connectResult := make(chan error, 1)
	go func() { connectResult <- server.Connect(stream) }()

	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("first Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send initial config")
	}
	stream.received <- readyMessage(1)
	readyDeadline := time.NewTimer(time.Second)
	defer readyDeadline.Stop()
	for {
		snapshot, ok := registry.Snapshot(testAgentID)
		if ok && snapshot.Capacity == 1 && !snapshot.LastReady.IsZero() {
			break
		}
		select {
		case <-readyDeadline.C:
			t.Fatal("Controller did not record initial Ready")
		case <-time.After(time.Millisecond):
		}
	}

	publicationAt := time.Now()
	tasks.enqueue(claim)
	registry.WakeTaskDispatch()
	select {
	case message := <-stream.sent:
		assignment := message.GetTaskAssignment()
		if assignment == nil || assignment.TaskId != taskID || assignment.AssignmentId != assignmentID {
			t.Fatalf("TaskAssignment = %#v", assignment)
		}
		if elapsed := time.Since(publicationAt); elapsed >= time.Second {
			t.Fatalf("wake dispatch latency = %s, want < 1s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("published task was not dispatched before pull interval")
	}

	secondClaim := claim
	secondClaim.Assignment.Record.AssignmentID = ids.NewAt(ids.KindAssignment, now, 276)
	tasks.setAssignments([]etcd.TaskAssignment{claim})
	tasks.enqueue(secondClaim)
	registry.WakeTaskDispatch()
	select {
	case message := <-stream.sent:
		t.Fatalf("repeated wake delivered beyond capacity: %#v", message)
	case <-time.After(100 * time.Millisecond):
	}
	if calls := tasks.claimCalls(); calls != 1 {
		t.Fatalf("ClaimNextTask calls after repeated wake = %d, want 1", calls)
	}

	cancel()
	if err := <-connectResult; err != nil {
		t.Fatalf("Connect() cancellation error = %v", err)
	}
}

// Rationale: a ConfigUpdate replaces the worker contract. A queued task wake
// must not reuse capacity reported under the old contract before a fresh Ready.
func TestConnectWakeRequiresFreshReadyAfterConfigUpdate(t *testing.T) {
	now := testTime()
	startedAt := now
	taskID := ids.NewAt(ids.KindTask, now, 280)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 281),
		PlanID: ids.NewAt(ids.KindPlan, now, 282), RenderGeneration: 7,
		Type: etcd.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 283),
		Steps:          []etcd.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 284)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 285), TaskID: taskID,
			Executor: etcd.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
			AssignedAt: now, Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
			ExecutionEpoch: 1, ExecutionMode: etcd.TaskExecutionModeForward,
		}},
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
	}
	authenticator := authorizedAuthenticator()
	authenticator.authorization.Config.MaxConcurrentTasks = 2
	registry := NewRegistry()
	tasks := &wakeTaskStore{fakeTaskStore: fakeTaskStore{
		tasks: map[string]etcd.Versioned[etcd.TaskRecord]{taskID: claim.Task},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(11))
	server := New(authenticator, registry, tasks, &fakePlanResolver{plan: plan})
	server.now = func() time.Time { return now }
	connectResult := make(chan error, 1)
	go func() { connectResult <- server.Connect(stream) }()
	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("first Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send initial config")
	}
	stream.received <- readyMessage(1)
	readyDeadline := time.NewTimer(time.Second)
	for {
		snapshot, ok := registry.Snapshot(testAgentID)
		if ok && snapshot.Capacity == 1 && !snapshot.LastReady.IsZero() {
			break
		}
		select {
		case <-readyDeadline.C:
			t.Fatal("Controller did not record initial Ready")
		case <-time.After(time.Millisecond):
		}
	}
	readyDeadline.Stop()

	nextConfig := proto.Clone(authenticator.authorization.Config).(*agentpb.AgentConfig)
	nextConfig.PullIntervalSeconds++
	authenticator.setConfiguration(nextConfig)
	stream.received <- readyMessage(1)
	pendingDeadline := time.NewTimer(time.Second)
	for {
		snapshot, ok := registry.Snapshot(testAgentID)
		if ok && snapshot.LastReady.IsZero() && snapshot.Capacity == 0 {
			break
		}
		select {
		case <-pendingDeadline.C:
			t.Fatalf("partial-capacity config mismatch retained readiness: %+v", snapshot)
		case <-time.After(time.Millisecond):
		}
	}
	pendingDeadline.Stop()

	tasks.enqueue(claim)
	registry.WakeTaskDispatch()
	select {
	case message := <-stream.sent:
		t.Fatalf("config-pending wake delivered assignment: %#v", message)
	case <-time.After(100 * time.Millisecond):
	}
	if calls := tasks.claimCalls(); calls != 0 {
		t.Fatalf("ClaimNextTask calls while config replacement pending = %d, want 0", calls)
	}

	stream.received <- readyMessage(authenticator.authorization.Config.MaxConcurrentTasks)
	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("replacement Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send replacement config")
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || !snapshot.LastReady.IsZero() || snapshot.Capacity != 0 {
		t.Fatalf("post-config readiness snapshot = %+v, found = %v", snapshot, ok)
	}

	registry.WakeTaskDispatch()
	select {
	case message := <-stream.sent:
		t.Fatalf("stale wake delivered assignment: %#v", message)
	case <-time.After(100 * time.Millisecond):
	}
	if calls := tasks.claimCalls(); calls != 0 {
		t.Fatalf("ClaimNextTask calls before fresh Ready = %d, want 0", calls)
	}

	stream.received <- readyMessage(1)
	select {
	case message := <-stream.sent:
		if assignment := message.GetTaskAssignment(); assignment == nil || assignment.TaskId != taskID {
			t.Fatalf("fresh Ready assignment = %#v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh Ready did not dispatch queued task")
	}
	cancel()
	if err := <-connectResult; err != nil {
		t.Fatalf("Connect() cancellation error = %v", err)
	}
}

// Rationale: assignment resolution owns transient Script, Secret, and Entry
// plaintext even when a lifecycle fence rejects send admission.
func TestConnectDispatchRejectedAssignmentClearsScriptArtifacts(t *testing.T) {
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), testAgentID, 1)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close()
	if err := registry.StopAssignments(context.Background(), testAgentID, 1); err != nil {
		t.Fatalf("stop assignments: %v", err)
	}

	body := []byte("transient script")
	secret := []byte("transient secret")
	entry := []byte("transient entry")
	artifacts := &agentpb.ScriptAssignmentArtifacts{
		Bodies:  []*agentpb.ScriptBodyArtifact{{Body: body}},
		Secrets: []*agentpb.ScriptSecretArtifact{{Value: secret}},
		Entries: []*agentpb.ScriptEntryArtifact{{Value: entry}},
	}
	assignment := &agentpb.TaskAssignment{ScriptArtifacts: artifacts}
	var stream agentpb.AgentChannel_ConnectServer
	sent, err := (&Server{}).dispatchResolvedTaskAssignment(
		session,
		stream,
		etcd.TaskAssignment{},
		assignment,
		false,
	)
	if err != nil || sent {
		t.Fatalf("rejected dispatch = (%v, %v), want (false, nil)", sent, err)
	}
	for name, value := range map[string][]byte{"body": body, "secret": secret, "entry": entry} {
		if !bytes.Equal(value, make([]byte, len(value))) {
			t.Fatalf("%s plaintext was not cleared: %q", name, value)
		}
	}
	if artifacts.Bodies[0].Body != nil || artifacts.Secrets[0].Value != nil ||
		artifacts.Entries[0].Value != nil {
		t.Fatal("cleared assignment retained plaintext slices")
	}
}

// Rationale: revocation may race expensive plan resolution. The old
// generation must be fenced before the resolved assignment can enter Send.
func TestConnectRevocationRacingPlanResolutionDoesNotSendAssignment(t *testing.T) {
	now := testTime()
	startedAt := now
	taskID := ids.NewAt(ids.KindTask, now, 290)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 291),
		PlanID: ids.NewAt(ids.KindPlan, now, 292), RenderGeneration: 7,
		Type: etcd.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 293),
		Steps:          []etcd.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 294)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 295), TaskID: taskID,
			Executor: etcd.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
			AssignedAt: now, Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
			ExecutionEpoch: 1, ExecutionMode: etcd.TaskExecutionModeForward,
		}},
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
	}
	tasks := &fakeTaskStore{claims: []etcd.TaskAssignment{claim}}
	resolver := &blockingPlanResolver{
		plan: plan, entered: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry()
	stream := newLiveStream(ctx)
	stream.received <- authenticateMessage(testAgentID, testToken(12))
	connectResult := make(chan error, 1)
	go func() {
		connectResult <- New(authorizedAuthenticator(), registry, tasks, resolver).Connect(stream)
	}()
	select {
	case message := <-stream.sent:
		if message.GetConfigUpdate() == nil {
			t.Fatalf("first Controller message = %#v, want ConfigUpdate", message)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller did not send initial config")
	}
	stream.received <- readyMessage(1)
	select {
	case <-resolver.entered:
	case <-time.After(time.Second):
		t.Fatal("plan resolution did not begin")
	}
	if err := registry.Revoke(context.Background(), testAgentID, 1); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	close(resolver.release)
	if err := <-connectResult; err != nil {
		t.Fatalf("Connect() after revocation = %v", err)
	}
	select {
	case message := <-stream.sent:
		t.Fatalf("revoked session delivered assignment: %#v", message)
	default:
	}
}

func TestServerRejectsMissingAssignmentIdentityOnAgentWrites(t *testing.T) {
	t.Parallel()
	server := &Server{}
	if err := server.recordTaskEvent(
		context.Background(), testAgentID, 1,
		&agentpb.TaskEvent{PlanHash: make([]byte, 32), ExecutionEpoch: 1, Ordinal: 1},
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("recordTaskEvent(missing assignment) error = %v, want validation failed", err)
	}
	if err := server.acknowledge(
		context.Background(), testAgentID, 1, &agentpb.TaskAck{PlanHash: make([]byte, 32)},
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("acknowledge(missing assignment) error = %v, want validation failed", err)
	}
}

// Rationale: transport cancellation must always release the registry's online
// state so lifecycle removal can complete its bounded wait.
func TestConnectCancellationMarksSessionOffline(t *testing.T) {
	registry := NewRegistry()
	stream := &scriptedStream{
		messages: []*agentpb.AgentMessage{authenticateMessage(testAgentID, testToken(6))},
		recvErr:  context.Canceled,
	}

	if err := New(authorizedAuthenticator(), registry, nil, nil).Connect(stream); err != nil {
		t.Fatalf("connect cancellation: %v", err)
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || snapshot.Online {
		t.Fatalf("snapshot = %+v, found = %v", snapshot, ok)
	}
}

// Rationale: even a dependency error containing credential material must be
// replaced with a generic boundary error and must not reach a response.
func TestAuthenticationFailureDoesNotLeakToken(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	authenticator := &fakeAuthenticator{err: errors.New("rejected token " + string(secret))}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, append([]byte(nil), secret...)),
	}}

	err := New(authenticator, NewRegistry(), nil, nil).Connect(stream)
	if strings.Contains(err.Error(), string(secret)) {
		t.Fatalf("credential leaked in error: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent %d messages after authentication failure", len(stream.sent))
	}
	var want Token
	copy(want[:], secret)
	if !bytes.Equal(authenticator.seenToken[:], want[:]) {
		t.Fatal("authenticator did not receive the decoded token")
	}
}

func authorizedAuthenticator() *fakeAuthenticator {
	return &fakeAuthenticator{authorization: Authorization{
		Generation: 1,
		Config: &agentpb.AgentConfig{
			PullIntervalSeconds: 5,
			MaxConcurrentTasks:  4,
			Labels:              map[string]string{"role": "local"},
		},
	}}
}

func authenticateMessage(id string, token []byte) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{AgentId: id, Token: token},
	}}
}

func readyMessage(capacity int32) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
		Ready: &agentpb.Ready{Capacity: capacity, Version: "v0.4.2"},
	}}
}

func testToken(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, tokenSize)
}

func testTime() time.Time {
	return time.Date(2026, time.August, 20, 10, 30, 0, 0, time.UTC)
}

func testExecutionPlan(t *testing.T, task etcd.TaskRecord) *agentpb.ExecutionPlan {
	t.Helper()
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" + strings.Repeat("a", 64) + "\n")
	yamlHash := sha256.Sum256(yaml)
	artifactID := ids.NewAt(ids.KindConfig, task.CreatedAt, 25)
	unsealed := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: task.Target, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: task.PlanID},
					{Key: "com.groundplane.render-generation", Value: "7"},
					{Key: "com.groundplane.service-id", Value: task.Target},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{task.Target},
			}},
		}},
	}
	sealed, err := executionplan.Seal(unsealed)
	if err != nil {
		t.Fatalf("seal test execution plan: %v", err)
	}
	return sealed
}

// Rationale: reconnect redispatches an existing durable assignment; it must
// carry the original absolute deadline rather than restart Task timeout from
// the new stream's delivery time.
func TestTaskAssignmentMessageUsesDurableExecutionEpochAcrossSessions(t *testing.T) {
	now := testTime()
	startedAt := now
	task := etcd.TaskRecord{
		ID:               ids.NewAt(ids.KindTask, now, 8201),
		OperationID:      ids.NewAt(ids.KindOperation, now, 8202),
		PlanID:           ids.NewAt(ids.KindPlan, now, 8203),
		RenderGeneration: 7, Type: etcd.TaskDeploy,
		Target:         ids.NewAt(ids.KindService, now, 8204),
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 8205)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	deadline := now.Add(37 * time.Second)
	claim := etcd.TaskAssignment{
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 8206),
			TaskID:       task.ID, Executor: etcd.TaskExecutorAgent,
			AgentID: testAgentID, AgentGeneration: 1,
			AssignedAt: now, Deadline: deadline, ExecutionEpoch: 1,
			ExecutionMode: etcd.TaskExecutionModeForward, RecoveryDeadline: deadline.Add(time.Minute),
		}},
	}
	tasks := &fakeTaskStore{tasks: map[string]etcd.Versioned[etcd.TaskRecord]{task.ID: claim.Task}}
	server := New(
		authorizedAuthenticator(), NewRegistry(), tasks,
		&fakePlanResolver{plan: plan},
	)
	server.now = func() time.Time { return now.Add(29 * time.Second) }
	first, err := server.taskAssignmentMessage(context.Background(), claim, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetExecutionEpoch() != 1 {
		t.Fatalf("first delivery execution epoch = %d, want 1", first.GetExecutionEpoch())
	}
	tasks.durableEvents = []etcd.TaskEventRecord{{Identity: etcd.TaskEventIdentity{
		AssignmentID: claim.Assignment.Record.AssignmentID,
		AgentID:      testAgentID, AgentGeneration: 1,
		TaskID: task.ID, StepID: task.Steps[0].ID, Attempt: 1, Ordinal: 1,
	}}}
	second, err := server.taskAssignmentMessage(context.Background(), claim, true)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := server.taskAssignmentMessage(context.Background(), claim, true)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetExecutionEpoch() != 1 || replayed.GetExecutionEpoch() != 1 ||
		second.ForwardDeadline == nil || !second.ForwardDeadline.AsTime().Equal(deadline) {
		t.Fatalf("recovered assignments = %#v / %#v, want stable epoch 1 and deadline %s", second, replayed, deadline)
	}
	if len(tasks.eventRevisions) != 0 {
		t.Fatalf("assignment reconstruction read event attempts: %v", tasks.eventRevisions)
	}
}

// Rationale: recovered plan and private-input resolution can cross the
// immutable deadline; the final send boundary must still leave timeout
// terminalization to the scheduler without emitting any assignment frame.
func TestConnectDoesNotDispatchRecoveredAssignmentThatExpiresDuringResolution(t *testing.T) {
	now := testTime()
	current := now.Add(-time.Nanosecond)
	resolved := false
	startedAt := now
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 8210), OperationID: ids.NewAt(ids.KindOperation, now, 8211),
		PlanID: ids.NewAt(ids.KindPlan, now, 8212), RenderGeneration: 7, Type: etcd.TaskDeploy,
		Target:         ids.NewAt(ids.KindService, now, 8213),
		Steps:          []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 8214)}},
		TimeoutSeconds: 120, Status: etcd.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 8215), TaskID: task.ID,
			Executor: etcd.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
			AssignedAt: now.Add(-time.Minute), Deadline: now,
			ExecutionEpoch: 1, ExecutionMode: etcd.TaskExecutionModeForward,
			RecoveryDeadline: now.Add(time.Minute),
		}},
	}
	tasks := &fakeTaskStore{
		assignments: []etcd.TaskAssignment{claim},
		tasks:       map[string]etcd.Versioned[etcd.TaskRecord]{task.ID: claim.Task},
	}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(13)), readyMessage(1),
	}}
	server := New(authorizedAuthenticator(), NewRegistry(), tasks, &fakePlanResolver{
		plan: plan,
		onResolve: func() {
			resolved = true
			current = now
		},
	})
	server.now = func() time.Time { return current }
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if len(stream.sent) != 1 || stream.sent[0].GetConfigUpdate() == nil {
		t.Fatalf("Controller messages = %#v, want only ConfigUpdate", stream.sent)
	}
	if !resolved || len(tasks.eventRevisions) != 0 {
		t.Fatalf(
			"deadline-crossing resolution = resolved %t, revisions %v; want resolved true and event-attempt reconstruction absent",
			resolved,
			tasks.eventRevisions,
		)
	}
}
