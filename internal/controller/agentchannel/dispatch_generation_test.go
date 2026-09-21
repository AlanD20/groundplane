package agentchannel

import (
	bytes "bytes"
	context "context"
	hex "encoding/hex"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	etcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	testing "testing"
	time "time"
)

// Rationale: rotation can cancel a stream after its Ready pull commits a real
// durable claim; once the prior-generation fence is active, that old stream
// must not receive the assignment returned by the racing repository call.
func TestConnectDoesNotDeliverAssignmentClaimedDuringPriorGenerationFence(t *testing.T) {
	now := testTime()
	taskID := ids.NewAt(ids.KindTask, now, 120)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 121),
		IdempotencyKey: "channel-fence-0001",
		Owner:          testtaskjournal.PlatformTaskOwner(), Actor: testtaskjournal.TaskActorOperator,
		Executor: testtaskjournal.TaskExecutorAgent,
		PlanID:   ids.NewAt(ids.KindPlan, now, 122), RenderGeneration: 7,
		Type: testtaskjournal.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 123),
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 124)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending,
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
		RenderGeneration: 7, Type: testtaskjournal.TaskDeploy,
		Target: ids.NewAt(ids.KindService, now, 23),
		Params: map[string]string{"strategy": "blue-green"},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 24)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	planHash := append([]byte(nil), plan.PlanHash...)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 26)
	deadline := now.Add(37 * time.Second)
	versioned := testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5}
	tasks := &fakeTaskStore{
		claims: []etcd.TaskAssignment{{
			Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
				Record: testtaskassignments.TaskAssignmentRecord{
					AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
					AgentID: testAgentID, AgentGeneration: 1,
					AssignedAt: now, Deadline: deadline, ExecutionEpoch: 1,
					ExecutionMode: testtaskassignments.TaskExecutionModeForward, RecoveryDeadline: deadline.Add(time.Minute),
				},
			},
			Task: versioned,
		}},
		tasks: map[string]testkeyvalue.Versioned[etcd.TaskRecord]{taskID: versioned},
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
		tasks.ackTerminal != testtaskjournal.TaskStatusCompleted {
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
		tasks.events[0].State != testtaskjournal.TaskEventStateCompleted ||
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
		RenderGeneration: 7, Type: testtaskjournal.TaskDeploy,
		Target:         ids.NewAt(ids.KindService, now, 273),
		Steps:          []testtaskjournal.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 274)}},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	assignmentID := ids.NewAt(ids.KindAssignment, now, 275)
	claim := etcd.TaskAssignment{
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
				AgentID: testAgentID, AgentGeneration: 1, AssignedAt: now,
				Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
				ExecutionEpoch: 1, ExecutionMode: testtaskassignments.TaskExecutionModeForward,
			},
		},
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
	}
	authenticator := authorizedAuthenticator()
	authenticator.authorization.Config.PullIntervalSeconds = 60
	registry := NewRegistry()
	tasks := &wakeTaskStore{fakeTaskStore: fakeTaskStore{
		tasks: map[string]testkeyvalue.Versioned[etcd.TaskRecord]{taskID: claim.Task},
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
		Type: testtaskjournal.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 283),
		Steps:          []testtaskjournal.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 284)}},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: ids.NewAt(ids.KindAssignment, now, 285), TaskID: taskID,
				Executor: testtaskjournal.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
				AssignedAt: now, Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
				ExecutionEpoch: 1, ExecutionMode: testtaskassignments.TaskExecutionModeForward,
			},
		},
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
	}
	authenticator := authorizedAuthenticator()
	authenticator.authorization.Config.MaxConcurrentTasks = 2
	registry := NewRegistry()
	tasks := &wakeTaskStore{fakeTaskStore: fakeTaskStore{
		tasks: map[string]testkeyvalue.Versioned[etcd.TaskRecord]{taskID: claim.Task},
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
		Type: testtaskjournal.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 293),
		Steps:          []testtaskjournal.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 294)}},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: ids.NewAt(ids.KindAssignment, now, 295), TaskID: taskID,
				Executor: testtaskjournal.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
				AssignedAt: now, Deadline: now.Add(time.Minute), RecoveryDeadline: now.Add(2 * time.Minute),
				ExecutionEpoch: 1, ExecutionMode: testtaskassignments.TaskExecutionModeForward,
			},
		},
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
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
		RenderGeneration: 7, Type: testtaskjournal.TaskDeploy,
		Target: ids.NewAt(ids.KindService, now, 8204),
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 8205)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	deadline := now.Add(37 * time.Second)
	claim := etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: ids.NewAt(ids.KindAssignment, now, 8206),
				TaskID:       task.ID, Executor: testtaskjournal.TaskExecutorAgent,
				AgentID: testAgentID, AgentGeneration: 1,
				AssignedAt: now, Deadline: deadline, ExecutionEpoch: 1,
				ExecutionMode: testtaskassignments.TaskExecutionModeForward, RecoveryDeadline: deadline.Add(time.Minute),
			},
		},
	}
	tasks := &fakeTaskStore{tasks: map[string]testkeyvalue.Versioned[etcd.TaskRecord]{task.ID: claim.Task}}
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
	tasks.durableEvents = []testtaskjournal.TaskEventRecord{{Identity: testtaskjournal.TaskEventIdentity{
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
		PlanID: ids.NewAt(ids.KindPlan, now, 8212), RenderGeneration: 7, Type: testtaskjournal.TaskDeploy,
		Target: ids.NewAt(ids.KindService, now, 8213),
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 8214)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusRunning,
		NextEventSequence: 1, CreatedAt: now, StartedAt: &startedAt,
	}
	plan := testExecutionPlan(t, task)
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	claim := etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task, Revision: 5, ReadRevision: 5},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: ids.NewAt(ids.KindAssignment, now, 8215), TaskID: task.ID,
				Executor: testtaskjournal.TaskExecutorAgent, AgentID: testAgentID, AgentGeneration: 1,
				AssignedAt: now.Add(-time.Minute), Deadline: now,
				ExecutionEpoch: 1, ExecutionMode: testtaskassignments.TaskExecutionModeForward,
				RecoveryDeadline: now.Add(time.Minute),
			},
		},
	}
	tasks := &fakeTaskStore{
		assignments: []etcd.TaskAssignment{claim},
		tasks:       map[string]testkeyvalue.Versioned[etcd.TaskRecord]{task.ID: claim.Task},
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
