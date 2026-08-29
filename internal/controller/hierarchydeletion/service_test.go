package hierarchydeletion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fixedIDs struct{ task string }

func (g fixedIDs) NewTaskID() string { return g.task }
func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func finalizer(kind ActionKind, target ActionTargetKind, id string, revision int64, salt string) ControllerFinalizerInput {
	return ControllerFinalizerInput{Finalizer: controllerFinalizers[kind], TargetKind: target, TargetID: id, FixedInputRevision: revision, FixedInputDigest: digest(salt + "/input"), BatchCount: 1}
}
func node(target ActionTargetKind, id string, parentKind ActionTargetKind, parentID string, kind ActionKind, revision int64) MembershipNode {
	input := ProcedureInput{Kind: ProcedureControllerFinalizer, ControllerFinalizer: ptr(finalizer(kind, target, id, revision, id))}
	if typed, ok := agentProcedures[kind]; ok {
		taskType := "remove"
		if kind == ActionAttachDetach {
			taskType = "detach"
		}
		input = ProcedureInput{Kind: ProcedureAgentChild, AgentChild: &AgentChildInput{TaskType: taskType, TypedProcedure: typed, InputDigest: digest(id + "/input"), Timeout: time.Minute}}
	}
	_ = parentKind
	_ = parentID
	return MembershipNode{NodeID: string(kind) + "\x00" + string(target) + "\x00" + id, TargetKind: target, ID: id, ActionKind: kind, TargetRevision: revision, ProcedureInput: input}
}

func ptr[T any](value T) *T { return &value }

type fakeExecutor struct {
	mu    sync.Mutex
	order []string
}

func (e *fakeExecutor) Execute(_ context.Context, _ Operation, action Action) (AgentTerminalProof, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.order = append(e.order, action.ID)
	return AgentTerminalProof{ChildOperationID: action.Procedure.AgentChild.ChildOperationID, AttemptID: "attempt-1", TaskID: "tsk_child", AssignmentID: "assignment-1", AttemptGeneration: 1, ReceiptRevision: 10, ReceiptDigest: digest("receipt"), ProgressKey: "progress/key", ProgressDigest: digest("progress"), TerminalTaskDigest: digest("task"), Terminal: AgentTerminalCompleted, ResultDigest: digest("result"), CheckpointDigest: digest("checkpoint")}, nil
}

type fakeRepository struct {
	mu                 sync.Mutex
	operation          Operation
	byOperation        map[string]Operation
	resolvedTarget     TargetKind
	begin              BeginDeletion
	snapshot           FrozenMembership
	actions            []Action
	completed          map[string]bool
	failCompletionOnce bool
	rootPrepared       bool
}

func newFakeRepository(snapshot FrozenMembership) *fakeRepository {
	return &fakeRepository{snapshot: snapshot, byOperation: map[string]Operation{}, completed: map[string]bool{}}
}

func (r *fakeRepository) ResolveTargetKind(_ context.Context, requested TargetKind, _ string) (TargetKind, error) {
	if r.resolvedTarget != "" {
		return r.resolvedTarget, nil
	}
	return requested, nil
}

func (r *fakeRepository) BeginDeletion(_ context.Context, b BeginDeletion) (BeginResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.resolvedTarget != "" && b.TargetKind != r.resolvedTarget {
		return BeginResult{}, errors.New("wrong deletion authority")
	}
	r.begin = b
	if existing, ok := r.byOperation[b.OperationID]; ok {
		return BeginResult{Operation: existing, Existing: true}, nil
	}
	if r.operation.ID != "" && r.operation.Phase != PhaseRetained {
		return BeginResult{}, errors.New("target already deleting")
	}
	r.operation = Operation{ID: b.OperationID, TaskOperationID: b.TaskOperationIDCandidate, Kind: b.OperationKind, TargetKind: b.TargetKind, TargetID: b.TargetID, TaskID: b.TaskIDCandidate, Phase: PhasePlanning, DeadlineAt: b.DeadlineAt, CreatedAt: b.CreatedAt, UpdatedAt: b.CreatedAt}
	r.byOperation[b.OperationID] = r.operation
	return BeginResult{Operation: r.operation}, nil
}

func TestDeleteUsesBackingAuthorityBehindProjectRoute(t *testing.T) {
	t.Parallel()
	repository := newFakeRepository(FrozenMembership{})
	repository.resolvedTarget = TargetBackingService
	service := NewService(repository, &fakeExecutor{}, fixedIDs{task: "task_01M15540AH211T0MA5QQ4KT50C"}, fixedClock{
		now: time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC),
	})

	accepted, err := service.Delete(context.Background(), DeleteRequest{
		TargetKind: TargetProject, TargetID: "project-1", IdempotencyKey: "backing-delete-key",
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if accepted.TaskID != "task_01M15540AH211T0MA5QQ4KT50C" || repository.begin.OperationKind != OperationBackingDelete ||
		repository.begin.TargetKind != TargetBackingService {
		t.Fatalf("Delete() = %#v, begin = %#v", accepted, repository.begin)
	}
	intent := repository.begin.IdempotencyIntent
	if intent.RouteTemplate != ProjectDeleteRoute || intent.ScopeKind != TargetProject ||
		intent.ScopeID != "project-1" {
		t.Fatalf("Delete() idempotency intent = %#v", intent)
	}
}
func (r *fakeRepository) OperationByTask(_ context.Context, task string) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task != r.operation.TaskID {
		return Operation{}, errors.New("not found")
	}
	return r.operation, nil
}
func (r *fakeRepository) FreezeMembership(_ context.Context, _ Operation) (FrozenMembership, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.operation.SnapshotRevision = r.snapshot.Revision
	r.operation.CoordinationEpoch = r.snapshot.CoordinationEpoch
	r.byOperation[r.operation.ID] = r.operation
	return r.snapshot, nil
}
func (r *fakeRepository) BindPlan(_ context.Context, _ Operation, planned []PlannedAction) ([]Action, error) {
	actions := make([]Action, 0, len(planned))
	for index, item := range planned {
		procedure := Procedure{Kind: item.ProcedureInput.Kind}
		if item.ProcedureInput.Kind == ProcedureAgentChild {
			input := item.ProcedureInput.AgentChild
			procedure.AgentChild = &AgentChildProcedure{ChildOperationID: fmt.Sprintf("op_0%025d", index+1), TaskType: input.TaskType, TypedProcedure: input.TypedProcedure, InputDigest: input.InputDigest, Timeout: input.Timeout}
		} else {
			input := item.ProcedureInput.ControllerFinalizer
			procedure.ControllerFinalizer = &ControllerFinalizerProcedure{Finalizer: input.Finalizer, FixedInputRevision: input.FixedInputRevision, CompareTemplateDigest: digest(item.ID + "/compare-template"), MutationTemplateDigest: digest(item.ID + "/mutation-template"), PostconditionTemplateDigest: digest(item.ID + "/postcondition-template")}
		}
		actions = append(actions, Action{ID: item.ID, NodeID: item.NodeID, Ordinal: item.Ordinal, OperationID: item.OperationID, Kind: item.Kind, TargetKind: item.TargetKind, TargetID: item.TargetID, TargetRevision: item.TargetRevision, PrerequisiteOrdinals: append([]int(nil), item.PrerequisiteOrdinals...), State: ActionPending, Procedure: procedure})
	}
	return actions, nil
}
func (r *fakeRepository) AppendPlan(_ context.Context, _ Operation, actions []Action, start int, sealed bool) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if start != len(r.actions) {
		return Operation{}, errors.New("cursor")
	}
	r.actions = append(r.actions, actions...)
	r.operation.PlanCursor = len(r.actions)
	r.operation.ActionCount = len(r.actions)
	r.operation.PlanSealed = sealed
	if sealed {
		r.operation.Phase = PhaseExecuting
	}
	r.byOperation[r.operation.ID] = r.operation
	return r.operation, nil
}
func (r *fakeRepository) ReadyActions(_ context.Context, _ Operation, limit int) ([]Action, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ready := make([]Action, 0, limit)
	for index := range r.actions {
		action := r.actions[index]
		if action.State != ActionPending {
			continue
		}
		if action.Ordinal != len(r.completed) {
			continue
		}
		action.State = ActionRunning
		action.Attempt++
		r.actions[index] = action
		ready = append(ready, action)
		if len(ready) == limit {
			break
		}
	}
	return ready, nil
}
func (r *fakeRepository) complete(action Action) (Operation, error) {
	if r.failCompletionOnce {
		r.failCompletionOnce = false
		for i := range r.actions {
			if r.actions[i].ID == action.ID {
				r.actions[i].State = ActionPending
			}
		}
		return Operation{}, errors.New("completion crash")
	}
	for i := range r.actions {
		if r.actions[i].ID == action.ID {
			r.actions[i].State = ActionSucceeded
		}
	}
	r.completed[action.ID] = true
	r.operation.SucceededCount = len(r.completed)
	if len(r.completed) == len(r.actions) {
		r.operation.Phase = PhaseSummarizing
	}
	r.byOperation[r.operation.ID] = r.operation
	return r.operation, nil
}
func (r *fakeRepository) ConsumeAgentTerminal(_ context.Context, _ Operation, action Action, proof AgentTerminalProof) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := proof.Validate(action); err != nil {
		return Operation{}, err
	}
	return r.complete(action)
}
func (r *fakeRepository) PrepareRootFinalization(_ context.Context, _ Operation, _ Action) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rootPrepared = true
	r.operation.Phase = PhaseFinalizing
	r.byOperation[r.operation.ID] = r.operation
	return r.operation, nil
}
func (r *fakeRepository) CompleteControllerAction(_ context.Context, _ Operation, action Action) (Operation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.complete(action)
}
func (r *fakeRepository) GetDeletionTaskIDAtRevision(_ context.Context, _ TargetKind, _ string, _ int64) (*string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operation.ID == "" || r.operation.Phase == PhaseRetained {
		return nil, nil
	}
	value := r.operation.TaskID
	return &value, nil
}
func service(snapshot FrozenMembership) (*Service, *fakeRepository, *fakeExecutor) {
	repo := newFakeRepository(snapshot)
	executor := &fakeExecutor{}
	return NewService(repo, executor, fixedIDs{task: "tsk_00000000000000000000000001"}, fixedClock{now: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}), repo, executor
}

func TestBuildPlanUsesStablePostorder(t *testing.T) {
	operation := Operation{ID: "del_test", TargetKind: TargetTenant, TargetID: "tenant-1"}
	entry := node(ActionTargetEntry, "entry-1", "", "", ActionEntryRemove, 38)
	environment := node(ActionTargetEnvironment, "env-1", "", "", ActionEnvironmentFinalize, 39)
	environment.PrerequisiteNodeIDs = []string{entry.NodeID}
	project := node(ActionTargetProject, "project-1", "", "", ActionProjectFinalize, 37)
	project.PrerequisiteNodeIDs = []string{environment.NodeID}
	snapshot := FrozenMembership{Revision: 42, RootRevision: 40, RootProcedureInput: finalizer(ActionTenantFinalize, ActionTargetTenant, "tenant-1", 40, "tenant"), RootPrerequisiteNodeIDs: []string{project.NodeID}, CoordinationEpoch: 3, Nodes: []MembershipNode{project, environment, entry}}
	plan, err := BuildPlan(operation, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"entry-1", "env-1", "project-1", "tenant-1"}
	for index, target := range want {
		if plan.Actions[index].TargetID != target || plan.Actions[index].Ordinal != index {
			t.Fatalf("action %d = %#v", index, plan.Actions[index])
		}
		for _, prerequisite := range plan.Actions[index].PrerequisiteOrdinals {
			if prerequisite >= index {
				t.Fatalf("action %d has non-postorder prerequisite %d", index, prerequisite)
			}
		}
	}
	again, _ := BuildPlan(operation, snapshot)
	for i := range plan.Actions {
		if plan.Actions[i].ID != again.Actions[i].ID {
			t.Fatal("identity changed")
		}
	}
}
func TestBuildPlanRejectsReverseReference(t *testing.T) {
	_, err := BuildPlan(Operation{ID: "del", TargetKind: TargetProject, TargetID: "project-1"}, FrozenMembership{Revision: 1, RootRevision: 1, RootProcedureInput: finalizer(ActionProjectFinalize, ActionTargetProject, "project-1", 1, "project"), CoordinationEpoch: 1, ReverseReferences: []ReverseReference{{SourceKind: ActionTargetRoute, SourceID: "route-1", TargetKind: ActionTargetProject, TargetID: "project-1"}}})
	if err == nil {
		t.Fatal("expected fence")
	}
}

func TestBuildPlanUsesBackingFacadeRoot(t *testing.T) {
	operation := Operation{ID: "del_backing", Kind: OperationBackingDelete, TargetKind: TargetBackingService, TargetID: "bks-1"}
	plan, err := BuildPlan(operation, FrozenMembership{Revision: 8, RootRevision: 8, RootProcedureInput: finalizer(ActionBackingServiceFinalize, ActionTargetBackingService, "bks-1", 8, "backing"), CoordinationEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	root := plan.Actions[len(plan.Actions)-1]
	if root.Kind != ActionBackingServiceFinalize || root.TargetKind != ActionTargetBackingService || root.TargetID != "bks-1" {
		t.Fatalf("wrong backing root: %#v", root)
	}
}

func TestBuildPlanAllowsMultipleActionsForOneTarget(t *testing.T) {
	cleanup := node(ActionTargetEnvironment, "env-1", "", "", ActionEnvironmentAgentCleanup, 6)
	finalize := node(ActionTargetEnvironment, "env-1", "", "", ActionEnvironmentFinalize, 6)
	finalize.PrerequisiteNodeIDs = []string{cleanup.NodeID}
	operation := Operation{ID: "del_project", Kind: OperationProjectDelete, TargetKind: TargetProject, TargetID: "project-1"}
	plan, err := BuildPlan(operation, FrozenMembership{Revision: 8, RootRevision: 8, RootProcedureInput: finalizer(ActionProjectFinalize, ActionTargetProject, "project-1", 8, "project"), RootPrerequisiteNodeIDs: []string{finalize.NodeID}, CoordinationEpoch: 1, Nodes: []MembershipNode{finalize, cleanup}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Actions[0].Kind != ActionEnvironmentAgentCleanup || plan.Actions[1].Kind != ActionEnvironmentFinalize {
		t.Fatalf("wrong repeated-target order: %#v", plan.Actions)
	}
}

func TestEnvironmentPlanRemovesDefaultComponentsBeforeRoot(t *testing.T) {
	caddy := node(ActionTargetComponent, "caddy", "", "", ActionComponentRemove, 12)
	tunnel := node(ActionTargetComponent, "cloudflare-tunnel", "", "", ActionComponentRemove, 12)
	cleanup := node(ActionTargetEnvironment, "env-1", "", "", ActionEnvironmentAgentCleanup, 12)
	cleanup.PrerequisiteNodeIDs = []string{caddy.NodeID, tunnel.NodeID}
	operation := Operation{ID: "del_environment", Kind: OperationEnvironmentDelete, TargetKind: TargetEnvironment, TargetID: "env-1"}
	plan, err := BuildPlan(operation, FrozenMembership{Revision: 12, RootRevision: 12, RootProcedureInput: finalizer(ActionEnvironmentFinalize, ActionTargetEnvironment, "env-1", 12, "environment"), RootPrerequisiteNodeIDs: []string{cleanup.NodeID}, CoordinationEpoch: 1, Nodes: []MembershipNode{cleanup, tunnel, caddy}})
	if err != nil {
		t.Fatal(err)
	}
	root := plan.Actions[len(plan.Actions)-1]
	if root.Kind != ActionEnvironmentFinalize {
		t.Fatalf("wrong root: %#v", root)
	}
	if plan.Actions[0].Kind != ActionComponentRemove || plan.Actions[1].Kind != ActionComponentRemove || plan.Actions[2].Kind != ActionEnvironmentAgentCleanup {
		t.Fatalf("default components not child-first: %#v", plan.Actions)
	}
}
func TestDeleteIdempotencyAndConcurrency(t *testing.T) {
	svc, _, _ := service(FrozenMembership{Revision: 1, RootRevision: 1, RootProcedureInput: finalizer(ActionTenantFinalize, ActionTargetTenant, "tenant-1", 1, "tenant"), CoordinationEpoch: 1})
	request := DeleteRequest{TargetKind: TargetTenant, TargetID: "tenant-1", IdempotencyKey: "key-1"}
	first, err := svc.Delete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Delete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Existing || first.TaskID != second.TaskID {
		t.Fatal("canonical task not returned")
	}
	if _, err = svc.Delete(context.Background(), DeleteRequest{TargetKind: TargetTenant, TargetID: "tenant-1", IdempotencyKey: "key-2"}); err == nil {
		t.Fatal("different key not fenced")
	}
}
func TestExecuteStopsBeforeRootForAtomicTaskAcknowledgement(t *testing.T) {
	svc, repo, executor := service(FrozenMembership{Revision: 2, RootRevision: 2, RootProcedureInput: finalizer(ActionEnvironmentFinalize, ActionTargetEnvironment, "env-1", 2, "environment"), CoordinationEpoch: 1})
	accepted, err := svc.Delete(context.Background(), DeleteRequest{TargetKind: TargetEnvironment, TargetID: "env-1", IdempotencyKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if err = svc.Execute(context.Background(), accepted.TaskID); err != nil {
		t.Fatal(err)
	}
	if len(executor.order) != 0 {
		t.Fatal("controller finalizer escaped repository transaction")
	}
	if !repo.rootPrepared {
		t.Fatal("root was not prepared for Task acknowledgement")
	}
	if len(repo.completed) != 0 {
		t.Fatal("root completion was written before Task acknowledgement")
	}
}
