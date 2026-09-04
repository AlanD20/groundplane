package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type workerComponentActionStub struct {
	evidence *agentpb.DNSResolverObservationEvidence
}

type workerRollbackActionStub struct {
	candidateDigest        []byte
	previousDigest         []byte
	candidateEvidence      *agentpb.DNSResolverObservationEvidence
	rollbackEvidence       *agentpb.DNSResolverObservationEvidence
	events                 *[]string
	publishErr             error
	commitFinalizeErr      error
	rollbackObservationErr error
	rollbackFinalizeErr    error
}

type workerComposeHelper struct {
	request **agentpb.ComposeHelperRequest
	events  *[]string
}

func (helper workerComposeHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	*helper.request = proto.Clone(request).(*agentpb.ComposeHelperRequest)
	apply := request.GetPlan().GetSteps()[0].GetComposeApply()
	*helper.events = append(*helper.events, "compose:"+apply.GetArtifactId())
	return &agentpb.ComposeHelperResponse{
		Schema: composeHelperSchema, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}, nil
}

func (runtime workerRollbackActionStub) record(event string) {
	if runtime.events != nil {
		*runtime.events = append(*runtime.events, event)
	}
}

func (runtime workerComponentActionStub) ExecuteComponentAction(
	context.Context,
	Assignment,
	*agentpb.ExecutionStep,
	ManagedConfigPayload,
) (*ComponentActionResult, error) {
	return &ComponentActionResult{DNSResolverObservation: runtime.evidence}, nil
}

func (workerComponentActionStub) FinalizeManagedConfig(
	context.Context,
	Assignment,
	*agentpb.ExecutionStep,
	bool,
) (ManagedConfigTransactionState, error) {
	return ManagedConfigTransactionState{}, nil
}

func (runtime workerRollbackActionStub) ExecuteComponentAction(
	_ context.Context,
	_ Assignment,
	step *agentpb.ExecutionStep,
	_ ManagedConfigPayload,
) (*ComponentActionResult, error) {
	action := step.GetComponentApply()
	if action.GetManagedConfigContent() {
		runtime.record("managed-config-apply")
		if runtime.publishErr != nil {
			return &ComponentActionResult{}, runtime.publishErr
		}
		return &ComponentActionResult{ManagedConfig: &ManagedConfigTransactionState{
			Live:     managedConfigTestFileState(runtime.candidateDigest),
			Previous: managedConfigTestFileState(runtime.previousDigest),
		}}, nil
	}
	if bytes.Equal(action.GetArtifactDigest(), runtime.candidateDigest) {
		runtime.record("candidate-observation")
		if runtime.candidateEvidence != nil {
			return &ComponentActionResult{DNSResolverObservation: runtime.candidateEvidence}, nil
		}
		return nil, errors.New("candidate resolver is unhealthy")
	}
	if bytes.Equal(action.GetArtifactDigest(), runtime.previousDigest) {
		runtime.record("rollback-observation")
		if runtime.rollbackObservationErr != nil {
			return nil, runtime.rollbackObservationErr
		}
		return &ComponentActionResult{DNSResolverObservation: runtime.rollbackEvidence}, nil
	}
	return nil, errors.New("unexpected resolver observation digest")
}

func (runtime workerRollbackActionStub) FinalizeManagedConfig(
	_ context.Context,
	_ Assignment,
	_ *agentpb.ExecutionStep,
	commit bool,
) (ManagedConfigTransactionState, error) {
	state := ManagedConfigTransactionState{Previous: managedConfigTestFileState(runtime.previousDigest)}
	if commit {
		runtime.record("managed-config-commit")
		state.Live = managedConfigTestFileState(runtime.candidateDigest)
		return state, runtime.commitFinalizeErr
	}
	if !commit {
		runtime.record("managed-config-rollback")
		state.Live = managedConfigTestFileState(runtime.previousDigest)
		return state, runtime.rollbackFinalizeErr
	}
	return state, nil
}

func managedConfigTestFileState(digest []byte) ManagedConfigFileState {
	state := ManagedConfigFileState{Present: len(digest) != 0}
	copy(state.SHA256[:], digest)
	return state
}

// Rationale: Agent-local typed observation evidence must survive the action
// runtime and worker result union without Controller synthesis.
func TestWorkerReturnsDNSResolverObservationFromGenericComponentAction(t *testing.T) {
	evidence := &agentpb.DNSResolverObservationEvidence{ComponentId: "cmp_exact", RenderGeneration: 7}
	pool := NewWorkerPool(4, "/var/lib/groundplane/vol", nil, testLogger())
	pool.SetComponentActionRuntime(workerComponentActionStub{evidence: evidence})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assignment := Assignment{
		AssignmentID:   workerTestAssignmentID,
		TaskID:         workerTestTaskID,
		ExecutionEpoch: 1,
		Plan: &agentpb.ExecutionPlan{
			Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			Steps: []*agentpb.ExecutionStep{{
				StepId: workerTestStepID, TimeoutSeconds: 1,
				Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{}},
			}},
		},
	}
	reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
	pool.reservations[assignment.TaskID] = reservation
	pool.execute(context.Background(), reservation)
	var result *TaskResult
	for range 3 {
		output := <-pool.Outputs()
		if output.Result != nil {
			result = output.Result
		}
	}
	if result == nil || result.Compose == nil {
		t.Fatalf("worker result = %#v", result)
	}
	got := result.Compose.GetDnsResolverCandidateObservation()
	if got.GetComponentId() != evidence.GetComponentId() ||
		got.GetRenderGeneration() != evidence.GetRenderGeneration() {
		t.Fatalf("worker result = %#v", result)
	}
}

// Rationale: once a managed resolver candidate has been applied, failure of
// its serving check must publish a distinct catalog proof for the compensated
// prior digest rather than terminalizing on rollback mechanics alone.
func TestWorkerReturnsDNSResolverRollbackObservationAfterCandidateCompensation(t *testing.T) {
	result, previous := runWorkerResolverRollbackScenario(t, workerRollbackActionStub{})
	rollback := result.Compose.GetDnsResolverRollbackObservation()
	if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
		result.Compose.GetDnsResolverCandidateObservation() != nil ||
		rollback.GetComponentId() != "cmp_exact" ||
		!bytes.Equal(rollback.GetArtifactSha256(), previous[:]) {
		t.Fatalf("worker rollback result = %#v", result)
	}
}

// Rationale: an UPDATE that fails after candidate service activation must
// restore the exact predecessor Compose artifact before proving its serving
// observation; restoring only managed configuration is insufficient.
func TestWorkerRestoresPredecessorComposeArtifactBeforeRollbackObservation(t *testing.T) {
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	previous := sha256.Sum256([]byte("previous Corefile"))
	events := make([]string, 0, 6)
	pool := NewWorkerPool(8, "/var/lib/groundplane/vol", nil, testLogger())
	pool.SetComponentActionRuntime(workerRollbackActionStub{
		candidateDigest: candidate[:], previousDigest: previous[:], events: &events,
		rollbackEvidence: &agentpb.DNSResolverObservationEvidence{
			ComponentId: "cmp_exact", ArtifactId: workerPreviousConfigArtifactID,
			ArtifactSha256: previous[:], RenderGeneration: 6,
		},
	})
	assignment, managed := workerResolverRollbackAssignment(candidate, previous)
	assignment.Plan.Steps = append([]*agentpb.ExecutionStep{{
		StepId: "activate-service", TimeoutSeconds: 1,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: workerCandidateComposeArtifactID, ServiceIds: []string{workerTestServiceID},
			ForceRecreate: true, NoDependencies: true,
		}},
	}}, assignment.Plan.Steps...)
	assignment.Plan.Artifacts = []*agentpb.ComposeArtifact{
		workerComponentComposeArtifact(workerCandidateComposeArtifactID, workerCurrentPlanID, "7", "candidate"),
		workerComponentComposeArtifact(workerPreviousComposeArtifactID, workerPreviousPlanID, "6", "previous"),
	}
	var composeRequest *agentpb.ComposeHelperRequest
	compose, err := NewComposeRuntime(
		workerComposeHelper{request: &composeRequest, events: &events},
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "groundplane-infra"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.compose = compose
	if err := pool.managedConfigs.Register(assignment); err != nil {
		t.Fatal(err)
	}
	workerAcceptManagedConfig(t, pool, assignment, managed, []byte("candidate Corefile"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
	pool.reservations[assignment.TaskID] = reservation
	pool.execute(context.Background(), reservation)
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
		result.Compose.GetReconciliationRequired() {
		t.Fatalf("worker result = %#v", result)
	}
	rollback := result.Compose.GetDnsResolverRollbackObservation()
	if rollback == nil || rollback.GetArtifactId() != workerPreviousConfigArtifactID ||
		rollback.GetRenderGeneration() != 6 || !bytes.Equal(rollback.GetArtifactSha256(), previous[:]) {
		t.Fatalf("worker rollback observation = %#v", rollback)
	}
	rollbackPlan := composeRequest.GetPlan()
	rollbackApply := rollbackPlan.GetSteps()[0].GetComposeApply()
	if len(rollbackPlan.GetPlanHash()) != sha256.Size || len(rollbackPlan.GetArtifacts()) != 1 ||
		rollbackPlan.GetArtifacts()[0].GetArtifactId() != workerPreviousComposeArtifactID ||
		rollbackApply.GetArtifactId() != workerPreviousComposeArtifactID ||
		!rollbackApply.GetForceRecreate() || !rollbackApply.GetNoDependencies() {
		t.Fatalf("sealed Compose compensation request = %#v", composeRequest)
	}
	expectedEvents := []string{
		"compose:" + workerCandidateComposeArtifactID, "managed-config-apply", "candidate-observation",
		"managed-config-rollback", "compose:" + workerPreviousComposeArtifactID, "rollback-observation",
	}
	if len(events) != len(expectedEvents) {
		t.Fatalf("event order = %v, want %v", events, expectedEvents)
	}
	for index, expected := range expectedEvents {
		if events[index] != expected {
			t.Fatalf("event order = %v, want %v", events, expectedEvents)
		}
	}
}

// Rationale: a failure before the managed-config step has not changed the
// serving resolver, so the Agent must not manufacture candidate or rollback
// observation evidence.
func TestWorkerReturnsNoDNSResolverRollbackObservationBeforeMutation(t *testing.T) {
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	previous := sha256.Sum256([]byte("previous Corefile"))
	pool := NewWorkerPool(8, "/var/lib/groundplane/vol", nil, testLogger())
	assignment, _ := workerResolverRollbackAssignment(candidate, previous)
	assignment.Plan.Steps = append([]*agentpb.ExecutionStep{{
		StepId: "before-publish", TimeoutSeconds: 1,
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{}},
	}}, assignment.Plan.Steps...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
	pool.reservations[assignment.TaskID] = reservation
	pool.execute(context.Background(), reservation)
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
		result.Compose.GetFailedStepId() != "before-publish" ||
		result.Compose.GetDnsResolverCandidateObservation() != nil ||
		result.Compose.GetDnsResolverRollbackObservation() != nil {
		t.Fatalf("worker pre-mutation result = %#v", result)
	}
}

// Rationale: a terminal failure is safe to acknowledge as compensated only
// when both rollback mechanics and the catalog observation of the restored
// serving resolver succeed.
func TestWorkerFailsClosedWhenDNSResolverRollbackCannotBeProven(t *testing.T) {
	tests := []struct {
		name    string
		runtime workerRollbackActionStub
	}{
		{name: "rollback finalization", runtime: workerRollbackActionStub{
			rollbackFinalizeErr: errors.New("rollback failed"),
		}},
		{name: "restored serving observation", runtime: workerRollbackActionStub{
			rollbackObservationErr: errors.New("restored resolver is unhealthy"),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, _ := runWorkerResolverRollbackScenario(t, test.runtime)
			if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
				!result.Compose.GetReconciliationRequired() ||
				result.Compose.GetDnsResolverRollbackObservation() != nil {
				t.Fatalf("worker unproven rollback result = %#v", result)
			}
		})
	}
}

func TestWorkerRollsBackAmbiguousManagedConfigPublication(t *testing.T) {
	result, previous := runWorkerResolverRollbackScenario(t, workerRollbackActionStub{
		publishErr: errors.New("managed-config response was lost"),
	})
	rollback := result.Compose.GetDnsResolverRollbackObservation()
	if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
		result.Compose.GetReconciliationRequired() ||
		!bytes.Equal(rollback.GetArtifactSha256(), previous[:]) {
		t.Fatalf("worker ambiguous publication result = %#v", result)
	}
}

func TestWorkerRollsBackManagedConfigCommitFailure(t *testing.T) {
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	result, previous := runWorkerResolverRollbackScenario(t, workerRollbackActionStub{
		candidateEvidence: &agentpb.DNSResolverObservationEvidence{
			ComponentId: "cmp_exact", ArtifactSha256: candidate[:], RenderGeneration: 7,
		},
		commitFinalizeErr: errors.New("managed-config commit response was lost"),
	})
	rollback := result.Compose.GetDnsResolverRollbackObservation()
	if result.Terminal != TaskTerminalFailed || result.Compose == nil ||
		result.Compose.GetReconciliationRequired() ||
		!bytes.Equal(rollback.GetArtifactSha256(), previous[:]) {
		t.Fatalf("worker commit failure result = %#v", result)
	}
}

func runWorkerResolverRollbackScenario(
	t *testing.T,
	actionRuntime workerRollbackActionStub,
) (TaskResult, [sha256.Size]byte) {
	t.Helper()
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	previous := sha256.Sum256([]byte("previous Corefile"))
	pool := NewWorkerPool(8, "/var/lib/groundplane/vol", nil, testLogger())
	actionRuntime.candidateDigest = candidate[:]
	actionRuntime.previousDigest = previous[:]
	actionRuntime.rollbackEvidence = &agentpb.DNSResolverObservationEvidence{
		ComponentId: "cmp_exact", ArtifactSha256: previous[:], RenderGeneration: 7,
	}
	pool.SetComponentActionRuntime(actionRuntime)
	assignment, managed := workerResolverRollbackAssignment(candidate, previous)
	if err := pool.managedConfigs.Register(assignment); err != nil {
		t.Fatal(err)
	}
	workerAcceptManagedConfig(t, pool, assignment, managed, []byte("candidate Corefile"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
	pool.reservations[assignment.TaskID] = reservation
	pool.execute(context.Background(), reservation)
	return nextWorkerResult(t, pool), previous
}

func workerResolverRollbackAssignment(
	candidate [sha256.Size]byte,
	previous [sha256.Size]byte,
) (Assignment, *agentpb.ExecutionStep) {
	managed := &agentpb.ExecutionStep{
		StepId: "publish", TimeoutSeconds: 1,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId: "cmp_exact", DefinitionDigest: bytes.Repeat([]byte{1}, sha256.Size),
			CatalogDigest: bytes.Repeat([]byte{2}, sha256.Size), ActionId: "activate-config",
			ArtifactId: "cfg_exact", ArtifactDigest: candidate[:], Generation: 7,
			ManagedConfigContent: true, ExpectedPreviousArtifactDigest: previous[:],
			ExpectedPreviousArtifactId: workerPreviousConfigArtifactID, ExpectedPreviousGeneration: 6,
		}},
	}
	observation := &agentpb.ExecutionStep{
		StepId: "observe", TimeoutSeconds: 1, PrerequisiteStepId: managed.GetStepId(),
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId: "cmp_exact", DefinitionDigest: bytes.Repeat([]byte{1}, sha256.Size),
			CatalogDigest: bytes.Repeat([]byte{2}, sha256.Size), ActionId: "observe-serving",
			ArtifactId: "cfg_exact", ArtifactDigest: candidate[:], Generation: 7,
		}},
	}
	assignment := Assignment{
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID,
		ExecutionEpoch: 1,
		Plan: &agentpb.ExecutionPlan{
			Schema:                 executionplan.SchemaVersion,
			PlanId:                 workerCurrentPlanID,
			TargetId:               workerComponentID,
			RenderGeneration:       7,
			Operation:              agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
			ComponentLifecycleMode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
			Steps:                  []*agentpb.ExecutionStep{managed, observation},
		},
	}
	return assignment, managed
}

func workerComponentComposeArtifact(
	artifactID string,
	planID string,
	generation string,
	name string,
) *agentpb.ComposeArtifact {
	yaml := []byte("services:\n  resolver:\n    image: example.invalid/" + name + "@sha256:" + strings.Repeat("a", 64) + "\n")
	digest := sha256.Sum256(yaml)
	return &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
		ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: workerTestServiceID, ComposeName: "resolver", ExpectedReplicas: 1,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: planID},
				{Key: "com.groundplane.render-generation", Value: generation},
				{Key: "com.groundplane.service-id", Value: workerTestServiceID},
			},
		}},
	}
}

func workerAcceptManagedConfig(
	t *testing.T,
	pool *WorkerPool,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	content []byte,
) {
	t.Helper()
	digest := sha256.Sum256(content)
	planHash := hashForPlan(assignment.Plan)
	base := &agentpb.ManagedConfigTransfer{
		AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		PlanHash: planHash[:], StepId: step.GetStepId(),
	}
	header := proto.Clone(base).(*agentpb.ManagedConfigTransfer)
	header.Record = &agentpb.ManagedConfigTransfer_Header{Header: &agentpb.ManagedConfigTransferHeader{
		ArtifactId: step.GetComponentApply().GetArtifactId(), MediaType: managedconfig.MediaTypeTextUTF8,
		Length: uint64(len(content)), Sha256: digest[:],
	}}
	chunk := proto.Clone(base).(*agentpb.ManagedConfigTransfer)
	chunk.Record = &agentpb.ManagedConfigTransfer_Chunk{Chunk: &agentpb.ManagedConfigTransferChunk{
		Sequence: 1, Content: content,
	}}
	end := proto.Clone(base).(*agentpb.ManagedConfigTransfer)
	end.Record = &agentpb.ManagedConfigTransfer_End{End: &agentpb.ManagedConfigTransferEnd{ChunkCount: 1}}
	for _, transfer := range []*agentpb.ManagedConfigTransfer{header, chunk, end} {
		if err := pool.managedConfigs.Accept(context.Background(), transfer); err != nil {
			t.Fatal(err)
		}
	}
}

const (
	workerTestAssignmentID           = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestTaskID                 = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerOtherTaskID                = "task_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	workerTestStepID                 = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestServiceID              = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestArtifactID             = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerComponentID                = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerCurrentPlanID              = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerPreviousPlanID             = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	workerCandidateComposeArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	workerPreviousComposeArtifactID  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAY"
	workerPreviousConfigArtifactID   = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// Rationale: every published Task event must have exactly one consumable receipt,
// while replayed or identity-changed acknowledgements must remain conflicts.
func TestWorkerPoolAcceptsReceiptForNonBlockingTaskEventAndRejectsReplay(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	var planHash PlanHash
	planHash[0] = 1
	progress := TaskProgress{
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID, StepID: workerTestStepID,
		PlanHash: planHash, ExecutionEpoch: 1, Ordinal: 1, State: TaskProgressRunning,
	}
	pool.emitProgress(context.Background(), progress)
	output := <-pool.Outputs()
	if output.Progress == nil {
		t.Fatalf("output = %#v, want progress event", output)
	}
	ack := &agentpb.TaskEventAck{
		TaskId: output.Progress.TaskID, AssignmentId: output.Progress.AssignmentID,
		StepId: output.Progress.StepID, PlanHash: output.Progress.PlanHash[:],
		ExecutionEpoch: output.Progress.ExecutionEpoch, Ordinal: output.Progress.Ordinal,
		State: agentpb.TaskState_TASK_STATE_RUNNING,
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatalf("AcceptTaskEventAck(receipt) error = %v", err)
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcceptTaskEventAck(replay) error = %v, want state conflict", err)
	}
	changed := *ack
	changed.Ordinal++
	if err := pool.AcceptTaskEventAck(context.Background(), &changed); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcceptTaskEventAck(changed identity) error = %v, want state conflict", err)
	}
}

// Rationale: a canceled publication can finish cleanup after its acknowledgement;
// unique registration ownership must keep a replacement receipt for the same key.
func TestWorkerPoolReceiptCleanupPreservesReplacementRegistration(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	var planHash PlanHash
	planHash[0] = 1
	progress := TaskProgress{
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID, StepID: workerTestStepID,
		PlanHash: planHash, ExecutionEpoch: 1, Ordinal: 1, State: TaskProgressRunning,
	}
	key := taskEventAckKeyForProgress(progress)
	first, ok := pool.registerTaskEventAck(key, nil)
	if !ok {
		t.Fatal("registerTaskEventAck(first) rejected a new event")
	}
	ack := &agentpb.TaskEventAck{
		TaskId: progress.TaskID, AssignmentId: progress.AssignmentID, StepId: progress.StepID,
		PlanHash: progress.PlanHash[:], ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal,
		State: agentpb.TaskState_TASK_STATE_RUNNING,
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatalf("AcceptTaskEventAck(first) error = %v", err)
	}
	if _, ok := pool.registerTaskEventAck(key, nil); !ok {
		t.Fatal("registerTaskEventAck(replacement) rejected an available event key")
	}
	pool.removeTaskEventAck(key, first)
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatalf("AcceptTaskEventAck(replacement) error = %v", err)
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcceptTaskEventAck(replacement replay) error = %v, want state conflict", err)
	}
}

// Rationale: replaying one live assignment must neither execute twice nor
// replace the cancellation authority reserved by the first delivery.
func TestWorkerPoolDeduplicatesMatchingLiveAssignmentAndRejectsHashMismatch(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(2, "/var/lib/groundplane/vol", nil, testLogger())
	started := make(chan struct{}, 2)
	pool.executeStep = func(ctx context.Context, _ *agentpb.ExecutionStep) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(replay) error = %v", err)
	}
	mismatch := workerAssignment(workerTestTaskID, "plan-b")
	if err := pool.Submit(ctx, mismatch); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Submit(mismatch) error = %v, want state conflict", err)
	}
	stale := assignment
	stale.AssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := pool.Submit(ctx, stale); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Submit(stale assignment) error = %v, want state conflict", err)
	}
	if capacity := pool.Capacity(); capacity != 1 {
		t.Fatalf("Capacity() = %d, want 1 reservation remaining", capacity)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first assignment did not start")
	}
	select {
	case <-started:
		t.Fatal("matching replay executed concurrently")
	default:
	}
	if err := pool.Abort(
		ctx, workerTestTaskID, "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Abort(stale assignment) error = %v, want state conflict", err)
	}
	if err := pool.Abort(ctx, workerTestTaskID, assignment.AssignmentID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	result := nextWorkerResult(t, pool)
	if result.TaskID != workerTestTaskID || result.PlanHash != hashForPlan(assignment.Plan) ||
		result.Terminal != TaskTerminalAborted {
		t.Fatalf("result = %#v", result)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not join workers")
	}
}

// Rationale: TaskAbort can overtake worker dequeue, so cancellation ownership
// must exist while queued and must suppress every step side effect.
func TestWorkerPoolRetainsQueuedAbortAndReservationCapacity(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	executed := make(chan struct{}, 1)
	pool.executeStep = func(context.Context, *agentpb.ExecutionStep) error {
		executed <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if capacity := pool.Capacity(); capacity != 0 {
		t.Fatalf("Capacity() = %d, want queued reservation to consume the slot", capacity)
	}
	if err := pool.Abort(ctx, workerTestTaskID, assignment.AssignmentID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalAborted {
		t.Fatalf("terminal = %v, want aborted", result.Terminal)
	}
	select {
	case <-executed:
		t.Fatal("queued aborted assignment executed a step")
	default:
	}
	cancel()
	<-done
}

// Rationale: the gRPC receive goroutine is the only task/control reader; a
// full pool must reject promptly instead of blocking TaskAbort or Shutdown.
func TestWorkerPoolSubmitReturnsConflictWithoutBlockingWhenFull(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	ctx := context.Background()
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	returned := make(chan error, 1)
	go func() { returned <- pool.Submit(ctx, workerAssignment(workerOtherTaskID, "plan-b")) }()
	select {
	case err := <-returned:
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Submit(full) error = %v, want state conflict", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Submit(full) blocked the control loop")
	}
}

func TestWorkerPoolRejectsMissingAssignmentIdentity(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	assignment.AssignmentID = ""
	if err := pool.Submit(context.Background(), assignment); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Submit(missing assignment) error = %v, want internal", err)
	}
}

// Rationale: Run returning is the shutdown join boundary; cancellation must
// reach active task contexts and wait for their executor cleanup.
func TestWorkerPoolCancellationJoinsActiveWorkers(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	started := make(chan struct{})
	exited := make(chan struct{})
	pool.executeStep = func(ctx context.Context, _ *agentpb.ExecutionStep) error {
		close(started)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
		select {
		case <-exited:
		default:
			t.Fatal("Run() returned before executor cleanup")
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not join canceled worker")
	}
}

// Rationale: the validated assignment remains immutable across all task steps, then its transient
// adapter credential must be destroyed at the WorkerPool reservation ownership boundary.
func TestWorkerPoolCompletionClearsAdapterPlanSecret(t *testing.T) {
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	password := []byte("URL_safe-1")
	plan := &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{{
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: &agentpb.AdapterProcedure{
			Password: password,
		}},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	reservation := &taskReservation{
		assignment: Assignment{TaskID: workerTestTaskID, Plan: plan},
		ctx:        ctx, cancel: cancel,
	}
	pool.reservations[workerTestTaskID] = reservation
	pool.complete(context.Background(), reservation, TaskResult{TaskID: workerTestTaskID})
	if plan.Steps[0].GetAdapterProcedure().Password != nil {
		t.Fatal("WorkerPool retained an adapter password after reservation completion")
	}
	for _, character := range password {
		if character != 0 {
			t.Fatal("WorkerPool did not clear the owned adapter password bytes")
		}
	}
}

func workerAssignment(taskID, plan string) Assignment {
	planID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if plan == "plan-b" {
		planID = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	}
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlHash := sha256.Sum256(yaml)
	unsealed := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		TargetId:  workerTestServiceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:  workerTestArtifactID,
			OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: workerTestServiceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: workerTestServiceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: workerTestArtifactID, ServiceIds: []string{workerTestServiceID},
			}},
		}},
	}
	sealed, err := executionplan.Seal(unsealed)
	if err != nil {
		panic(err)
	}
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       taskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: sealed, ExecutionEpoch: 1,
		ExecutionMode:   agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: time.Now().Add(time.Minute), RecoveryDeadline: time.Now().Add(2 * time.Minute),
	}
}

func nextWorkerResult(t *testing.T, pool *WorkerPool) TaskResult {
	t.Helper()
	for {
		select {
		case output := <-pool.Outputs():
			if output.Result != nil {
				return *output.Result
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for worker result")
		}
	}
}

func TestBlueprintCandidatePlanUsesReleaseExecutor(t *testing.T) {
	plan := &agentpb.ExecutionPlan{
		Operation:                 agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{}}},
	}
	if !isReleaseExecution(plan) {
		t.Fatal("Blueprint candidate plan was routed to the generic executor")
	}
	plan.CandidateReleaseProcedure = nil
	if isReleaseExecution(plan) {
		t.Fatal("ordinary Blueprint plan was routed to the release executor")
	}
}
