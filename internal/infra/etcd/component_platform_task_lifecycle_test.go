package etcd

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func testPlatformComponentComposeArtifact(artifactID string, serviceID string, configDigest string) *agentpb.ComposeArtifact {
	return &agentpb.ComposeArtifact{
		ArtifactId:  artifactID,
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
		ProjectName: "groundplane-infra",
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ImageConfigDigest: mustDecodeTestDigest(configDigest),
		}},
	}
}

func mustDecodeTestDigest(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(err)
	}
	return decoded
}

func TestPlatformComponentAcknowledgementIgnoresNilControllerResult(t *testing.T) {
	t.Parallel()
	repository := &TaskRepository{}
	change, err := repository.preparePlatformComponentTaskAcknowledgement(
		context.Background(),
		TaskRecord{Executor: TaskExecutorController},
		TaskStatusCompleted,
		nil,
		1,
	)
	if err != nil || change.applies {
		t.Fatalf("preparePlatformComponentTaskAcknowledgement() = %#v, %v", change, err)
	}
}

// Rationale: promotion requires Agent-provided exact-generation proof rather
// than a Controller-synthesized generic Compose project summary.
func TestPlatformComponentObservationBindsPromotionToExactAgentProof(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0).UTC()
	input := PlatformComponentTaskRenderInput{
		ComponentID:        ids.NewAt(ids.KindComponent, now, 1),
		GeneratedServiceID: ids.NewAt(ids.KindService, now, 2),
		ArtifactID:         ids.NewAt(ids.KindConfig, now, 3), ArtifactSHA256: strings.Repeat("1", 64),
		ImageConfigDigest: strings.Repeat("9", 64),
	}
	valid := platformDNSProof(input, 7, now)
	if err := validatePlatformComponentObservation(input, 7, valid); err != nil {
		t.Fatalf("validatePlatformComponentObservation() error = %v", err)
	}
	missing := valid
	missing.DNSResolverCandidateObservation = nil
	if err := validatePlatformComponentObservation(input, 7, missing); err == nil {
		t.Fatal("validatePlatformComponentObservation() accepted missing evidence")
	}
	wrongGeneration := valid
	wrongEvidence := *valid.DNSResolverCandidateObservation
	wrongEvidence.RenderGeneration++
	wrongGeneration.DNSResolverCandidateObservation = &wrongEvidence
	if err := validatePlatformComponentObservation(input, 7, wrongGeneration); err == nil {
		t.Fatal("validatePlatformComponentObservation() accepted another render generation")
	}
	wrongConfig := valid
	wrongConfigEvidence := *valid.DNSResolverCandidateObservation
	wrongConfigEvidence.ImageConfigDigest = strings.Repeat("8", 64)
	wrongConfig.DNSResolverCandidateObservation = &wrongConfigEvidence
	if err := validatePlatformComponentObservation(input, 7, wrongConfig); err == nil {
		t.Fatal("validatePlatformComponentObservation() accepted another image config digest")
	}
}

func TestPlatformComponentAcknowledgementAtomicallyPublishesObservation(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	now := time.Date(2026, time.August, 30, 3, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	records[0], err = SetComponentRuntime(records[0], []string{serviceID}, "", true)
	if err != nil {
		t.Fatalf("SetComponentRuntime() error = %v", err)
	}
	current, err := components.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatalf("CreatePlatformComponent() error = %v", err)
	}
	desiredSHA256, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	planID := ids.NewAt(ids.KindPlan, now, 2)
	taskID := ids.NewAt(ids.KindTask, now, 3)
	stepID := ids.NewAt(ids.KindStep, now, 4)
	input := PlatformComponentTaskRenderInput{
		PlanID: planID, TaskID: taskID, ComponentID: current.Record.Desired.ID,
		DesiredSHA256: desiredSHA256, BaselineGeneration: 1, BaselineSHA256: strings.Repeat("1", 64),
		HostResolutionInputRevision: 1, HostResolutionSHA256: strings.Repeat("2", 64),
		Config: *current.Record.Desired.Config.CoreDNS, GeneratedServiceID: serviceID,
		DefinitionSHA256: strings.Repeat("3", 64), CatalogSHA256: strings.Repeat("4", 64),
		ActionID: "activate-config", ArtifactID: ids.NewAt(ids.KindConfig, now, 5),
		ComposeArtifactID: ids.NewAt(ids.KindConfig, now, 6), OwnershipPlanID: planID, OwnershipGeneration: 1,
		ImageRepository: "coredns/coredns", ImageIndexDigest: strings.Repeat("7", 64),
		ImageConfigDigest: strings.Repeat("9", 64),
		ImageChildDigest:  strings.Repeat("8", 64), ImageReference: "coredns/coredns@sha256:" + strings.Repeat("8", 64),
		ImageOS: "linux", ImageArchitecture: "amd64",
		ArtifactSHA256: strings.Repeat("5", 64), ArtifactLength: 1, PlanSHA256: strings.Repeat("6", 64),
		ExecutionPlanSHA256: strings.Repeat("7", 64),
	}
	input.ComposeArtifact = testPlatformComponentComposeArtifact(
		input.ComposeArtifactID, input.GeneratedServiceID, input.ImageConfigDigest,
	)
	for name, digest := range map[string]string{"missing": "", "zero": strings.Repeat("0", 64)} {
		invalid := input
		invalid.ImageConfigDigest = digest
		if _, err := encodePlatformComponentTaskRenderInput(invalid); err == nil {
			t.Fatalf("encodePlatformComponentTaskRenderInput() accepted %s image config identity", name)
		}
	}
	renderValue, err := encodePlatformComponentTaskRenderInput(input)
	if err != nil {
		t.Fatalf("encodePlatformComponentTaskRenderInput() error = %v", err)
	}
	if result, transactErr := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: platformComponentTaskRenderInputKey(planID), Value: renderValue},
		{Type: MutationPut, Key: platformComponentTaskActiveKey(input.ComponentID), Value: []byte(taskID)},
	}); transactErr != nil || !result.Succeeded {
		t.Fatalf("seed render input transaction = %#v, %v", result, transactErr)
	}
	current, err = components.GetComponent(ctx, current.Record.Desired.ID)
	if err != nil {
		t.Fatalf("GetComponent() error = %v", err)
	}
	finishedAt := now.Add(time.Minute)
	task := TaskRecord{
		ID: taskID, Actor: TaskActorOperator, Executor: TaskExecutorAgent, PlanID: planID,
		PlanHash: input.ExecutionPlanSHA256, RenderGeneration: 1, Type: TaskUpdate, Target: input.ComponentID,
		Params: map[string]string{
			TaskResourceKindParam:                   TaskResourceComponent,
			TaskPlatformComponentDesiredSHA256Param: desiredSHA256,
		},
		Steps:      []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 7)}, {Kind: TaskStepOperation, ID: stepID}},
		FinishedAt: &finishedAt,
		TerminalAssignment: &TaskTerminalAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 8),
			AgentID:      ids.NewAt(ids.KindAgent, now, 9), AgentGeneration: 2,
		},
	}
	result := platformDNSProof(input, 1, finishedAt)
	change, err := tasks.preparePlatformComponentTaskAcknowledgement(
		ctx, task, TaskStatusCompleted, &result, current.ReadRevision,
	)
	if err != nil {
		t.Fatalf("preparePlatformComponentTaskAcknowledgement() error = %v", err)
	}
	if len(change.mutations) < 2 || change.mutations[0].Key != componentObservationKey(input.ComponentID) {
		t.Fatalf("platform Component acknowledgement mutations = %#v", change.mutations)
	}
	acknowledgementMutations := append([]Mutation(nil), change.mutations...)
	acknowledgementMutations = append(acknowledgementMutations, Mutation{
		Type: MutationDelete, Key: platformComponentTaskActiveKey(input.ComponentID),
	})
	if transaction, transactErr := store.Transact(
		ctx,
		change.conditions,
		acknowledgementMutations,
	); transactErr != nil ||
		!transaction.Succeeded {
		t.Fatalf("acknowledgement transaction = %#v, %v", transaction, transactErr)
	}
	clearPlatformComponentTaskChange(change)
	observed, found, err := components.GetPlatformComponentObservation(ctx, input.ComponentID)
	if err != nil || !found {
		t.Fatalf("GetPlatformComponentObservation() = %#v, %v, %v", observed, found, err)
	}
	if observed.Record.TaskID != task.ID || observed.Record.StepID != stepID ||
		observed.Record.AgentID != task.TerminalAssignment.AgentID || observed.Record.PlanID != planID ||
		observed.Record.ComposeArtifactID != input.ComposeArtifactID || observed.Record.InputSHA256 != input.HostResolutionSHA256 ||
		observed.Record.CorefileSHA256 != input.ArtifactSHA256 || observed.Record.Revision != 1 ||
		observed.Record.PredecessorTaskID != "" {
		t.Fatalf("Component observation lineage = %#v", observed.Record)
	}
	task.Status = TaskStatusCompleted
	task.Result = cloneTaskResult(&result)
	if err := tasks.validatePlatformComponentTaskAcknowledgementReplay(ctx, task, observed.ReadRevision); err != nil {
		t.Fatalf("validatePlatformComponentTaskAcknowledgementReplay() error = %v", err)
	}
	corrupt := observed.Record
	corrupt.TaskID = ids.NewAt(ids.KindTask, now, 10)
	corruptValue, err := encodeComponentObservation(corrupt)
	if err != nil {
		t.Fatalf("encode corrupt Component observation: %v", err)
	}
	corruption, err := store.Transact(ctx, []Condition{{
		Key: componentObservationKey(input.ComponentID), ModRevision: observed.Revision,
	}}, []Mutation{{Type: MutationPut, Key: componentObservationKey(input.ComponentID), Value: corruptValue}})
	if err != nil || !corruption.Succeeded {
		t.Fatalf("corrupt observation transaction = %#v, %v", corruption, err)
	}
	if err := tasks.validatePlatformComponentTaskAcknowledgementReplay(ctx, task, corruption.Revision); err != nil {
		t.Fatalf("immutable Task replay depended on replaced latest observation: %v", err)
	}
}

func TestPlatformComponentDisableAcknowledgementReplayAcceptsEmptyDigests(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	planID := ids.NewAt(ids.KindPlan, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	stepID := ids.NewAt(ids.KindStep, now, 3)
	serviceID := ids.NewAt(ids.KindService, now, 4)
	componentID := records[0].Desired.ID
	agentID := ids.NewAt(ids.KindAgent, now, 5)
	input := PlatformComponentTaskRenderInput{
		PlanID: planID, TaskID: taskID, ComponentID: componentID,
		DesiredSHA256: strings.Repeat("1", 64), BaselineGeneration: 1,
		BaselineSHA256: strings.Repeat("2", 64), HostResolutionInputRevision: 1,
		HostResolutionSHA256: strings.Repeat("3", 64), Config: *records[0].Desired.Config.CoreDNS,
		GeneratedServiceID: serviceID, DefinitionSHA256: strings.Repeat("4", 64),
		CatalogSHA256: strings.Repeat("5", 64), ActionID: "activate-config",
		ArtifactID: ids.NewAt(ids.KindConfig, now, 6), ComposeArtifactID: ids.NewAt(ids.KindConfig, now, 7),
		OwnershipPlanID: planID, OwnershipGeneration: 1, ArtifactSHA256: strings.Repeat("6", 64),
		ArtifactLength: 1, DisableService: true, PlanSHA256: strings.Repeat("7", 64),
		ExecutionPlanSHA256: strings.Repeat("a", 64),
		ImageRepository:     "coredns/coredns", ImageIndexDigest: strings.Repeat("8", 64),
		ImageConfigDigest: strings.Repeat("b", 64),
		ImageChildDigest:  strings.Repeat("9", 64), ImageReference: "coredns/coredns@sha256:" + strings.Repeat("9", 64),
		ImageOS: "linux", ImageArchitecture: "amd64",
	}
	renderValue, err := encodePlatformComponentTaskRenderInput(input)
	if err != nil {
		t.Fatalf("encodePlatformComponentTaskRenderInput() error = %v", err)
	}
	task := TaskRecord{
		ID: taskID, Actor: TaskActorOperator, Executor: TaskExecutorAgent,
		Status: TaskStatusCompleted, PlanID: planID, PlanHash: input.ExecutionPlanSHA256,
		RenderGeneration: 1, Type: TaskUpdate, Target: componentID,
		Params: map[string]string{
			TaskResourceKindParam:                   TaskResourceComponent,
			TaskPlatformComponentDesiredSHA256Param: input.DesiredSHA256,
		},
		Steps: []TaskStepRecord{{Kind: TaskStepOperation, ID: stepID}},
		TerminalAssignment: &TaskTerminalAssignmentRecord{
			AssignmentID: ids.NewAt(ids.KindAssignment, now, 8),
			AgentID:      agentID, AgentGeneration: 1,
		},
	}
	task.Result = &TaskResultRecord{Kind: TaskResultCompose}
	observation := ComponentObservationRecord{
		ComponentID: componentID, ServiceID: serviceID, PlanID: planID,
		ComposeArtifactID: input.ComposeArtifactID, Enabled: false, Healthy: false,
		DesiredGeneration: 1, RenderGeneration: 1, AgentID: agentID, AgentGeneration: 1,
		BaselineGeneration: 1, OwnershipGeneration: 1, ObservedAt: now,
		TaskID: taskID, StepID: stepID, Revision: 1,
	}
	observationValue, err := encodeComponentObservation(observation)
	if err != nil {
		t.Fatalf("encodeComponentObservation() error = %v", err)
	}
	seed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: platformComponentTaskRenderInputKey(planID), Value: renderValue},
		{Type: MutationPut, Key: componentObservationKey(componentID), Value: observationValue},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed disabled replay state = %#v, %v", seed, err)
	}
	if err := tasks.validatePlatformComponentTaskAcknowledgementReplay(ctx, task, seed.Revision); err != nil {
		t.Fatalf("validatePlatformComponentTaskAcknowledgementReplay() error = %v", err)
	}

	corrupt := observation
	corrupt.InputSHA256 = strings.Repeat("8", 64)
	corruptValue, err := encodeEnvelope("component_observation", corrupt)
	if err != nil {
		t.Fatalf("encode corrupt disabled observation: %v", err)
	}
	corruption, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: componentObservationKey(componentID), Value: corruptValue,
	}})
	if err != nil || !corruption.Succeeded {
		t.Fatalf("corrupt disabled observation transaction = %#v, %v", corruption, err)
	}
	if err := tasks.validatePlatformComponentTaskAcknowledgementReplay(ctx, task, corruption.Revision); err != nil {
		t.Fatalf("disabled immutable Task replay depended on mutable observation: %v", err)
	}
}

func platformDNSProof(
	input PlatformComponentTaskRenderInput,
	renderGeneration uint64,
	observedAt time.Time,
) TaskResultRecord {
	return TaskResultRecord{
		Kind: TaskResultCompose,
		DNSResolverCandidateObservation: testDurableDNSProof(
			input.ComponentID, input.GeneratedServiceID, input.ArtifactID,
			input.ArtifactSHA256, renderGeneration, observedAt, 1,
		),
	}
}
