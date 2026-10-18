package hierarchydeletion

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-61: cleanup precedes metadata retirement and ancestors. Sorting input
// membership differently must not change accepted action/dependency identity.
func TestNativeHierarchyPlanPreservesChildFirstDependencies(t *testing.T) {
	entry := planningNode(ActionTargetEntry, "entry", ActionEntryRemove)
	component := planningNode(ActionTargetComponent, "router", ActionComponentRemove)
	cleanup := planningNode(ActionTargetEnvironment, "environment", ActionEnvironmentAgentCleanup)
	cleanup.PrerequisiteNodeIDs = []string{entry.NodeID, component.NodeID}
	environment := planningNode(ActionTargetEnvironment, "environment", ActionEnvironmentFinalize)
	environment.PrerequisiteNodeIDs = []string{cleanup.NodeID}
	project := planningNode(ActionTargetProject, "project", ActionProjectFinalize)
	project.PrerequisiteNodeIDs = []string{environment.NodeID}
	operation := Operation{ID: "del_test", Kind: OperationTenantDelete, TargetKind: TargetTenant, TargetID: "tenant"}
	snapshot := FrozenMembership{Revision: 1, RootRevision: 1, CoordinationEpoch: 1,
		RootProcedureInput: finalizer(ActionTenantFinalize, ActionTargetTenant, "tenant", 1, "tenant"),
		RootPrerequisiteNodeIDs: []string{
			project.NodeID,
		}, Nodes: []MembershipNode{project, environment, cleanup, component, entry}}
	plan, err := BuildPlan(operation, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	positions := make(map[string]int)
	for index, action := range plan.Actions {
		positions[action.NodeID] = index
		for _, prerequisite := range action.PrerequisiteOrdinals {
			if prerequisite >= index {
				t.Fatal("cleanup dependency can run after its parent finalizer")
			}
		}
	}
	if positions[component.NodeID] >= positions[cleanup.NodeID] ||
		positions[entry.NodeID] >= positions[cleanup.NodeID] ||
		positions[cleanup.NodeID] >= positions[environment.NodeID] ||
		positions[environment.NodeID] >= positions[project.NodeID] ||
		plan.Actions[len(plan.Actions)-1].Kind != ActionTenantFinalize {
		t.Fatal("removal graph lost child-first resource retirement")
	}
	for left, right := 0, len(snapshot.Nodes)-1; left < right; left, right = left+1, right-1 {
		snapshot.Nodes[left], snapshot.Nodes[right] = snapshot.Nodes[right], snapshot.Nodes[left]
	}
	again, err := BuildPlan(operation, snapshot)
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatalf("membership iteration order changed removal identity: %v", err)
	}
}

func TestNativeHierarchyPlanRejectsExternalReferencesAndBrokenDependencies(t *testing.T) {
	for _, fault := range []string{"external reference", "cycle", "missing dependency", "duplicate node"} {
		t.Run(fault, func(t *testing.T) {
			node := planningNode(ActionTargetEnvironment, "environment", ActionEnvironmentAgentCleanup)
			snapshot := FrozenMembership{Revision: 1, RootRevision: 1, CoordinationEpoch: 1,
				RootProcedureInput:      finalizer(ActionProjectFinalize, ActionTargetProject, "project", 1, "project"),
				RootPrerequisiteNodeIDs: []string{node.NodeID}, Nodes: []MembershipNode{node}}
			switch fault {
			case "external reference":
				snapshot.ReverseReferences = []ReverseReference{
					{
						SourceKind: ActionTargetRoute,
						SourceID:   "outside",
						TargetKind: ActionTargetProject,
						TargetID:   "project",
					},
				}
			case "cycle":
				snapshot.Nodes[0].PrerequisiteNodeIDs = []string{node.NodeID}
			case "missing dependency":
				snapshot.Nodes[0].PrerequisiteNodeIDs = []string{"absent"}
			case "duplicate node":
				snapshot.Nodes = append(snapshot.Nodes, node)
			}
			if _, err := BuildPlan(Operation{ID: "del_test", Kind: OperationProjectDelete, TargetKind: TargetProject, TargetID: "project"}, snapshot); err == nil {
				t.Fatal("unsafe membership produced an executable removal graph")
			}
		})
	}
}

// Backing deletion has no Tenant root; it must retire its facade rather than
// accidentally treating the backing Project as an ordinary Project.
func TestNativeHierarchyPlanKeepsBackingFacadeRoot(t *testing.T) {
	plan, err := BuildPlan(
		Operation{
			ID:         "del_backing",
			Kind:       OperationBackingDelete,
			TargetKind: TargetBackingService,
			TargetID:   "backing",
		},
		FrozenMembership{Revision: 1, RootRevision: 1, CoordinationEpoch: 1,
			RootProcedureInput: finalizer(
				ActionBackingServiceFinalize,
				ActionTargetBackingService,
				"backing",
				1,
				"backing",
			)},
	)
	if err != nil || len(plan.Actions) != 1 || plan.Actions[0].Kind != ActionBackingServiceFinalize ||
		plan.Actions[0].TargetKind != ActionTargetBackingService {
		t.Fatalf("backing removal root: %v", err)
	}
}

// TASK-61: planning resumes bounded append batches and rejects a binder that
// substitutes captured host input. It does not execute or finalize resources.
func TestNativeHierarchyPlanningResumesBoundedPublication(t *testing.T) {
	repository := &planningRepository{snapshot: FrozenMembership{Revision: 1, RootRevision: 1, CoordinationEpoch: 1,
		RootProcedureInput: finalizer(ActionTenantFinalize, ActionTargetTenant, "tenant", 1, "tenant")}}
	for index := 0; index < PlanBatchSize+1; index++ {
		node := planningNode(ActionTargetEntry, fmt.Sprint(index), ActionEntryRemove)
		repository.snapshot.Nodes = append(repository.snapshot.Nodes, node)
		repository.snapshot.RootPrerequisiteNodeIDs = append(repository.snapshot.RootPrerequisiteNodeIDs, node.NodeID)
	}
	service := planningService(repository)
	accepted, err := service.Delete(
		t.Context(),
		DeleteRequest{TargetKind: TargetTenant, TargetID: "tenant", IdempotencyKey: "remove-tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := service.PreparePlan(t.Context(), accepted.TaskID)
	if err != nil || ready || len(repository.actions) != PlanBatchSize || repository.operation.PlanSealed {
		t.Fatalf("first planning batch: %v", err)
	}
	ready, err = service.PreparePlan(t.Context(), accepted.TaskID)
	if err != nil || ready || len(repository.actions) != PlanBatchSize+2 || !repository.operation.PlanSealed {
		t.Fatalf("resumed planning batch: %v", err)
	}
	ready, err = service.PreparePlan(t.Context(), accepted.TaskID)
	if err != nil || !ready || repository.appendCalls != 2 {
		t.Fatalf("sealed graph was not reused: %v", err)
	}
	for _, action := range repository.actions {
		if action.State != ActionPending {
			t.Fatal("planning performed a resource effect")
		}
	}
	cleanup := planningNode(ActionTargetEnvironment, "environment", ActionEnvironmentAgentCleanup)
	repository = &planningRepository{
		changeBoundInput: true,
		snapshot: FrozenMembership{Revision: 1, RootRevision: 1, CoordinationEpoch: 1,
			RootProcedureInput:      finalizer(ActionTenantFinalize, ActionTargetTenant, "tenant", 1, "tenant"),
			RootPrerequisiteNodeIDs: []string{cleanup.NodeID}, Nodes: []MembershipNode{cleanup}},
	}
	service = planningService(repository)
	accepted, err = service.Delete(
		t.Context(),
		DeleteRequest{TargetKind: TargetTenant, TargetID: "tenant", IdempotencyKey: "remove-tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.PreparePlan(t.Context(), accepted.TaskID); err == nil || len(repository.actions) != 0 {
		t.Fatal("changed captured cleanup input reached publication")
	}
}

// The service must replay before resolving a now-deleted target. The recording
// port supplies replay classification; actual storage replay remains separate QA.
func TestNativeHierarchyDeletePreservesReplayBeforeTargetResolution(t *testing.T) {
	repository := &planningRepository{}
	service := planningService(repository)
	request := DeleteRequest{TargetKind: TargetProject, TargetID: "project", IdempotencyKey: "remove-project"}
	first, err := service.Delete(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	repository.replay, repository.resolveErr = true, errs.New(errs.KindProjectNotFound, "removed")
	again, err := service.Delete(t.Context(), request)
	if err != nil || !again.Existing || again.TaskID != first.TaskID || again.OperationID != first.OperationID ||
		repository.resolveCalls != 1 {
		t.Fatalf("retained replay tried to resolve its removed resource: %v", err)
	}
	repository.replayErr = errs.New(errs.KindIdempotencyMismatch, "different deletion intent")
	if _, err := service.Delete(t.Context(), DeleteRequest{TargetKind: TargetProject, TargetID: "other", IdempotencyKey: request.IdempotencyKey}); !errors.Is(
		err,
		repository.replayErr,
	) {
		t.Fatalf("replay mismatch was hidden: %v", err)
	}
	repository = &planningRepository{resolvedTarget: TargetBackingService}
	if _, err := planningService(repository).Delete(t.Context(), request); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) ||
		repository.begin.OperationID != "" {
		t.Fatalf("ordinary Project route accepted Backing deletion: %v", err)
	}
}
