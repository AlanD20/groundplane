package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func publishBlueprintRequirementCandidateForAttach(
	t *testing.T,
	store *attachTestStore,
	tasks *TaskRepository,
	scope AttachCreateScope,
	attach Versioned[AttachRecord],
	condition core.RequirementCondition,
) (TaskRecord, int64) {
	t.Helper()
	ctx := context.Background()
	task := environmentBlueprintTestTask(t, scope.Project.Record, scope.Environment.Record, 930)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration + 1)
	publicationID := ids.NewULID()
	artifactID := ids.NewAt(ids.KindConfig, task.CreatedAt, 931)
	releaseID := ids.NewAt(ids.KindDeployment, task.CreatedAt, 932)
	service := scope.Services[0].Record.Desired
	probeStepID := ids.NewAt(ids.KindStep, task.CreatedAt, 935)
	compensateStepID := ids.NewAt(ids.KindStep, task.CreatedAt, 936)
	task.Params[TaskReleasePublicationParam] = publicationID
	task.Params[TaskComposeArtifactParam] = artifactID
	task.Steps = append(task.Steps,
		TaskStepRecord{Kind: TaskStepOperation, ID: probeStepID},
		TaskStepRecord{Kind: TaskStepOperation, ID: compensateStepID},
	)

	workload := releaseTestWorkloadSeal(service.Image)
	plan := blueprintRequirementGateCandidatePlan(t, task, service, releaseID, workload)
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatalf("describe Blueprint requirement fixture candidate: %v", err)
	}
	task.PlanHash = hex.EncodeToString(descriptor.PlanHash)
	projection := cloneEnvironmentComposeProjection(scope.ComposeProjection.Record)
	projection.RevisionID = task.ID
	projection.RenderGeneration = uint64(task.RenderGeneration)
	projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(plan.Artifacts[0])
	if err != nil {
		t.Fatalf("marshal Blueprint requirement fixture artifact: %v", err)
	}
	projection.NormalizedCompose = append([]byte(nil), plan.Artifacts[0].CanonicalYaml...)
	render := ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: task.PlanID, ArtifactID: artifactID,
		ServiceID: service.ID, ServiceName: service.Name, CandidateWorkload: workload,
		Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		TenantID: task.Owner.TenantID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: task.Owner.ProjectID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: task.Owner.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir: scope.Environment.Record.VolumeDir, Projection: projection,
		ServiceDependencyPlans: projection.ServiceDependencyPlans,
	}
	rawRender, err := EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatalf("encode Blueprint requirement fixture render input: %v", err)
	}
	renderDigest, err := domain.Digest(rawRender)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: task.Owner.EnvironmentID, ServiceID: service.ID,
		OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
		GroupOperationID: task.OperationID, GroupMemberOrdinal: 1,
		CandidateWorkload: workload, Tag: "requirement-gate", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: artifactID, RenderInputDigest: renderDigest,
		CreatedAt: task.CreatedAt, Actor: string(task.Actor), OriginatingTaskID: task.ID,
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: task.Owner.TenantID,
			ProjectID: task.Owner.ProjectID, EnvironmentID: task.Owner.EnvironmentID,
		},
	}
	ledger, err := NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := ledger.Stage(ctx, ReleaseStage{
		PublicationID: publicationID, OperationID: task.OperationID, CreatedAt: task.CreatedAt,
		Members: []ReleaseStageMember{{
			Intent: intent, RenderInput: rawRender,
			Checkpoint: domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: task.CreatedAt},
		}},
	})
	if err != nil {
		t.Fatalf("stage Blueprint requirement fixture candidate: %v", err)
	}

	requirements := core.BlueprintRequirements{
		Authored: []core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: attach.Record.Name},
			Condition: condition, Phases: []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		Resolved: []core.ResolvedRequirement{{
			Target: core.ResolvedRequirementTarget{
				Kind: core.RequirementTargetBackingAttach, Name: attach.Record.Name,
				ID: attach.Record.ID, TaskID: attach.Record.TaskID, Revision: attach.Revision,
			},
			Condition: condition, Phases: []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		ResolutionRevision: attach.ReadRevision,
	}
	dag, err := core.BuildBlueprintRequirementDAG(task.ID, requirements, []string{task.Steps[0].ID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := NewBlueprintRequirementGate(task, requirements.ResolutionRevision, dag)
	if err != nil {
		t.Fatal(err)
	}
	task.Params[TaskBlueprintRequirementGateSHA256Param] = gate.DAGDigest
	releasePublication, err := ledger.PrepareBlueprintReleasePublication(ctx, nil, BlueprintReleasePublicationEvidence{
		Manifest: manifest, EnvironmentID: task.Owner.EnvironmentID, Task: task,
		CandidateReleaseDescriptor: descriptor, Plan: plan, PublishedAt: task.CreatedAt,
	})
	if err != nil {
		t.Fatalf("prepare Blueprint requirement fixture publication: %v", err)
	}
	defer releasePublication.Clear()
	seedBlueprintRequirementTask(t, store, task)
	gateValue, err := encodeBlueprintRequirementGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	headValue, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: task.Owner.EnvironmentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mutations := append(cloneBlueprintReleaseMutations(releasePublication.mutations),
		Mutation{Type: MutationPut, Key: environmentBlueprintHeadKey(task.Owner.EnvironmentID), Value: headValue},
		Mutation{Type: MutationPut, Key: blueprintRequirementGateKey(task.ID), Value: gateValue},
		Mutation{Type: MutationPut, Key: environmentMutationEpochKey(task.Owner.EnvironmentID), Value: epochValue},
	)
	defer clearMutations(mutations)
	published, err := store.Transact(ctx, releasePublication.conditions, mutations)
	if err != nil || !published.Succeeded {
		t.Fatalf("publish Blueprint requirement fixture candidate = %#v, %v", published, err)
	}
	return task, published.Revision
}

func blueprintRequirementGateCandidatePlan(
	t *testing.T,
	task TaskRecord,
	service core.Service,
	releaseID string,
	workload domain.WorkloadSeal,
) *agentpb.ExecutionPlan {
	t.Helper()
	artifactID := task.Params[TaskComposeArtifactParam]
	forwardStepID, probeStepID, compensateStepID := task.Steps[0].ID, task.Steps[1].ID, task.Steps[2].ID
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: service.ID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
			ForwardStepIDs: []string{forwardStepID},
			ServingPredecessor: &executionplan.ServingPredecessorInput{
				ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
			},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: "gp-" + strings.ToLower(task.Owner.EnvironmentID),
				Services: []executionplan.CandidateServiceIdentity{
					{ServiceID: service.ID, ReleaseID: releaseID},
				},
				ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	canonicalYAML := []byte("services:\n  " + service.Name + ":\n    image: " + service.Image + "\n")
	yamlDigest := sha256.Sum256(canonicalYAML)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: task.Owner.EnvironmentID, ProjectName: "gp-" + strings.ToLower(task.Owner.EnvironmentID),
		CanonicalYaml: canonicalYAML, YamlSha256: yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + task.Owner.TenantID + "/" +
			task.Owner.ProjectID + "/" + task.Owner.EnvironmentID,
		Services: []*agentpb.ComposeService{{
			ServiceId: service.ID, ComposeName: service.Name, ExpectedReplicas: workload.ReplicaCount,
			HasHealthcheck: true, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: workload.LocalImageID,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.environment-id", Value: task.Owner.EnvironmentID},
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: task.PlanID},
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.render-generation", Value: strconv.Itoa(int(task.RenderGeneration))},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
				{Key: "com.groundplane.service-id", Value: service.ID},
			},
		}},
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetId:         task.Owner.EnvironmentID, Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: forwardStepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
					ArtifactId: artifactID, ServiceIds: []string{service.ID}, ForceRecreate: true, NoDependencies: true,
				}},
			},
			{
				StepId: probeStepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
					CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
						CandidateArtifactId: artifactID, ServiceId: service.ID, CandidateReleaseId: releaseID,
					},
				},
			},
			{
				StepId: compensateStepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: forwardStepID,
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: artifactID, ServiceId: service.ID, CandidateReleaseId: releaseID,
					},
				},
			},
		},
		CandidateReleaseProcedure: procedure,
	})
	if err != nil {
		t.Fatalf("seal Blueprint requirement fixture plan: %v", err)
	}
	return plan
}
