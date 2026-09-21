package taskplanning

import (
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: a second deployment has a real serving predecessor and emits
// strategy-specific restoration steps, not first-deployment absence steps.
// The real producer must pass the same sealed descriptor validation as dispatch.
func TestPrepareRedeployBindsServingRestoration(t *testing.T) {
	for _, strategy := range []domain.Strategy{domain.StrategyRecreate, domain.StrategyBlueGreen} {
		t.Run(string(strategy), func(t *testing.T) {
			resolver, task, input := redeployRestorationInput(t, strategy)
			prepared, plan, err := resolver.PrepareReleaseTask(t.Context(), task, input)
			if err != nil {
				t.Fatalf("PrepareReleaseTask(second deployment): %v", err)
			}
			member := plan.GetCandidateReleaseProcedure().GetMembers()[0]
			if member.GetServingPredecessor() == nil || member.GetCandidateAbsence() != nil ||
				len(plan.Artifacts) != 2 {
				t.Fatal("second deployment lost its sealed predecessor")
			}
			if strategy == domain.StrategyRecreate {
				for _, artifact := range plan.Artifacts {
					workloads := 0
					for _, service := range artifact.Services {
						if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
							workloads++
							if service.ExpectedReplicas != 2 {
								t.Fatal("recreate restoration lost the sealed two-replica count")
							}
						}
					}
					if workloads != 1 {
						t.Fatal("recreate artifact omitted its logical workload set")
					}
				}
			}
			descriptor, err := executionplan.DescribeCandidateRelease(plan)
			if err != nil || executionplan.CandidateReleaseDescriptorMatchesPlan(descriptor, plan) != nil ||
				prepared.PlanHash != hex.EncodeToString(plan.PlanHash) {
				t.Fatalf("serving descriptor does not bind producer plan: %v", err)
			}
			replayed, err := resolver.buildReleasePlan(t.Context(), prepared, input)
			if err != nil || !proto.Equal(plan, replayed) {
				t.Fatalf("durable redeploy replay diverged: %v", err)
			}
		})
	}
}

// Rationale: admitting existing predecessor payloads must not admit a mixed
// recovery family, a different candidate, or recovery outside the descriptor.
func TestPreparedRedeployRejectsRestorationTampering(t *testing.T) {
	for _, strategy := range []domain.Strategy{domain.StrategyRecreate, domain.StrategyBlueGreen} {
		t.Run(string(strategy), func(t *testing.T) {
			resolver, task, input := redeployRestorationInput(t, strategy)
			_, original, err := resolver.PrepareReleaseTask(t.Context(), task, input)
			if err != nil {
				t.Fatal(err)
			}
			tests := []struct {
				name   string
				mutate func(*agentpb.ExecutionPlan)
			}{
				{"probe policy", func(plan *agentpb.ExecutionPlan) {
					plan.Steps[3].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
				}},
				{"compensation policy", func(plan *agentpb.ExecutionPlan) {
					plan.Steps[4].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
				}},
				{"candidate Release", func(plan *agentpb.ExecutionPlan) {
					if value := plan.Steps[3].GetServiceRecreateProbe(); value != nil {
						value.CandidateReleaseId = input.Members[0].Intent.PriorServingReleaseID
					} else {
						plan.Steps[3].GetServiceProxyProbe().AlternateReleaseId = input.Members[0].Intent.PriorServingReleaseID
					}
				}},
				{"candidate Service", func(plan *agentpb.ExecutionPlan) {
					if value := plan.Steps[4].GetServiceRecreateCompensate(); value != nil {
						value.ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
					} else {
						plan.Steps[4].GetServiceProxyCompensate().ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
					}
				}},
				{"mixed absence compensation", func(plan *agentpb.ExecutionPlan) {
					member := plan.CandidateReleaseProcedure.Members[0]
					plan.Steps[4].Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{
						CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
							CandidateArtifactId: member.CandidateArtifactId, ServiceId: member.ServiceId,
							CandidateReleaseId: member.CandidateReleaseId,
						},
					}
				}},
				{"unreferenced restoration", func(plan *agentpb.ExecutionPlan) {
					extra := proto.CloneOf(plan.Steps[3])
					extra.StepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FB5"
					plan.Steps = append(plan.Steps, extra)
				}},
				{"absence pair for serving predecessor", func(plan *agentpb.ExecutionPlan) {
					member := plan.CandidateReleaseProcedure.Members[0]
					plan.Steps[3].Payload = &agentpb.ExecutionStep_CandidateRestorationProbe{
						CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
							CandidateArtifactId: member.CandidateArtifactId, ServiceId: member.ServiceId,
							CandidateReleaseId: member.CandidateReleaseId,
						},
					}
					plan.Steps[4].Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{
						CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
							CandidateArtifactId: member.CandidateArtifactId, ServiceId: member.ServiceId,
							CandidateReleaseId: member.CandidateReleaseId,
						},
					}
				}},
				{"different predecessor", func(plan *agentpb.ExecutionPlan) {
					if value := plan.Steps[4].GetServiceRecreateCompensate(); value != nil {
						value.PriorReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FB6"
					} else {
						plan.Steps[4].GetServiceProxyCompensate().PriorReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FB6"
					}
				}},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					plan := proto.CloneOf(original)
					test.mutate(plan)
					if _, err := executionplan.Seal(plan); err == nil {
						t.Fatal("Seal accepted tampered restoration authority")
					}
				})
			}
		})
	}
}

func redeployRestorationInput(
	t *testing.T,
	strategy domain.Strategy,
) (*TaskPlanResolver, etcd.TaskRecord, etcd.ReleaseTaskRenderInput) {
	t.Helper()
	reader, task := blueprintPlanTestState(t)
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "example/api:next", Expose: []string{"8080"},
			HealthCheck: &composetypes.HealthCheckConfig{Test: composetypes.HealthCheckTest{"CMD", "true"}},
			Networks:    map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
		}},
		Networks: composetypes.Networks{"frontend": {}},
		Volumes:  composetypes.Volumes{"app-data": {}},
	}
	var err error
	reader.projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	const (
		publicationID = "publication-redeploy"
		releaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		priorID       = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	task.Type, task.Target = testtaskjournal.TaskDeploy, serviceID
	task.Params[testreleaserender.TaskReleasePublicationParam] = publicationID
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB0"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB1"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB2"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB3"},
		{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FB4"},
	}
	prior := releaseTestWorkload("example/api:prior")
	member := testreleaserender.ReleaseTaskRenderMember{
		Intent: domain.Intent{ID: releaseID, EnvironmentID: reader.environment.ID, ServiceID: serviceID,
			OperationID: task.OperationID, OperationKind: domain.OperationDeploy,
			CandidateWorkload: releaseTestWorkload("example/api:next"), Strategy: strategy,
			OnFailure: domain.OnFailureSwitchBack, PriorServingReleaseID: priorID},
		Render: testreleaserender.ReleaseRenderInput{ReleaseID: releaseID, PlanID: task.PlanID,
			ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAZ", PriorArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAY",
			ServiceID: serviceID, ServiceName: "api", CandidateWorkload: releaseTestWorkload("example/api:next"),
			PriorWorkload: &prior, Strategy: strategy, PriorStrategy: strategy,
			CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			ProxyPorts: []uint16{
				8080,
			}, ProxyGeneration: 2, PriorProxyGeneration: 1, ProxyImage: testServiceProxyImage(),
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection},
	}
	if strategy == domain.StrategyBlueGreen {
		member.Intent.Slot = domain.SlotGreen
		member.Render.Slot, member.Render.PriorSlot = domain.SlotGreen, domain.SlotBlue
		member.Render.CandidateTarget, member.Render.PriorTarget = domain.WorkloadGreen, domain.WorkloadBlue
	} else {
		member.Intent.CandidateWorkload.ReplicaCount = 2
		member.Render.CandidateWorkload.ReplicaCount = 2
		member.Render.PriorWorkload.ReplicaCount = 2
	}
	for _, candidate := range []bool{false, true} {
		target, id, generation := member.Render.PriorTarget, priorID, uint64(1)
		if candidate {
			target, id, generation = member.Render.CandidateTarget, releaseID, 2
		}
		config, err := domain.RenderProxyConfig("api", id, target, generation, []uint16{8080})
		if err != nil {
			t.Fatal(err)
		}
		if candidate {
			member.Render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		} else {
			member.Render.PriorProxyDigest = hex.EncodeToString(config.SHA256[:])
		}
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	priorSource := member.Render
	priorSource.ReleaseID, priorSource.ArtifactID = priorID, member.Render.PriorArtifactID
	priorSource.CandidateWorkload = *member.Render.PriorWorkload
	priorSource.Strategy, priorSource.Slot = member.Render.PriorStrategy, member.Render.PriorSlot
	priorSource.CandidateTarget = member.Render.PriorTarget
	priorSource.ProxyGeneration, priorSource.ProxyConfigDigest = member.Render.PriorProxyGeneration, member.Render.PriorProxyDigest
	priorArtifacts, err := resolver.RenderRetainedServiceRuntime(
		t.Context(), testreleaserender.ServiceLifecycleRelease{Current: priorSource},
	)
	if err != nil {
		t.Fatal(err)
	}
	priorBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(priorArtifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	member.Render.PriorRuntime = &testtaskassignments.ReleaseNativePredecessorAuthority{
		ServiceID:       serviceID,
		CurrentArtifact: priorBytes,
	}
	return resolver, task, etcd.ReleaseTaskRenderInput{PublicationID: publicationID,
		Operation: testreleases.ReleaseOperationHead{OperationID: task.OperationID, PublicationID: publicationID,
			EnvironmentID: reader.environment.ID, FailurePolicy: domain.OnFailureSwitchBack},
		Members: []testreleaserender.ReleaseTaskRenderMember{member}}
}
