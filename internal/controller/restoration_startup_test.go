package controller

import (
	"crypto/sha256"
	"slices"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: the helper's public selector must validate a real Controller plan
// and select only the acknowledged workload/proxy, even when its rendered
// predecessor declares a dependency on another configured Service.
func assertRestorationStartupScope(t *testing.T, task etcd.TaskRecord, plan *agentpb.ExecutionPlan) {
	t.Helper()
	member := plan.GetCandidateReleaseProcedure().GetMembers()[0]
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "blue-green predecessor"
		}
		t.Run(name, func(t *testing.T) {
			project := &composetypes.Project{Services: composetypes.Services{
				"api": {Image: releaseTestWorkload("example/api:prior").LocalImageID,
					DependsOn: composetypes.DependsOnConfig{
						"configured": {Condition: "service_started", Required: true},
					}},
				"configured": {Image: "example/configured:1"},
			}}
			input := composeRenderTestInput(project)
			input.Identities.Services = []ComposeResourceIdentity{
				{ID: member.ServiceId, Name: "api"},
				{ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAZ", Name: "configured"},
			}
			identity := ComposeReleaseIdentity{
				ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", Image: project.Services["api"].Image,
				Strategy: domain.StrategyRecreate, Target: domain.WorkloadSingleton,
				ServingTarget: domain.WorkloadSingleton, ServingReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ServingProxyGeneration: 1,
			}
			want := []string{"api"}
			if addressable {
				api := project.Services["api"]
				api.Expose = []string{"8080"}
				project.Services["api"] = api
				identity.Strategy, identity.Target, identity.ServingTarget = domain.StrategyBlueGreen, domain.WorkloadBlue, domain.WorkloadBlue
				identity.ProxyImage = testServiceProxyImage()
				want = []string{"api", "api--blue"}
			}
			input.Releases = map[string]ComposeReleaseIdentity{member.ServiceId: identity}
			predecessor, err := RenderCompose(input)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(predecessor)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			request := &agentpb.ComposeHelperRequest{
				Schema: composehelper.SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				TaskId: task.ID, OperationId: task.OperationID, Plan: plan, TimeoutSeconds: 30,
				StepId: member.ServingPredecessor.CompensateStepId,
				RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
					TaskId: task.ID, OperationId: task.OperationID, PlanHash: slices.Clone(plan.PlanHash), AuthoritySha256: make([]byte, 32),
					EnvironmentId: task.Owner.EnvironmentID, CandidateArtifactId: member.CandidateArtifactId,
					Candidates: []*agentpb.ReleaseRestorationCandidate{
						{ServiceId: member.ServiceId, ReleaseId: member.CandidateReleaseId,
							Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR},
					},
					AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
						KeyRevision: 1, RevisionId: task.ID, RenderGeneration: 7, ComposeArtifact: encoded, ComposeArtifactSha256: digest[:],
					},
				},
			}
			selected, err := composehelper.StartupServices(request)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, service := range selected {
				names = append(names, service.ComposeName)
			}
			slices.Sort(names)
			if !slices.Equal(names, want) {
				t.Fatalf("sealed restoration startup = %v, want %v", names, want)
			}
			if addressable {
				assertRestorationProxyProbe(t, request, predecessor)
			}
		})
	}
}
