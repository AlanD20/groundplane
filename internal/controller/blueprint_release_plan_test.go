package controller

import (
	"context"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: Blueprint candidate Compose rendering must carry the sealed
// predecessor topology even for an initial candidate with baseline identity;
// otherwise the shared renderer rejects the rebuilt task before assignment.
func TestPrepareBlueprintReleaseTaskCarriesPredecessorComposeIdentity(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	const candidateReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	task.Params[etcd.TaskReleasePublicationParam] = "publication"
	member := etcd.ReleaseTaskRenderMember{
		Intent: domain.Intent{
			ID: candidateReleaseID, EnvironmentID: reader.environment.ID, ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
			Image: "example/api:next", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack,
		},
		Render: etcd.ReleaseRenderInput{
			ReleaseID: candidateReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[EnvironmentBlueprintArtifactParam],
			ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api", Image: "example/api:next",
			Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
		},
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, plan, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, BlueprintReleasePlanInput{
		Members:       []etcd.ReleaseTaskRenderMember{member},
		ApplyStepIDs:  []string{task.Steps[0].ID},
		HealthStepIDs: []string{task.Steps[1].ID},
		PostStepIDs:   [][]string{nil},
	})
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask() error = %v", err)
	}
	if plan == nil || len(plan.Artifacts) != 1 {
		t.Fatalf("Blueprint Release plan = %#v, want one artifact", plan)
	}
}
