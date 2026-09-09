package executionplan

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A Volume add has exactly directory-create then Docker-create; a slug edit
// has exactly a read-only Docker verification. Neither shape can select a
// Service, materialize a workload file, or execute Compose dependencies.
func validVolumeResourceOnlyPlan(plan *agentpb.ExecutionPlan) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
		ids.Validate(ids.KindVolume, plan.GetTargetId()) != nil || len(plan.GetArtifacts()) != 1 ||
		len(plan.GetSteps()) < 1 || len(plan.GetSteps()) > 2 {
		return false
	}
	artifact := plan.Artifacts[0]
	if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return false
	}
	ensure := plan.Steps[len(plan.Steps)-1].GetManagedVolumeEnsure()
	if ensure == nil || ensure.ArtifactId != artifact.GetArtifactId() || ensure.VolumeId != plan.TargetId {
		return false
	}
	if len(plan.Steps) == 1 {
		return ensure.RequireExisting
	}
	directories := plan.Steps[0].GetManagedVolumeDirectoriesEnsure()
	return !ensure.RequireExisting && directories != nil && directories.ArtifactId == artifact.GetArtifactId() &&
		len(directories.VolumeIds) == 1 && directories.VolumeIds[0] == plan.TargetId
}

func validVolumeResourceOnlyOwnership(
	plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact, labels map[string]string,
) bool {
	if !validVolumeResourceOnlyPlan(plan) || !proto.Equal(plan.Artifacts[0], artifact) ||
		ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil {
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	return err == nil && generation > 0 && generation < plan.RenderGeneration &&
		strconv.FormatUint(generation, 10) == labels[labelRenderGen]
}
