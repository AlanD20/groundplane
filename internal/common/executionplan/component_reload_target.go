package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Historical runtime generations may receive newer files only after an exact
// non-forced up of that Component, never via an unrelated two-step action.
func retainedComponentReloadTarget(
	artifact *agentpb.ComposeArtifact,
	service *agentpb.ComposeService,
	action *agentpb.ExecutionStep,
	steps []*agentpb.ExecutionStep,
) bool {
	if !validManagedComposeImage(service) || ids.Validate(ids.KindComponent, service.GetOwnerComponentId()) != nil {
		return false
	}
	for _, step := range steps {
		if step == action {
			return false
		}
		apply := step.GetComposeApply()
		if apply.GetArtifactId() == artifact.GetArtifactId() && len(apply.GetServiceIds()) == 1 &&
			apply.ServiceIds[0] == service.ServiceId && apply.NoDependencies && !apply.ForceRecreate && !apply.FullReconcile {
			return true
		}
	}
	return false
}
