package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RollingRetirement selects the sealed old replica for this exact cleanup
// phase. A shared recreate Compose name is insufficient to authorize removal.
func RollingRetirement(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) (rollingrelease.Replica, error) {
	remove := step.GetComposeWorkloadRemove()
	if remove == nil {
		return rollingrelease.Replica{}, errs.New(errs.KindValidationFailed, "rolling retirement step is missing")
	}
	member, err := RollingMember(plan, remove.GetServiceId())
	if err != nil {
		return rollingrelease.Replica{}, err
	}
	ordinal, phase, err := RollingStepPosition(member, step.GetStepId())
	if err != nil {
		return rollingrelease.Replica{}, err
	}
	rolling, err := OpenRollingPlan(member)
	if err != nil {
		return rollingrelease.Replica{}, err
	}
	changes, err := rolling.Changes()
	if err != nil || ordinal == 0 || int(ordinal) > len(changes) {
		return rollingrelease.Replica{}, errs.New(errs.KindValidationFailed, "rolling retirement change is invalid")
	}
	var replica *rollingrelease.Replica
	switch phase {
	case rollingrelease.RemovePredecessor:
		replica = changes[ordinal-1].Retire
	case rollingrelease.RemoveRetained:
		replica = changes[ordinal-1].Retained
	}
	if replica == nil || replica.ArtifactID != remove.GetArtifactId() {
		return rollingrelease.Replica{}, errs.New(
			errs.KindValidationFailed,
			"rolling retirement differs from its captured predecessor",
		)
	}
	return *replica, nil
}
