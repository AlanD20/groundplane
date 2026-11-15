package serviceruntimerecord

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CandidateWorkloadObservation selects read-only log ownership from a sealed
// candidate. A candidate has not become a native predecessor or serving runtime.
// Its proxy is excluded because the stable proxy may still serve the old Release.
func CandidateWorkloadObservation(
	environmentID, serviceID, releaseID, planID string,
	generation uint64,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ServiceObservationTarget, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		ids.Validate(ids.KindDeployment, releaseID) != nil || artifact == nil {
		return nil, invalidRuntimeIdentity()
	}
	// The authenticated capture may include unrelated Services or a stable
	// proxy retaining older plans. Validate only the selected candidate's
	// execution labels against this plan; keep the captured content unchanged.
	selected := proto.Clone(artifact).(*agentpb.ComposeArtifact)
	selected.Services = nil
	target := &agentpb.ServiceObservationTarget{
		EnvironmentId: environmentID,
		ServiceId:     serviceID,
		ReleaseId:     releaseID,
	}
	for _, workload := range artifact.Services {
		if workload == nil {
			return nil, invalidRuntimeIdentity()
		}
		if workload.ServiceId != serviceID ||
			workload.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		labels := make(map[string]string, len(workload.ExpectedLabels))
		for _, pair := range workload.ExpectedLabels {
			if pair == nil {
				return nil, invalidRuntimeIdentity()
			}
			labels[pair.Key] = pair.Value
		}
		// A captured artifact can also contain an inactive slot or predecessor.
		// Only this published candidate Release selects log containers.
		if labels["com.groundplane.release-id"] != releaseID {
			continue
		}
		selected.Services = append(selected.Services, workload)
		role, slot := "", ""
		switch workload.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
			role = "singleton"
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			role, slot = "slot", workload.Slot
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
			role = "replica"
		default:
			return nil, invalidRuntimeIdentity()
		}
		if target.RuntimeRole != "" && (target.RuntimeRole != role || target.Slot != slot) {
			return nil, invalidRuntimeIdentity()
		}
		target.RuntimeRole, target.Slot = role, slot
		ownerPlan, ownerGeneration, err := observationOwnership(workload)
		if err != nil {
			return nil, err
		}
		if labels["com.groundplane.environment-id"] != environmentID ||
			labels["com.groundplane.service-id"] != serviceID ||
			labels["com.groundplane.release-id"] != releaseID {
			return nil, invalidRuntimeIdentity()
		}
		target.Workloads = append(
			target.Workloads,
			&agentpb.ServiceWorkloadObservation{
				PlanId:           ownerPlan,
				RenderGeneration: ownerGeneration,
				ComposeName:      workload.ComposeName,
				ReplicaOrdinal:   workload.RollingReplicaOrdinal,
			},
		)
	}
	if err := executionplan.ValidateCapturedArtifact(planID, generation, selected); err != nil {
		return nil, err
	}
	if err := serviceobservation.ValidateTarget(target); err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, err)
	}
	return target, nil
}
