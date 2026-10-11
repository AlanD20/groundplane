package executionplan

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReleaseWorkloadService is the shared physical selector for helper startup
// and Agent readiness. A rolling ordinal never selects every replica of a
// logical Service, and it cannot also carry a singleton/blue-green target.
func ReleaseWorkloadService(
	artifact *agentpb.ComposeArtifact,
	serviceID, target string,
	replicaOrdinal uint32,
) (*agentpb.ComposeService, error) {
	if artifact == nil {
		return nil, errs.New(errs.KindValidationFailed, "release workload artifact is unavailable")
	}
	if replicaOrdinal == 0 {
		if validReleaseTarget(target) {
			if service := releaseTargetService(artifact, serviceID, target); service != nil {
				return service, nil
			}
		}
		return nil, errs.New(errs.KindValidationFailed, "release workload target is absent from its Compose artifact")
	}
	if target != "" || replicaOrdinal > rollingrelease.MaximumReplicas {
		return nil, errs.New(errs.KindValidationFailed, "rolling workload requires only a replica ordinal from 1 to 16")
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA ||
			service.GetRollingReplicaOrdinal() != replicaOrdinal {
			continue
		}
		if selected != nil {
			return nil, errs.New(errs.KindValidationFailed, "rolling workload ordinal is duplicated")
		}
		selected = service
	}
	if selected == nil {
		return nil, errs.New(errs.KindValidationFailed, "rolling workload ordinal is absent from its Compose artifact")
	}
	if err := validateRollingReplica(selected); err != nil {
		return nil, err
	}
	return selected, nil
}

func validateRollingReplica(service *agentpb.ComposeService) error {
	name, err := rollingrelease.ReplicaComposeName(expectedReleaseLabel(service), service.GetRollingReplicaOrdinal())
	if err != nil || service.GetComposeName() != name || service.GetExpectedReplicas() != 1 ||
		!service.GetHasHealthcheck() || !workloadimage.LocalIDValid(service.GetImageReference()) ||
		service.GetSlot() != "" || len(service.GetProxyConfigJson()) != 0 || len(service.GetProxyConfigSha256()) != 0 ||
		service.GetOwnerComponentId() != "" || service.GetPostgresToolsImage() != "" {
		return errs.New(
			errs.KindValidationFailed,
			"rolling replica requires an exact image, singleton identity and enabled healthcheck",
		)
	}
	roleMatches, ordinalMatches := false, false
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == labelRuntimeRole {
			roleMatches = label.GetValue() == "replica"
		}
		if label.GetKey() == labelReplicaOrdinal {
			ordinalMatches = label.GetValue() == strconv.FormatUint(uint64(service.GetRollingReplicaOrdinal()), 10)
		}
	}
	if !roleMatches || !ordinalMatches {
		return errs.New(
			errs.KindValidationFailed,
			"rolling replica ownership does not identify its runtime role and ordinal",
		)
	}
	return nil
}

// RollingReplicaService resolves physical identity, never the first logical
// Service match: one logical Service can own many independently replaced replicas.
func RollingReplicaService(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	replica rollingrelease.Replica,
) (*agentpb.ComposeService, error) {
	if artifact == nil || artifact.GetArtifactId() != replica.ArtifactID {
		return nil, errs.New(errs.KindValidationFailed, "rolling replica artifact is unavailable")
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID || service.GetComposeName() != replica.ComposeName {
			continue
		}
		if selected != nil {
			return nil, errs.New(errs.KindValidationFailed, "rolling replica identity is duplicated")
		}
		selected = service
	}
	if selected == nil || expectedReleaseLabel(selected) != replica.ReleaseID ||
		!workloadimage.LocalIDValid(selected.GetImageReference()) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"rolling replica does not match its captured Release, image and count",
		)
	}
	if replica.ContainerID != "" {
		if selected.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
			selected.GetExpectedReplicas() < 2 || selected.GetExpectedReplicas() > rollingrelease.MaximumReplicas ||
			replica.ComposeReplica != replica.Ordinal || replica.ComposeReplica > selected.GetExpectedReplicas() {
			return nil, errs.New(
				errs.KindValidationFailed,
				"captured container does not select a replica of the sealed recreate workload",
			)
		}
		return selected, nil
	}
	if selected.GetExpectedReplicas() != 1 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"scaled predecessor requires an exact captured container identity",
		)
	}
	switch selected.GetRole() {
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
		if err := validateRollingReplica(selected); err != nil {
			return nil, err
		}
		if selected.GetRollingReplicaOrdinal() != replica.Ordinal {
			return nil, errs.New(errs.KindValidationFailed, "rolling replica ordinal differs from its captured plan")
		}
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
		if replica.Ordinal != 1 ||
			selected.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
				!selected.GetHasHealthcheck() {
			return nil, errs.New(
				errs.KindValidationFailed,
				"rolling predecessor is not an independently addressable singleton",
			)
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "rolling replica has an unsupported runtime role")
	}
	return selected, nil
}

// ValidateRollingArtifacts binds the procedure to the sealed artifacts. It
// supplements normal artifact validation; metadata alone cannot authorize effects.
func ValidateRollingArtifacts(plan rollingrelease.Plan, artifacts []*agentpb.ComposeArtifact) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	indexed := make(map[string]*agentpb.ComposeArtifact, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil || indexed[artifact.GetArtifactId()] != nil {
			return errs.New(errs.KindValidationFailed, "rolling artifacts are absent or duplicated")
		}
		indexed[artifact.GetArtifactId()] = artifact
	}
	for groupIndex, group := range [][]rollingrelease.Replica{plan.Prior, plan.Candidate, plan.Retained} {
		if len(group) == 0 {
			continue
		}
		artifact := indexed[group[0].ArtifactID]
		candidate := indexed[plan.Candidate[0].ArtifactID]
		if artifact == nil || candidate == nil ||
			artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetOwnerId() != candidate.GetOwnerId() ||
			artifact.GetProjectName() != candidate.GetProjectName() {
			return errs.New(
				errs.KindValidationFailed,
				"rolling replicas must belong to the same Environment Compose project",
			)
		}
		image := ""
		names := make(map[string]bool, len(group))
		for _, replica := range group {
			service, err := RollingReplicaService(indexed[replica.ArtifactID], plan.ServiceID, replica)
			if err != nil {
				return err
			}
			if groupIndex == 1 && service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
				return errs.New(
					errs.KindValidationFailed,
					"rolling candidate must use individually named replica workloads",
				)
			}
			if replica.ContainerID != "" && int(service.GetExpectedReplicas()) != len(group) {
				return errs.New(
					errs.KindValidationFailed,
					"rolling plan does not capture every scaled predecessor container",
				)
			}
			if (groupIndex == 2 || groupIndex == 0 && len(plan.Retained) != 0) &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
				return errs.New(
					errs.KindValidationFailed,
					"rolling retained cleanup requires a captured blue-green slot pair",
				)
			}
			if image != "" && image != service.GetImageReference() {
				return errs.New(
					errs.KindValidationFailed,
					"rolling replicas of one Release have different workload images",
				)
			}
			image = service.GetImageReference()
			names[replica.ComposeName] = true
		}
		for _, service := range artifact.GetServices() {
			if service.GetServiceId() == plan.ServiceID &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
				expectedReleaseLabel(service) == group[0].ReleaseID &&
				!names[service.GetComposeName()] {
				return errs.New(errs.KindValidationFailed, "rolling plan omits a workload from its captured Release")
			}
		}
	}
	return nil
}
