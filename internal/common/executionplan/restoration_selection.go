package executionplan

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type CandidateRestorationSelection struct {
	ServiceID string
	ReleaseID string
	Target    agentpb.ReleaseRestorationTarget
}

// RestorationTargetForService reads an already-selected authority. Missing or
// duplicate members return UNSPECIFIED, which executors must reject.
func RestorationTargetForService(
	authority *agentpb.ReleaseRestorationAuthority,
	serviceID string,
) agentpb.ReleaseRestorationTarget {
	selected := agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED
	found := false
	for _, member := range authority.GetCandidates() {
		if serviceID == "" || member.GetServiceId() != serviceID {
			continue
		}
		if found {
			return agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED
		}
		found, selected = true, member.GetTarget()
	}
	return selected
}

// SelectBlueprintRestorationMembers classifies only the supplied immutable
// applied witness. The caller owns its fixed-revision capture and final CAS.
// No host observation or desired runtime intent participates in selection.
func SelectBlueprintRestorationMembers(
	candidates []CandidateServiceIdentity,
	predecessor *agentpb.ComposeArtifact,
) ([]CandidateRestorationSelection, error) {
	if len(candidates) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "restoration candidate set is empty")
	}
	ordered := slices.Clone(candidates)
	slices.SortFunc(
		ordered,
		func(a, b CandidateServiceIdentity) int { return strings.Compare(a.ServiceID, b.ServiceID) },
	)
	result := make([]CandidateRestorationSelection, 0, len(ordered))
	for index, candidate := range ordered {
		if ids.Validate(ids.KindService, candidate.ServiceID) != nil ||
			ids.Validate(ids.KindDeployment, candidate.ReleaseID) != nil ||
			index > 0 && ordered[index-1].ServiceID == candidate.ServiceID {
			return nil, errs.New(errs.KindValidationFailed, "restoration candidate identity is invalid")
		}
		target, err := blueprintMemberRestorationTarget(candidate, predecessor)
		if err != nil {
			return nil, err
		}
		result = append(
			result,
			CandidateRestorationSelection{
				ServiceID: candidate.ServiceID,
				ReleaseID: candidate.ReleaseID,
				Target:    target,
			},
		)
	}
	return result, nil
}

func blueprintMemberRestorationTarget(
	candidate CandidateServiceIdentity,
	predecessor *agentpb.ComposeArtifact,
) (agentpb.ReleaseRestorationTarget, error) {
	workloads, proxies, configured, placeholders := 0, 0, 0, 0
	slots := make(map[string]bool, 2)
	for _, service := range predecessor.GetServices() {
		if service.GetServiceId() != candidate.ServiceID {
			continue
		}
		labels := make(map[string]string, len(service.GetExpectedLabels()))
		for _, label := range service.GetExpectedLabels() {
			if label == nil || label.GetKey() == "" {
				return 0, invalidRestorationMember()
			}
			if _, duplicate := labels[label.GetKey()]; duplicate {
				return 0, invalidRestorationMember()
			}
			labels[label.GetKey()] = label.GetValue()
		}
		releaseID, role := labels["com.groundplane.release-id"], labels["com.groundplane.runtime-role"]
		switch service.GetRole() {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED:
			if releaseID != "" || role != "" || service.GetOwnerComponentId() != "" {
				return 0, invalidRestorationMember()
			}
			configured++
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			if role != "proxy" {
				return 0, invalidRestorationMember()
			}
			proxies++
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
			wantRole := "singleton"
			if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
				wantRole = "slot"
				slot := service.GetSlot()
				if slot != "blue" && slot != "green" || labels["com.groundplane.slot"] != slot || slots[slot] {
					return 0, invalidRestorationMember()
				}
				slots[slot] = true
			} else if service.GetSlot() != "" || labels["com.groundplane.slot"] != "" {
				return 0, invalidRestorationMember()
			}
			if role != wantRole ||
				service.GetComposeName() == "" || service.GetImageReference() == "" || service.GetExpectedReplicas() == 0 ||
				service.GetOwnerComponentId() != "" {
				return 0, invalidRestorationMember()
			}
			if releaseID == "" && wantRole == "slot" {
				placeholders++
				continue
			}
			if ids.Validate(ids.KindDeployment, releaseID) != nil || releaseID == candidate.ReleaseID {
				return 0, invalidRestorationMember()
			}
			workloads++
		default:
			return 0, invalidRestorationMember()
		}
	}
	if workloads > 1 || proxies > 1 || configured > 1 || workloads > 0 && configured > 0 ||
		proxies > 0 && workloads == 0 {
		return 0, invalidRestorationMember()
	}
	if placeholders != 0 && (placeholders != 1 || workloads != 1 || proxies != 1 || len(slots) != 2) {
		return 0, invalidRestorationMember()
	}
	if workloads == 1 {
		return agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR, nil
	}
	return agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE, nil
}

func invalidRestorationMember() error {
	return errs.New(errs.KindValidationFailed, "applied restoration member runtime is incomplete or ambiguous")
}
