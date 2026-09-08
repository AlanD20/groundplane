package executionplan

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// retainedOwnershipReason is deliberately closed and static. It is safe to
// expose in validation diagnostics because it carries no plan, artifact,
// service, label, or YAML data.
type retainedOwnershipReason string

const (
	retainedOwnershipInvalidInput    retainedOwnershipReason = "invalid_input"
	retainedOwnershipArtifactSet     retainedOwnershipReason = "artifact_set"
	retainedOwnershipArtifactBinding retainedOwnershipReason = "artifact_binding"
	retainedOwnershipProcedure       retainedOwnershipReason = "procedure"
	retainedOwnershipCandidate       retainedOwnershipReason = "candidate_missing"
	retainedOwnershipGeneration      retainedOwnershipReason = "generation"
	retainedOwnershipRetainedRole    retainedOwnershipReason = "retained_role"
	retainedOwnershipService         retainedOwnershipReason = "service_binding"
	retainedOwnershipHistoricalApply retainedOwnershipReason = "historical_apply"
	retainedOwnershipStepSelection   retainedOwnershipReason = "step_selection"
)

func retainedOwnershipReject(reason retainedOwnershipReason) (bool, retainedOwnershipReason) {
	return false, reason
}

// Retained native entries are observation-only. Publication separately binds
// them to the captured applied artifact and exact serving Release revisions.
// The Agent rejects any plan capable of selecting this retained workload.
func validBlueprintRetainedOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
) (bool, retainedOwnershipReason) {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != plan.GetTargetId() || labels[labelComponentID] != "" ||
		validateID(ids.KindPlan, labels[labelPlanID]) != nil {
		return retainedOwnershipReject(retainedOwnershipInvalidInput)
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, candidate := range plan.GetArtifacts() {
		if candidate == nil || candidate.GetArtifactId() == "" {
			return retainedOwnershipReject(retainedOwnershipArtifactSet)
		}
		if _, duplicate := artifacts[candidate.GetArtifactId()]; duplicate {
			return retainedOwnershipReject(retainedOwnershipArtifactSet)
		}
		artifacts[candidate.GetArtifactId()] = candidate
	}
	if !proto.Equal(artifacts[artifact.GetArtifactId()], artifact) {
		return retainedOwnershipReject(retainedOwnershipArtifactBinding)
	}
	managedSources := make(map[string]map[string]bool)
	managedRemoves := make(map[string]map[string]string)
	if procedure := plan.GetManagedComponentProcedure(); procedure != nil {
		if validateManagedComponentProcedure(plan, artifacts) != nil {
			return retainedOwnershipReject(retainedOwnershipProcedure)
		}
		for _, source := range procedure.GetServices() {
			if managedSources[source.GetSourceArtifactId()] == nil {
				managedSources[source.GetSourceArtifactId()] = make(map[string]bool)
			}
			managedSources[source.GetSourceArtifactId()][source.GetServiceId()] = true
			if managedRemoves[source.GetSourceArtifactId()] == nil {
				managedRemoves[source.GetSourceArtifactId()] = make(map[string]string)
			}
			managedRemoves[source.GetSourceArtifactId()][source.GetServiceId()] = source.GetRemoveStepId()
		}
	}
	candidates := make(map[string]bool)
	nativePriorArtifacts := make(map[string]map[string]bool)
	candidateArtifactID := ""
	if procedure := plan.GetCandidateReleaseProcedure(); procedure != nil {
		if len(procedure.Members) == 0 {
			return retainedOwnershipReject(retainedOwnershipCandidate)
		}
		for _, member := range procedure.Members {
			if member == nil || validateID(ids.KindConfig, member.CandidateArtifactId) != nil {
				return retainedOwnershipReject(retainedOwnershipCandidate)
			}
			if serving := member.GetServingPredecessor(); serving != nil {
				for _, artifactID := range []string{serving.GetPriorArtifactId(), serving.GetRetainedPriorArtifactId()} {
					if artifactID == "" {
						continue
					}
					if nativePriorArtifacts[artifactID] == nil {
						nativePriorArtifacts[artifactID] = make(map[string]bool)
					}
					nativePriorArtifacts[artifactID][member.GetServiceId()] = true
				}
			}
			if candidateArtifactID == "" {
				candidateArtifactID = member.CandidateArtifactId
			} else if candidateArtifactID != member.CandidateArtifactId {
				return retainedOwnershipReject(retainedOwnershipCandidate)
			}
			candidates[member.ServiceId] = true
		}
		if artifacts[candidateArtifactID] == nil {
			return retainedOwnershipReject(retainedOwnershipCandidate)
		}
	} else if len(managedSources) == 0 {
		for artifactID := range artifacts {
			if candidateArtifactID != "" {
				return retainedOwnershipReject(retainedOwnershipCandidate)
			}
			candidateArtifactID = artifactID
		}
	} else {
		for artifactID := range artifacts {
			if len(managedSources[artifactID]) == 0 {
				if candidateArtifactID != "" {
					return retainedOwnershipReject(retainedOwnershipCandidate)
				}
				candidateArtifactID = artifactID
			}
		}
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	if err != nil || generation == 0 || generation >= plan.RenderGeneration ||
		strconv.FormatUint(generation, 10) != labels[labelRenderGen] {
		return retainedOwnershipReject(retainedOwnershipGeneration)
	}
	found := false
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID {
			continue
		}
		if !nativePriorArtifacts[artifact.GetArtifactId()][serviceID] && candidates[serviceID] {
			return retainedOwnershipReject(retainedOwnershipCandidate)
		}
		found = true
		if service.GetOwnerComponentId() != "" {
			return retainedOwnershipReject(retainedOwnershipRetainedRole)
		}
		switch service.GetRole() {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			if !workloadimage.LocalIDValid(service.GetImageReference()) ||
				validateID(ids.KindDeployment, expectedReleaseLabel(service)) != nil {
				return retainedOwnershipReject(retainedOwnershipRetainedRole)
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			if !validManagedComposeImage(service) || !retainedProxyHasRelease(artifact, service) {
				return retainedOwnershipReject(retainedOwnershipRetainedRole)
			}
		default:
			return retainedOwnershipReject(retainedOwnershipRetainedRole)
		}
	}
	if !found || len(plan.GetSteps()) == 0 {
		return retainedOwnershipReject(retainedOwnershipService)
	}
	seenManagedRemoves := make(map[string]bool)
	selected := func(artifactID string, serviceIDs []string) bool {
		if blueprintManagedServiceSelection(
			plan.Operation,
			&agentpb.ComposeApply{ArtifactId: artifactID, ServiceIds: serviceIDs},
			artifacts,
		) {
			for _, selectedID := range serviceIDs {
				if artifactID != candidateArtifactID && !managedSources[artifactID][selectedID] {
					return false
				}
			}
			return true
		}
		if artifactID != candidateArtifactID || len(serviceIDs) == 0 {
			return false
		}
		for _, selectedID := range serviceIDs {
			if !candidates[selectedID] {
				return false
			}
		}
		return true
	}
	for _, step := range plan.Steps {
		if step == nil {
			return retainedOwnershipReject(retainedOwnershipStepSelection)
		}
		switch payload := step.Payload.(type) {
		case *agentpb.ExecutionStep_ComposeApply:
			apply := payload.ComposeApply
			if apply == nil || apply.ArtifactId != candidateArtifactID && len(managedSources[apply.ArtifactId]) != 0 ||
				!apply.NoDependencies || apply.FullReconcile || !selected(apply.ArtifactId, apply.ServiceIds) {
				if apply != nil && apply.ArtifactId != candidateArtifactID && len(managedSources[apply.ArtifactId]) != 0 {
					return retainedOwnershipReject(retainedOwnershipHistoricalApply)
				}
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_ComposeStop:
			if payload.ComposeStop == nil || payload.ComposeStop.ArtifactId != candidateArtifactID && len(managedSources[payload.ComposeStop.ArtifactId]) != 0 ||
				!selected(payload.ComposeStop.ArtifactId, payload.ComposeStop.ServiceIds) {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_ComposeRemove:
			remove := payload.ComposeRemove
			if remove == nil || remove.WholeProject || !selected(remove.ArtifactId, remove.ServiceIds) {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
			if remove.ArtifactId != candidateArtifactID {
				if len(remove.ServiceIds) != 1 || managedRemoves[remove.ArtifactId][remove.ServiceIds[0]] != step.GetStepId() ||
					seenManagedRemoves[remove.ArtifactId+"\x00"+remove.ServiceIds[0]] {
					return retainedOwnershipReject(retainedOwnershipStepSelection)
				}
				seenManagedRemoves[remove.ArtifactId+"\x00"+remove.ServiceIds[0]] = true
			}
		case *agentpb.ExecutionStep_WaitHealthy:
			if payload.WaitHealthy == nil || payload.WaitHealthy.ArtifactId != candidateArtifactID && len(managedSources[payload.WaitHealthy.ArtifactId]) != 0 ||
				!selected(payload.WaitHealthy.ArtifactId, payload.WaitHealthy.ServiceIds) {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_CandidateRestorationProbe:
			probe := payload.CandidateRestorationProbe
			if probe == nil || probe.CandidateArtifactId != candidateArtifactID || !candidates[probe.ServiceId] {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_CandidateRestorationCompensate:
			compensate := payload.CandidateRestorationCompensate
			if compensate == nil || compensate.CandidateArtifactId != candidateArtifactID || !candidates[compensate.ServiceId] {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_RunScript:
			if payload.RunScript == nil || !candidates[payload.RunScript.ServiceId] {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		case *agentpb.ExecutionStep_MaterializeFile, *agentpb.ExecutionStep_EnvironmentDirectoryCreate,
			*agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure, *agentpb.ExecutionStep_ManagedNetworkEnsure, *agentpb.ExecutionStep_ManagedVolumeEnsure,
			*agentpb.ExecutionStep_ComponentApply:
			// Existing step validators bind these closed setup/Component actions
			// to their declared resource and managed owner; none selects natives.
		default:
			return retainedOwnershipReject(retainedOwnershipStepSelection)
		}
	}
	for artifactID, services := range managedRemoves {
		for serviceID := range services {
			if artifactID != candidateArtifactID && !seenManagedRemoves[artifactID+"\x00"+serviceID] {
				return retainedOwnershipReject(retainedOwnershipStepSelection)
			}
		}
	}
	return true, ""
}

func retainedProxyHasRelease(artifact *agentpb.ComposeArtifact, proxy *agentpb.ComposeService) bool {
	if !validSealedProxyConfig(proxy.ProxyConfigJson, proxy.ProxyConfigSha256) {
		return false
	}
	for _, workload := range artifact.Services {
		if workload.GetServiceId() != proxy.ServiceId ||
			workload.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if _, err := ProxyConfigGeneration(proxy.ProxyConfigJson, expectedReleaseLabel(workload)); err == nil {
			return true
		}
	}
	return false
}
