package executionplan

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validRetainedOwnership(
	plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact, serviceID string, labels map[string]string,
) (bool, retainedOwnershipReason) {
	if plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		return validBlueprintRetainedOwnership(plan, artifact, serviceID, labels)
	}
	if validOrdinaryRetainedOwnership(plan, artifact, serviceID, labels) {
		return true, ""
	}
	return retainedOwnershipReject(retainedOwnershipInvalidInput)
}

// Historical ownership is lawful only for the exact native predecessor named
// by this ordinary Release. It never authorizes applying an old artifact as a
// forward candidate, or selecting an unrelated Service from historical state.
func validOrdinaryRetainedOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DEPLOY &&
		plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		labels[labelComponentID] != "" || ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil {
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	if err != nil || generation == 0 || generation > plan.GetRenderGeneration() ||
		strconv.FormatUint(generation, 10) != labels[labelRenderGen] {
		return false
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, value := range plan.GetArtifacts() {
		if value == nil || artifacts[value.GetArtifactId()] != nil {
			return false
		}
		artifacts[value.GetArtifactId()] = value
	}
	if !proto.Equal(artifacts[artifact.GetArtifactId()], artifact) {
		return false
	}
	var selected *agentpb.CandidateReleaseMember
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		prior := member.GetServingPredecessor()
		if member.GetServiceId() != serviceID || prior == nil ||
			artifact.GetArtifactId() != prior.GetPriorArtifactId() && artifact.GetArtifactId() != prior.GetRetainedPriorArtifactId() {
			continue
		}
		candidate := artifacts[member.GetCandidateArtifactId()]
		if selected != nil || candidate == nil || candidate.OwnerId != artifact.OwnerId ||
			candidate.ProjectName != artifact.ProjectName || candidate.AuthorizedVolumeDir != artifact.AuthorizedVolumeDir ||
			validateCandidateServingPredecessorReferences(prior, serviceID, member.GetCandidateArtifactId(), artifacts) != nil {
			return false
		}
		selected = member
	}
	if selected == nil {
		return false
	}
	for _, step := range plan.GetSteps() {
		switch payload := step.GetPayload().(type) {
		case *agentpb.ExecutionStep_ComposeApply:
			if payload.ComposeApply.GetArtifactId() == artifact.ArtifactId {
				return false
			}
		case *agentpb.ExecutionStep_ComposeStop:
			if payload.ComposeStop.GetArtifactId() == artifact.ArtifactId {
				return false
			}
		case *agentpb.ExecutionStep_WaitHealthy:
			if payload.WaitHealthy.GetArtifactId() == artifact.ArtifactId {
				return false
			}
		case *agentpb.ExecutionStep_ComposeRemove:
			remove := payload.ComposeRemove
			if remove.GetArtifactId() == artifact.ArtifactId &&
				(remove.GetWholeProject() || !slices.Equal(remove.GetServiceIds(), []string{serviceID}) ||
					artifact.ArtifactId != selected.GetServingPredecessor().GetPriorArtifactId() ||
					!slices.Contains(selected.GetForwardStepIds(), step.GetStepId()) ||
					step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD) {
				return false
			}
		}
	}
	return true
}
