package composehelper

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateRequest(
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperRequest, *agentpb.ExecutionStep, *agentpb.ComposeArtifact, error) {
	if request == nil || request.Schema != SchemaVersion {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper request schema is unsupported")
	}
	if err := executionplan.RejectUnknown(request); err != nil {
		return nil, nil, nil, err
	}
	if ids.Validate(ids.KindAssignment, request.AssignmentId) != nil ||
		ids.Validate(ids.KindTask, request.TaskId) != nil ||
		ids.Validate(ids.KindOperation, request.OperationId) != nil {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper task identity is invalid")
	}
	if request.TimeoutSeconds == 0 || request.TimeoutSeconds > maximumTimeout {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper timeout is invalid")
	}
	plan, err := executionplan.Validate(request.Plan)
	if err != nil {
		return nil, nil, nil, err
	}
	var selected *agentpb.ExecutionStep
	for _, step := range plan.Steps {
		if step.StepId == request.StepId {
			selected = step
			break
		}
	}
	if selected == nil || request.TimeoutSeconds > selected.TimeoutSeconds {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper step selection is invalid")
	}
	artifactID := ""
	var artifact *agentpb.ComposeArtifact
	switch payload := selected.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		artifactID = payload.ComposeApply.ArtifactId
	case *agentpb.ExecutionStep_ManagedNetworkEnsure:
		artifactID = payload.ManagedNetworkEnsure.ArtifactId
	case *agentpb.ExecutionStep_ManagedVolumeEnsure:
		artifactID = payload.ManagedVolumeEnsure.ArtifactId
	case *agentpb.ExecutionStep_ComposeStop:
		artifactID = payload.ComposeStop.ArtifactId
	case *agentpb.ExecutionStep_ComposeRemove:
		artifactID = payload.ComposeRemove.ArtifactId
	case *agentpb.ExecutionStep_ManagedNetworkRemove, *agentpb.ExecutionStep_ManagedVolumeRemove:
		artifactID = ""
	case *agentpb.ExecutionStep_ComponentApply:
		artifact, err = componentActionArtifact(
			plan.GetOperation(), plan.GetSteps(), plan.GetArtifacts(),
			payload.ComponentApply.GetArtifactId(), payload.ComponentApply.GetComponentId(),
		)
		if err != nil {
			return nil, nil, nil, err
		}
	case *agentpb.ExecutionStep_ComposeWorkloadApply:
		artifactID = payload.ComposeWorkloadApply.ArtifactId
	case *agentpb.ExecutionStep_ServiceProxySwitch:
		artifactID = payload.ServiceProxySwitch.CandidateArtifactId
	case *agentpb.ExecutionStep_ServiceProxyProbe:
		artifactID = payload.ServiceProxyProbe.CandidateArtifactId
	case *agentpb.ExecutionStep_ServiceProxyCompensate:
		artifactID = payload.ServiceProxyCompensate.CandidateArtifactId
	case *agentpb.ExecutionStep_ServiceRecreateCompensate:
		artifactID = payload.ServiceRecreateCompensate.ArtifactId
	case *agentpb.ExecutionStep_CandidateRestorationProbe:
		artifactID = payload.CandidateRestorationProbe.CandidateArtifactId
	case *agentpb.ExecutionStep_CandidateRestorationCompensate:
		artifactID = payload.CandidateRestorationCompensate.CandidateArtifactId
	default:
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper step payload is unsupported")
	}
	if artifact == nil {
		for _, candidate := range plan.Artifacts {
			if candidate.ArtifactId == artifactID {
				artifact = candidate
				break
			}
		}
	}
	if artifactID != "" && artifact == nil {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Compose helper artifact selection is invalid")
	}
	candidateRestoration := selected.GetCandidateRestorationProbe() != nil ||
		selected.GetCandidateRestorationCompensate() != nil
	if candidateRestoration != (request.GetRestorationAuthority() != nil) {
		return nil, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Compose helper restoration authority presence is invalid",
		)
	}
	owned := proto.CloneOf(request)
	owned.Plan = plan
	for _, step := range owned.Plan.Steps {
		if step.StepId == request.StepId {
			selected = step
			break
		}
	}
	for _, candidate := range owned.Plan.Artifacts {
		if artifact != nil && candidate.ArtifactId == artifact.ArtifactId {
			artifact = candidate
			break
		}
	}
	return owned, selected, artifact, nil
}

func componentActionArtifact(
	operation agentpb.PlanOperation,
	steps []*agentpb.ExecutionStep,
	artifacts []*agentpb.ComposeArtifact,
	materializationID string,
	componentID string,
) (*agentpb.ComposeArtifact, error) {
	if operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		materializations := 0
		boundArtifactID := ""
		for _, step := range steps {
			if step == nil {
				return nil, errs.New(errs.KindValidationFailed, "Compose helper Component prerequisite is missing")
			}
			materialization := step.GetMaterializeFile()
			if materialization != nil && materialization.GetMaterializationId() == materializationID {
				materializations++
				boundArtifactID = materialization.GetArtifactId()
			}
		}
		if materializations != 1 {
			return nil, errs.New(errs.KindValidationFailed, "Compose helper Component materialization is not unique")
		}
		bound := make([]*agentpb.ComposeArtifact, 0, 1)
		for _, artifact := range artifacts {
			if artifact != nil && artifact.GetArtifactId() == boundArtifactID {
				bound = append(bound, artifact)
			}
		}
		if len(bound) != 1 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Compose helper Component materialization artifact is invalid",
			)
		}
		artifacts = bound
	}
	var selected *agentpb.ComposeArtifact
	for _, artifact := range artifacts {
		for _, service := range artifact.GetServices() {
			if service.GetOwnerComponentId() != componentID {
				continue
			}
			if selected != nil {
				return nil, errs.New(errs.KindValidationFailed, "Compose helper Component target is not unique")
			}
			selected = artifact
		}
	}
	if selected == nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose helper Component target is absent")
	}
	return selected, nil
}
