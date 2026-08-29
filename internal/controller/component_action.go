package controller

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ComponentActionPlanInput struct {
	VolumeRoot       string
	Envelope         componentsdk.ActionEnvelope
	PlanID           string
	StepIDs          []string
	RenderGeneration uint64
	ComposeArtifact  *agentpb.ComposeArtifact
	EnsureService    bool
}

func BuildComponentActionExecutionPlan(input ComponentActionPlanInput) (*ExecutionPlan, error) {
	componentID := input.Envelope.ComponentID().String()
	definitionDigest := input.Envelope.DefinitionDigest()
	catalogDigest := input.Envelope.CatalogDigest()
	artifact := input.Envelope.Artifact()
	artifactDigest := artifact.Digest()
	if input.EnsureService && len(input.StepIDs) != 4 || !input.EnsureService && len(input.StepIDs) != 1 {
		return nil, errs.New(errs.KindInternal, "Component action step identities are invalid")
	}
	steps := []*agentpb.ExecutionStep{{
		StepId: input.StepIDs[0], TimeoutSeconds: 300,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId:      componentID,
			DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
			CatalogDigest:    append([]byte(nil), catalogDigest[:]...),
			ActionId:         string(input.Envelope.ActionID()),
			ArtifactId:       artifact.ID().String(),
			ArtifactDigest:   append([]byte(nil), artifactDigest[:]...),
			Generation:       input.Envelope.Generation(),
		}},
	}}
	var artifacts []*agentpb.ComposeArtifact
	if input.EnsureService {
		if input.ComposeArtifact == nil || len(input.ComposeArtifact.GetServices()) != 1 {
			return nil, errs.New(errs.KindInternal, "Component Service ensure artifact is invalid")
		}
		serviceID := input.ComposeArtifact.GetServices()[0].GetServiceId()
		artifactID := input.ComposeArtifact.GetArtifactId()
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: input.StepIDs[1], TimeoutSeconds: 300,
				PrerequisiteStepId: input.StepIDs[0],
				Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}},
			},
			&agentpb.ExecutionStep{
				StepId: input.StepIDs[2], TimeoutSeconds: 300,
				PrerequisiteStepId: input.StepIDs[1],
				Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}},
			},
			&agentpb.ExecutionStep{
				StepId: input.StepIDs[3], TimeoutSeconds: 300,
				PrerequisiteStepId: input.StepIDs[2],
				Payload: &agentpb.ExecutionStep_HostResolutionApply{
					HostResolutionApply: &agentpb.HostResolutionApply{
						ComponentId: componentID,
						Generation:  input.Envelope.Generation(),
					},
				},
			},
		)
		artifacts = []*agentpb.ComposeArtifact{input.ComposeArtifact}
	} else if input.ComposeArtifact != nil {
		return nil, errs.New(errs.KindInternal, "Component reload carries an unused Compose artifact")
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: input.VolumeRoot,
		PlanID:     input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		TargetID:  componentID, Artifacts: artifacts, Steps: steps,
	})
}

func BuildComponentDisableExecutionPlan(
	volumeRoot string,
	componentID string,
	planID string,
	stepIDs []string,
	renderGeneration uint64,
	composeArtifact *agentpb.ComposeArtifact,
) (*ExecutionPlan, error) {
	if len(stepIDs) != 2 || composeArtifact == nil || len(composeArtifact.GetServices()) != 1 {
		return nil, errs.New(errs.KindInternal, "Component disable procedure is invalid")
	}
	serviceID := composeArtifact.GetServices()[0].GetServiceId()
	artifactID := composeArtifact.GetArtifactId()
	return BuildPlan(PlanBuildInput{
		VolumeRoot: volumeRoot,
		PlanID:     planID, RenderGeneration: renderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		TargetID:  componentID,
		Artifacts: []*agentpb.ComposeArtifact{composeArtifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: stepIDs[0], TimeoutSeconds: 300,
				Payload: &agentpb.ExecutionStep_HostResolutionRestore{
					HostResolutionRestore: &agentpb.HostResolutionRestore{
						ComponentId: componentID, Generation: renderGeneration,
					},
				},
			},
			{
				StepId: stepIDs[1], TimeoutSeconds: 300,
				PrerequisiteStepId: stepIDs[0],
				Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}},
			},
		},
	})
}
