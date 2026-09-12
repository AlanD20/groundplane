package controller

import (
	"bytes"
	"crypto/sha256"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ComponentActionPlanInput struct {
	VolumeRoot                     string
	Envelope                       componentsdk.ActionEnvelope
	PlanID                         string
	StepIDs                        []string
	RenderGeneration               uint64
	ComposeArtifact                *agentpb.ComposeArtifact
	RollbackComposeArtifact        *agentpb.ComposeArtifact
	EnsureService                  bool
	ObservationAction              componentsdk.ActionID
	ExpectedPreviousArtifactDigest []byte
	ExpectedPreviousArtifactID     string
	ExpectedPreviousGeneration     uint64
}

func BuildComponentActionExecutionPlan(input ComponentActionPlanInput) (*ExecutionPlan, error) {
	componentID := input.Envelope.ComponentID().String()
	definitionDigest := input.Envelope.DefinitionDigest()
	catalogDigest := input.Envelope.CatalogDigest()
	artifact := input.Envelope.Artifact()
	artifactDigest := artifact.Digest()
	if input.ObservationAction == "" || input.EnsureService && len(input.StepIDs) != 4 ||
		!input.EnsureService && len(input.StepIDs) != 2 ||
		(len(input.ExpectedPreviousArtifactDigest) == 0) != (input.ExpectedPreviousArtifactID == "") ||
		(len(input.ExpectedPreviousArtifactDigest) == 0) != (input.ExpectedPreviousGeneration == 0) {
		return nil, errs.New(errs.KindInternal, "Component action step identities are invalid")
	}
	lifecycleMode, err := componentActionLifecycleMode(
		input.EnsureService,
		input.RollbackComposeArtifact,
		input.ExpectedPreviousArtifactDigest,
	)
	if err != nil {
		return nil, err
	}
	steps := []*agentpb.ExecutionStep{{
		StepId: input.StepIDs[0], TimeoutSeconds: 300,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId:                    componentID,
			DefinitionDigest:               append([]byte(nil), definitionDigest[:]...),
			CatalogDigest:                  append([]byte(nil), catalogDigest[:]...),
			ActionId:                       string(input.Envelope.ActionID()),
			ArtifactId:                     artifact.ID().String(),
			ArtifactDigest:                 append([]byte(nil), artifactDigest[:]...),
			ExpectedPreviousArtifactDigest: append([]byte(nil), input.ExpectedPreviousArtifactDigest...),
			ExpectedPreviousArtifactId:     input.ExpectedPreviousArtifactID,
			ExpectedPreviousGeneration:     input.ExpectedPreviousGeneration,
			Generation:                     input.Envelope.Generation(),
			ManagedConfigContent:           true,
		}},
	}}
	observation := protoComponentAction(input.Envelope, input.ObservationAction)
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
				Payload:            &agentpb.ExecutionStep_ComponentApply{ComponentApply: observation},
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
	} else {
		if input.ComposeArtifact == nil || len(input.ComposeArtifact.GetServices()) != 1 {
			return nil, errs.New(errs.KindInternal, "Component reload observation artifact is invalid")
		}
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: input.StepIDs[1], TimeoutSeconds: 300,
			PrerequisiteStepId: input.StepIDs[0],
			Payload:            &agentpb.ExecutionStep_ComponentApply{ComponentApply: observation},
		})
	}
	artifacts = []*agentpb.ComposeArtifact{input.ComposeArtifact}
	if input.RollbackComposeArtifact != nil {
		artifacts = append(artifacts, input.RollbackComposeArtifact)
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: input.VolumeRoot,
		PlanID:     input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		TargetID:  componentID, Artifacts: artifacts, Steps: steps,
		ComponentLifecycleMode: lifecycleMode,
	})
}

func componentActionLifecycleMode(
	ensureService bool,
	rollbackComposeArtifact *agentpb.ComposeArtifact,
	expectedPreviousArtifactDigest []byte,
) (agentpb.ComponentLifecycleMode, error) {
	switch len(expectedPreviousArtifactDigest) {
	case 0, sha256.Size:
	default:
		return agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED, errs.New(
			errs.KindInternal,
			"Component action predecessor digest is invalid",
		)
	}
	if rollbackComposeArtifact != nil {
		if !ensureService || len(expectedPreviousArtifactDigest) != sha256.Size {
			return agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED, errs.New(
				errs.KindInternal,
				"Component action rollback artifact is invalid",
			)
		}
		return agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE, nil
	}
	if ensureService {
		return agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE, nil
	}
	return agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE, nil
}

func protoComponentAction(
	envelope componentsdk.ActionEnvelope,
	actionID componentsdk.ActionID,
) *agentpb.ComponentApply {
	definitionDigest := envelope.DefinitionDigest()
	catalogDigest := envelope.CatalogDigest()
	artifact := envelope.Artifact()
	artifactDigest := artifact.Digest()
	return &agentpb.ComponentApply{
		ComponentId: envelope.ComponentID().String(), DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
		CatalogDigest: append([]byte(nil), catalogDigest[:]...), ActionId: string(actionID),
		ArtifactId: artifact.ID().String(), ArtifactDigest: append([]byte(nil), artifactDigest[:]...),
		Generation: envelope.Generation(),
	}
}

type ComponentDisablePlanInput struct {
	VolumeRoot                     string
	Envelope                       componentsdk.ActionEnvelope
	PlanID                         string
	StepIDs                        []string
	RenderGeneration               uint64
	ComposeArtifact                *agentpb.ComposeArtifact
	ObservationAction              componentsdk.ActionID
	ExpectedPreviousArtifactDigest []byte
}

func BuildComponentDisableExecutionPlan(input ComponentDisablePlanInput) (*ExecutionPlan, error) {
	if len(input.StepIDs) != 2 || input.ComposeArtifact == nil ||
		len(input.ComposeArtifact.GetServices()) != 1 || input.ObservationAction == "" ||
		len(input.ExpectedPreviousArtifactDigest) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "Component disable procedure is invalid")
	}
	rollbackObservation := protoComponentAction(input.Envelope, input.ObservationAction)
	if rollbackObservation.GetManagedConfigContent() ||
		!bytes.Equal(rollbackObservation.GetArtifactDigest(), input.ExpectedPreviousArtifactDigest) {
		return nil, errs.New(errs.KindInternal, "Component disable rollback observation is invalid")
	}
	componentID := input.Envelope.ComponentID().String()
	serviceID := input.ComposeArtifact.GetServices()[0].GetServiceId()
	artifactID := input.ComposeArtifact.GetArtifactId()
	return BuildPlan(PlanBuildInput{
		VolumeRoot: input.VolumeRoot,
		PlanID:     input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		TargetID:  componentID,
		Artifacts: []*agentpb.ComposeArtifact{input.ComposeArtifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: input.StepIDs[0], TimeoutSeconds: 300,
				Payload: &agentpb.ExecutionStep_HostResolutionRestore{
					HostResolutionRestore: &agentpb.HostResolutionRestore{
						ComponentId: componentID, Generation: input.RenderGeneration,
					},
				},
			},
			{
				StepId:             input.StepIDs[1],
				TimeoutSeconds:     300,
				PrerequisiteStepId: input.StepIDs[0],
				Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}},
			},
		},
		ComponentLifecycleMode:       agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE,
		ComponentRollbackObservation: rollbackObservation,
	})
}
