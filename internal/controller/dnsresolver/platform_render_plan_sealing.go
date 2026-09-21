package dnsresolver

import (
	"encoding/hex"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func sealPlatformComponentTaskPlanHash(
	task etcd.TaskRecord,
	input platformcomponents.PlatformComponentTaskRenderInput,
	observationAction componentsdk.ActionID,
) (platformcomponents.PlatformComponentTaskRenderInput, error) {
	definitionDigest, err := componentDigest(input.DefinitionSHA256)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	catalogDigest, err := componentDigest(input.CatalogSHA256)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	artifactDigest, err := componentDigest(input.ArtifactSHA256)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	componentID, err := componentsdk.NewComponentID(input.ComponentID)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	artifactID, err := componentsdk.NewArtifactID(input.ArtifactID)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	artifact, err := componentsdk.NewArtifactReference(artifactID, artifactDigest)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: componentsdk.ActionID(input.ActionID), Artifact: artifact,
		Generation: uint64(task.RenderGeneration),
	})
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	previousArtifactDigest, err := hex.DecodeString(input.ExpectedPreviousArtifactSHA256)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"platform Component prior artifact digest is invalid",
		)
	}
	stepIDs := make([]string, len(task.Steps))
	for index, step := range task.Steps {
		stepIDs[index] = step.ID
	}
	var execution *agentpb.ExecutionPlan
	if input.DisableService {
		execution, err = taskplan.BuildComponentDisable(taskplan.ComponentDisableInput{
			VolumeRoot: environmentpath.DefaultVolumeRoot, Envelope: envelope, PlanID: task.PlanID,
			StepIDs: stepIDs, RenderGeneration: uint64(task.RenderGeneration),
			ComposeArtifact: input.ComposeArtifact, ObservationAction: observationAction,
			ExpectedPreviousArtifactDigest: previousArtifactDigest,
		})
	} else {
		execution, err = taskplan.BuildComponentAction(taskplan.ComponentActionInput{
			VolumeRoot: environmentpath.DefaultVolumeRoot, Envelope: envelope, PlanID: task.PlanID,
			StepIDs: stepIDs, RenderGeneration: uint64(task.RenderGeneration),
			ComposeArtifact: input.ComposeArtifact, RollbackComposeArtifact: input.RollbackComposeArtifact,
			EnsureService: input.EnsureService, ObservationAction: observationAction,
			ExpectedPreviousArtifactDigest: previousArtifactDigest,
			ExpectedPreviousArtifactID:     input.ExpectedPreviousArtifactID,
			ExpectedPreviousGeneration:     input.ExpectedPreviousGeneration,
		})
	}
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	input.ExecutionPlanSHA256 = hex.EncodeToString(execution.GetPlanHash())
	return input, nil
}

func componentTaskEnsureService(task etcd.TaskRecord) (bool, error) {
	switch len(task.Steps) {
	case 2:
		return false, nil
	case 4:
		return true, nil
	default:
		return false, errs.New(errs.KindInternal, "platform Component Task procedure is invalid")
	}
}
