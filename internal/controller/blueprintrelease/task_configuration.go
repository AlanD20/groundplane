package blueprintrelease

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

type preparedWorkConfiguration struct {
	Capture   runtimeconfiguration.WorkCapture
	Secrets   tasksecretpins.Prepared
	Input     releaserender.BlueprintReleaseWorkInput
	Reference workexecution.Input
	Value     []byte
}

func (service *Service) prepareWorkConfiguration(ctx context.Context, input prepareInput,
	rollout releaserender.BlueprintServiceRolloutCapture,
	files []taskmaterialization.Record,
) (result preparedWorkConfiguration, err error) {
	capture, err := service.ledger.PrepareReleaseWorkConfiguration(ctx, input.Work,
		input.AllocateNamed(ids.KindConfig, "release-configuration/"+input.Work.Item.Item.ID),
		uint64(input.Projection.RenderGeneration), files)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	secrets, err := service.ledger.PrepareReleaseWorkSecretPins(ctx, input.Work, input.OperationID, capture)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, service.ledger.AbandonReleaseWorkSecretPins(ctx, input.Work, secrets))
		}
	}()
	rolloutReference, err := releaserender.BlueprintRolloutInput(rollout)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	sealed := releaserender.BlueprintReleaseWorkInput{
		OperationID: input.OperationID, Rollout: rolloutReference, Configuration: capture,
	}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil {
			return preparedWorkConfiguration{}, err
		}
		sealed.Secrets = &binding
	}
	if err := sealed.ValidateRollout(rollout); err != nil {
		return preparedWorkConfiguration{}, err
	}
	value, reference, err := releaserender.EncodeBlueprintReleaseWorkInput(sealed)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	return preparedWorkConfiguration{Capture: capture, Secrets: secrets, Input: sealed,
		Reference: reference, Value: value}, nil
}
