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
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedWorkConfiguration struct {
	Capture   runtimeconfiguration.WorkCapture
	Secrets   tasksecretpins.Prepared
	Input     releaserender.ReleaseWorkInput
	Reference workexecution.Input
	Value     []byte
}

func (service *Service) prepareWorkConfiguration(ctx context.Context, input prepareInput,
	rollout releaserender.ServiceRolloutCapture,
	files []taskmaterialization.Record,
) (result preparedWorkConfiguration, err error) {
	var capture runtimeconfiguration.WorkCapture
	if input.Configuration != nil {
		capture = *input.Configuration
		capture.Destinations = append([]string(nil), capture.Destinations...)
		if len(files) != 0 || capture.Matches(input.Work.Task.Task, input.Work.Item.Item) != nil ||
			capture.Generation != uint64(input.Projection.RenderGeneration) {
			return preparedWorkConfiguration{}, errs.New(
				errs.KindStateConflict,
				"Release configuration differs from its accepted capture",
			)
		}
	} else {
		capture, err = service.ledger.PrepareReleaseWorkConfiguration(ctx, input.Work,
			input.AllocateNamed(ids.KindConfig, "release-configuration/"+input.Work.Item.Item.ID),
			uint64(input.Projection.RenderGeneration), files)
		if err != nil {
			return preparedWorkConfiguration{}, err
		}
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
	rolloutReference, err := releaserender.ServiceRolloutInput(rollout)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	sealed := releaserender.ReleaseWorkInput{
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
	value, reference, err := releaserender.EncodeReleaseWorkInput(sealed)
	if err != nil {
		return preparedWorkConfiguration{}, err
	}
	return preparedWorkConfiguration{Capture: capture, Secrets: secrets, Input: sealed,
		Reference: reference, Value: value}, nil
}
