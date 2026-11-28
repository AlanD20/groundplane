package gitdelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RegistryJobSnapshot fences the build to the current binding and deployment.
// The caller must verify provider and Runner identity before issuing access.
type RegistryJobSnapshot struct {
	Run        Run
	Conditions []keyvalue.Condition
}

func (service *Service) CaptureRegistryJob(ctx context.Context, id string) (RegistryJobSnapshot, error) {
	if ids.Validate(ids.KindOperation, id) != nil {
		return RegistryJobSnapshot{}, registryJobDenied()
	}
	view, err := service.repository.run(ctx, id)
	if errs.IsNotFound(err) {
		return RegistryJobSnapshot{}, registryJobDenied()
	}
	if err != nil {
		return RegistryJobSnapshot{}, err
	}
	if !view.isLatest {
		return RegistryJobSnapshot{}, registryJobDenied()
	}
	if view.run.Phase == Dispatching {
		return RegistryJobSnapshot{}, errs.New(errs.KindStateConflict, "GitHub dispatch has not been confirmed")
	}
	if view.run.Phase != Building || view.run.GitHubRunID <= 0 || view.run.GitHubRunAttempt <= 0 || view.head <= 0 {
		return RegistryJobSnapshot{}, registryJobDenied()
	}
	serviceID := view.run.Binding.ServiceID
	return RegistryJobSnapshot{
		Run: view.run,
		Conditions: []keyvalue.Condition{
			condition(runKey(id), view.rev),
			condition(headKey(serviceID), view.head),
			condition(bindingKey(serviceID), view.run.BindingRevision),
		},
	}, nil
}

func registryJobDenied() *errs.Error {
	return errs.New(errs.KindRequestForbidden, "build job is not authorized")
}
