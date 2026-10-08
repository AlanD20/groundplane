package backingservices

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type creationImageRegistry interface {
	Resolve(context.Context, string) (imagefetch.Plan, error)
}

type creationImage struct {
	localID      string
	architecture string
	variant      string
}

// sealCreationImage captures exact host bytes before publishing native runtime.
// Desired state keeps the authored family tag; execution never resolves it again.
func (service *CreationService) sealCreationImage(ctx context.Context, reference string) (creationImage, error) {
	if service.agents == nil || service.images == nil || service.imageRegistry == nil {
		return creationImage{}, errs.New(errs.KindInternal, "Backing creation image authority is unavailable")
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return creationImage{}, err
	}
	selected, err := service.imageRegistry.Resolve(ctx, reference)
	if err != nil {
		return creationImage{}, err
	}
	if selected.Requested != reference || selected.Validate() != nil {
		return creationImage{}, errs.New(errs.KindStateConflict, "Backing creation image selection changed")
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID,
		[]workloadseal.Selection{{Requested: &workloadseal.Requested{Reference: selected.Reference(), Replicas: 1}}})
	if err != nil {
		return creationImage{}, err
	}
	return creationImage{
		localID:      seals[0].LocalImageID,
		architecture: selected.Architecture,
		variant:      selected.Variant,
	}, nil
}
