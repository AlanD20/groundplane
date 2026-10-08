package backingservices

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// sealCreationImage captures exact host bytes before publishing native runtime.
// Desired state keeps the authored family tag; execution never resolves it again.
func (service *CreationService) sealCreationImage(ctx context.Context, reference string) (string, error) {
	if service.agents == nil || service.images == nil {
		return "", errs.New(errs.KindInternal, "Backing creation image authority is unavailable")
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return "", err
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID,
		[]workloadseal.Selection{{Requested: &workloadseal.Requested{Reference: reference, Replicas: 1}}})
	if err != nil {
		return "", err
	}
	return seals[0].LocalImageID, nil
}
