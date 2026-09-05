package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/infra/docker/workloadimages"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

func configureAgentWorkloadImages(ctx context.Context, target *agent.Client, resources *agentRuntimeResources) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	resources.images = engine
	resolver, err := workloadimages.New(engine)
	if err != nil {
		return err
	}
	return target.SetWorkloadImageResolver(resolver)
}
