package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingaccesscheck"
	"github.com/AlanD20/groundplane/internal/infra/docker/serviceobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/workloadimages"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

func configureAgentDockerReads(
	ctx context.Context,
	target *agent.Client,
	resources *agentRuntimeResources,
	componentActions *componentaction.Runtime,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	resources.dockerReads = engine
	resolver, err := workloadimages.New(engine)
	if err != nil {
		return err
	}
	if err := target.SetWorkloadImageResolver(resolver); err != nil {
		return err
	}
	observer, err := serviceobserver.NewWithEngine(engine)
	if err != nil {
		return err
	}
	if err := target.SetServiceObserver(observer); err != nil {
		return err
	}
	checks, err := backingaccesscheck.NewExchange(engine)
	if err != nil {
		return err
	}
	if err := target.SetBackingAccessChecker(checks); err != nil {
		return err
	}
	edgeChecks, err := componentaction.NewEdgeConnectionChecker(componentActions, engine)
	if err != nil {
		return err
	}
	return target.SetEdgeConnectionChecker(edgeChecks)
}
