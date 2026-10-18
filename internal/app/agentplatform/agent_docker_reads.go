package agentplatform

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

// ConfigureDockerReads returns the owned Docker connection even when later wiring
// fails, so the composition owner can close all acquired resources together.
func ConfigureDockerReads(
	ctx context.Context,
	target *agent.Client,
	componentActions *componentaction.Runtime,
) (*client.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	resolver, err := workloadimages.New(engine)
	if err != nil {
		return engine, err
	}
	if err := target.SetWorkloadImageResolver(resolver); err != nil {
		return engine, err
	}
	observer, err := serviceobserver.NewWithEngine(engine)
	if err != nil {
		return engine, err
	}
	if err := target.SetServiceObserver(observer); err != nil {
		return engine, err
	}
	checks, err := backingaccesscheck.NewExchange(engine)
	if err != nil {
		return engine, err
	}
	if err := target.SetBackingAccessChecker(checks); err != nil {
		return engine, err
	}
	edgeChecks, err := componentaction.NewEdgeConnectionChecker(componentActions, engine)
	if err != nil {
		return engine, err
	}
	return engine, target.SetEdgeConnectionChecker(edgeChecks)
}
