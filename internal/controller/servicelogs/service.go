package servicelogs

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServiceReader interface {
	GetService(context.Context, string) (etcdstore.Versioned[etcd.ServiceRecord], error)
}

type Service struct {
	environments *environmentcapability.Reader
	services     ServiceReader
	releases     *etcd.ReleaseLedger
	registry     *agentchannel.Registry
}

func New(
	environments *environmentcapability.Reader,
	services ServiceReader,
	releases *etcd.ReleaseLedger,
	registry *agentchannel.Registry,
) *Service {
	return &Service{environments: environments, services: services, releases: releases, registry: registry}
}

func (service *Service) OpenEnvironment(
	ctx context.Context,
	environmentID string,
	tail uint32,
	follow bool,
) (*agentchannel.LogSubscription, error) {
	if service == nil || service.environments == nil || service.services == nil || service.releases == nil ||
		service.registry == nil {
		return nil, errs.New(errs.KindInternal, "environment logs are not configured")
	}
	resolved, err := service.releases.ResolveEnvironmentLogTargets(ctx, environmentID, 128)
	if err != nil {
		return nil, err
	}
	targets := make([]agentchannel.LogTarget, 0, len(resolved))
	for _, target := range resolved {
		targets = append(targets, agentchannel.LogTarget{
			EnvironmentID: environmentID,
			ServiceID:     target.ServiceID,
			ServiceName:   target.ServiceName,
			ReleaseID:     target.ReleaseID,
		})
	}
	return service.registry.OpenLogs(
		ctx,
		agentchannel.LogScope{EnvironmentID: environmentID},
		targets,
		tail,
		follow,
	)
}

func (service *Service) OpenService(
	ctx context.Context,
	serviceID string,
	tail uint32,
	follow bool,
) (*agentchannel.LogSubscription, error) {
	if service == nil || service.services == nil || service.releases == nil || service.registry == nil {
		return nil, errs.New(errs.KindInternal, "service logs are not configured")
	}
	versioned, err := service.services.GetService(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	record := versioned.Record
	targets := make([]agentchannel.LogTarget, 0, 1)
	release, err := service.releases.ResolveServing(
		ctx, record.EnvironmentID, record.Desired.ID, versioned.ReadRevision,
	)
	if err == nil {
		targets = append(targets, agentchannel.LogTarget{
			EnvironmentID: record.EnvironmentID,
			ServiceID:     record.Desired.ID,
			ServiceName:   record.Desired.Name,
			ReleaseID:     release.Intent.ID,
		})
	} else if !errors.Is(err, errs.New(errs.KindReleaseNotFound, "")) {
		return nil, err
	}
	return service.registry.OpenLogs(
		ctx,
		agentchannel.LogScope{EnvironmentID: record.EnvironmentID, ServiceID: record.Desired.ID},
		targets,
		tail,
		follow,
	)
}
