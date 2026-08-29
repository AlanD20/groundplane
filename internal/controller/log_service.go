package controller

import (
	"context"
	"errors"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type LogService struct {
	environments *environmentcapability.Reader
	services     ServiceReader
	releases     *etcd.ReleaseLedger
	registry     *agentchannel.Registry
}

func NewLogService(
	environments *environmentcapability.Reader,
	services ServiceReader,
	releases *etcd.ReleaseLedger,
	registry *agentchannel.Registry,
) *LogService {
	return &LogService{environments: environments, services: services, releases: releases, registry: registry}
}

func (service *LogService) OpenEnvironment(
	ctx context.Context,
	environmentID string,
	tail uint32,
	follow bool,
) (*agentchannel.LogSubscription, error) {
	if service == nil || service.environments == nil || service.services == nil || service.releases == nil || service.registry == nil {
		return nil, errs.New(errs.KindInternal, "environment logs are not configured")
	}
	if _, err := service.environments.Get(ctx, environmentcapability.GetInput{ID: environmentID}); err != nil {
		return nil, err
	}
	page, err := service.services.ListServices(ctx, environmentID, etcd.PageRequest{Limit: 129})
	if err != nil {
		return nil, err
	}
	if len(page.Items) > 128 || page.NextCursor != "" {
		return nil, errs.New(errs.KindStateConflict, "environment exceeds 128 log service targets")
	}
	sort.Slice(page.Items, func(left, right int) bool {
		return page.Items[left].Record.Desired.ID < page.Items[right].Record.Desired.ID
	})
	targets := make([]agentchannel.LogTarget, 0, len(page.Items))
	for _, versioned := range page.Items {
		record := versioned.Record
		release, resolveErr := service.releases.ResolveServing(
			ctx, environmentID, record.Desired.ID, page.Revision,
		)
		if errors.Is(resolveErr, errs.New(errs.KindReleaseNotFound, "")) {
			continue
		}
		if resolveErr != nil {
			return nil, resolveErr
		}
		targets = append(targets, agentchannel.LogTarget{
			EnvironmentID: environmentID,
			ServiceID:     record.Desired.ID,
			ServiceName:   record.Desired.Name,
			ReleaseID:     release.Intent.ID,
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

func (service *LogService) OpenService(
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
