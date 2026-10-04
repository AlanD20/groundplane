package servicelogs

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	environmentcapability "github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServiceReader interface {
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
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
	targets := make([]agentchannel.LogTarget, 0)
	pageRequest := etcdstore.PageRequest{Limit: 128}
	seenCursors := make(map[string]bool)
	for {
		page, err := service.services.ListServices(ctx, environmentID, pageRequest)
		if err != nil {
			return nil, err
		}
		if page.Revision <= 0 || pageRequest.Revision != 0 && page.Revision != pageRequest.Revision {
			return nil, errs.New(errs.KindStateConflict, "Environment log snapshot changed")
		}
		pageRequest.Revision = page.Revision
		for _, record := range page.Items {
			if record.ReadRevision != page.Revision || record.Record.EnvironmentID != environmentID {
				return nil, errs.New(errs.KindStateConflict, "Environment log source is outside its snapshot")
			}
			target, err := serviceobservation.CaptureLogSource(ctx, service.releases, record)
			if err != nil {
				return nil, err
			}
			if target != nil {
				targets = append(
					targets,
					agentchannel.LogTarget{Runtime: target, ServiceName: record.Record.Desired.Name},
				)
			}
			if len(targets) > 128 {
				return nil, errs.New(errs.KindStateConflict, "Environment exceeds the log target limit")
			}
		}
		if page.NextCursor == "" {
			break
		}
		if seenCursors[page.NextCursor] || ctx.Err() != nil {
			return nil, errs.New(errs.KindStateConflict, "Environment log pagination could not complete")
		}
		seenCursors[page.NextCursor] = true
		pageRequest.Cursor = page.NextCursor
	}
	sort.Slice(targets, func(left, right int) bool {
		return targets[left].Runtime.ServiceId < targets[right].Runtime.ServiceId
	})
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
	target, err := serviceobservation.CaptureLogSource(ctx, service.releases, versioned)
	if err != nil {
		return nil, err
	}
	if target != nil {
		targets = append(targets, agentchannel.LogTarget{
			Runtime:     target,
			ServiceName: record.Desired.Name,
		})
	}
	return service.registry.OpenLogs(
		ctx,
		agentchannel.LogScope{EnvironmentID: record.EnvironmentID, ServiceID: record.Desired.ID},
		targets,
		tail,
		follow,
	)
}
