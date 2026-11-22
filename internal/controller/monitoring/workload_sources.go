package monitoring

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type WorkloadSourceReader struct {
	services ServiceInventory
	releases serviceobservation.Releases
}

func NewWorkloadSourceReader(
	services ServiceInventory,
	releases serviceobservation.Releases,
) (*WorkloadSourceReader, error) {
	if services == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "workload monitoring source readers are required")
	}
	return &WorkloadSourceReader{services: services, releases: releases}, nil
}

func (reader *WorkloadSourceReader) Capture(
	ctx context.Context,
	target InventoryTarget,
) (CapturedWorkload, bool) {
	if reader == nil {
		return CapturedWorkload{}, false
	}
	authority, ok := serviceobservation.CaptureMonitoring(ctx, reader.releases, target.Service)
	if !ok {
		return CapturedWorkload{}, false
	}
	return CapturedWorkload{
		Authority: authority, ObservationTarget: authority.Target(), VolumeTargets: authority.Volumes(),
		Expected: authority.ExpectedReplicas(), Fingerprint: authority.Fingerprint(),
	}, true
}

func (reader *WorkloadSourceReader) Recheck(ctx context.Context, captured []WorkloadCapture) []bool {
	result := make([]bool, len(captured))
	if reader == nil || len(captured) == 0 {
		return result
	}
	byEnvironment := make(map[string][]int)
	for index, item := range captured {
		byEnvironment[item.Target.Service.Record.EnvironmentID] = append(
			byEnvironment[item.Target.Service.Record.EnvironmentID],
			index,
		)
	}
	for environmentID, indexes := range byEnvironment {
		current, ok := reader.currentServices(ctx, environmentID, captured, indexes)
		if !ok {
			continue
		}
		for _, index := range indexes {
			serviceID := captured[index].Target.Service.Record.Desired.ID
			service, found := current[serviceID]
			result[index] = found && serviceobservation.RecheckMonitoring(
				ctx,
				reader.releases,
				service,
				captured[index].Source.Authority,
			)
		}
	}
	return result
}

func (reader *WorkloadSourceReader) currentServices(
	ctx context.Context,
	environmentID string,
	captured []WorkloadCapture,
	indexes []int,
) (map[string]etcdstore.Versioned[servicerecord.ServiceRecord], bool) {
	wanted := make(map[string]bool, len(indexes))
	minimumRevision := int64(0)
	for _, index := range indexes {
		service := captured[index].Target.Service
		wanted[service.Record.Desired.ID] = true
		if service.ReadRevision > minimumRevision {
			minimumRevision = service.ReadRevision
		}
	}
	current := make(map[string]etcdstore.Versioned[servicerecord.ServiceRecord], len(indexes))
	request := etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit}
	seen := make(map[string]bool)
	for ctx.Err() == nil {
		page, err := reader.services.ListServices(ctx, environmentID, request)
		if err != nil || page.Revision < minimumRevision ||
			request.Revision != 0 && page.Revision != request.Revision ||
			len(page.Items) > request.Limit {
			return nil, false
		}
		request.Revision = page.Revision
		for _, service := range page.Items {
			if service.ReadRevision != page.Revision || service.Record.EnvironmentID != environmentID {
				return nil, false
			}
			if wanted[service.Record.Desired.ID] {
				current[service.Record.Desired.ID] = service
			}
		}
		if len(current) == len(wanted) || page.NextCursor == "" {
			return current, true
		}
		if seen[page.NextCursor] {
			return nil, false
		}
		seen[page.NextCursor], request.Cursor = true, page.NextCursor
	}
	return nil, false
}
