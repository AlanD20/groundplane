package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func (service *Service) listBlueprintZones(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[zonerecord.Record], error) {
	zones := []etcd.Versioned[zonerecord.Record](nil)
	cursor := ""
	for {
		page, err := service.repository.ListZones(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		zones = append(zones, page.Items...)
		if page.NextCursor == "" {
			return zones, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintServices(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ServiceRecord], error) {
	services := []etcd.Versioned[etcd.ServiceRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		services = append(services, page.Items...)
		if page.NextCursor == "" {
			return services, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintScripts(
	ctx context.Context,
	environmentID string,
	repository environmentBlueprintRepository,
) ([]etcd.Versioned[scriptrecord.Record], int64, error) {
	scripts := []etcd.Versioned[scriptrecord.Record](nil)
	cursor := ""
	readRevision := int64(0)
	for {
		page, err := repository.ListScripts(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, 0, err
		}
		if readRevision == 0 {
			readRevision = page.Revision
		} else if page.Revision != readRevision {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Script snapshot changed while listing")
		}
		scripts = append(scripts, page.Items...)
		if page.NextCursor == "" {
			return scripts, readRevision, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintRoutes(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[routerecord.Record], error) {
	routes := []etcd.Versioned[routerecord.Record](nil)
	cursor := ""
	for {
		page, err := service.repository.ListRoutes(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		routes = append(routes, page.Items...)
		if page.NextCursor == "" {
			return routes, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintComponents(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ComponentRecord], error) {
	componentRecords := []etcd.Versioned[etcd.ComponentRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		componentRecords = append(componentRecords, page.Items...)
		if page.NextCursor == "" {
			return componentRecords, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintEntries(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[entryrecord.Record], error) {
	entriesByID := make(map[string]etcd.Versioned[entryrecord.Record])
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			entriesByID[record.Entry.ID] = etcd.Versioned[entryrecord.Record]{
				Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
			}
		}
	}
	cursor := ""
	for {
		page, err := service.repository.ListEntries(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if _, projected := entriesByID[item.Record.Entry.ID]; !projected {
				entriesByID[item.Record.Entry.ID] = item
			}
		}
		if page.NextCursor == "" {
			entries := make([]etcd.Versioned[entryrecord.Record], 0, len(entriesByID))
			for _, item := range entriesByID {
				entries = append(entries, item)
			}
			sort.Slice(entries, func(left, right int) bool {
				return entries[left].Record.Entry.ID < entries[right].Record.Entry.ID
			})
			return entries, nil
		}
		cursor = page.NextCursor
	}
}

func (service *Service) listBlueprintAttaches(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.AttachRecord], int64, error) {
	attaches := []etcd.Versioned[etcd.AttachRecord](nil)
	cursor := ""
	revision := int64(0)
	for {
		page, err := service.repository.ListAttaches(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: etcd.MaximumPageLimit, Cursor: cursor},
		)
		if err != nil {
			return nil, 0, err
		}
		if page.Revision <= 0 || revision != 0 && page.Revision != revision {
			return nil, 0, errs.New(errs.KindInternal, "Blueprint Attach topology pages changed revision")
		}
		revision = page.Revision
		attaches = append(attaches, page.Items...)
		if page.NextCursor == "" {
			return attaches, revision, nil
		}
		cursor = page.NextCursor
	}
}
