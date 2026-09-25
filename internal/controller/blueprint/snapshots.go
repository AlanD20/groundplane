package blueprint

import (
	"context"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func (service *Service) listBlueprintZones(
	ctx context.Context,
	environmentID string,
) ([]etcdstore.Versioned[zonerecord.Record], error) {
	zones := []etcdstore.Versioned[zonerecord.Record](nil)
	cursor := ""
	for {
		page, err := service.repository.ListZones(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
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
) ([]etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	services := []etcdstore.Versioned[servicerecord.ServiceRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
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
) ([]etcdstore.Versioned[scriptrecord.Record], int64, error) {
	scripts := []etcdstore.Versioned[scriptrecord.Record](nil)
	cursor := ""
	readRevision := int64(0)
	for {
		page, err := repository.ListScripts(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
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
) ([]etcdstore.Versioned[routerecord.Record], error) {
	routes := []etcdstore.Versioned[routerecord.Record](nil)
	cursor := ""
	for {
		page, err := service.repository.ListRoutes(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
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
) ([]etcdstore.Versioned[componentrecord.Record], error) {
	componentRecords := []etcdstore.Versioned[componentrecord.Record](nil)
	cursor := ""
	for {
		page, err := service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
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
) ([]etcdstore.Versioned[entryrecord.Record], error) {
	desired, found, err := service.repository.GetEnvironmentDesiredInput(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	identities, hasIdentities, err := service.repository.GetEnvironmentOwnedIdentities(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if found != hasIdentities || found && (identities.Revision != desired.Revision ||
		identities.Record.RevisionID != desired.Record.RevisionID) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Entry identity snapshot changed")
	}
	entriesByID := make(map[string]etcdstore.Versioned[entryrecord.Record])
	entriesByKey := make(map[string]string)
	cursor := ""
	readRevision := int64(0)
	for {
		page, err := service.repository.ListEntries(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		if readRevision == 0 {
			readRevision = page.Revision
		} else if page.Revision != readRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Entry snapshot changed while listing")
		}
		for _, item := range page.Items {
			key := entrycontroller.BlueprintKey(item.Record)
			if key == "" {
				return nil, errs.New(errs.KindInternal, "Environment Entry has no Blueprint identity")
			}
			if priorID, duplicate := entriesByKey[key]; duplicate && priorID != item.Record.Entry.ID {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Entry identity is duplicated")
			}
			entriesByKey[key] = item.Record.Entry.ID
			entriesByID[item.Record.Entry.ID] = item
		}
		if page.NextCursor == "" {
			current, currentFound, err := service.repository.GetEnvironmentDesiredInput(ctx, environmentID)
			if err != nil {
				return nil, err
			}
			if currentFound != found || found && (current.Revision != desired.Revision ||
				current.Record.RevisionID != desired.Record.RevisionID) {
				return nil, errs.New(errs.KindStateConflict, "Environment desired input changed while listing Entries")
			}
			currentIdentities, currentIdentitiesFound, err := service.repository.GetEnvironmentOwnedIdentities(ctx, environmentID)
			if err != nil {
				return nil, err
			}
			if currentIdentitiesFound != hasIdentities || hasIdentities &&
				(currentIdentities.Revision != identities.Revision ||
					currentIdentities.Record.RevisionID != identities.Record.RevisionID) {
				return nil, errs.New(errs.KindStateConflict, "Environment Entry identities changed while listing")
			}
			if found {
				authored, err := authoredEntryRecords(desired.Record, identities.Record)
				if err != nil {
					return nil, err
				}
				for _, record := range authored {
					key := entrycontroller.BlueprintKey(record)
					if flatID, exists := entriesByKey[key]; exists && flatID != record.Entry.ID {
						// An identity-changing Apply can select its replacement before
						// the prior flat record is cleaned up. Reconciliation must use
						// the head's ID; the old effect remains owned by the unit ledger.
						delete(entriesByID, flatID)
					}
					entriesByID[record.Entry.ID] = etcdstore.Versioned[entryrecord.Record]{
						Record: record, Revision: desired.Revision, ReadRevision: desired.ReadRevision,
					}
				}
			}
			entries := make([]etcdstore.Versioned[entryrecord.Record], 0, len(entriesByID))
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
) ([]etcdstore.Versioned[attachrecord.Record], int64, error) {
	attaches := []etcdstore.Versioned[attachrecord.Record](nil)
	cursor := ""
	revision := int64(0)
	for {
		page, err := service.repository.ListAttaches(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit, Cursor: cursor},
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
