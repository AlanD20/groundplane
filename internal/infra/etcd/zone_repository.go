package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneRepository owns Zone allocation mutations while its reads join the
// selected immutable Environment desired projection.
type ZoneRepository struct {
	store hierarchyStore
}

func NewZoneRepository(store etcdstore.Store) (*ZoneRepository, error) {
	return newZoneRepository(store)
}

func newZoneRepository(store hierarchyStore) (*ZoneRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Zone store is required")
	}
	return &ZoneRepository{store: store}, nil
}

func (repository *ZoneRepository) GetZone(ctx context.Context, id string) (Versioned[ZoneRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	if err := validateID(ids.KindNetwork, id); err != nil {
		return Versioned[ZoneRecord]{}, err
	}
	return findZoneAtRevision(ctx, repository.store, id, 0)
}

func (repository *ZoneRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ZoneRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Page[ZoneRecord]{}, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ZoneRecord]{}, err
	}
	limit, revision, lastID, query, err := normalizePageRequest(
		request, "zones", "environment", environmentID, "", ids.KindNetwork,
	)
	if err != nil {
		return Page[ZoneRecord]{}, err
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, environmentID, revision,
	)
	if err != nil {
		return Page[ZoneRecord]{}, err
	}
	if !found {
		return Page[ZoneRecord]{Items: []Versioned[ZoneRecord]{}, Revision: projection.ReadRevision}, nil
	}
	desired := append([]EnvironmentZoneProjection(nil), projection.Record.DesiredZones...)
	sort.Slice(desired, func(left, right int) bool {
		return desired[left].Desired.ID < desired[right].Desired.ID
	})
	start := sort.Search(len(desired), func(index int) bool {
		return desired[index].Desired.ID > lastID
	})
	end := min(start+limit, len(desired))
	items := make([]Versioned[ZoneRecord], 0, end-start)
	for _, value := range desired[start:end] {
		joined, joinErr := joinEnvironmentZone(projection, value)
		if joinErr != nil {
			return Page[ZoneRecord]{}, joinErr
		}
		items = append(items, joined)
	}
	next := ""
	if end < len(desired) {
		next, err = encodeCursor(cursorPayload{
			Version: cursorVersion, Revision: projection.ReadRevision,
			LastID: desired[end-1].Desired.ID, Query: query,
		})
		if err != nil {
			return Page[ZoneRecord]{}, err
		}
	}
	return Page[ZoneRecord]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}

func findZoneAtRevision(
	ctx context.Context,
	store hierarchyStore,
	zoneID string,
	revision int64,
) (Versioned[ZoneRecord], error) {
	start := ""
	fixedRevision := revision
	var matched *Versioned[ZoneRecord]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return Versioned[ZoneRecord]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return Versioned[ZoneRecord]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
		}
		if fixedRevision == 0 {
			fixedRevision = page.ReadRevision
		}
		for index := range page.Values {
			value := &page.Values[index]
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, environmentDesiredHeadScanPrefix), "/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return Versioned[ZoneRecord]{}, corruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
				ctx, store, environmentID, fixedRevision,
			)
			if projectionErr != nil {
				return Versioned[ZoneRecord]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredZones {
				if desired.Desired.ID != zoneID {
					continue
				}
				if matched != nil {
					return Versioned[ZoneRecord]{}, corruptEnvironmentComposeProjection()
				}
				joined, joinErr := joinEnvironmentZone(projection, desired)
				if joinErr != nil {
					return Versioned[ZoneRecord]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return Versioned[ZoneRecord]{}, errs.New(errs.KindInternal, "Environment desired head scan did not advance")
		}
	}
	if matched == nil {
		return Versioned[ZoneRecord]{}, errs.New(errs.KindZoneNotFound, "Zone was not found")
	}
	return *matched, nil
}
