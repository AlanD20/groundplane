package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
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

func (repository *ZoneRepository) GetZone(ctx context.Context, id string) (etcdstore.Versioned[zonerecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[zonerecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindNetwork, id); err != nil {
		return etcdstore.Versioned[zonerecord.Record]{}, err
	}
	return findZoneAtRevision(ctx, repository.store, id, 0)
}

func (repository *ZoneRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[zonerecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[zonerecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[zonerecord.Record]{}, err
	}
	limit, revision, lastID, query, err := recordquery.NormalizePageRequest(
		request, "zones", "environment", environmentID, "", ids.KindNetwork,
	)
	if err != nil {
		return etcdstore.Page[zonerecord.Record]{}, err
	}
	projection, found, err := blueprints.ReadCurrentProjection(
		ctx, repository.store, environmentID, revision,
	)
	if err != nil {
		return etcdstore.Page[zonerecord.Record]{}, err
	}
	if !found {
		return etcdstore.Page[zonerecord.Record]{Items: []etcdstore.Versioned[zonerecord.Record]{}, Revision: projection.ReadRevision}, nil
	}
	desired := append([]projectionrecord.EnvironmentZoneProjection(nil), projection.Record.DesiredZones...)
	sort.Slice(desired, func(left, right int) bool {
		return desired[left].Desired.ID < desired[right].Desired.ID
	})
	start := sort.Search(len(desired), func(index int) bool {
		return desired[index].Desired.ID > lastID
	})
	end := min(start+limit, len(desired))
	items := make([]etcdstore.Versioned[zonerecord.Record], 0, end-start)
	for _, value := range desired[start:end] {
		joined, joinErr := joinEnvironmentZone(projection, value)
		if joinErr != nil {
			return etcdstore.Page[zonerecord.Record]{}, joinErr
		}
		items = append(items, joined)
	}
	next := ""
	if end < len(desired) {
		next, err = recordcodec.EncodeCursor(recordcodec.Cursor{
			Version: recordcodec.CursorVersion, Revision: projection.ReadRevision,
			LastID: desired[end-1].Desired.ID, Query: query,
		})
		if err != nil {
			return etcdstore.Page[zonerecord.Record]{}, err
		}
	}
	return etcdstore.Page[zonerecord.Record]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}

func findZoneAtRevision(
	ctx context.Context,
	store hierarchyStore,
	zoneID string,
	revision int64,
) (etcdstore.Versioned[zonerecord.Record], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[zonerecord.Record]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
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
				return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := blueprints.ReadCurrentProjection(
				ctx, store, environmentID, fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[zonerecord.Record]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredZones {
				if desired.Desired.ID != zoneID {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
				}
				joined, joinErr := joinEnvironmentZone(projection, desired)
				if joinErr != nil {
					return etcdstore.Versioned[zonerecord.Record]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindInternal, "Environment desired head scan did not advance")
		}
	}
	if matched == nil {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindZoneNotFound, "Zone was not found")
	}
	return *matched, nil
}
