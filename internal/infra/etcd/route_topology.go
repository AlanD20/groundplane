package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func findRouteAtRevision(
	ctx context.Context,
	store hierarchyStore,
	routeID string,
	revision int64,
) (Versioned[RouteRecord], error) {
	start := ""
	fixedRevision := revision
	var matched *Versioned[RouteRecord]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start,
			Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return Versioned[RouteRecord]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return Versioned[RouteRecord]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
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
				strings.TrimPrefix(value.Key, environmentDesiredHeadScanPrefix),
				"/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return Versioned[RouteRecord]{}, corruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
				ctx,
				store,
				environmentID,
				fixedRevision,
			)
			if projectionErr != nil {
				return Versioned[RouteRecord]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredRoutes {
				if desired.Desired.ID != routeID {
					continue
				}
				if matched != nil {
					return Versioned[RouteRecord]{}, corruptEnvironmentComposeProjection()
				}
				joined, joinErr := routeRecordFromDesiredProjection(ctx, store, projection, desired)
				if joinErr != nil {
					return Versioned[RouteRecord]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return Versioned[RouteRecord]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan did not advance",
			)
		}
	}
	if matched == nil {
		return Versioned[RouteRecord]{}, errs.New(errs.KindRouteNotFound, "Route was not found")
	}
	return *matched, nil
}

func listRoutesFromDesiredHead(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	request PageRequest,
) (Page[RouteRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Page[RouteRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[RouteRecord]{}, err
	}
	limit, revision, lastID, query, err := normalizePageRequest(
		request, "routes", "environment", environmentID, "", ids.KindRoute,
	)
	if err != nil {
		return Page[RouteRecord]{}, err
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, store, environmentID, revision)
	if err != nil {
		return Page[RouteRecord]{}, err
	}
	if !found {
		return Page[RouteRecord]{Items: []Versioned[RouteRecord]{}, Revision: projection.ReadRevision}, nil
	}
	desired := append([]EnvironmentRouteProjection(nil), projection.Record.DesiredRoutes...)
	sort.Slice(desired, func(left, right int) bool { return desired[left].Desired.ID < desired[right].Desired.ID })
	start := sort.Search(len(desired), func(index int) bool { return desired[index].Desired.ID > lastID })
	end := min(start+limit, len(desired))
	items := make([]Versioned[RouteRecord], 0, end-start)
	for _, route := range desired[start:end] {
		joined, joinErr := routeRecordFromDesiredProjection(ctx, store, projection, route)
		if joinErr != nil {
			return Page[RouteRecord]{}, joinErr
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
			return Page[RouteRecord]{}, err
		}
	}
	return Page[RouteRecord]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}

func routeAtProjection(
	ctx context.Context,
	store hierarchyStore,
	projection EnvironmentComposeProjection,
	projectionRevision int64,
	readRevision int64,
	routeID string,
) (RouteRecord, error) {
	versioned := Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: projectionRevision, ReadRevision: readRevision,
	}
	for _, desired := range projection.DesiredRoutes {
		if desired.Desired.ID == routeID {
			joined, err := routeRecordFromDesiredProjection(ctx, store, versioned, desired)
			if err != nil {
				return RouteRecord{}, err
			}
			return joined.Record, nil
		}
	}
	return RouteRecord{}, errs.New(errs.KindRouteNotFound, "Route was not found")
}
