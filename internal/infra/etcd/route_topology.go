package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
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
) (etcdstore.Versioned[routerecord.Record], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[routerecord.Record]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start,
			Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[routerecord.Record]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[routerecord.Record]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
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
				return etcdstore.Versioned[routerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
				ctx,
				store,
				environmentID,
				fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[routerecord.Record]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredRoutes {
				if desired.Desired.ID != routeID {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[routerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
				}
				joined, joinErr := projectionrecord.ReadRoute(ctx, store, projection, desired)
				if joinErr != nil {
					return etcdstore.Versioned[routerecord.Record]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[routerecord.Record]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan did not advance",
			)
		}
	}
	if matched == nil {
		return etcdstore.Versioned[routerecord.Record]{}, errs.New(errs.KindRouteNotFound, "Route was not found")
	}
	return *matched, nil
}

func listRoutesFromDesiredHead(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[routerecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	limit, revision, lastID, query, err := normalizePageRequest(
		request, "routes", "environment", environmentID, "", ids.KindRoute,
	)
	if err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[routerecord.Record]{}, err
	}
	if !found {
		return etcdstore.Page[routerecord.Record]{Items: []etcdstore.Versioned[routerecord.Record]{}, Revision: projection.ReadRevision}, nil
	}
	desired := append([]projectionrecord.EnvironmentRouteProjection(nil), projection.Record.DesiredRoutes...)
	sort.Slice(desired, func(left, right int) bool { return desired[left].Desired.ID < desired[right].Desired.ID })
	start := sort.Search(len(desired), func(index int) bool { return desired[index].Desired.ID > lastID })
	end := min(start+limit, len(desired))
	items := make([]etcdstore.Versioned[routerecord.Record], 0, end-start)
	for _, route := range desired[start:end] {
		joined, joinErr := projectionrecord.ReadRoute(ctx, store, projection, route)
		if joinErr != nil {
			return etcdstore.Page[routerecord.Record]{}, joinErr
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
			return etcdstore.Page[routerecord.Record]{}, err
		}
	}
	return etcdstore.Page[routerecord.Record]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}

func routeAtProjection(
	ctx context.Context,
	store hierarchyStore,
	projection projectionrecord.EnvironmentComposeProjection,
	projectionRevision int64,
	readRevision int64,
	routeID string,
) (routerecord.Record, error) {
	versioned := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: projectionRevision, ReadRevision: readRevision,
	}
	for _, desired := range projection.DesiredRoutes {
		if desired.Desired.ID == routeID {
			joined, err := projectionrecord.ReadRoute(ctx, store, versioned, desired)
			if err != nil {
				return routerecord.Record{}, err
			}
			return joined.Record, nil
		}
	}
	return routerecord.Record{}, errs.New(errs.KindRouteNotFound, "Route was not found")
}
