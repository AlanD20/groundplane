package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceRepository) GetService(
	ctx context.Context,
	serviceID string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindService, serviceID); err != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	return environmentqueries.FindServiceAtRevision(ctx, repository.store, serviceID, 0)
}

func (repository *ServiceRepository) GetServiceRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
	serviceID string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	hierarchy := &HierarchyRepository{Reader: hierarchyrecord.NewReader(repository.store), store: repository.store, ProjectionReader: environmentqueries.NewProjectionReader(repository.store)}
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
	if err != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	return servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: projection.ReadRevision}, serviceID,
		blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID))
}

func (repository *ServiceRepository) GetServiceByName(
	ctx context.Context,
	environmentID string,
	name string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	if name == "" {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindValidationFailed, "Service name is required")
	}
	projection, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, 0)
	if err != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	for _, service := range projection.Record.DesiredServices {
		if service.Desired.Name == name &&
			!environmentqueries.IsComponentService(projection.Record.Components, service.Desired.ID) {
			return servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: projection.ReadRevision}, service.Desired.ID,
				blueprints.EnvironmentBlueprintHeadKey(environmentID))
		}
	}
	return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
}

func (repository *ServiceRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	limit, revision, lastID, query, err := recordquery.NormalizePageRequest(
		request, "services", "environment", environmentID, "", ids.KindService,
	)
	if err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	projection, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Page[servicerecord.ServiceRecord]{Items: []etcdstore.Versioned[servicerecord.ServiceRecord]{}, Revision: projection.ReadRevision}, nil
	}
	desired := environmentqueries.OrdinaryServices(projection.Record)
	sort.Slice(desired, func(left, right int) bool { return desired[left].Desired.ID < desired[right].Desired.ID })
	start := sort.Search(len(desired), func(index int) bool { return desired[index].Desired.ID > lastID })
	end := min(start+limit, len(desired))
	items := make([]etcdstore.Versioned[servicerecord.ServiceRecord], 0, end-start)
	for _, service := range desired[start:end] {
		joined, joinErr := servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: projection.ReadRevision}, service.Desired.ID,
			blueprints.EnvironmentBlueprintHeadKey(environmentID))
		if joinErr != nil {
			return etcdstore.Page[servicerecord.ServiceRecord]{}, joinErr
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
			return etcdstore.Page[servicerecord.ServiceRecord]{}, err
		}
	}
	return etcdstore.Page[servicerecord.ServiceRecord]{Items: items, NextCursor: next, Revision: projection.ReadRevision}, nil
}
