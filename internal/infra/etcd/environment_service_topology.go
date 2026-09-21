package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentDesiredHeadScanPrefix = "/v1/records/environment-blueprints/"

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
	return findServiceAtRevision(ctx, repository.store, serviceID, 0)
}

func (repository *ServiceRepository) GetServiceRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
	serviceID string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	hierarchy := &HierarchyRepository{store: repository.store}
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
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, environmentID, 0)
	if err != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	for _, service := range projection.Record.DesiredServices {
		if service.Desired.Name == name &&
			!componentGeneratedService(projection.Record.Components, service.Desired.ID) {
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
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	if !found {
		return etcdstore.Page[servicerecord.ServiceRecord]{Items: []etcdstore.Versioned[servicerecord.ServiceRecord]{}, Revision: projection.ReadRevision}, nil
	}
	desired := ordinaryEnvironmentServices(projection.Record)
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

func currentEnvironmentProjectionAtRevision(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	hierarchy := &HierarchyRepository{store: store}
	return hierarchy.getEnvironmentComposeProjectionAtRevision(ctx, environmentID, revision)
}

func findServiceAtRevision(
	ctx context.Context,
	store hierarchyStore,
	serviceID string,
	revision int64,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	start := ""
	fixedRevision := revision
	var matched *etcdstore.Versioned[servicerecord.ServiceRecord]
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 200, Revision: fixedRevision,
		})
		if err != nil {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindInternal, "Environment desired head scan is invalid")
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
				return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
				ctx,
				store,
				environmentID,
				fixedRevision,
			)
			if projectionErr != nil {
				return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredServices {
				if desired.Desired.ID != serviceID {
					continue
				}
				if componentGeneratedService(projection.Record.Components, serviceID) {
					continue
				}
				if matched != nil {
					return etcdstore.Versioned[servicerecord.ServiceRecord]{}, projectionrecord.CorruptEnvironmentComposeProjection()
				}
				joined, joinErr := servicerecord.ReadJoined(ctx, store, servicerecord.DesiredSelection{Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: projection.ReadRevision}, serviceID,
					blueprints.EnvironmentBlueprintHeadKey(environmentID))
				if joinErr != nil {
					return etcdstore.Versioned[servicerecord.ServiceRecord]{}, joinErr
				}
				matched = &joined
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
				errs.KindInternal,
				"Environment desired head scan did not advance",
			)
		}
	}
	if matched == nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	return *matched, nil
}

func ordinaryEnvironmentServices(projection projectionrecord.EnvironmentComposeProjection) []servicerecord.EnvironmentServiceProjection {
	result := make([]servicerecord.EnvironmentServiceProjection, 0, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if componentGeneratedService(projection.Components, service.Desired.ID) {
			continue
		}
		result = append(result, service)
	}
	return result
}

func componentGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}
