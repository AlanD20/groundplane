package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackingServiceRecord is a read-only facade over the three durable hierarchy
// records that make up one backing service. It is never persisted itself.
type BackingServiceRecord struct {
	Authentication   core.BackingAuthentication
	ProjectID        string
	EnvironmentID    string
	ServiceID        string
	BackingNetworkID string
}

// BackingServiceRepository joins every facade member at one MVCC revision.
// Its cursor is bound to the backing-services collection even though the
// Project owner index supplies the stable ordering.
type BackingServiceRepository struct {
	store     hierarchyStore
	hierarchy *HierarchyRepository
}

func NewBackingServiceRepository(store etcdstore.Store) (*BackingServiceRepository, error) {
	return newBackingServiceRepository(store)
}

func newBackingServiceRepository(store hierarchyStore) (*BackingServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service store is required")
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		return nil, err
	}
	return &BackingServiceRepository{store: store, hierarchy: hierarchy}, nil
}

func (repository *BackingServiceRepository) GetBackingService(
	ctx context.Context,
	projectID string,
) (etcdstore.Versioned[BackingServiceRecord], error) {
	project, err := repository.hierarchy.GetProject(ctx, projectID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindProjectNotFound {
			return etcdstore.Versioned[BackingServiceRecord]{}, backingServiceNotFound()
		}
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
		return etcdstore.Versioned[BackingServiceRecord]{}, backingServiceNotFound()
	}
	return repository.composeBackingService(ctx, project)
}

func (repository *BackingServiceRepository) ListBackingServices(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[BackingServiceRecord], error) {
	projects, err := listIndexPage(
		ctx,
		repository.store,
		"backing-services",
		"platform",
		"-",
		hierarchyrecord.ProjectPlatformOwnerPrefix,
		hierarchyrecord.ProjectKey,
		ids.KindProject,
		request,
		hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
		func(record hierarchyrecord.ProjectRecord) bool {
			return record.Kind == hierarchyrecord.ProjectKindBacking && record.TenantID == ""
		},
	)
	if err != nil {
		return etcdstore.Page[BackingServiceRecord]{}, err
	}
	page := etcdstore.Page[BackingServiceRecord]{
		Items:      make([]etcdstore.Versioned[BackingServiceRecord], len(projects.Items)),
		NextCursor: projects.NextCursor,
		Revision:   projects.Revision,
	}
	for index, project := range projects.Items {
		item, err := repository.composeBackingService(ctx, project)
		if err != nil {
			return etcdstore.Page[BackingServiceRecord]{}, err
		}
		page.Items[index] = item
	}
	return page, nil
}

func (repository *BackingServiceRepository) composeBackingService(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
) (etcdstore.Versioned[BackingServiceRecord], error) {
	if project.ReadRevision <= 0 || project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
		return etcdstore.Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service Project projection is inconsistent",
		)
	}
	environmentID, err := repository.singleOwnerID(
		ctx, hierarchyrecord.EnvironmentOwnerPrefix(project.Record.ID), ids.KindEnvironment, project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	environmentValue, err := repository.primaryAtRevision(
		ctx, hierarchyrecord.EnvironmentKey(environmentID), project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	if environment.ID != environmentID || environment.ProjectID != project.Record.ID || environment.Name != "main" {
		return etcdstore.Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service Environment projection is inconsistent",
		)
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx,
		repository.store,
		environmentID,
		project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	if !found || len(projection.Record.DesiredServices) != 1 {
		return etcdstore.Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal, "Backing-service desired projection is inconsistent",
		)
	}
	serviceID := projection.Record.DesiredServices[0].Desired.ID
	service, err := joinEnvironmentService(
		ctx, repository.store, projection, serviceID, environmentBlueprintHeadKey(environmentID),
	)
	if err != nil {
		return etcdstore.Versioned[BackingServiceRecord]{}, err
	}
	if service.Record.EnvironmentID != environmentID || service.Record.Desired.Adapter == "" {
		return etcdstore.Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service Service projection is inconsistent",
		)
	}
	return etcdstore.Versioned[BackingServiceRecord]{
		Record: BackingServiceRecord{
			Authentication: service.Record.Desired.Authentication,
			ProjectID:      project.Record.ID, EnvironmentID: environmentID, ServiceID: serviceID,
			BackingNetworkID: service.Record.BackingNetworkID,
		},
		Revision: max(
			project.Revision,
			environmentValue.ModRevision,
			service.Revision,
			ServiceRuntimeRevision(service),
		),
		ReadRevision: project.ReadRevision,
	}, nil
}

func (repository *BackingServiceRepository) singleOwnerID(
	ctx context.Context,
	prefix string,
	kind ids.Kind,
	revision int64,
) (string, error) {
	result, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 2, Revision: revision})
	if err != nil {
		return "", err
	}
	if result.ReadRevision != revision || len(result.Values) != 1 || result.More {
		return "", errs.New(errs.KindInternal, "Backing-service hierarchy must contain exactly one child")
	}
	value := result.Values[0]
	if err := validateListKey(prefix, value.Key, kind); err != nil {
		return "", errs.New(errs.KindInternal, "Backing-service owner index contains an invalid key")
	}
	id := strings.TrimPrefix(value.Key, prefix)
	if string(value.Value) != id {
		return "", errs.New(errs.KindInternal, "Backing-service owner index value does not match its key")
	}
	return id, nil
}

func (repository *BackingServiceRepository) primaryAtRevision(
	ctx context.Context,
	key string,
	revision int64,
) (*etcdstore.KeyValue, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result.ReadRevision != revision || len(result.Values) != 1 || result.Values[0] == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service owner index references a missing primary record")
	}
	return result.Values[0], nil
}

func backingServiceNotFound() error {
	return errs.New(errs.KindBackingServiceNotFound, "backing service was not found")
}
