package etcd

import (
	"context"
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

func NewBackingServiceRepository(store Store) (*BackingServiceRepository, error) {
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
) (Versioned[BackingServiceRecord], error) {
	project, err := repository.hierarchy.GetProject(ctx, projectID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindProjectNotFound {
			return Versioned[BackingServiceRecord]{}, backingServiceNotFound()
		}
		return Versioned[BackingServiceRecord]{}, err
	}
	if project.Record.Kind != ProjectKindBacking || project.Record.TenantID != "" {
		return Versioned[BackingServiceRecord]{}, backingServiceNotFound()
	}
	return repository.composeBackingService(ctx, project)
}

func (repository *BackingServiceRepository) ListBackingServices(
	ctx context.Context,
	request PageRequest,
) (Page[BackingServiceRecord], error) {
	projects, err := listIndexPage(
		ctx,
		repository.store,
		"backing-services",
		"platform",
		"-",
		projectPlatformOwnerPrefix,
		projectKey,
		ids.KindProject,
		request,
		decodeProject,
		func(record ProjectRecord) string { return record.ID },
		func(record ProjectRecord) bool {
			return record.Kind == ProjectKindBacking && record.TenantID == ""
		},
	)
	if err != nil {
		return Page[BackingServiceRecord]{}, err
	}
	page := Page[BackingServiceRecord]{
		Items:      make([]Versioned[BackingServiceRecord], len(projects.Items)),
		NextCursor: projects.NextCursor,
		Revision:   projects.Revision,
	}
	for index, project := range projects.Items {
		item, err := repository.composeBackingService(ctx, project)
		if err != nil {
			return Page[BackingServiceRecord]{}, err
		}
		page.Items[index] = item
	}
	return page, nil
}

func (repository *BackingServiceRepository) composeBackingService(
	ctx context.Context,
	project Versioned[ProjectRecord],
) (Versioned[BackingServiceRecord], error) {
	if project.ReadRevision <= 0 || project.Record.Kind != ProjectKindBacking || project.Record.TenantID != "" {
		return Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service Project projection is inconsistent",
		)
	}
	environmentID, err := repository.singleOwnerID(
		ctx, environmentOwnerPrefix(project.Record.ID), ids.KindEnvironment, project.ReadRevision,
	)
	if err != nil {
		return Versioned[BackingServiceRecord]{}, err
	}
	environmentValue, err := repository.primaryAtRevision(
		ctx, environmentKey(environmentID), project.ReadRevision,
	)
	if err != nil {
		return Versioned[BackingServiceRecord]{}, err
	}
	environment, err := decodeEnvironment(environmentValue.Value)
	if err != nil {
		return Versioned[BackingServiceRecord]{}, err
	}
	if environment.ID != environmentID || environment.ProjectID != project.Record.ID || environment.Name != "main" {
		return Versioned[BackingServiceRecord]{}, errs.New(
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
		return Versioned[BackingServiceRecord]{}, err
	}
	if !found || len(projection.Record.DesiredServices) != 1 {
		return Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal, "Backing-service desired projection is inconsistent",
		)
	}
	serviceID := projection.Record.DesiredServices[0].Desired.ID
	service, err := joinEnvironmentService(
		ctx, repository.store, projection, serviceID, environmentBlueprintHeadKey(environmentID),
	)
	if err != nil {
		return Versioned[BackingServiceRecord]{}, err
	}
	if service.Record.EnvironmentID != environmentID || service.Record.Desired.Adapter == "" {
		return Versioned[BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service Service projection is inconsistent",
		)
	}
	return Versioned[BackingServiceRecord]{
		Record: BackingServiceRecord{
			Authentication: service.Record.Desired.Authentication,
			ProjectID:      project.Record.ID, EnvironmentID: environmentID, ServiceID: serviceID,
			BackingNetworkID: service.Record.BackingNetworkID,
		},
		Revision: max(
			project.Revision,
			environmentValue.ModRevision,
			service.Revision,
			serviceRuntimeRevision(service),
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
	result, err := repository.store.Range(ctx, RangeRequest{Prefix: prefix, Limit: 2, Revision: revision})
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
) (*KeyValue, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
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
