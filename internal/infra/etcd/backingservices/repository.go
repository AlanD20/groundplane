package backingservices

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Record is a read-only projection over the three durable hierarchy
// records that make up one backing service. It is never persisted itself.
type Record struct {
	Authentication   core.BackingAuthentication
	ProjectID        string
	EnvironmentID    string
	ServiceID        string
	BackingNetworkID string
}

// Repository joins every projection member at one MVCC revision.
// Its cursor is bound to the backing-services collection even though the
// Project owner index supplies the stable ordering.
type Repository struct {
	store backingServiceReader
}

type backingServiceReader interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

func NewRepository(store backingServiceReader) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service store is required")
	}
	return &Repository{store: store}, nil
}

func (repository *Repository) GetBackingService(
	ctx context.Context,
	projectID string,
) (etcdstore.Versioned[Record], error) {
	project, err := repository.getProject(ctx, projectID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindProjectNotFound {
			return etcdstore.Versioned[Record]{}, backingServiceNotFound()
		}
		return etcdstore.Versioned[Record]{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
		return etcdstore.Versioned[Record]{}, backingServiceNotFound()
	}
	return repository.composeBackingService(ctx, project)
}

func (repository *Repository) getProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	store := repository.store
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, id); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return recordquery.Get(
		ctx, store, hierarchyrecord.ProjectKey(id), id, errs.KindProjectNotFound, hierarchyrecord.DecodeProject,
		func(record hierarchyrecord.ProjectRecord) string { return record.ID },
	)
}

func (repository *Repository) ListBackingServices(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	projects, err := recordquery.ListIndex(
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
		return etcdstore.Page[Record]{}, err
	}
	page := etcdstore.Page[Record]{
		Items:      make([]etcdstore.Versioned[Record], len(projects.Items)),
		NextCursor: projects.NextCursor,
		Revision:   projects.Revision,
	}
	for index, project := range projects.Items {
		item, err := repository.composeBackingService(ctx, project)
		if err != nil {
			return etcdstore.Page[Record]{}, err
		}
		page.Items[index] = item
	}
	return page, nil
}

func (repository *Repository) composeBackingService(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
) (etcdstore.Versioned[Record], error) {
	if project.ReadRevision <= 0 || project.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		project.Record.TenantID != "" {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"Backing-service Project projection is inconsistent",
		)
	}
	environmentID, err := repository.singleOwnerID(
		ctx, hierarchyrecord.EnvironmentOwnerPrefix(project.Record.ID), ids.KindEnvironment, project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	environmentValue, err := repository.primaryAtRevision(
		ctx, hierarchyrecord.EnvironmentKey(environmentID), project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if environment.ID != environmentID || environment.ProjectID != project.Record.ID || environment.Name != "main" {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"Backing-service Environment projection is inconsistent",
		)
	}
	projection, found, err := blueprints.ReadCurrentProjection(
		ctx,
		repository.store,
		environmentID,
		project.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !found || len(projection.Record.DesiredServices) != 1 {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal, "Backing-service desired projection is inconsistent",
		)
	}
	serviceID := projection.Record.DesiredServices[0].Desired.ID
	service, err := servicerecord.ReadJoined(
		ctx,
		repository.store,
		servicerecord.DesiredSelection{
			Services:     projection.Record.DesiredServices,
			Revision:     projection.Revision,
			ReadRevision: projection.ReadRevision,
		},
		serviceID,
		blueprints.EnvironmentBlueprintHeadKey(environmentID),
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if service.Record.EnvironmentID != environmentID || service.Record.Desired.Adapter == "" {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"Backing-service Service projection is inconsistent",
		)
	}
	return etcdstore.Versioned[Record]{
		Record: Record{
			Authentication: service.Record.Desired.Authentication,
			ProjectID:      project.Record.ID, EnvironmentID: environmentID, ServiceID: serviceID,
			BackingNetworkID: service.Record.BackingNetworkID,
		},
		Revision: max(
			project.Revision,
			environmentValue.ModRevision,
			service.Revision,
			servicerecord.ServiceRuntimeRevision(service),
		),
		ReadRevision: project.ReadRevision,
	}, nil
}

func (repository *Repository) singleOwnerID(
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
	if err := recordquery.ValidateListKey(prefix, value.Key, kind); err != nil {
		return "", errs.New(errs.KindInternal, "Backing-service owner index contains an invalid key")
	}
	id := strings.TrimPrefix(value.Key, prefix)
	if string(value.Value) != id {
		return "", errs.New(errs.KindInternal, "Backing-service owner index value does not match its key")
	}
	return id, nil
}

func (repository *Repository) primaryAtRevision(
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
