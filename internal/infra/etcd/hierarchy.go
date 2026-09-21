package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// HierarchyRepository owns durable Tenant, Project, and Environment persistence.
type HierarchyRepository struct {
	*hierarchyrecord.Reader
	*environmentqueries.ProjectionReader
	store hierarchyStore
}

// EnvironmentBlueprintRepository owns final Environment Blueprint publication.
// Its dedicated transaction executor is mandatory and is not available to
// ordinary hierarchy persistence paths.
type EnvironmentBlueprintRepository struct {
	*HierarchyRepository
	transactions environmentBlueprintTransactionStore
}

func NewHierarchyRepository(store etcdstore.Store) (*HierarchyRepository, error) {
	return newHierarchyRepository(store)
}

// NewEnvironmentBlueprintRepository constructs the only repository authorized
// to publish a final Environment Blueprint transaction.
func NewEnvironmentBlueprintRepository(store EnvironmentBlueprintStore) (*EnvironmentBlueprintRepository, error) {
	return newEnvironmentBlueprintRepository(store, store)
}

func newHierarchyRepository(store hierarchyStore) (*HierarchyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	return &HierarchyRepository{Reader: hierarchyrecord.NewReader(store), store: store, ProjectionReader: environmentqueries.NewProjectionReader(store)}, nil
}

func newEnvironmentBlueprintRepository(
	store hierarchyStore,
	transactions environmentBlueprintTransactionStore,
) (*EnvironmentBlueprintRepository, error) {
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		return nil, err
	}
	if transactions == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint transaction executor is required")
	}
	return &EnvironmentBlueprintRepository{
		HierarchyRepository: hierarchy,
		transactions:        transactions,
	}, nil
}

func (repository *HierarchyRepository) diagnoseCreate(ctx context.Context, primary string, slug string) error {
	primaryResult, err := repository.store.Get(ctx, primary)
	if err != nil {
		return err
	}
	slugResult, err := repository.store.Get(ctx, slug)
	if err != nil {
		return err
	}
	if slugResult.Entry != nil {
		return errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if primaryResult.Entry != nil {
		return errs.New(errs.KindStateConflict, "stable id is already in use")
	}
	return errs.New(errs.KindStateConflict, "hierarchy changed during create")
}
