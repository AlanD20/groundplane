package softwareactivation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns immutable activation input and typed staging results. Native
// work owns scheduling and attempts; there is no second activation queue.
type Repository struct{ store store }

func NewRepository(value etcdstore.Store) (*Repository, error) { return newRepository(value) }

func newRepository(value store) (*Repository, error) {
	if value == nil {
		return nil, errs.New(errs.KindInternal, "software activation store is required")
	}
	return &Repository{store: value}, nil
}

func (repository *Repository) Get(ctx context.Context, taskID string) (etcdstore.Versioned[Record], error) {
	return repository.At(ctx, taskID, 0)
}

func (repository *Repository) At(ctx context.Context, taskID string, revision int64) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed,
			"software activation task identity is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{Key(taskID)}, Revision: revision})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result == nil || result.ReadRevision <= 0 || revision > 0 && result.ReadRevision != revision || len(result.Values) != 1 {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindStorageUnavailable, "software activation snapshot is unavailable")
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindTaskNotFound, "software activation was not found")
	}
	record, err := Decode(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if record.TaskID != taskID {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"software activation key does not match its Task",
		)
	}
	return etcdstore.Versioned[Record]{
		Record:       record,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}
