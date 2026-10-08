package softwareactivation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
}

// Repository exposes immutable reads. Publication, running checkpoints and
// terminalization stay in the root Task repository so they are atomic with the
// Task queue/claim/idempotency lifecycle.
type Repository struct{ store store }

func NewRepository(value etcdstore.Store) (*Repository, error) { return newRepository(value) }

func newRepository(value store) (*Repository, error) {
	if value == nil {
		return nil, errs.New(errs.KindInternal, "software activation store is required")
	}
	return &Repository{store: value}, nil
}

func (repository *Repository) Get(ctx context.Context, taskID string) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed,
			"software activation task identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, Key(taskID))
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result == nil || result.Entry == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindTaskNotFound, "software activation was not found")
	}
	record, err := Decode(result.Entry.Value)
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
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, nil
}
