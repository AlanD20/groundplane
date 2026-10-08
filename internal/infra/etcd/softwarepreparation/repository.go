package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumCheckpointConflicts = 3

type store interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Repository owns progress monotonicity and compare-and-swap publication.
// Initial record creation remains in the Task repository so Task admission and
// its first durable projection commit atomically.
type Repository struct{ store store }

func NewRepository(value etcdstore.Store) (*Repository, error) { return newRepository(value) }

func newRepository(value store) (*Repository, error) {
	if value == nil {
		return nil, errs.New(errs.KindInternal, "software preparation store is required")
	}
	return &Repository{store: value}, nil
}

func (repository *Repository) Get(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindValidationFailed, "software preparation task identity is invalid",
		)
	}
	result, err := repository.store.Get(ctx, Key(taskID))
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result == nil || result.Entry == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindTaskNotFound, "software preparation was not found")
	}
	record, err := Decode(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if record.TaskID != taskID {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindInternal, "software preparation key does not match its task",
		)
	}
	return etcdstore.Versioned[Record]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *Repository) List(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	return recordquery.ListPrimary(
		ctx, repository.store, "software_preparations", "platform", "-", Prefix, ids.KindTask, request,
		Decode, func(record Record) string { return record.TaskID }, func(Record) bool { return true },
	)
}

func (repository *Repository) Checkpoint(
	ctx context.Context,
	taskID string,
	operationID string,
	inputSHA256 string,
	progress preparation.Progress,
	updatedAt time.Time,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	for conflict := 0; conflict < maximumCheckpointConflicts; conflict++ {
		current, err := repository.Get(ctx, taskID)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if current.Record.OperationID != operationID || current.Record.InputSHA256 != inputSHA256 {
			return etcdstore.Versioned[Record]{}, errs.New(
				errs.KindStateConflict, "software preparation checkpoint authority changed",
			)
		}
		if err := validateTransition(current.Record.Progress, progress, current.Record.Source); err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if current.Record.Progress.Equal(progress) {
			return current, nil
		}
		if !updatedAt.After(current.Record.UpdatedAt) {
			updatedAt = current.Record.UpdatedAt.Add(time.Nanosecond)
		}
		next := current.Record
		next.Progress, next.UpdatedAt = progress, updatedAt.UTC()
		value, err := encode(next)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		result, err := repository.store.Transact(ctx,
			[]etcdstore.Condition{{Key: Key(taskID), ModRevision: current.Revision}},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: Key(taskID), Value: value}},
		)
		clear(value)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if result.Succeeded {
			return etcdstore.Versioned[Record]{
				Record:       next,
				Revision:     result.Revision,
				ReadRevision: result.Revision,
			}, nil
		}
	}
	return etcdstore.Versioned[Record]{}, errs.New(
		errs.KindStateConflict, "software preparation checkpoint changed repeatedly",
	)
}

func validateTransition(current, next preparation.Progress, source preparation.ResolvedSource) error {
	if err := preparation.ValidateProgress(next, source); err != nil {
		return err
	}
	if current.Result.Agent != nil &&
		(next.Result.Agent == nil || *current.Result.Agent != *next.Result.Agent) ||
		current.Result.Controller != nil &&
			(next.Result.Controller == nil || *current.Result.Controller != *next.Result.Controller) {
		return errs.New(errs.KindStateConflict, "software preparation output checkpoint changed")
	}
	if current.Phase == preparation.PhaseVerified || current.Phase == preparation.PhaseFailed {
		if !current.Equal(next) {
			return errs.New(errs.KindStateConflict, "terminal software preparation progress changed")
		}
		return nil
	}
	if next.Phase == preparation.PhaseAccepted {
		return errs.New(errs.KindStateConflict, "software preparation progress regressed")
	}
	return nil
}
