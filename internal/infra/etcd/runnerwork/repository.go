package runnerwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func NewRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Runner execution storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(ctx context.Context, workID string, revision int64) (keyvalue.Versioned[Input], error) {
	if ids.Validate(ids.KindWorkItem, workID) != nil || revision < 0 {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindValidationFailed, "Runner work input identity is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{InputKey(workID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner work input read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner work input read is incomplete")
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindRequestNotFound, "Runner execution input is unavailable")
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if input.WorkItemID != workID {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner execution input owner is corrupt")
	}
	return keyvalue.Versioned[Input]{Record: input, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, nil
}

func (repository *Repository) LoadTask(ctx context.Context, taskID string, revision int64) (keyvalue.Versioned[Input], error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindValidationFailed, "Runner Task input identity is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{TaskInputKey(taskID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner Task input read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner Task input read is incomplete")
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindRequestNotFound, "Runner Task input is unavailable")
	}
	workID := string(read.Values[0].Value)
	if ids.Validate(ids.KindWorkItem, workID) != nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner Task input index is corrupt")
	}
	input, err := repository.Load(ctx, workID, read.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if input.Record.TaskID != taskID {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "Runner Task input index changed owner")
	}
	return input, nil
}

func (input Input) MatchWork(item workexecution.Item) error {
	ref, err := input.Reference()
	if err != nil {
		return err
	}
	if item.Validate() != nil || item.Operation != workexecution.Runner ||
		item.Placement.Executor != workexecution.Controller || item.ID != input.WorkItemID ||
		item.TaskID != input.TaskID || item.TargetID != input.Desired.ID || item.Input == nil || *item.Input != ref {
		return errs.New(errs.KindStateConflict, "Runner work does not match its captured runtime input")
	}
	return nil
}
