package etcdconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func New(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "etcd configuration execution storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(ctx context.Context, workID string, revision int64) (keyvalue.Versioned[Input], error) {
	if ids.Validate(ids.KindWorkItem, workID) != nil || revision < 0 {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindValidationFailed, "etcd configuration work identity is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{InputKey(workID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindStorageUnavailable, "etcd configuration input read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || (revision > 0 && read.ReadRevision != revision) || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindStateConflict, "etcd configuration input is unavailable")
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Input]{}, err
	}
	if input.WorkItemID != workID {
		return keyvalue.Versioned[Input]{}, errs.New(errs.KindInternal, "etcd configuration input owner is corrupt")
	}
	return keyvalue.Versioned[Input]{Record: input, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, nil
}
