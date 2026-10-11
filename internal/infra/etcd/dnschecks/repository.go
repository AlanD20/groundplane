package dnschecks

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

func NewRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "DNS check store is required")
	}
	return &Repository{store: backend}, nil
}

// Active absence and latest selection are captured by the Component repository
// at the same MVCC revision as runtime. Do not add independent latest reads here.
func PreparePublication(record Record) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if record.Phase != Ready {
		return nil, nil, errs.New(errs.KindValidationFailed, "DNS check publication requires fresh input")
	}
	value, err := Encode(record)
	if err != nil {
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{
		{Key: RecordKey(record.Owner.TaskID)},
		{Key: OperationKey(record.Owner.OperationID)},
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: conditions[0].Key, Value: value},
		{Type: keyvalue.MutationPut, Key: conditions[1].Key, Value: []byte(record.Owner.TaskID)},
		{Type: keyvalue.MutationPut, Key: ActiveKey(record.Source.ComponentID), Value: []byte(record.Owner.TaskID)},
		{Type: keyvalue.MutationPut, Key: LatestKey(record.Source.ComponentID), Value: []byte(record.Owner.TaskID)},
	}
	return conditions, mutations, nil
}

func (repository *Repository) Load(ctx context.Context, taskID string) (keyvalue.Versioned[Record], error) {
	return repository.loadAt(ctx, taskID, 0)
}

func (repository *Repository) loadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "DNS check Task id is invalid")
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{RecordKey(taskID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "DNS check read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindRequestNotFound, "DNS check was not found")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "DNS check owner is corrupt")
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func (repository *Repository) Latest(ctx context.Context, componentID string) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindComponent, componentID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "DNS check Component id is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{LatestKey(componentID)}})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest DNS check read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindRequestNotFound,
			"CoreDNS has no retained connection check",
		)
	}
	loaded, err := repository.loadAt(ctx, string(read.Values[0].Value), read.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if loaded.Record.Source.ComponentID != componentID {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"latest DNS check belongs to another Component",
		)
	}
	return loaded, nil
}
