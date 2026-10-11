package connectorchecks

import (
	"context"
	"maps"
	"slices"

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
		return nil, errs.New(errs.KindInternal, "connector check store is required")
	}
	return &Repository{store: backend}, nil
}

// PreparePublication is joined to Task publication and exact Secret pin
// reservation by the native Controller publisher. It performs no remote work.
func PreparePublication(record Record) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if record.Phase != Ready {
		return nil, nil, errs.New(errs.KindValidationFailed, "connector check publication must be fresh")
	}
	value, err := Encode(record)
	if err != nil {
		return nil, nil, err
	}
	key, reference := Key(record.Owner.TaskID), ReferenceKey(record.Connector.Connector.ID, record.Owner.TaskID)
	operation := OperationKey(record.Owner.OperationID)
	environmentReference := EnvironmentReferenceKey(record.Owner.EnvironmentID, record.Owner.TaskID)
	return []keyvalue.Condition{
		{Key: key},
		{Key: reference},
		{Key: operation},
		{Key: environmentReference},
	}, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: key, Value: value},
		{Type: keyvalue.MutationPut, Key: reference, Value: []byte(record.Owner.TaskID)},
		{Type: keyvalue.MutationPut, Key: operation, Value: []byte(record.Owner.TaskID)},
		{Type: keyvalue.MutationPut, Key: environmentReference, Value: []byte(record.Owner.TaskID)},
	}, nil
}

func (repository *Repository) Load(ctx context.Context, taskID string) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "connector check Task id is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID)}})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "connector check read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindRequestNotFound,
			"connector connection check was not found",
		)
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID {
		record.Clear()
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "connector check owner is corrupt")
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

// Clone exposes detached captured input to its executor, never to a public API.
func Clone(record Record) Record {
	copyOfRecord := record
	copyOfRecord.Connector.Connector.Credentials = maps.Clone(record.Connector.Connector.Credentials)
	copyOfRecord.SecretSources = slices.Clone(record.SecretSources)
	copyOfRecord.Stages = slices.Clone(record.Stages)
	copyOfRecord.Cleanup.UploadIDs = slices.Clone(record.Cleanup.UploadIDs)
	if record.StartedAt != nil {
		startedAt := *record.StartedAt
		copyOfRecord.StartedAt = &startedAt
	}
	if record.FinishedAt != nil {
		finishedAt := *record.FinishedAt
		copyOfRecord.FinishedAt = &finishedAt
	}
	if record.Credentials != nil {
		credentials := *record.Credentials
		credentials.Ciphertext = slices.Clone(record.Credentials.Ciphertext)
		copyOfRecord.Credentials = &credentials
	}
	return copyOfRecord
}
