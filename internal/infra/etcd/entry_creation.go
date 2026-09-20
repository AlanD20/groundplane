package etcd

import (
	"context"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EntryRepository) CreateEntry(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record entryrecord.Record,
	generation EntryValueGeneration,
) (etcdstore.Versioned[entryrecord.Record], error) {
	if err := validateEntryHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	primaryValue, err := entryrecord.EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(record, generation)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(record.Entry.ID),
			entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
	)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryWriteConditions(record, generationKey, 0, 0),
		fence.transactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(record.Entry.ID), Value: primaryValue},
		{
			Type: etcdstore.MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			Value: []byte(record.Entry.ID),
		},
		{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return etcdstore.Versioned[entryrecord.Record]{}, classifyEntryWriteConflict(
			result.FailureReads, record, 0, 0, fence,
		)
	}
	return etcdstore.Versioned[entryrecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// CreateEntryIdempotent atomically commits desired metadata, its owner index,
// one immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) CreateEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record entryrecord.Record,
	generation EntryValueGeneration,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateEntryHierarchy(ctx, environment, project, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Entry creation marker must be a completed direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := entryrecord.EncodeRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(record, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(record.Entry.ID),
			entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	plan, err := newIdempotencyMutationPlan(
		append(
			entryWriteConditions(record, generationKey, 0, 0),
			fence.transactionConditions()...,
		),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(record.Entry.ID), Value: primaryValue},
			{
				Type: etcdstore.MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
				Value: []byte(record.Entry.ID),
			},
			{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyEntryWriteConflict(values, record, 0, 0, fence)
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
