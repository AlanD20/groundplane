package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateEntryIdempotent atomically commits desired metadata, its owner index,
// one immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) CreateEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record entryrecord.Record,
	generation entryrecord.EntryValueGeneration,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := entryrecord.ValidateEntryHierarchy(ctx, environment, project, record); err != nil {
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
	generationKey, generationValue, err := entryrecord.PrepareEntryGeneration(record, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.LoadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(record.Entry.ID),
			entryrecord.EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	plan, err := NewIdempotencyMutationPlan(
		append(
			entryrecord.EntryWriteConditions(record, generationKey, 0, 0),
			fence.TransactionConditions()...,
		),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(record.Entry.ID), Value: primaryValue},
			{
				Type: etcdstore.MutationPut, Key: entryrecord.EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
				Value: []byte(record.Entry.ID),
			},
			{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*etcdstore.KeyValue) error {
			return entryrecord.ClassifyEntryWriteConflict(values, record, 0, 0, fence)
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
