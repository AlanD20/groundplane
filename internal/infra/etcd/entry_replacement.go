package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EntryRepository) ReplaceEntry(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[entryrecord.Record],
	desired core.EnvEntry,
	valueGenerationID string,
	generation EntryValueGeneration,
) (etcdstore.Versioned[entryrecord.Record], error) {
	replacement, err := entryrecord.ReplaceDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if err := validateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if err := validateEntryVersion(current); err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	primaryValue, err := entryrecord.EncodeRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(replacement, generation)
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
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
		entryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
		fence.transactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(current.Record.Entry.ID), Value: primaryValue},
		{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[entryrecord.Record]{}, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[entryrecord.Record]{}, classifyEntryWriteConflict(
			result.FailureReads, current.Record, current.Revision, ownerRevision, fence,
		)
	}
	return etcdstore.Versioned[entryrecord.Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// ReplaceEntryIdempotent atomically commits replacement desired metadata, one
// immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) ReplaceEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[entryrecord.Record],
	desired core.EnvEntry,
	valueGenerationID string,
	generation EntryValueGeneration,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	replacement, err := entryrecord.ReplaceDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.EnvironmentID ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetEntry ||
		marker.ReplayTarget.ID != current.Record.Entry.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit marker must be a completed Environment-scoped direct mutation for the target Entry",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := entryrecord.EncodeRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(replacement, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	plan, err := NewIdempotencyMutationPlan(
		append(
			entryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
			fence.transactionConditions()...,
		),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(current.Record.Entry.ID), Value: primaryValue},
			{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyEntryWriteConflict(
				values, current.Record, current.Revision, ownerRevision, fence,
			)
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
