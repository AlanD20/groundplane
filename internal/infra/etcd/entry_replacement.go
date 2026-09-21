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

// ReplaceEntryIdempotent atomically commits replacement desired metadata, one
// immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) ReplaceEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[entryrecord.Record],
	desired core.EnvEntry,
	valueGenerationID string,
	generation entryrecord.EntryValueGeneration,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	replacement, err := entryrecord.ReplaceDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := entryrecord.ValidateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := entryrecord.ValidateEntryVersion(current); err != nil {
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
	generationKey, generationValue, err := entryrecord.PrepareEntryGeneration(replacement, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.LoadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(current.Record.Entry.ID),
			entryrecord.EntryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
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
			entryrecord.EntryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
			fence.TransactionConditions()...,
		),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(current.Record.Entry.ID), Value: primaryValue},
			{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*etcdstore.KeyValue) error {
			return entryrecord.ClassifyEntryWriteConflict(
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
