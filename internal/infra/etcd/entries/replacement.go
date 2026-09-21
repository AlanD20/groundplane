package entries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Repository) ReplaceEntry(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[Record],
	desired core.EnvEntry,
	valueGenerationID string,
	generation EntryValueGeneration,
) (etcdstore.Versioned[Record], error) {
	replacement, err := ReplaceDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := ValidateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := ValidateEntryVersion(current); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	primaryValue, err := EncodeRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := PrepareEntryGeneration(replacement, generation)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.LoadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			RecordKey(current.Record.Entry.ID),
			EntryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}

	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		EntryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
		fence.TransactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: RecordKey(current.Record.Entry.ID), Value: primaryValue},
		{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[Record]{}, ClassifyEntryWriteConflict(
			result.FailureReads, current.Record, current.Revision, ownerRevision, fence,
		)
	}
	return etcdstore.Versioned[Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
