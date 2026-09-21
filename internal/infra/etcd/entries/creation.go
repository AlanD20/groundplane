package entries

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Repository) CreateEntry(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record Record,
	generation EntryValueGeneration,
) (etcdstore.Versioned[Record], error) {
	if err := ValidateEntryHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	primaryValue, err := EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := PrepareEntryGeneration(record, generation)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.LoadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			RecordKey(record.Entry.ID),
			EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
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
		EntryWriteConditions(record, generationKey, 0, 0),
		fence.TransactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: RecordKey(record.Entry.ID), Value: primaryValue},
		{
			Type: etcdstore.MutationPut, Key: EntryOwnerKey(record.EnvironmentID, record.Entry.ID),
			Value: []byte(record.Entry.ID),
		},
		{Type: etcdstore.MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[Record]{}, ClassifyEntryWriteConflict(
			result.FailureReads, record, 0, 0, fence,
		)
	}
	return etcdstore.Versioned[Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
