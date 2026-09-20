package etcd

import (
	"context"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *EntryRepository) DeleteEntry(
	ctx context.Context,
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	current Versioned[entryrecord.Record],
) (int64, error) {
	if err := validateEntryHierarchy(ctx, environment, project, current.Record); err != nil {
		return 0, err
	}
	if err := validateEntryVersion(current); err != nil {
		return 0, err
	}
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryrecord.RecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return 0, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return 0, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryDeleteConditions(current, ownerRevision),
		fence.transactionConditions()...,
	)
	scriptConditions, err := prepareEntryScriptAbsence(
		ctx,
		repository.store,
		current.Record.Entry.ID,
		fence.readAtRevision(),
	)
	if err != nil {
		return 0, err
	}
	baseCount := len(conditions)
	conditions = append(conditions, scriptConditions...)
	classified := classifyEntryScriptAbsenceConflict([]string{current.Record.Entry.ID}, baseCount,
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyEntryDeleteConflict(values, current, ownerRevision, fence)
		})
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: entryrecord.RecordKey(current.Record.Entry.ID)},
		{Type: etcdstore.MutationDelete, Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID)},
		{
			Type: etcdstore.MutationDelete, Key: entryPlainValueGenerationPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		{
			Type: etcdstore.MutationDelete, Key: entrySecretValueGenerationPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		epochMutation,
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return 0, classified(result.Revision, result.FailureReads)
	}
	return result.Revision, nil
}
