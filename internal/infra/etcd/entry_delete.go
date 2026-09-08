package etcd

import "context"

func (repository *EntryRepository) DeleteEntry(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
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
			entryRecordKey(current.Record.Entry.ID),
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
		func(_ int64, values []*KeyValue) error {
			return classifyEntryDeleteConflict(values, current, ownerRevision, fence)
		})
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationDelete, Key: entryRecordKey(current.Record.Entry.ID)},
		{Type: MutationDelete, Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID)},
		{
			Type: MutationDelete, Key: entryPlainValueGenerationPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		{
			Type: MutationDelete, Key: entrySecretValueGenerationPrefix + current.Record.Entry.ID + "/",
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
