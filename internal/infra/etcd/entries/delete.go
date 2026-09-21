package entries

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Repository) DeleteEntry(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[Record],
) (int64, error) {
	if err := ValidateEntryHierarchy(ctx, environment, project, current.Record); err != nil {
		return 0, err
	}
	if err := ValidateEntryVersion(current); err != nil {
		return 0, err
	}
	fence, ownerRevision, err := repository.LoadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			RecordKey(current.Record.Entry.ID),
			EntryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return 0, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return 0, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryDeleteConditions(current, ownerRevision),
		fence.TransactionConditions()...,
	)
	scriptConditions, err := PrepareEntryScriptAbsence(
		ctx,
		repository.store,
		current.Record.Entry.ID,
		fence.ReadRevision(),
	)
	if err != nil {
		return 0, err
	}
	baseCount := len(conditions)
	conditions = append(conditions, scriptConditions...)
	classified := ClassifyEntryScriptAbsenceConflict([]string{current.Record.Entry.ID}, baseCount,
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyEntryDeleteConflict(values, current, ownerRevision, fence)
		})
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: RecordKey(current.Record.Entry.ID)},
		{Type: etcdstore.MutationDelete, Key: EntryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID)},
		{
			Type: etcdstore.MutationDelete, Key: entryvalues.PlainPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		{
			Type: etcdstore.MutationDelete, Key: entryvalues.SecretPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		epochMutation,
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		defer etcdstore.ClearValues(result.FailureReads)
		return 0, classified(result.Revision, result.FailureReads)
	}
	return result.Revision, nil
}
