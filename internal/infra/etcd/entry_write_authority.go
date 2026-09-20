package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EntryRepository) loadEntryMutationFence(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	domainKeys []string,
	ownerIndex int,
	entryID string,
) (environmentMutationFenceEvidence, int64, error) {
	keys := append([]string(nil), domainKeys...)
	environmentIndex := len(keys)
	keys = append(keys, hierarchyrecord.EnvironmentKey(environment.Record.ID))
	projectIndex := len(keys)
	keys = append(keys, hierarchyrecord.ProjectKey(project.Record.ID))
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: keys,
	})
	if err != nil {
		return environmentMutationFenceEvidence{}, 0, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return environmentMutationFenceEvidence{}, 0, errs.New(
			errs.KindInternal,
			"Entry mutation fixed-revision evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			return environmentMutationFenceEvidence{}, 0, errs.New(
				errs.KindInternal,
				"Entry mutation fixed-revision evidence is corrupt",
			)
		}
	}
	if result.Values[environmentIndex] == nil {
		return environmentMutationFenceEvidence{}, 0, errs.New(
			errs.KindEnvironmentNotFound,
			"Environment was not found",
		)
	}
	if result.Values[environmentIndex].ModRevision != environment.Revision {
		return environmentMutationFenceEvidence{}, 0, stateConflict("Environment", environment.Record.ID)
	}
	if result.Values[projectIndex] == nil {
		return environmentMutationFenceEvidence{}, 0, errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if result.Values[projectIndex].ModRevision != project.Revision {
		return environmentMutationFenceEvidence{}, 0, stateConflict("Project", project.Record.ID)
	}
	ownerRevision := int64(0)
	if ownerIndex >= 0 {
		if ownerIndex >= len(domainKeys) || result.Values[ownerIndex] == nil ||
			string(result.Values[ownerIndex].Value) != entryID {
			return environmentMutationFenceEvidence{}, 0, errs.New(
				errs.KindInternal,
				"Entry owner index is missing or corrupt",
			)
		}
		ownerRevision = result.Values[ownerIndex].ModRevision
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		environment.Record.ID,
		result.ReadRevision,
	)
	if err != nil {
		return environmentMutationFenceEvidence{}, 0, err
	}
	return fence, ownerRevision, nil
}

func entryWriteConditions(
	record entryrecord.Record,
	generationKey string,
	entryRevision int64,
	ownerRevision int64,
) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: entryrecord.RecordKey(record.Entry.ID), ModRevision: entryRevision},
		{Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID), ModRevision: ownerRevision},
		{Key: generationKey},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEntry), record.Entry.ID)},
	}
	return conditions
}

func entryDeleteConditions(
	current etcdstore.Versioned[entryrecord.Record],
	ownerRevision int64,
) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: entryrecord.RecordKey(current.Record.Entry.ID), ModRevision: current.Revision},
		{Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID), ModRevision: ownerRevision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEntry), current.Record.Entry.ID)},
	}
	return conditions
}

func validateEntryHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record entryrecord.Record,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := entryrecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision ||
		project.Revision <= 0 || project.ReadRevision < project.Revision ||
		record.EnvironmentID != environment.Record.ID || environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Entry hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateEntryVersion(current etcdstore.Versioned[entryrecord.Record]) error {
	if err := entryrecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Entry version metadata is invalid")
	}
	return nil
}
