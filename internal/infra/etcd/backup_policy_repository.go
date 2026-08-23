package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBackupSourceEnsureAttempts = 3

// BackupPolicyRepository owns the Environment singleton and immutable source
// catalog. Protected replacement is composed separately because it also owns
// Connector references, age-key creation, and exact replay bytes.
type BackupPolicyRepository struct {
	store hierarchyStore
	now   func() time.Time
}

func NewBackupPolicyRepository(store Store) (*BackupPolicyRepository, error) {
	return newBackupPolicyRepository(store)
}

func newBackupPolicyRepository(store hierarchyStore) (*BackupPolicyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Backup Policy store is required")
	}
	return &BackupPolicyRepository{store: store, now: time.Now}, nil
}

func (repository *BackupPolicyRepository) EnsureBackupSource(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	kind core.BackupSourceKind,
	targetID string,
) (Versioned[BackupSourceRecord], error) {
	if err := validateBackupSourceHierarchy(ctx, environment, project, kind, targetID); err != nil {
		return Versioned[BackupSourceRecord]{}, err
	}
	for attempt := 0; attempt < maximumBackupSourceEnsureAttempts; attempt++ {
		existing, found, err := repository.getBackupSourceByIdentity(
			ctx,
			environment.Record.ID,
			kind,
			targetID,
		)
		if err != nil {
			return Versioned[BackupSourceRecord]{}, err
		}
		if found {
			return existing, nil
		}
		record := BackupSourceRecord{
			ID: ids.New(ids.KindBackupSource), EnvironmentID: environment.Record.ID,
			Kind: kind, TargetID: targetID, CreatedAt: repository.now().UTC(),
		}
		created, err := repository.createBackupSource(ctx, environment, project, record)
		if err == nil {
			return created, nil
		}
		errorKind, ok := errs.KindOf(err)
		if !ok || errorKind != errs.KindStateConflict || attempt == maximumBackupSourceEnsureAttempts-1 {
			return Versioned[BackupSourceRecord]{}, err
		}
	}
	return Versioned[BackupSourceRecord]{}, errs.New(
		errs.KindInternal,
		"Backup source ensure retry bound was not enforced",
	)
}

func (repository *BackupPolicyRepository) GetBackupSource(
	ctx context.Context,
	sourceID string,
) (Versioned[BackupSourceRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BackupSourceRecord]{}, err
	}
	if err := validateID(ids.KindBackupSource, sourceID); err != nil {
		return Versioned[BackupSourceRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		backupSourceKey(sourceID),
		sourceID,
		errs.KindBackupSourceNotFound,
		decodeBackupSourceRecord,
		func(record BackupSourceRecord) string { return record.ID },
	)
}

func (repository *BackupPolicyRepository) ListBackupSources(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[BackupSourceRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[BackupSourceRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"backup-sources",
		"environment",
		environmentID,
		backupSourceEnvironmentPrefix(environmentID),
		backupSourceKey,
		ids.KindBackupSource,
		request,
		decodeBackupSourceRecord,
		func(record BackupSourceRecord) string { return record.ID },
		func(record BackupSourceRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *BackupPolicyRepository) GetBackupPolicy(
	ctx context.Context,
	environmentID string,
) (Versioned[BackupPolicyRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BackupPolicyRecord]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[BackupPolicyRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, backupPolicyKey(environmentID))
	if err != nil {
		return Versioned[BackupPolicyRecord]{}, false, err
	}
	if result == nil {
		return Versioned[BackupPolicyRecord]{}, false, errs.New(
			errs.KindInternal,
			"Backup Policy read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[BackupPolicyRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeBackupPolicyRecord(result.Entry.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return Versioned[BackupPolicyRecord]{}, false, corruptRecord()
	}
	return Versioned[BackupPolicyRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *BackupPolicyRepository) createBackupSource(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record BackupSourceRecord,
) (Versioned[BackupSourceRecord], error) {
	value, err := encodeBackupSourceRecord(record)
	if err != nil {
		return Versioned[BackupSourceRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: backupSourceKey(record.ID)},
		{Key: backupSourceEnvironmentKey(record.EnvironmentID, record.ID)},
		{Key: backupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID)},
	}, []Mutation{
		{Type: MutationPut, Key: backupSourceKey(record.ID), Value: value},
		{
			Type: MutationPut, Key: backupSourceEnvironmentKey(record.EnvironmentID, record.ID),
			Value: []byte(record.ID),
		},
		{
			Type: MutationPut, Key: backupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID),
			Value: []byte(record.ID),
		},
	})
	if err != nil {
		return Versioned[BackupSourceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[BackupSourceRecord]{}, classifyBackupSourceCreateConflict(
			result.FailureReads,
			environment,
			project,
		)
	}
	return Versioned[BackupSourceRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *BackupPolicyRepository) getBackupSourceByIdentity(
	ctx context.Context,
	environmentID string,
	kind core.BackupSourceKind,
	targetID string,
) (Versioned[BackupSourceRecord], bool, error) {
	index, err := repository.store.Get(ctx, backupSourceIdentityKey(environmentID, kind, targetID))
	if err != nil {
		return Versioned[BackupSourceRecord]{}, false, err
	}
	if index == nil {
		return Versioned[BackupSourceRecord]{}, false, errs.New(
			errs.KindInternal,
			"Backup source identity read is empty",
		)
	}
	if index.Entry == nil {
		return Versioned[BackupSourceRecord]{ReadRevision: index.ReadRevision}, false, nil
	}
	sourceID := string(index.Entry.Value)
	if err := validateID(ids.KindBackupSource, sourceID); err != nil {
		return Versioned[BackupSourceRecord]{}, false, corruptRecord()
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			backupSourceKey(sourceID),
			backupSourceEnvironmentKey(environmentID, sourceID),
		},
		Revision: index.ReadRevision,
	})
	if err != nil {
		return Versioned[BackupSourceRecord]{}, false, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil ||
		string(stored.Values[1].Value) != sourceID {
		return Versioned[BackupSourceRecord]{}, false, corruptRecord()
	}
	record, err := decodeBackupSourceRecord(stored.Values[0].Value)
	if err != nil || record.ID != sourceID || record.EnvironmentID != environmentID ||
		record.Kind != kind || record.TargetID != targetID {
		return Versioned[BackupSourceRecord]{}, false, corruptRecord()
	}
	return Versioned[BackupSourceRecord]{
		Record: record, Revision: stored.Values[0].ModRevision, ReadRevision: stored.ReadRevision,
	}, true, nil
}

func validateBackupSourceHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	kind core.BackupSourceKind,
	targetID string,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	probe := BackupSourceRecord{
		ID: ids.New(ids.KindBackupSource), EnvironmentID: environment.Record.ID,
		Kind: kind, TargetID: targetID, CreatedAt: time.Now().UTC(),
	}
	if err := validateBackupSourceRecord(probe); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision ||
		project.Revision <= 0 || project.ReadRevision < project.Revision ||
		environment.Record.ProjectID != project.Record.ID || project.Record.Kind != ProjectKindTenant ||
		project.Record.TenantID == "" {
		return errs.New(errs.KindValidationFailed, "Backup source hierarchy is invalid")
	}
	return nil
}

func classifyBackupSourceCreateConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
) error {
	if len(values) != 8 {
		return errs.New(errs.KindInternal, "Backup source create compare evidence is incomplete")
	}
	if values[2] != nil {
		return errs.New(errs.KindStateConflict, "Backup source identity was created concurrently")
	}
	if values[0] != nil || values[1] != nil {
		return errs.New(errs.KindStateConflict, "Backup source stable identity collided")
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Backup source hierarchy deletion is in progress")
		}
	}
	return errs.New(errs.KindStateConflict, "Backup source state changed")
}
