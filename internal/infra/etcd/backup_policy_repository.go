package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBackupSourceEnsureAttempts = 3

type backupSourceCreationEvidence struct {
	environment   etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	project       etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	mutationEpoch etcdstore.Versioned[backupruntime.EnvironmentMutationEpochRecord]
}

// BackupPolicyRepository owns the Environment singleton and immutable source
// catalog. Protected replacement is composed separately because it also owns
// Connector references, age-key creation, and exact replay bytes.
type BackupPolicyRepository struct {
	store hierarchyStore
	now   func() time.Time
}

func NewBackupPolicyRepository(store etcdstore.Store) (*BackupPolicyRepository, error) {
	return newBackupPolicyRepository(store)
}

func newBackupPolicyRepository(store hierarchyStore) (*BackupPolicyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup policy store is required")
	}
	return &BackupPolicyRepository{store: store, now: time.Now}, nil
}

func (repository *BackupPolicyRepository) EnsureBackupSource(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	kind core.BackupSourceKind,
	targetID string,
) (etcdstore.Versioned[backuppolicy.BackupSourceRecord], error) {
	if err := validateBackupSourceHierarchy(ctx, environment, project, kind, targetID); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	for attempt := 0; attempt < maximumBackupSourceEnsureAttempts; attempt++ {
		existing, found, err := repository.getBackupSourceByIdentity(
			ctx,
			environment.Record.ID,
			kind,
			targetID,
		)
		if err != nil {
			return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
		}
		if found {
			return existing, nil
		}
		creation, err := repository.loadBackupSourceCreationEvidence(
			ctx,
			environment,
			project,
			kind,
			targetID,
			existing.ReadRevision,
		)
		if err != nil {
			return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
		}
		record := backuppolicy.BackupSourceRecord{
			ID: ids.New(ids.KindBackupSource), EnvironmentID: environment.Record.ID,
			Kind: kind, TargetID: targetID, CreatedAt: repository.now().UTC(),
		}
		created, err := repository.createBackupSource(ctx, creation, record)
		if err == nil {
			return created, nil
		}
		errorKind, ok := errs.KindOf(err)
		if !ok || errorKind != errs.KindStateConflict || attempt == maximumBackupSourceEnsureAttempts-1 {
			return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
		}
	}
	return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, errs.New(
		errs.KindInternal,
		"backup source ensure retry bound was not enforced",
	)
}

func (repository *BackupPolicyRepository) GetBackupSource(
	ctx context.Context,
	sourceID string,
) (etcdstore.Versioned[backuppolicy.BackupSourceRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		backuppolicy.BackupSourceKey(sourceID),
		sourceID,
		errs.KindBackupSourceNotFound,
		backuppolicy.DecodeBackupSourceRecord,
		func(record backuppolicy.BackupSourceRecord) string { return record.ID },
	)
}

func (repository *BackupPolicyRepository) ListBackupSources(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[backuppolicy.BackupSourceRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[backuppolicy.BackupSourceRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"backup-sources",
		"environment",
		environmentID,
		backuppolicy.BackupSourceEnvironmentPrefix(environmentID),
		backuppolicy.BackupSourceKey,
		ids.KindBackupSource,
		request,
		backuppolicy.DecodeBackupSourceRecord,
		func(record backuppolicy.BackupSourceRecord) string { return record.ID },
		func(record backuppolicy.BackupSourceRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *BackupPolicyRepository) GetBackupPolicy(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[backuppolicy.BackupPolicyRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, backuppolicy.BackupPolicyKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup policy read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := backuppolicy.DecodeBackupPolicyRecord(result.Entry.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *BackupPolicyRepository) createBackupSource(
	ctx context.Context,
	evidence backupSourceCreationEvidence,
	record backuppolicy.BackupSourceRecord,
) (etcdstore.Versioned[backuppolicy.BackupSourceRecord], error) {
	value, err := backuppolicy.EncodeBackupSourceRecord(record)
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	defer clear(value)
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(evidence.mutationEpoch.Record)
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	defer clear(epochValue)
	environment := evidence.environment
	project := evidence.project
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: backuppolicy.BackupSourceKey(record.ID)},
		{Key: backuppolicy.BackupSourceEnvironmentKey(record.EnvironmentID, record.ID)},
		{Key: backuppolicy.BackupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID)},
		{
			Key:         hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			ModRevision: evidence.mutationEpoch.Revision,
		},
		{Key: hierarchyrecord.EnvironmentOperationLockKey(environment.Record.ID)},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backuppolicy.BackupSourceKey(record.ID), Value: value},
		{
			Type: etcdstore.MutationPut, Key: backuppolicy.BackupSourceEnvironmentKey(record.EnvironmentID, record.ID),
			Value: []byte(record.ID),
		},
		{
			Type: etcdstore.MutationPut, Key: backuppolicy.BackupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID),
			Value: []byte(record.ID),
		},
		{
			Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			Value: epochValue,
		},
	})
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, classifyBackupSourceCreateConflict(
			result.FailureReads,
			evidence,
		)
	}
	return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *BackupPolicyRepository) loadBackupSourceCreationEvidence(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	kind core.BackupSourceKind,
	targetID string,
	revision int64,
) (backupSourceCreationEvidence, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backuppolicy.BackupSourceIdentityKey(environment.Record.ID, kind, targetID),
			hierarchyrecord.EnvironmentKey(environment.Record.ID),
			hierarchyrecord.ProjectKey(project.Record.ID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID),
			deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID),
			deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
			hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			hierarchyrecord.EnvironmentOperationLockKey(environment.Record.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return backupSourceCreationEvidence{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 8 {
		return backupSourceCreationEvidence{}, errs.New(
			errs.KindInternal,
			"backup source creation evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] != nil {
		return backupSourceCreationEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup source identity was created concurrently",
		)
	}
	if result.Values[1] == nil {
		return backupSourceCreationEvidence{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	currentEnvironment, err := hierarchyrecord.DecodeEnvironment(result.Values[1].Value)
	if err != nil || currentEnvironment.ID != environment.Record.ID ||
		currentEnvironment.ProjectID != project.Record.ID {
		return backupSourceCreationEvidence{}, recordcodec.CorruptRecord()
	}
	if result.Values[2] == nil {
		return backupSourceCreationEvidence{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	currentProject, err := hierarchyrecord.DecodeProject(result.Values[2].Value)
	if err != nil || currentProject.ID != project.Record.ID || currentProject.TenantID != project.Record.TenantID ||
		currentProject.Kind != hierarchyrecord.ProjectKindTenant {
		return backupSourceCreationEvidence{}, recordcodec.CorruptRecord()
	}
	for _, index := range []int{3, 4, 5} {
		if result.Values[index] != nil {
			return backupSourceCreationEvidence{}, errs.New(
				errs.KindResourceInUse,
				"backup source hierarchy deletion is in progress",
			)
		}
	}
	if result.Values[7] != nil {
		return backupSourceCreationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"environment persistence operation is in progress",
		)
	}
	if result.Values[6] == nil {
		return backupSourceCreationEvidence{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(result.Values[6].Value)
	if err != nil || epoch.EnvironmentID != environment.Record.ID {
		return backupSourceCreationEvidence{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	return backupSourceCreationEvidence{
		environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record: currentEnvironment, Revision: result.Values[1].ModRevision, ReadRevision: result.ReadRevision,
		},
		project: etcdstore.Versioned[hierarchyrecord.ProjectRecord]{
			Record: currentProject, Revision: result.Values[2].ModRevision, ReadRevision: result.ReadRevision,
		},
		mutationEpoch: etcdstore.Versioned[backupruntime.EnvironmentMutationEpochRecord]{
			Record: epoch, Revision: result.Values[6].ModRevision, ReadRevision: result.ReadRevision,
		},
	}, nil
}

func (repository *BackupPolicyRepository) getBackupSourceByIdentity(
	ctx context.Context,
	environmentID string,
	kind core.BackupSourceKind,
	targetID string,
) (etcdstore.Versioned[backuppolicy.BackupSourceRecord], bool, error) {
	index, err := repository.store.Get(ctx, backuppolicy.BackupSourceIdentityKey(environmentID, kind, targetID))
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, err
	}
	if index == nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup source identity read is empty",
		)
	}
	if index.Entry == nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{ReadRevision: index.ReadRevision}, false, nil
	}
	sourceID := string(index.Entry.Value)
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, recordcodec.CorruptRecord()
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backuppolicy.BackupSourceKey(sourceID),
			backuppolicy.BackupSourceEnvironmentKey(environmentID, sourceID),
		},
		Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil ||
		string(stored.Values[1].Value) != sourceID {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, recordcodec.CorruptRecord()
	}
	record, err := backuppolicy.DecodeBackupSourceRecord(stored.Values[0].Value)
	if err != nil || record.ID != sourceID || record.EnvironmentID != environmentID ||
		record.Kind != kind || record.TargetID != targetID {
		return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{}, false, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[backuppolicy.BackupSourceRecord]{
		Record: record, Revision: stored.Values[0].ModRevision, ReadRevision: stored.ReadRevision,
	}, true, nil
}

func validateBackupSourceHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	kind core.BackupSourceKind,
	targetID string,
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
	probe := backuppolicy.BackupSourceRecord{
		ID: ids.New(ids.KindBackupSource), EnvironmentID: environment.Record.ID,
		Kind: kind, TargetID: targetID, CreatedAt: time.Now().UTC(),
	}
	if err := backuppolicy.ValidateBackupSourceRecord(probe); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision ||
		project.Revision <= 0 || project.ReadRevision < project.Revision ||
		environment.Record.ProjectID != project.Record.ID || project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		project.Record.TenantID == "" {
		return errs.New(errs.KindValidationFailed, "backup source hierarchy is invalid")
	}
	return nil
}

func classifyBackupSourceCreateConflict(
	values []*etcdstore.KeyValue,
	evidence backupSourceCreationEvidence,
) error {
	if len(values) != 10 {
		return errs.New(errs.KindInternal, "backup source create compare evidence is incomplete")
	}
	environment := evidence.environment
	project := evidence.project
	if values[2] != nil {
		return errs.New(errs.KindStateConflict, "backup source identity was created concurrently")
	}
	if values[0] != nil || values[1] != nil {
		return errs.New(errs.KindStateConflict, "backup source stable identity collided")
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
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
			return errs.New(errs.KindResourceInUse, "backup source hierarchy deletion is in progress")
		}
	}
	if values[9] != nil {
		return errs.New(errs.KindResourceInUse, "environment persistence operation is in progress")
	}
	if values[8] == nil {
		return errs.New(errs.KindInternal, "environment mutation epoch is missing")
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(values[8].Value)
	if err != nil || epoch.EnvironmentID != environment.Record.ID {
		return errs.New(errs.KindInternal, "environment mutation epoch is corrupt")
	}
	if values[8].ModRevision != evidence.mutationEpoch.Revision {
		return stateConflict("environment mutation epoch", environment.Record.ID)
	}
	return errs.New(errs.KindStateConflict, "backup source state changed")
}
