package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	// These are durable wire values, kept infra-local to preserve the import matrix.
	backupConnectorKindS3Compatible = "s3-compatible"
	backupAttachStatusReady         = "ready"
)

// ManualBackupRunInput contains only newly allocated operation identities and
// an optional caller-selected read revision. The repository derives evidence.
type ManualBackupRunInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	FixedRevision int64
	CreatedAt     time.Time
	Initiator     backupruntime.BackupRunInitiator
	ScheduledAt   *time.Time
}

type BackupPostgresIdentity struct {
	Database string
	Role     string
}

// BackupPostgresIdentityResolver opens only the fixed-revision encrypted facts
// supplied by the repository.
type BackupPostgresIdentityResolver func(
	context.Context,
	etcdstore.Versioned[attachrecord.Record],
	attachrecord.EncryptedFacts,
	func(BackupPostgresIdentity) error,
) error

type PreparedManualBackupRun struct {
	Run         backupruntime.BackupRunRecord
	Owner       TaskOwner
	Publication *PreparedBackupRunPublication
}

type preparedBackupRunState struct {
	mu         sync.Mutex
	repository *BackupRuntimeRepository
	plan       backupRunPublicationPlan
	consumed   bool
}

// PreparedBackupRunPublication is shared-state one-shot authority.
type PreparedBackupRunPublication struct{ state *preparedBackupRunState }

// PrepareManualBackupRun derives all durable evidence from one fixed revision.
func (repository *BackupRuntimeRepository) PrepareManualBackupRun(
	ctx context.Context,
	input ManualBackupRunInput,
	resolvePostgres BackupPostgresIdentityResolver,
) (PreparedManualBackupRun, error) {
	if repository == nil || repository.store == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindInternal,
			"backup runtime repository is not configured",
		)
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return PreparedManualBackupRun{}, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(
			ids.KindTask,
			input.TaskID,
		) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(
			ids.KindPlan,
			input.PlanID,
		) != nil || !backupruntime.ValidBackupRuntimeInstant(input.CreatedAt) ||
		input.FixedRevision < 0 || resolvePostgres == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindValidationFailed,
			"manual backup run input is invalid",
		)
	}
	anchorKeys := []string{
		hierarchyrecord.EnvironmentKey(input.EnvironmentID),
		backuppolicy.BackupPolicyKey(input.EnvironmentID),
	}
	var anchor *etcdstore.GetManyResult
	var err error
	if input.FixedRevision == 0 {
		anchor, err = repository.readCurrentKeys(ctx, anchorKeys)
	} else {
		anchor, err = repository.readFixedKeys(ctx, anchorKeys, input.FixedRevision)
	}
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[1] == nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindStateConflict,
			"backup Environment or policy is unavailable",
		)
	}
	fixedRevision := anchor.ReadRevision
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(anchor.Values[0].Value)
	policy, policyErr := backuppolicy.DecodeBackupPolicyRecord(anchor.Values[1].Value)
	if environmentErr != nil || policyErr != nil || environment.ID != input.EnvironmentID ||
		policy.EnvironmentID != input.EnvironmentID {
		return PreparedManualBackupRun{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	if !policy.Enabled || len(policy.SourceIDs) == 0 ||
		len(policy.SourceIDs) > backuppolicy.MaximumBackupPolicySources {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindStateConflict,
			"backup policy is disabled or unconfigured",
		)
	}
	owner, err := repository.manualBackupOwner(ctx, environment, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	connector, connectorRevision, credentialsRevision, hasDirect, err := repository.manualBackupConnector(
		ctx,
		policy.ConnectorID,
		input.EnvironmentID,
		fixedRevision,
	)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	initiator := input.Initiator
	if initiator == "" {
		initiator = backupruntime.BackupRunInitiatorOperator
	}
	if initiator != backupruntime.BackupRunInitiatorOperator && initiator != backupruntime.BackupRunInitiatorSchedule {
		return PreparedManualBackupRun{}, errs.New(errs.KindValidationFailed, "backup run initiator is invalid")
	}
	if initiator == backupruntime.BackupRunInitiatorOperator && input.ScheduledAt != nil {
		return PreparedManualBackupRun{}, errs.New(
			errs.KindValidationFailed,
			"operator backup run cannot have a schedule",
		)
	}
	if initiator == backupruntime.BackupRunInitiatorSchedule &&
		(input.ScheduledAt == nil || !backupruntime.ValidBackupRuntimeInstant(input.ScheduledAt.UTC()) || input.ScheduledAt.After(input.CreatedAt)) {
		return PreparedManualBackupRun{}, errs.New(errs.KindValidationFailed, "scheduled backup run time is invalid")
	}
	run := backupruntime.BackupRunRecord{
		TaskID:                        input.TaskID,
		OperationID:                   input.OperationID,
		EnvironmentID:                 input.EnvironmentID,
		PolicyRevision:                anchor.Values[1].ModRevision,
		RetentionKeep:                 int64(policy.Keep),
		Initiator:                     initiator,
		ConnectorID:                   policy.ConnectorID,
		ConnectorRevision:             connectorRevision,
		ConnectorEndpoint:             connector.Connector.Endpoint,
		ConnectorBucket:               connector.Connector.Bucket,
		ConnectorPrefix:               connector.Connector.Prefix,
		ConnectorRegion:               connector.Connector.Region,
		ConnectorPathStyle:            connector.Connector.PathStyle,
		ConnectorHasDirectCredentials: hasDirect,
		ConnectorCredentialsRevision:  credentialsRevision,
		Encryption:                    backupruntime.BackupRuntimeEncryption(policy.Encryption),
		State:                         backupruntime.BackupRunQueued,
		CreatedAt:                     input.CreatedAt.UTC(),
		UpdatedAt:                     input.CreatedAt.UTC(),
	}
	if input.ScheduledAt != nil {
		scheduledAt := input.ScheduledAt.UTC()
		run.ScheduledAt = &scheduledAt
	}
	if err := repository.manualBackupKey(ctx, &run, fixedRevision); err != nil {
		return PreparedManualBackupRun{}, err
	}
	sourceKeys := make([]string, len(policy.SourceIDs))
	for index, sourceID := range policy.SourceIDs {
		sourceKeys[index] = backuppolicy.BackupSourceKey(sourceID)
	}
	sourceRead, err := repository.readFixedKeys(ctx, sourceKeys, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	defer clearKeyValues(sourceRead.Values)
	run.Sources = make([]backupruntime.BackupRunSourceAttemptRecord, len(sourceRead.Values))
	for index, value := range sourceRead.Values {
		if value == nil {
			return PreparedManualBackupRun{}, errs.New(
				errs.KindStateConflict,
				"backup source is unavailable",
			)
		}
		source, decodeErr := backuppolicy.DecodeBackupSourceRecord(value.Value)
		if decodeErr != nil || source.ID != policy.SourceIDs[index] ||
			source.EnvironmentID != input.EnvironmentID {
			return PreparedManualBackupRun{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		attempt, prepareErr := repository.prepareManualBackupSource(
			ctx, run, source, value.ModRevision, uint32(index), fixedRevision, resolvePostgres,
		)
		if prepareErr != nil {
			return PreparedManualBackupRun{}, prepareErr
		}
		run.Sources[index] = attempt
	}
	lock := backupruntime.BackupOperationLockRecord{
		EnvironmentID: input.EnvironmentID, OperationID: input.OperationID, TaskID: input.TaskID,
		Kind: backupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublication(ctx, run, lock, fixedRevision)
	if err != nil {
		return PreparedManualBackupRun{}, err
	}
	publication := &PreparedBackupRunPublication{
		state: &preparedBackupRunState{repository: repository, plan: plan},
	}
	return PreparedManualBackupRun{
		Run: cloneBackupRunPublicationRecord(run), Owner: owner, Publication: publication,
	}, nil
}

func (repository *BackupRuntimeRepository) manualBackupOwner(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
	fixedRevision int64,
) (TaskOwner, error) {
	read, err := repository.readFixedKeys(
		ctx,
		[]string{hierarchyrecord.ProjectKey(environment.ProjectID)},
		fixedRevision,
	)
	if err != nil {
		return TaskOwner{}, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil {
		return TaskOwner{}, errs.New(errs.KindStateConflict, "backup Project is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(read.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return TaskOwner{}, errs.New(
			errs.KindStateConflict,
			"backing Environments cannot run consumer backups",
		)
	}
	tenantRead, err := repository.readFixedKeys(
		ctx,
		[]string{hierarchyrecord.TenantKey(project.TenantID)},
		fixedRevision,
	)
	if err != nil {
		return TaskOwner{}, err
	}
	defer clearKeyValues(tenantRead.Values)
	if tenantRead.Values[0] == nil {
		return TaskOwner{}, errs.New(errs.KindStateConflict, "backup Tenant is unavailable")
	}
	tenant, err := hierarchyrecord.DecodeTenant(tenantRead.Values[0].Value)
	if err != nil || tenant.ID != project.TenantID {
		return TaskOwner{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	return EnvironmentTaskOwner(project, environment)
}

func (repository *BackupRuntimeRepository) manualBackupConnector(
	ctx context.Context,
	connectorID string,
	environmentID string,
	fixedRevision int64,
) (connectorrecord.Record, int64, int64, bool, error) {
	read, err := repository.readFixedKeys(ctx, []string{
		connectorrecord.RecordKey(connectorID), connectorrecord.CredentialValueKey(connectorID),
	}, fixedRevision)
	if err != nil {
		return connectorrecord.Record{}, 0, 0, false, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict,
			"backup Connector is unavailable",
		)
	}
	connector, err := connectorrecord.DecodeRecord(read.Values[0].Value)
	if err != nil || connector.Connector.ID != connectorID ||
		connector.Connector.EnvironmentID != environmentID ||
		connector.Connector.Kind != backupConnectorKindS3Compatible {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict,
			"backup Connector evidence changed",
		)
	}
	hasDirect := connectorrecord.HasDirectCredentials(connector)
	if !hasDirect {
		if read.Values[1] != nil {
			return connectorrecord.Record{}, 0, 0, false, errs.New(
				errs.KindStateConflict, "backup Connector credential evidence changed",
			)
		}
		return connector, read.Values[0].ModRevision, 0, false, nil
	}
	if read.Values[1] == nil {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict, "backup Connector credentials are unavailable",
		)
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(read.Values[1].Value)
	if err != nil || credentials.ConnectorID != connectorID {
		clear(credentials.Ciphertext)
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict, "backup Connector credential evidence changed",
		)
	}
	clear(credentials.Ciphertext)
	return connector, read.Values[0].ModRevision, read.Values[1].ModRevision, true, nil
}

func (repository *BackupRuntimeRepository) manualBackupKey(
	ctx context.Context,
	run *backupruntime.BackupRunRecord,
	fixedRevision int64,
) error {
	if run.Encryption == backupruntime.BackupRuntimeEncryptionNone {
		return nil
	}
	if run.Encryption != backupruntime.BackupRuntimeEncryptionAge {
		return errs.New(errs.KindStateConflict, "backup encryption strategy is unsupported")
	}
	read, err := repository.readFixedKeys(ctx, []string{
		backuppolicy.BackupKeyKey(run.EnvironmentID), backuppolicy.BackupKeyValueKey(run.EnvironmentID),
	}, fixedRevision)
	if err != nil {
		return err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return errs.New(errs.KindStateConflict, "backup age key is unavailable")
	}
	record, recordErr := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	value, valueErr := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[1].Value)
	defer clear(value.Ciphertext)
	if recordErr != nil || valueErr != nil || record.EnvironmentID != run.EnvironmentID ||
		value.EnvironmentID != run.EnvironmentID || record.KeyEra != value.KeyEra {
		return errs.New(errs.KindStateConflict, "backup age key evidence changed")
	}
	run.BackupKeyRecordRevision = read.Values[0].ModRevision
	run.BackupKeyValueRevision = read.Values[1].ModRevision
	run.KeyEra = record.KeyEra
	run.Recipient = record.Recipient
	return nil
}

func (repository *BackupRuntimeRepository) prepareManualBackupSource(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	source backuppolicy.BackupSourceRecord,
	sourceRevision int64,
	ordinal uint32,
	fixedRevision int64,
	resolvePostgres BackupPostgresIdentityResolver,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	pointID := ids.New(ids.KindRecoveryPoint)
	pointCreatedAt, err := ids.Timestamp(ids.KindRecoveryPoint, pointID)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	attempt := backupruntime.BackupRunSourceAttemptRecord{
		Ordinal:                ordinal,
		SourceID:               source.ID,
		Kind:                   backupruntime.BackupRuntimeSourceKind(source.Kind),
		TargetID:               source.TargetID,
		SourceRevision:         sourceRevision,
		RecoveryPointID:        pointID,
		RecoveryPointCreatedAt: pointCreatedAt,
		ObjectKey:              run.ConnectorPrefix + run.EnvironmentID + "/" + source.ID + "/" + pointID + "/artifact.bin",
		State:                  backupruntime.BackupSourceAttemptPending,
		Phase:                  backupruntime.BackupSourcePhaseCapture,
	}
	switch attempt.Kind {
	case BackupRuntimeSourceAttach:
		return repository.prepareManualPostgresSource(
			ctx,
			attempt,
			run.EnvironmentID,
			fixedRevision,
			resolvePostgres,
		)
	case BackupRuntimeSourceVolume:
		return repository.prepareManualVolumeSource(ctx, attempt, run.EnvironmentID, fixedRevision)
	case BackupRuntimeSourceConfig:
		if run.Encryption != backupruntime.BackupRuntimeEncryptionAge || source.TargetID != run.EnvironmentID {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
				errs.KindStateConflict, "config backup source requires age encryption",
			)
		}
		read, readErr := repository.readFixedKeys(
			ctx,
			[]string{hierarchyrecord.EnvironmentKey(run.EnvironmentID)},
			fixedRevision,
		)
		if readErr != nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, readErr
		}
		defer clearKeyValues(read.Values)
		if read.Values[0] == nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
				errs.KindStateConflict,
				"config target is unavailable",
			)
		}
		attempt.TargetRevision = read.Values[0].ModRevision
		attempt.Format = backupruntime.BackupRuntimeFormatConfig
		attempt.Snapshot.Config = &backupruntime.BackupConfigSourceSnapshot{
			ConfigSnapshotID: run.TaskID, ReadRevision: fixedRevision,
		}
		return attempt, nil
	default:
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStrategyNotImplemented, "backup source strategy is not implemented",
		)
	}
}

func (repository *BackupRuntimeRepository) prepareManualPostgresSource(
	ctx context.Context,
	attempt backupruntime.BackupRunSourceAttemptRecord,
	consumerEnvironmentID string,
	fixedRevision int64,
	resolvePostgres BackupPostgresIdentityResolver,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	read, err := repository.readFixedKeys(
		ctx, []string{attachrecord.AttachKey(attempt.TargetID), attachrecord.AttachFactsKey(attempt.TargetID)}, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach evidence is unavailable",
		)
	}
	attach, attachErr := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	facts, factsErr := attachrecord.DecodeAttachEncryptedFacts(read.Values[1].Value)
	if attachErr != nil || factsErr != nil || attach.ID != attempt.TargetID ||
		attach.EnvironmentID != consumerEnvironmentID || !attach.OwnsCredential() ||
		attach.Status != backupAttachStatusReady || facts.AttachID != attach.ID {
		clear(facts.Ciphertext)
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach evidence changed",
		)
	}
	defer clear(facts.Ciphertext)
	backingRead, err := repository.readFixedKeys(ctx, []string{
		hierarchyrecord.ProjectKey(attach.BackingProjectID),
		hierarchyrecord.EnvironmentKey(attach.BackingEnvironmentID),
		environmentBlueprintHeadKey(attach.BackingEnvironmentID),
	}, fixedRevision)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer clearKeyValues(backingRead.Values)
	if backingRead.Values[0] == nil || backingRead.Values[1] == nil ||
		backingRead.Values[2] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres backing evidence is unavailable",
		)
	}
	project, projectErr := hierarchyrecord.DecodeProject(backingRead.Values[0].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(backingRead.Values[1].Value)
	service, serviceErr := findServiceAtRevision(ctx, repository.store, attach.BackingServiceID, fixedRevision)
	if projectErr != nil || environmentErr != nil || serviceErr != nil || project.Kind != hierarchyrecord.ProjectKindBacking ||
		environment.ProjectID != project.ID ||
		environment.ID != attach.BackingEnvironmentID ||
		service.Record.EnvironmentID != environment.ID ||
		service.Record.Desired.Adapter != "postgres:16" ||
		service.Record.Desired.Image != "postgres:16-alpine" ||
		service.Revision != backingRead.Values[2].ModRevision {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres:16 backing evidence changed",
		)
	}
	identity := BackupPostgresIdentity{}
	err = resolvePostgres(
		ctx,
		etcdstore.Versioned[attachrecord.Record]{
			Record:       attach,
			Revision:     read.Values[0].ModRevision,
			ReadRevision: fixedRevision,
		},
		facts,
		func(value BackupPostgresIdentity) error {
			identity = value
			return nil
		},
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	if identity.Database == "" || identity.Role == "" {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach identity is incomplete",
		)
	}
	attempt.TargetRevision = read.Values[0].ModRevision
	attempt.Format = backupruntime.BackupRuntimeFormatPostgres
	attempt.Snapshot.Postgres = &backupruntime.BackupPostgresSourceSnapshot{
		ConsumerEnvironmentID:      consumerEnvironmentID,
		AttachID:                   attach.ID,
		AttachRevision:             read.Values[0].ModRevision,
		BackingProjectID:           project.ID,
		BackingProjectRevision:     backingRead.Values[0].ModRevision,
		BackingEnvironmentID:       environment.ID,
		BackingEnvironmentRevision: backingRead.Values[1].ModRevision,
		BackingServiceID:           service.Record.Desired.ID,
		BackingServiceRevision:     backingRead.Values[2].ModRevision,
		AttachFactsRevision:        read.Values[1].ModRevision,
		Database:                   identity.Database,
		Role:                       identity.Role,
	}
	return attempt, nil
}

func (repository *BackupRuntimeRepository) prepareManualVolumeSource(
	ctx context.Context,
	attempt backupruntime.BackupRunSourceAttemptRecord,
	environmentID string,
	fixedRevision int64,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	evidence, err := loadBackupVolumeProjectionEvidence(
		ctx, repository.store, environmentID, attempt.TargetID, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	if evidence.Environment.Record.VolumeDir == "" {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"backup Volume projection is unavailable",
		)
	}
	services, err := repository.manualBackupVolumeConsumers(
		ctx, environmentID, attempt.TargetID, evidence.Projection.Record, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	attempt.TargetRevision = evidence.Projection.Revision
	attempt.Format = backupruntime.BackupRuntimeFormatVolume
	attempt.Snapshot.Volume = &backupruntime.BackupVolumeSourceSnapshot{
		EnvironmentID:       environmentID,
		EnvironmentRevision: evidence.Environment.Revision,
		VolumeID:            evidence.Volume.ID,
		DesiredRevisionID:   evidence.Projection.Record.RevisionID,
		ProjectionRoot:      evidence.ProjectionRoot,
		DependencyDigest:    evidence.DependencyDigest,
		RenderGeneration:    evidence.Projection.Record.RenderGeneration,
		ComposeVolumeKey:    evidence.Volume.Key,
		DockerVolumeName:    "gp_vol_" + evidence.Volume.ID,
		AuthorizedVolumeDir: evidence.Environment.Record.VolumeDir,
		Services:            services,
	}
	return attempt, nil
}

func (repository *BackupRuntimeRepository) manualBackupVolumeConsumers(
	ctx context.Context,
	environmentID string,
	volumeID string,
	projection EnvironmentComposeProjection,
	fixedRevision int64,
) ([]backupruntime.BackupVolumeServiceSnapshot, error) {
	serviceKeys := make(map[string]string, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		serviceKeys[desired.Desired.ID] = desired.Desired.Name
	}
	mounts := make(map[string][]string)
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			mounts[mount.ServiceID] = append(mounts[mount.ServiceID], mount.Target)
		}
	}
	serviceIDs := make([]string, 0, len(mounts))
	for serviceID := range mounts {
		serviceIDs = append(serviceIDs, serviceID)
		sort.Strings(mounts[serviceID])
	}
	sort.Strings(serviceIDs)
	result := make([]backupruntime.BackupVolumeServiceSnapshot, 0)
	for _, serviceID := range serviceIDs {
		service, serviceErr := findServiceAtRevision(ctx, repository.store, serviceID, fixedRevision)
		if serviceErr != nil || service.Record.EnvironmentID != environmentID {
			return nil, errs.New(errs.KindStateConflict, "Volume consumer Service evidence changed")
		}
		mountPaths := mounts[service.Record.Desired.ID]
		if len(mountPaths) != 0 {
			composeKey := serviceKeys[service.Record.Desired.ID]
			if composeKey == "" && backupVolumeTargetsGeneratedService(
				projection.Components,
				service.Record.Desired.ID,
			) {
				composeKey = service.Record.Desired.Name
			}
			if composeKey == "" {
				return nil, errs.New(
					errs.KindStateConflict,
					"Volume consumer projection is incomplete",
				)
			}
			result = append(result, backupruntime.BackupVolumeServiceSnapshot{
				ServiceID: service.Record.Desired.ID, ServiceRevision: ServiceRuntimeRevision(service),
				ComposeKey: composeKey, MountPaths: mountPaths,
				PriorIntent: backupruntime.BackupServiceRuntimeIntent(service.Record.Runtime.RuntimeIntent),
			})
		}
	}
	return result, nil
}

func backupVolumeTargetsGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}

func (publication *PreparedBackupRunPublication) Record() backupruntime.BackupRunRecord {
	if publication == nil || publication.state == nil {
		return backupruntime.BackupRunRecord{}
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	return cloneBackupRunPublicationRecord(publication.state.plan.record)
}

func (publication *PreparedBackupRunPublication) Publish(
	ctx context.Context,
	task TaskRecord,
	sealed *agentpb.ExecutionPlan,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup run publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository == nil {
		publication.state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict, "backup run publication was already consumed",
		)
	}
	publication.state.consumed = true
	repository := publication.state.repository
	plan := publication.state.plan
	publication.state.repository = nil
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.mu.Unlock()
	defer plan.clear()
	if task.Actor != TaskActorOperator && task.Actor != TaskActorSystem {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "backup run task actor must be operator or system",
		)
	}
	initiation, err := newTaskInitiation(task.Owner, task.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutationPlan, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}

func (publication *PreparedBackupRunPublication) Clear() {
	if publication == nil || publication.state == nil {
		return
	}
	publication.state.mu.Lock()
	defer publication.state.mu.Unlock()
	publication.state.plan.clear()
	publication.state.plan = backupRunPublicationPlan{}
	publication.state.repository = nil
	publication.state.consumed = true
}

// validateExistingBackupRunPublication validates the committed winner named by
// the existing marker, never the losing candidate's freshly allocated ids.
func (repository *BackupRuntimeRepository) validateExistingBackupRunPublication(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	readRevision int64,
	commitRevision int64,
) error {
	var response struct {
		TaskID string `json:"task_id"`
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.TaskID == "" ||
		marker.Response.Status != 202 ||
		json.Unmarshal(marker.Response.Body, &response) != nil ||
		response.TaskID != marker.TaskID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	read, err := repository.readFixedKeys(ctx, []string{
		backupruntime.BackupRunKey(marker.TaskID),
		taskKey(marker.TaskID),
		hierarchyrecord.EnvironmentOperationLockKey(marker.Locator.ScopeID),
	}, readRevision)
	if err != nil {
		return err
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != 3 || read.Values[1] == nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	task, taskErr := decodeTaskRecord(read.Values[1].Value)
	if taskErr != nil || task.ID != marker.TaskID || task.Type != TaskBackup ||
		(task.Actor != TaskActorOperator && task.Actor != TaskActorSystem) || task.Executor != TaskExecutorAgent ||
		(task.RetryOf == "" && task.IdempotencyKey != marker.Locator.Key) ||
		task.idempotencyMarker == nil ||
		*task.idempotencyMarker != marker.Locator || task.Owner.EnvironmentID != marker.Locator.ScopeID {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if marker.State != idempotencyrecord.IdempotencyMarkerPending {
		return repository.validateTerminalBackupRunPublication(
			ctx,
			marker,
			task,
			read.Values[0],
			read.Values[1],
			readRevision,
			commitRevision,
		)
	}
	if read.Values[0] == nil || read.Values[2] == nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	run, runErr := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	lock, lockErr := backupruntime.DecodeBackupOperationLockRecord(read.Values[2].Value)
	if runErr != nil || lockErr != nil || validateBackupRunTaskBinding(task, run) != nil ||
		(run.RetryOfTaskID != "" && task.Actor != TaskActorOperator) ||
		(run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorOperator && task.Actor != TaskActorOperator) ||
		(run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorSchedule && task.Actor != TaskActorSystem) ||
		lock.TaskID != marker.TaskID || lock.OperationID != run.OperationID ||
		lock.EnvironmentID != run.EnvironmentID || lock.Kind != backupruntime.BackupOperationBackup ||
		!lock.CreatedAt.Equal(run.CreatedAt) || !lock.UpdatedAt.Equal(lock.CreatedAt) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	switch task.Status {
	case TaskStatusPending:
		return repository.validateQueuedBackupRunPublication(
			ctx, run, task, read.Values, readRevision, commitRevision,
		)
	case TaskStatusRunning:
		return repository.validateRunningBackupRunPublication(
			ctx, run, task, read.Values, readRevision, commitRevision,
		)
	default:
		return backupruntime.CorruptBackupRuntimeRecord()
	}
}

// Rationale: before claim, every publication record is still the exact value
// written atomically with the pending marker and must retain that revision.
func (repository *BackupRuntimeRepository) validateQueuedBackupRunPublication(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	primary []*etcdstore.KeyValue,
	readRevision int64,
	commitRevision int64,
) error {
	for _, value := range primary {
		if value == nil || value.ModRevision != commitRevision {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
	}
	if run.RetryOfTaskID == "" && run.Initiator == backupruntime.BackupRunInitiatorSchedule &&
		!repository.exactScheduledBackupRunSubordinates(ctx, run, readRevision, commitRevision) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if run.State != backupruntime.BackupRunQueued || !run.UpdatedAt.Equal(run.CreatedAt) ||
		task.NextEventSequence != 1 || !task.UpdatedAt.Equal(task.CreatedAt) ||
		task.StartedAt != nil || task.FinishedAt != nil || task.RetainUntil != nil ||
		!repository.exactBackupRunPublicationSubordinates(
			ctx, run, task, readRevision, commitRevision,
		) || !repository.exactBackupRunConfigCompanions(
		ctx, run, readRevision, commitRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

// Rationale: claim replaces only queue membership with one three-copy
// assignment. Later checkpoints may rewrite the run and Environment epoch, so
// replay proves those current records at one revision instead of requiring the
// pending publication revision everywhere.
func (repository *BackupRuntimeRepository) validateRunningBackupRunPublication(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	primary []*etcdstore.KeyValue,
	readRevision int64,
	publicationRevision int64,
) error {
	if primary[0].ModRevision < publicationRevision ||
		primary[1].ModRevision <= publicationRevision ||
		primary[2].ModRevision != publicationRevision ||
		(run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning) ||
		task.StartedAt == nil || task.FinishedAt != nil || task.RetainUntil != nil ||
		!repository.exactRunningBackupRunSubordinates(
			ctx,
			run,
			task,
			primary[0].ModRevision,
			primary[1].ModRevision,
			readRevision,
			publicationRevision,
		) || !repository.currentBackupRunConfigCompanions(
		ctx, run, readRevision, publicationRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

// Rationale: terminal replay is authorized by the generic Task retention
// contract and the immutable Backup terminal receipt, not by queue, assignment,
// lock, or pending-marker records that the terminal transaction must remove.
func (repository *BackupRuntimeRepository) validateTerminalBackupRunPublication(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	task TaskRecord,
	runValue *etcdstore.KeyValue,
	taskValue *etcdstore.KeyValue,
	readRevision int64,
	terminalRevision int64,
) error {
	if taskValue.ModRevision != terminalRevision || !isTerminalTaskStatus(task.Status) ||
		task.FinishedAt == nil || task.RetainUntil == nil ||
		!marker.TerminalAt.Equal(*task.FinishedAt) ||
		!marker.RetainUntil.Equal(*task.RetainUntil) ||
		(marker.State == idempotencyrecord.IdempotencyMarkerCompleted && task.Status != TaskStatusCompleted) ||
		(marker.State == idempotencyrecord.IdempotencyMarkerFailed && task.Status == TaskStatusCompleted) ||
		(marker.State != idempotencyrecord.IdempotencyMarkerCompleted && marker.State != idempotencyrecord.IdempotencyMarkerFailed) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if runValue != nil && !repository.exactTerminalBackupRunSubordinates(
		ctx, marker, task, runValue, readRevision, terminalRevision,
	) {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	tasks, err := newTaskRepository(repository.store)
	if err != nil {
		return err
	}
	versioned := etcdstore.Versioned[TaskRecord]{
		Record: task, Revision: terminalRevision, ReadRevision: readRevision,
	}
	if err := tasks.validateTaskRetentionReplay(ctx, task, readRevision); err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if err := tasks.validateBackupTerminalReceiptReplay(ctx, versioned); err != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}

// Rationale: terminalization releases transient execution ownership but keeps
// immutable Task history and owner indexes. The retained run derives their
// original publication revision, while the receipt and retention indexes must
// be atomic with the terminal marker and Task.
func (repository *BackupRuntimeRepository) exactTerminalBackupRunSubordinates(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
	task TaskRecord,
	runValue *etcdstore.KeyValue,
	readRevision int64,
	terminalRevision int64,
) bool {
	if runValue.ModRevision != terminalRevision {
		return false
	}
	run, err := backupruntime.DecodeBackupRunRecord(runValue.Value)
	if err != nil || validateBackupRunTaskBinding(task, run) != nil ||
		run.UpdatedAt != *task.FinishedAt {
		return false
	}
	wantState, err := backupRunStateForTaskStatus(task.Status)
	if err != nil || run.State != wantState {
		return false
	}
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		return false
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return false
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		return false
	}
	markerRetentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return false
	}
	keys := []string{
		membershipKey,
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		taskAssignmentIndexKey(task.ID),
	}
	keys = append(keys, ownerKeys...)
	terminalOffset := len(keys)
	keys = append(keys,
		backupTerminalReceiptKey(task.ID),
		taskRetentionIndexKey(task.ID, *task.RetainUntil),
		markerRetentionKey,
	)
	claimIndex := -1
	if task.TerminalAssignment != nil {
		claimIndex = len(keys)
		keys = append(keys, taskExecutionClaimKey(
			task.Executor, task.TerminalAssignment.AgentID, task.ID,
		))
	}
	read, err := repository.readFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[2] != nil || read.Values[3] != nil || read.Values[4] != nil ||
		(claimIndex >= 0 && read.Values[claimIndex] != nil) {
		return false
	}
	publicationRevision := read.Values[0].ModRevision
	if publicationRevision <= 0 || publicationRevision >= terminalRevision ||
		read.Values[0].Key != keys[0] || read.Values[0].Version != 1 ||
		string(read.Values[0].Value) != task.ID ||
		read.Values[1].Key != keys[1] || read.Values[1].ModRevision != publicationRevision {
		return false
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return false
	}
	defer clear(reference)
	if !bytes.Equal(read.Values[1].Value, reference) {
		return false
	}
	ownerOffset := 5
	for index := range ownerKeys {
		value := read.Values[ownerOffset+index]
		if value == nil || value.Key != ownerKeys[index] ||
			value.ModRevision != publicationRevision || string(value.Value) != task.ID {
			return false
		}
	}
	receiptValue := read.Values[terminalOffset]
	taskRetentionValue := read.Values[terminalOffset+1]
	markerRetentionValue := read.Values[terminalOffset+2]
	if receiptValue == nil || taskRetentionValue == nil || markerRetentionValue == nil ||
		receiptValue.ModRevision != terminalRevision ||
		taskRetentionValue.ModRevision != terminalRevision ||
		markerRetentionValue.ModRevision != terminalRevision {
		return false
	}
	retainedTaskID, err := idempotencyrecord.DecodeTaskReference(taskRetentionValue.Value)
	if err != nil || retainedTaskID != task.ID ||
		idempotencyrecord.DecodeRetentionReference(markerRetentionValue.Value, markerKey) != nil {
		return false
	}
	return repository.currentBackupRunConfigCompanions(
		ctx, run, readRevision, publicationRevision,
	)
}

// Rationale: replay is valid only when every same-commit membership and
// coordination record still names the marker's winning Task exactly.
func (repository *BackupRuntimeRepository) exactBackupRunPublicationSubordinates(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	readRevision int64,
	commitRevision int64,
) bool {
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		return false
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil {
		return false
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return false
	}
	keys := []string{
		membershipKey,
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
	}
	for _, exclusion := range exclusions {
		key, keyErr := backupruntime.BackupSourceTargetExclusionKey(exclusion.TargetKind, exclusion.TargetID)
		if keyErr != nil {
			return false
		}
		keys = append(keys, key)
	}
	keys = append(keys, ownerKeys...)
	keys = append(keys, hierarchyrecord.EnvironmentMutationEpochKey(run.EnvironmentID))
	read, err := repository.readFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision != commitRevision {
			return false
		}
	}
	if string(read.Values[0].Value) != task.ID {
		return false
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return false
	}
	defer clear(reference)
	for index := 1; index <= 3; index++ {
		if !bytes.Equal(read.Values[index].Value, reference) {
			return false
		}
	}
	exclusionOffset := 4
	for index, exclusion := range exclusions {
		expected, encodeErr := backupruntime.EncodeBackupSourceTargetExclusionRecord(exclusion)
		if encodeErr != nil {
			return false
		}
		matches := bytes.Equal(read.Values[exclusionOffset+index].Value, expected)
		clear(expected)
		if !matches {
			return false
		}
	}
	ownerOffset := exclusionOffset + len(exclusions)
	for index := range ownerKeys {
		if string(read.Values[ownerOffset+index].Value) != task.ID {
			return false
		}
	}
	epoch, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: run.EnvironmentID,
	})
	if err != nil {
		return false
	}
	defer clear(epoch)
	return bytes.Equal(read.Values[len(read.Values)-1].Value, epoch)
}

// Rationale: a running Task has exactly one assignment replacing its queue
// record, while all immutable publication indexes retain the pending marker's
// revision and the current run is paired with the current Environment epoch.
func (repository *BackupRuntimeRepository) exactRunningBackupRunSubordinates(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	task TaskRecord,
	runRevision int64,
	taskRevision int64,
	readRevision int64,
	publicationRevision int64,
) bool {
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		return false
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil {
		return false
	}
	ownerKeys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		return false
	}
	keys := []string{
		membershipKey,
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		taskAssignmentIndexKey(task.ID),
	}
	for _, exclusion := range exclusions {
		key, keyErr := backupruntime.BackupSourceTargetExclusionKey(exclusion.TargetKind, exclusion.TargetID)
		if keyErr != nil {
			return false
		}
		keys = append(keys, key)
	}
	keys = append(keys, ownerKeys...)
	keys = append(keys, hierarchyrecord.EnvironmentMutationEpochKey(run.EnvironmentID))
	read, err := repository.readFixedKeys(ctx, keys, readRevision)
	if err != nil {
		return false
	}
	defer clearKeyValues(read.Values)
	if len(read.Values) != len(keys) || read.Values[3] != nil || read.Values[4] == nil {
		return false
	}
	immutable := []int{0, 1, 2}
	exclusionOffset := 5
	for index := range exclusions {
		immutable = append(immutable, exclusionOffset+index)
	}
	ownerOffset := exclusionOffset + len(exclusions)
	for index := range ownerKeys {
		immutable = append(immutable, ownerOffset+index)
	}
	for _, index := range immutable {
		if read.Values[index] == nil || read.Values[index].Key != keys[index] ||
			read.Values[index].ModRevision != publicationRevision {
			return false
		}
	}
	if string(read.Values[0].Value) != task.ID {
		return false
	}
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return false
	}
	defer clear(reference)
	if !bytes.Equal(read.Values[1].Value, reference) ||
		!bytes.Equal(read.Values[2].Value, reference) {
		return false
	}
	for index, exclusion := range exclusions {
		expected, encodeErr := backupruntime.EncodeBackupSourceTargetExclusionRecord(exclusion)
		if encodeErr != nil {
			return false
		}
		matches := bytes.Equal(read.Values[exclusionOffset+index].Value, expected)
		clear(expected)
		if !matches {
			return false
		}
	}
	for index := range ownerKeys {
		if string(read.Values[ownerOffset+index].Value) != task.ID {
			return false
		}
	}
	epochIndex := len(read.Values) - 1
	if read.Values[epochIndex] == nil || read.Values[epochIndex].Key != keys[epochIndex] ||
		read.Values[epochIndex].ModRevision != runRevision {
		return false
	}
	epoch, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: run.EnvironmentID,
	})
	if err != nil {
		return false
	}
	defer clear(epoch)
	if !bytes.Equal(read.Values[epochIndex].Value, epoch) {
		return false
	}
	assignmentValue := read.Values[4]
	assignment, err := decodeTaskAssignment(assignmentValue.Value)
	if err != nil || assignmentValue.Key != keys[4] ||
		assignmentValue.ModRevision <= publicationRevision ||
		assignmentValue.ModRevision > taskRevision || assignment.TaskID != task.ID ||
		assignment.Executor != task.Executor || assignment.ClaimedTaskRevision != publicationRevision ||
		task.StartedAt == nil || !assignment.AssignedAt.Equal(*task.StartedAt) {
		return false
	}
	claimKeys := []string{
		taskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
		taskTimeoutIndexKey(task.ID, assignment.Deadline),
	}
	claim, err := repository.readFixedKeys(ctx, claimKeys, readRevision)
	if err != nil {
		return false
	}
	defer clearKeyValues(claim.Values)
	for index, value := range claim.Values {
		if value == nil || value.Key != claimKeys[index] ||
			value.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(value.Value, assignmentValue.Value) {
			return false
		}
	}
	return true
}

// Rationale: config snapshot progress is assignment-fenced and may advance
// after publication; its immutable bidirectional Task references retain the
// publication revision and bind the current validated snapshot identity.
func (repository *BackupRuntimeRepository) currentBackupRunConfigCompanions(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	readRevision int64,
	publicationRevision int64,
) bool {
	for _, source := range run.Sources {
		if source.Kind != backupruntime.BackupRuntimeSourceConfig {
			continue
		}
		snapshot := source.Snapshot.Config
		keys := []string{
			backupConfigSnapshotKey(snapshot.ConfigSnapshotID),
			backupConfigSnapshotTaskReferenceKey(run.TaskID, snapshot.ConfigSnapshotID),
			backupConfigSnapshotReferenceTaskKey(snapshot.ConfigSnapshotID, run.TaskID),
		}
		read, err := repository.readFixedKeys(ctx, keys, readRevision)
		if err != nil {
			return false
		}
		primaryRevisionValid := read.Values[0] != nil &&
			((run.RetryOfTaskID == "" && read.Values[0].ModRevision >= publicationRevision) ||
				run.RetryOfTaskID != "")
		if len(read.Values) != len(keys) || !primaryRevisionValid ||
			read.Values[1] == nil || read.Values[2] == nil ||
			read.Values[1].ModRevision != publicationRevision ||
			read.Values[2].ModRevision != publicationRevision ||
			string(read.Values[1].Value) != snapshot.ConfigSnapshotID ||
			string(read.Values[2].Value) != run.TaskID {
			clearKeyValues(read.Values)
			return false
		}
		stored, decodeErr := decodeBackupConfigSnapshotRecord(read.Values[0].Value)
		clearKeyValues(read.Values)
		createdAtValid := (run.RetryOfTaskID == "" && stored.CreatedAt.Equal(run.CreatedAt)) ||
			(run.RetryOfTaskID != "" && stored.CreatedAt.Before(run.CreatedAt))
		if decodeErr != nil || stored.SnapshotID != snapshot.ConfigSnapshotID ||
			stored.EnvironmentID != run.EnvironmentID || stored.SourceID != source.SourceID ||
			stored.State == BackupConfigSnapshotUninitialized ||
			stored.ReadRevision != snapshot.ReadRevision || !createdAtValid {
			return false
		}
	}
	return true
}

func cloneBackupRunPublicationRecord(record backupruntime.BackupRunRecord) backupruntime.BackupRunRecord {
	clone := record
	if record.ScheduledAt != nil {
		scheduledAt := *record.ScheduledAt
		clone.ScheduledAt = &scheduledAt
	}
	clone.Sources = make([]backupruntime.BackupRunSourceAttemptRecord, len(record.Sources))
	for index, source := range record.Sources {
		clone.Sources[index] = source
		clone.Sources[index].Snapshot = cloneBackupRunSourceSnapshot(source.Snapshot)
	}
	return clone
}

func cloneBackupRunSourceSnapshot(snapshot backupruntime.BackupRunSourceSnapshot) backupruntime.BackupRunSourceSnapshot {
	clone := snapshot
	if snapshot.Postgres != nil {
		postgres := *snapshot.Postgres
		clone.Postgres = &postgres
	}
	if snapshot.Volume != nil {
		volume := *snapshot.Volume
		volume.Services = append([]backupruntime.BackupVolumeServiceSnapshot(nil), snapshot.Volume.Services...)
		for index := range volume.Services {
			volume.Services[index].MountPaths = append(
				[]string(nil),
				volume.Services[index].MountPaths...)
		}
		clone.Volume = &volume
	}
	if snapshot.Config != nil {
		config := *snapshot.Config
		clone.Config = &config
	}
	return clone
}
