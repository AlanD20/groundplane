package app

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupsecrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeBackupCredentialFixture struct {
	store         *backupCredentialCASStore
	agents        *agentregistration.Repository
	queue         *workqueue.Repository
	delivery      *backup.WorkSecretDelivery
	crypt         *nativeBackupCredentialCrypt
	authorization agentregistration.WorkAuthorization
	attempt       *agentpb.WorkAttemptIdentity
	operations    []workexecution.Operation
	agentID       string
	token         [32]byte
	environmentID string
	connectorKey  string
	directKey     string
	secretRecord  secrets.Record
	secretKey     string
	sourceKey     string
}

func newNativeBackupCredentialFixture(t *testing.T, secretScope core.SecretScope) *nativeBackupCredentialFixture {
	t.Helper()
	ctx := t.Context()
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-10 * time.Second)
	f := &nativeBackupCredentialFixture{
		store:      &backupCredentialCASStore{memoryHierarchyStore: newMemoryHierarchyStore()},
		operations: []workexecution.Operation{workexecution.BackupPrune},
	}
	f.store.revision = 1
	var err error
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.agents, err = agentregistration.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	record := agentConfigurationTestRecord()
	f.token = [32]byte{7}
	tokenDigest := sha256.Sum256(f.token[:])
	record.TokenDigest = base64.RawURLEncoding.EncodeToString(tokenDigest[:])
	record.CreatedAt, record.TokenUpdatedAt = at.Add(-time.Hour), at.Add(-time.Hour)
	f.agentID = record.ID
	registered, err := f.agents.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.agents.MarkReady(ctx, record.ID, 1, registered.Revision, at.Add(-time.Minute), buildversion.BuildIdentity{Version: "test"}); err != nil {
		t.Fatal(err)
	}
	identity, err := f.agents.RegisterWorkProcess(ctx, record.ID, 1, [16]byte{1}, 1, f.operations)
	if err != nil {
		t.Fatal(err)
	}
	placement := workexecution.Placement{Executor: workexecution.Agent, HostID: identity.HostID}
	executor, err := f.queue.ReadExecutor(ctx, placement)
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(executor, identity, true, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, available)

	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: ids.New(ids.KindTenant),
		Slug: "credential-check", Name: "Credential check", Kind: hierarchy.ProjectKindTenant}
	projectValue := nativeBackupPut(t, f.store, hierarchy.ProjectKey(project.ID), project, hierarchy.EncodeProject)
	environment, err := hierarchy.NewProvisioningEnvironment("/var/lib/groundplane/vol", project,
		ids.New(ids.KindEnvironment), "qa", "10.40.0.0/24", ids.New(ids.KindTask), at.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	environment, err = hierarchy.CompleteEnvironmentProvisioning(environment, environment.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	f.environmentID = environment.ID
	environmentValue := nativeBackupPut(
		t,
		f.store,
		hierarchy.EnvironmentKey(environment.ID),
		environment,
		hierarchy.EncodeEnvironment,
	)
	connector, err := connectors.NewRecord(core.Connector{ID: ids.New(ids.KindConnector), EnvironmentID: environment.ID,
		Name: "backups", Kind: core.ConnectorKindS3Compatible, Endpoint: "https://objects.example.test", Bucket: "groundplane-backups",
		Prefix: "qa/", Region: "auto", PathStyle: true, Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
			core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
			core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialSecretRef, SecretRef: "BACKUP_SECRET"}}})
	if err != nil {
		t.Fatal(err)
	}
	direct, err := connectors.NewEncryptedCredentials(connector.Connector.ID, []byte("native-direct-envelope"))
	if err != nil {
		t.Fatal(err)
	}
	connectorRaw, err := connectors.EncodeRecord(connector)
	if err != nil {
		t.Fatal(err)
	}
	directRaw, err := connectors.EncodeEncryptedCredentials(direct)
	if err != nil {
		t.Fatal(err)
	}
	f.connectorKey, f.directKey = connectors.RecordKey(
		connector.Connector.ID,
	), connectors.CredentialValueKey(
		connector.Connector.ID,
	)
	connectorPublication, err := f.store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: f.connectorKey, Value: connectorRaw},
		{Type: keyvalue.MutationPut, Key: f.directKey, Value: directRaw}})
	if err != nil || !connectorPublication.Succeeded {
		t.Fatalf("Connector fixture publication: %v", err)
	}
	connectorDigest, directDigest := sha256.Sum256(connectorRaw), sha256.Sum256(directRaw)
	connectorValue := &agentpb.RevisionDigest{ModRevision: connectorPublication.Revision, Sha256: connectorDigest[:]}
	directValue := &agentpb.RevisionDigest{ModRevision: connectorPublication.Revision, Sha256: directDigest[:]}
	clear(connectorRaw)
	clear(directRaw)
	secret, err := secrets.NewProjectRecord(
		ids.New(ids.KindSecret),
		project.ID,
		"BACKUP_SECRET",
		core.SecretKindEnvVar,
		"",
		at.Add(-time.Hour),
	)
	if secretScope == core.SecretScopePlatform {
		secret, err = secrets.NewPlatformRecord(
			secret.Secret.ID,
			"BACKUP_SECRET",
			core.SecretKindEnvVar,
			"",
			at.Add(-time.Hour),
		)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.secretRecord = secret
	nativeBackupPut(t, f.store, secrets.RecordKey(secret.Secret.ID), secret, secrets.EncodeRecord)
	if _, err := f.store.Put(ctx, secrets.SecretScopedKey(secret.Secret), []byte(secret.Secret.ID)); err != nil {
		t.Fatal(err)
	}
	f.secretKey = secrets.ValueKey(secret.Secret.ID)
	ciphertext := []byte("native-secret-envelope")
	cipherDigest := sha256.Sum256(ciphertext)
	secretValue := nativeBackupPut(t, f.store, f.secretKey, secrets.EncryptedValue{SecretID: secret.Secret.ID,
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(cipherDigest[:]), Ciphertext: ciphertext}, secrets.EncodeEncryptedValue)
	source := backuppolicy.BackupSourceRecord{ID: ids.New(ids.KindBackupSource), EnvironmentID: environment.ID,
		Kind: core.BackupSourceVolume, TargetID: ids.New(ids.KindVolume), CreatedAt: at.Add(-time.Hour)}
	f.sourceKey = backuppolicy.BackupSourceKey(source.ID)
	nativeBackupPut(t, f.store, f.sourceKey, source, backuppolicy.EncodeBackupSourceRecord)
	point := nativeBackupCredentialPoint(source, connector, identity, at.Add(-time.Hour))
	pointValue := nativeBackupPut(
		t,
		f.store,
		backupruntime.BackupRecoveryPointKey(point.ID),
		point,
		backupruntime.EncodeBackupRecoveryPointRecord,
	)
	policyDigest := sha256.Sum256([]byte("captured-retention-policy"))
	policy := &agentpb.RevisionDigest{ModRevision: 1, Sha256: policyDigest[:]}
	connectorAuthority := &agentpb.BackupConnectorAuthority{
		ConnectorId:          connector.Connector.ID,
		Connector:            connectorValue,
		CanonicalEndpointUrl: connector.Connector.Endpoint,
		Region:               "auto",
		PathStyle:            proto.Bool(true),
		Prefix:               "qa/",
		AccessKeySlotId:      backupsecret.AccessKeySlotID,
		SecretKeySlotId:      backupsecret.SecretKeySlotID,
		AccessKeySlot:        directValue,
		SecretKeySlot:        secretValue,
	}
	artifactDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil {
		t.Fatal(err)
	}
	artifact := backupobject.Artifact{Key: point.ObjectKey, EnvironmentID: environment.ID, SourceID: source.ID,
		RecoveryPointID: point.ID, SourceFormat: backupobject.SourceFormat(point.SourceFormat), Encryption: backupobject.EncryptionNone,
		Evidence: backupobject.Evidence{SourceSizeBytes: 4096, StoredSizeBytes: 4096}}
	copy(artifact.Evidence.SourceSHA256[:], artifactDigest)
	copy(artifact.Evidence.StoredSHA256[:], artifactDigest)
	count, metadataDigest := artifact.MetadataEvidence()
	pruneObject := &agentpb.BackupPruneObject{Ordinal: 1, PointId: point.ID, Point: pointValue,
		Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: 4096, StoredSizeBytes: 4096,
			SourceSha256: artifactDigest, StoredSha256: artifactDigest}, MetadataCount: count, MetadataSha256: metadataDigest[:],
		Object: &agentpb.BackupObjectIdentity{
			Connector: connectorAuthority,
			Bucket:    "groundplane-backups",
			ObjectKey: point.ObjectKey,
			Discriminator: &agentpb.BackupObjectIdentity_VersionId{
				VersionId: &agentpb.BackupS3VersionId{Value: "version-1"},
			},
		}}
	step, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
		StepId: ids.New(
			ids.KindStep,
		), ExecutionId: ids.NewULID(), StepDeadlineUnixNano: uint64(at.Add(30 * time.Minute).UnixNano()),
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{RetentionPolicy: policy,
			Objects: []*agentpb.BackupPruneObject{pruneObject}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), &agentpb.BackupWorkInput{
		Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: project.ID, Project: projectValue, EnvironmentId: environment.ID, Environment: environmentValue,
		Steps: []*agentpb.BackupStepAuthority{step}})
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.SystemActor,
		Scope:  workexecution.Scope{TenantID: project.TenantID, ProjectID: project.ID, EnvironmentID: environment.ID},
		Action: workexecution.ActionBackupPrune, TargetID: environment.ID, Intent: reference, ExpectedItems: 1,
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute)}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), identity.HostID, input)
	if err != nil {
		t.Fatal(err)
	}
	marker := agentConfigurationTestMarker()
	marker.Kind, marker.State, marker.TaskID = idempotency.IdempotencyMarkerTask, idempotency.IdempotencyMarkerPending, task.ID
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, time.Time{}, time.Time{}
	marker.Locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environment.ID, Method: http.MethodPost, Route: "/environments/{id}/backup/run", Key: "native-credential-grant-check"}
	marker.Response = idempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(fmt.Sprintf(`{"task_id":%q}`, task.ID))}
	publication, err := backupwork.PrepareExecutionPublication(task, item, input, marker)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.New(ids.KindOperation)
	prune := backupruntime.BackupRecoveryPointPruneRecord{
		Point:          point.BackupRecoveryPointSnapshot,
		PointRevision:  pointValue.ModRevision,
		PolicyRevision: policy.ModRevision,
		PolicySHA256:   hex.EncodeToString(policyDigest[:]),
		TaskID:         task.ID,
		OperationID:    operationID,
		State:          backupruntime.BackupPruneAssigned,
		CreatedAt:      at.Add(-time.Second),
		UpdatedAt:      at,
	}
	pruneRaw, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	dispatchRaw, err := backupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(
		backupruntime.BackupRecoveryPointPruneDispatchRecord{
			TaskID: task.ID, OperationID: operationID, EnvironmentID: environment.ID, RecoveryPointIDs: []string{point.ID}, CreatedAt: at},
	)
	if err != nil {
		t.Fatal(err)
	}
	lockRaw, err := backupruntime.EncodeBackupOperationLockRecord(backupruntime.BackupOperationLockRecord{
		EnvironmentID: environment.ID, OperationID: operationID, TaskID: task.ID, Kind: backupruntime.BackupOperationPrune,
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	publication.Mutations = append(
		publication.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   backupruntime.BackupRecoveryPointPruneKey(point.ID),
			Value: pruneRaw,
		},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID),
			Value: dispatchRaw,
		},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   hierarchy.EnvironmentOperationLockKey(environment.ID),
			Value: lockRaw,
		},
	)
	f.commit(t, publication)
	f.assignAndStart(t, task.ID, item.ID, placement, at.Add(time.Second))
	f.authorization, err = f.agents.ResolveWorkIdentity(ctx, record.ID, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := backupsecrets.NewReader(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.crypt = &nativeBackupCredentialCrypt{}
	protector, err := secretvalue.NewProtector(f.crypt, f.crypt)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := backup.NewBackupSecretResolver(ctx, reader, protector)
	if err != nil {
		t.Fatal(err)
	}
	f.delivery, err = backup.NewWorkSecretDelivery(resolver, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func nativeBackupPut[T any](t *testing.T, store *backupCredentialCASStore, key string, record T,
	encode func(T) ([]byte, error),
) *agentpb.RevisionDigest {
	t.Helper()
	raw, err := encode(record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	revision, err := store.Put(t.Context(), key, raw)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: digest[:]}
}

func (f *nativeBackupCredentialFixture) commit(t *testing.T, change workqueue.Change) {
	t.Helper()
	result, err := f.queue.Commit(t.Context(), change)
	if err != nil || !result.Succeeded {
		t.Fatalf("native Backup fixture transaction: committed=%t, %v", result.Succeeded, err)
	}
}

func (f *nativeBackupCredentialFixture) assignAndStart(
	t *testing.T,
	taskID, workID string,
	placement workexecution.Placement,
	at time.Time,
) {
	t.Helper()
	ctx := t.Context()
	view, err := f.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := f.queue.ReadExecutor(ctx, placement)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := f.store.Get(ctx, workqueue.QueueKey(view.Item.Item))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := f.queue.ReadCandidate(ctx, workqueue.QueueEntry{TaskID: taskID, WorkItemID: workID,
		Revision: queued.Entry.ModRevision}, executor.Record.Identity, at, nil)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("native Backup fixture is not assignable: %v, wait=%v", err, candidate.Wait)
	}
	request := candidate.Assignment
	request.AttemptID, request.At, request.Deadline = ids.New(ids.KindAttempt), at, view.Item.Item.Deadline
	change, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, change)
	resume, err := f.queue.ReadResume(ctx, taskID, workID, attempt.ID, executor.Record.Identity)
	if err != nil {
		t.Fatal(err)
	}
	change, err = workqueue.PrepareStart(
		resume.Work.Task,
		resume.Work.Item,
		resume.Attempt,
		executor,
		at.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, change)
	resume, err = f.queue.ReadResume(ctx, taskID, workID, attempt.ID, executor.Record.Identity)
	if err != nil {
		t.Fatal(err)
	}
	f.attempt, err = workexecution.EncodeAgentAttempt(resume.Attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
}

func nativeBackupCredentialPoint(source backuppolicy.BackupSourceRecord, connector connectors.Record,
	identity workexecution.Identity, at time.Time,
) backupruntime.BackupRecoveryPointRecord {
	digest := sha256.Sum256([]byte("retained-Volume-artifact"))
	digestHex := hex.EncodeToString(digest[:])
	pointID := ids.NewAt(ids.KindRecoveryPoint, at, 1)
	key := "qa/" + source.EnvironmentID + "/" + source.ID + "/" + pointID + "/artifact.bin"
	return backupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: backupruntime.BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: backupruntime.BackupRecoveryPointTargetSnapshot{
				ID: pointID, EnvironmentID: source.EnvironmentID, SourceID: source.ID, SourceKind: backupruntime.BackupRuntimeSourceVolume,
				TargetID: source.TargetID, ConnectorID: connector.Connector.ID, ConnectorPrefix: "qa/", ConnectorEndpoint: connector.Connector.Endpoint,
				ConnectorBucket: "groundplane-backups", ConnectorRegion: "auto", ConnectorPathStyle: true, ObjectKey: key,
				SourceFormat: backupruntime.BackupRuntimeFormatVolume, Encryption: backupruntime.BackupRuntimeEncryptionNone, CreatedAt: at},
			Evidence: backupruntime.BackupArtifactEvidence{
				SourceSizeBytes: 4096,
				StoredSizeBytes: 4096,
				SourceSHA256:    digestHex,
				StoredSHA256:    digestHex,
			},
			VolumeArchive: backupruntime.BackupVolumeArchiveEvidence{
				EntryCount:            1,
				ContentManifestSHA256: digestHex,
				FullTreeSHA256:        digestHex,
				SourceSizeBytes:       4096,
				Manifest: backupruntime.BackupVolumeManifestReference{CursorRevision: 1,
					Binding: backupvolumetransfer.Binding{Attempt: workexecution.Attempt{
						ID: ids.New(
							ids.KindAttempt,
						), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), Number: 1,
						Executor: identity, Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: digestHex}, Direction: workexecution.Forward,
						AssignedAt: at, Deadline: at.Add(time.Minute)}, TransferID: ids.NewULID(), AuthorityDigest: digest,
						Direction: agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE}},
			},
			Object: backupruntime.BackupObjectIdentity{Target: backupruntime.BackupObjectTarget{
				ConnectorID: connector.Connector.ID, ConnectorPrefix: "qa/", ConnectorEndpoint: connector.Connector.Endpoint,
				ConnectorBucket: "groundplane-backups", ConnectorRegion: "auto", ConnectorPathStyle: true, ObjectKey: key},
				Discriminator: backupruntime.BackupObjectDiscriminator{
					Kind:  backupobject.DiscriminatorVersionID,
					Value: "version-1",
				}}},
		VerifiedAt: at.Add(time.Second),
	}
}
