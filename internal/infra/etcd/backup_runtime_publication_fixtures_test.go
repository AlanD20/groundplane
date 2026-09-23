package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	testing "testing"
	time "time"

	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func backupRuntimePrunePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch testbackupruntime.BackupRecoveryPointPruneDispatchRecord,
	pending []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	publicationConditions []testkeyvalue.Condition,
) (TaskRecord, *agentpb.ExecutionPlan, testidempotency.IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(dispatch.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	project, err := testhierarchy.DecodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(dispatch.CreatedAt)
	task.ID = dispatch.TaskID
	task.OperationID = dispatch.OperationID
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorSystem
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.Type = testtaskjournal.TaskBackupPrune
	task.Target = dispatch.EnvironmentID
	task.IdempotencyKey = "backup-prune-runtime-0001"
	sealed := backupRuntimeSealedPrunePlan(t, store, dispatch, pending, task.PlanID)
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = dispatch.EnvironmentID
	marker.Locator.Route = "/internal/backup-prunes"
	marker.Locator.Key = task.IdempotencyKey
	var initiationFence testkeyvalue.Condition
	for _, condition := range publicationConditions {
		if condition.ModRevision > 0 {
			initiationFence = condition
			break
		}
	}
	if initiationFence.Key == "" {
		t.Fatal("prune Task publication has no durable initiation fence")
	}
	initiation, err := newTaskInitiation(owner, testtaskjournal.TaskActorSystem, initiationFence)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}

func backupRuntimeSealedPrunePlan(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch testbackupruntime.BackupRecoveryPointPruneDispatchRecord,
	pending []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	planID string,
) *agentpb.ExecutionPlan {
	t.Helper()
	plan := &agentpb.ExecutionPlan{
		Schema:    executionplan.SchemaVersion,
		PlanId:    planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE,
		TargetId:  dispatch.EnvironmentID,
		Steps:     make([]*agentpb.ExecutionStep, len(pending)),
	}
	for index, version := range pending {
		point := version.Record.Point
		pointEntry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointKey(point.ID))
		sourceEntry := mustOptionalKey(t, store, testbackuppolicy.BackupSourceKey(point.SourceID))
		environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(point.EnvironmentID))
		connectorEntry := mustOptionalKey(t, store, testconnectors.RecordKey(point.ConnectorID))
		connector, err := testconnectors.DecodeRecord(connectorEntry.Value)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := hex.DecodeString(point.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		plan.Steps[index] = &agentpb.ExecutionStep{
			StepId:         ids.NewAt(ids.KindStep, dispatch.CreatedAt, int64(2500+index)),
			TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_BackupArtifactPrune{BackupArtifactPrune: &agentpb.BackupArtifactPrune{
				Ordinal: uint32(index + 1), PruneOperationId: dispatch.OperationID,
				PruneRevision: uint64(version.Revision), PointId: point.ID,
				PointRevision: uint64(pointEntry.ModRevision), SourceId: point.SourceID,
				SourceRevision: uint64(sourceEntry.ModRevision), EnvironmentId: point.EnvironmentID,
				EnvironmentRevision: uint64(environmentEntry.ModRevision), ConnectorId: point.ConnectorID,
				ConnectorRevision: uint64(connectorEntry.ModRevision),
				ConnectorEndpoint: connector.Connector.Endpoint, ConnectorBucket: connector.Connector.Bucket,
				ConnectorPrefix: connector.Connector.Prefix, ConnectorRegion: connector.Connector.Region,
				ConnectorAddressing: backupFixtureAddressing(connector.Connector.PathStyle),
				ProtectedObjectKey:  point.ObjectKey, StoredSizeBytes: uint64(point.SizeBytes),
				StoredSha256: digest,
			}},
		}
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

// Rationale: stable-id validation must fail before malformed identifiers can
// select arbitrary durable key suffixes.
func TestBackupRuntimeRepositoryRejectsInvalidArtifactIDsBeforeRead(t *testing.T) {
	t.Parallel()
	repository, _, _ := newBackupRuntimeBareFixture(t)
	if _, _, err := repository.GetBackupOrphan(context.Background(), "../bad"); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("GetBackupOrphan(invalid id) error = %v", err)
	}
	if _, err := repository.GetBackupRecoveryPoint(context.Background(), "../bad"); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("GetBackupRecoveryPoint(invalid id) error = %v", err)
	}
}

func newBackupRuntimeRepositoryFixture(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupRunRecord) {
	t.Helper()
	return newBackupRuntimeBareFixture(t)
}

func newBackupRuntimeBareFixture(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupRunRecord) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	connectorRepository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	connector := testConnectorRecord(t, environment.Record.ID, now, 900, "backup-store")
	credentials, err := testconnectors.NewEncryptedCredentials(
		connector.Connector.ID,
		[]byte("sealed-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	createdConnector, err := connectorRepository.CreateConnector(
		context.Background(), environment, project, connector, credentials,
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := newBackupRuntimeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	run := testBackupRun(now, now, newTestBackupRecipient(t))
	run.EnvironmentID = environment.Record.ID
	run.State = testbackupruntime.BackupRunQueued
	run.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	run.Sources[0].Snapshot.Postgres.ConsumerEnvironmentID = environment.Record.ID
	run.Sources[0].State = testbackupruntime.BackupSourceAttemptPending
	run.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCapture
	run.ConnectorID = createdConnector.Record.Connector.ID
	run.ConnectorEndpoint = createdConnector.Record.Connector.Endpoint
	run.ConnectorBucket = createdConnector.Record.Connector.Bucket
	run.ConnectorPrefix = createdConnector.Record.Connector.Prefix
	run.ConnectorRegion = createdConnector.Record.Connector.Region
	run.ConnectorPathStyle = createdConnector.Record.Connector.PathStyle
	run.ConnectorRevision = createdConnector.Revision
	run.ConnectorHasDirectCredentials = true
	run.ConnectorCredentialsRevision = createdConnector.Revision
	run.Sources[0].SizeBytes = 0
	run.Sources[0].SHA256 = ""
	run.Sources[0].ObjectKey = run.ConnectorPrefix + environment.Record.ID + "/" + run.Sources[0].SourceID + "/" +
		run.Sources[0].RecoveryPointID + "/artifact.bin"
	seedBackupRuntimePublicationEvidence(t, store, &run)
	return repository, store, run
}

func seedBackupRuntimePublicationEvidence(
	t *testing.T,
	store *memoryHierarchyStore,
	run *testbackupruntime.BackupRunRecord,
) {
	t.Helper()
	source := &run.Sources[0]
	snapshot := source.Snapshot.Postgres
	backingProject := testhierarchy.ProjectRecord{
		ID: snapshot.BackingProjectID, Slug: "postgres", Name: "Postgres",
		Kind: testhierarchy.ProjectKindBacking,
	}
	backingEnvironment, err := testhierarchy.NewProvisioningEnvironment(
		"/srv/groundplane",
		backingProject,
		snapshot.BackingEnvironmentID,
		"main",
		"10.199.0.0/24",
		newBackupRuntimeID(ids.KindTask, run.CreatedAt, 910),
		run.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	backingServiceID := snapshot.BackingServiceID
	backingNetworkID := newBackupRuntimeID(ids.KindNetwork, run.CreatedAt, 911)
	backingServiceRevisionID := newBackupRuntimeID(ids.KindTask, run.CreatedAt, 914)
	backingServiceProjection := withTestEnvironmentComposeArtifact(
		testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID:    backingEnvironment.ID,
			RevisionID:       backingServiceRevisionID,
			RenderGeneration: 1,
			DesiredServices: []testservices.EnvironmentServiceProjection{{
				EnvironmentID:    backingEnvironment.ID,
				BackingNetworkID: backingNetworkID,
				Desired: core.Service{
					ID: backingServiceID, Name: "postgres", Image: "postgres:16-alpine", Adapter: "postgres:16",
				},
			}},
		},
	)
	backingServiceProjectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(
		backingServiceProjection,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(backingServiceProjectionValue)
	backingServiceProjectionDigest := sha256.Sum256(backingServiceProjectionValue)
	backingServiceAudit := []byte("backup-runtime-backing-service-audit")
	backingServiceAuditDigest := sha256.Sum256(backingServiceAudit)
	backingServiceSeal := testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID: backingEnvironment.ID, RevisionID: backingServiceRevisionID,
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 1, ProjectionSchema: 1,
		AuditChunks: 1, AuditBytes: uint64(len(backingServiceAudit)), AuditSHA256: backingServiceAuditDigest,
		ProjectionChunks: 1, ProjectionBytes: uint64(len(backingServiceProjectionValue)),
		ProjectionSHA256: backingServiceProjectionDigest,
		ProjectionResources: uint32(
			len(backingServiceProjection.DesiredZones) +
				len(backingServiceProjection.DesiredServices) +
				len(backingServiceProjection.DesiredRoutes) +
				len(backingServiceProjection.Volumes) +
				len(backingServiceProjection.VolumeMounts) +
				len(backingServiceProjection.Components) +
				len(backingServiceProjection.Entries),
		),
		DependencyDigest: backingServiceProjectionDigest,
	}
	attach, err := testattachments.NewPendingAttachRecord(
		source.TargetID,
		run.EnvironmentID,
		"database",
		backingProject.ID,
		backingEnvironment.ID,
		backingServiceID,
		backingNetworkID,
		newBackupRuntimeID(ids.KindService, run.CreatedAt, 912),
		source.TargetID,
		nil,
		[]testattachments.FactSetMetadata{{Facts: []testattachments.FactDefinition{{Key: "pg16_URL", Secret: true}}}},
		newBackupRuntimeID(ids.KindTask, run.CreatedAt, 913),
		run.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	attach, err = testattachments.MarkAttachProvisioning(attach, attach.TaskID)
	if err == nil {
		attach, err = testattachments.CompleteAttachProvisioning(attach, attach.TaskID, true)
	}
	if err != nil {
		t.Fatal(err)
	}
	facts, err := testattachments.NewAttachEncryptedFacts(
		attach.ID,
		1,
		"age-x25519",
		"sha256",
		[]byte("sealed-postgres-facts"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(facts.Ciphertext)
	policy := testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	}
	digest, err := testenvironmentcoordination.PolicyScheduleDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	coordination := testenvironmentcoordination.EnvironmentCoordinationRecord{
		EnvironmentID: run.EnvironmentID, ScheduleClockFloor: run.CreatedAt,
		CurrentBackupScheduleState: &testenvironmentcoordination.CurrentBackupScheduleState{
			PolicyDigest: digest, Frequency: policy.Frequency, EnabledAt: run.CreatedAt,
			LastEvaluatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
		},
	}
	storedSource := backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "attach", source.TargetID, run.CreatedAt,
	)
	keyRecord := testbackuppolicy.BackupKeyRecord{
		EnvironmentID: run.EnvironmentID, Recipient: run.Recipient, KeyEra: run.KeyEra,
		CreatedAt: run.CreatedAt, RotatedAt: run.CreatedAt,
	}
	keyValue := testbackuppolicy.BackupKeyEncryptedValue{
		EnvironmentID: run.EnvironmentID, KeyEra: run.KeyEra,
		Ciphertext: []byte("wrapped-age-identity"),
	}
	encoders := []struct {
		key    string
		encode func() ([]byte, error)
	}{
		{
			testbackuppolicy.BackupPolicyKey(run.EnvironmentID),
			func() ([]byte, error) { return testbackuppolicy.EncodeBackupPolicyRecord(policy) },
		},
		{testenvironmentcoordination.Key(run.EnvironmentID), func() ([]byte, error) {
			return testenvironmentcoordination.Encode(coordination)
		}},
		{
			testbackuppolicy.BackupSourceKey(source.SourceID),
			func() ([]byte, error) { return testbackuppolicy.EncodeBackupSourceRecord(storedSource) },
		},
		{
			testattachments.AttachKey(attach.ID),
			func() ([]byte, error) { return testattachments.EncodeAttachRecord(attach) },
		},
		{
			testattachments.AttachFactsKey(attach.ID),
			func() ([]byte, error) { return testattachments.EncodeAttachEncryptedFacts(facts) },
		},
		{
			testhierarchy.ProjectKey(backingProject.ID),
			func() ([]byte, error) { return testhierarchy.EncodeProject(backingProject) },
		},
		{
			testhierarchy.EnvironmentKey(backingEnvironment.ID),
			func() ([]byte, error) { return testhierarchy.EncodeEnvironment(backingEnvironment) },
		},
		{
			testblueprints.EnvironmentBlueprintHeadKey(backingEnvironment.ID),
			func() ([]byte, error) { return testidempotency.EncodeTaskReference(backingServiceRevisionID) },
		},
		{
			testblueprints.EnvironmentBlueprintRootKey(backingEnvironment.ID, backingServiceRevisionID),
			func() ([]byte, error) { return testblueprints.EncodeEnvironmentBlueprintSeal(backingServiceSeal) },
		},
		{testblueprints.EnvironmentBlueprintChunkKeyFor(
			backingEnvironment.ID, backingServiceRevisionID, testblueprints.EnvironmentBlueprintChunkProjection, 0,
		), func() ([]byte, error) {
			return testblueprints.EncodeEnvironmentBlueprintChunk(testblueprints.EnvironmentBlueprintChunk{
				Family: testblueprints.EnvironmentBlueprintChunkProjection, LogicalLength: uint32(len(backingServiceProjectionValue)),
				Digest: backingServiceProjectionDigest, Data: backingServiceProjectionValue,
			})
		},
		},
		{testservices.ServiceRuntimeKey(backingServiceID), func() ([]byte, error) {
			return testservices.EncodeServiceRuntimeRecord(testservices.ServiceRuntimeRecord{
				EnvironmentID: backingEnvironment.ID, ServiceID: backingServiceID,
				BackingNetworkID: backingNetworkID,
				Runtime: core.ServiceRuntime{
					ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
				},
			})
		},
		},
		{
			testbackuppolicy.BackupKeyKey(run.EnvironmentID),
			func() ([]byte, error) { return testbackuppolicy.EncodeBackupKeyRecord(keyRecord) },
		},
		{
			testbackuppolicy.BackupKeyValueKey(run.EnvironmentID),
			func() ([]byte, error) { return testbackuppolicy.EncodeBackupKeyEncryptedValue(keyValue) },
		},
	}
	mutations := make([]testkeyvalue.Mutation, 0, len(encoders))
	for _, item := range encoders {
		value, encodeErr := item.encode()
		if encodeErr != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(encodeErr)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: item.key, Value: value},
		)
	}
	result, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed backup publication evidence = %#v, %v", result, err)
	}
	run.PolicyRevision = result.Revision
	run.BackupKeyRecordRevision = result.Revision
	run.BackupKeyValueRevision = result.Revision
	source.SourceRevision = result.Revision
	source.TargetRevision = result.Revision
	snapshot.AttachRevision = result.Revision
	snapshot.AttachFactsRevision = result.Revision
	snapshot.BackingProjectRevision = result.Revision
	snapshot.BackingEnvironmentRevision = result.Revision
	snapshot.BackingServiceRevision = result.Revision
}

func extendBackupRuntimePublicationSources(
	t *testing.T,
	store hierarchyStore,
	run *testbackupruntime.BackupRunRecord,
) {
	t.Helper()
	baseAttachRead, err := store.Get(context.Background(), testattachments.AttachKey(run.Sources[0].TargetID))
	if err != nil || baseAttachRead == nil || baseAttachRead.Entry == nil {
		t.Fatalf("read base backup Attach = %#v, %v", baseAttachRead, err)
	}
	baseAttach, err := testattachments.DecodeAttachRecord(baseAttachRead.Entry.Value)
	clear(baseAttachRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	baseFactsRead, err := store.Get(context.Background(), testattachments.AttachFactsKey(run.Sources[0].TargetID))
	if err != nil || baseFactsRead == nil || baseFactsRead.Entry == nil {
		t.Fatalf("read base backup Attach facts = %#v, %v", baseFactsRead, err)
	}
	baseFacts, err := testattachments.DecodeAttachEncryptedFacts(baseFactsRead.Entry.Value)
	clear(baseFactsRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baseFacts.Ciphertext)
	sourceIDs := make([]string, len(run.Sources))
	mutations := make([]testkeyvalue.Mutation, 0, len(run.Sources)*3)
	for index := range run.Sources {
		source := &run.Sources[index]
		sourceIDs[index] = source.SourceID
		value, err := testbackuppolicy.EncodeBackupSourceRecord(backupRuntimeSourceRecord(
			t, source.SourceID, run.EnvironmentID, "attach", source.TargetID, run.CreatedAt,
		))
		if err != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupSourceKey(source.SourceID), Value: value,
		})
		if index > 0 {
			attach := baseAttach
			attach.ID = source.TargetID
			attach.TaskID = newBackupRuntimeID(ids.KindTask, run.CreatedAt, int64(920+index))
			facts := baseFacts
			facts.AttachID = source.TargetID
			attachValue, encodeErr := testattachments.EncodeAttachRecord(attach)
			if encodeErr != nil {
				testkeyvalue.ClearMutationValues(mutations)
				t.Fatal(encodeErr)
			}
			factsValue, encodeErr := testattachments.EncodeAttachEncryptedFacts(facts)
			if encodeErr != nil {
				clear(attachValue)
				testkeyvalue.ClearMutationValues(mutations)
				t.Fatal(encodeErr)
			}
			mutations = append(
				mutations,
				testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testattachments.AttachKey(source.TargetID),
					Value: attachValue,
				},
				testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   testattachments.AttachFactsKey(source.TargetID),
					Value: factsValue,
				},
			)
		}
	}
	policyValue, err := testbackuppolicy.EncodeBackupPolicyRecord(testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: sourceIDs, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		testkeyvalue.ClearMutationValues(mutations)
		t.Fatal(err)
	}
	mutations = append(mutations, testkeyvalue.Mutation{
		Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupPolicyKey(run.EnvironmentID), Value: policyValue,
	})
	result, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("extend backup publication sources = %#v, %v", result, err)
	}
	run.PolicyRevision = result.Revision
	for index := range run.Sources {
		run.Sources[index].SourceRevision = result.Revision
		if index > 0 {
			run.Sources[index].TargetRevision = result.Revision
			run.Sources[index].Snapshot.Postgres.AttachRevision = result.Revision
			run.Sources[index].Snapshot.Postgres.AttachFactsRevision = result.Revision
			run.Sources[index].Snapshot.Postgres.BackingProjectRevision =
				run.Sources[0].Snapshot.Postgres.BackingProjectRevision
			run.Sources[index].Snapshot.Postgres.BackingEnvironmentRevision =
				run.Sources[0].Snapshot.Postgres.BackingEnvironmentRevision
			run.Sources[index].Snapshot.Postgres.BackingServiceRevision =
				run.Sources[0].Snapshot.Postgres.BackingServiceRevision
		}
	}
}

func backupRuntimeCurrentRevision(
	t *testing.T,
	store hierarchyStore,
	environmentID string,
) int64 {
	t.Helper()
	result, err := store.Get(context.Background(), testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.ReadRevision <= 0 {
		t.Fatalf("read backup publication revision = %#v, %v", result, err)
	}
	return result.ReadRevision
}

func backupRuntimeOperationLock(run testbackupruntime.BackupRunRecord) testbackupruntime.BackupOperationLockRecord {
	return testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
}

func (repository *BackupRuntimeRepository) createBackupRunForTest(
	ctx context.Context,
	run testbackupruntime.BackupRunRecord,
	fixedRevision int64,
) (testkeyvalue.Versioned[testbackupruntime.BackupRunRecord], error) {
	plan, err := repository.prepareBackupRunPublication(
		ctx,
		run,
		backupRuntimeOperationLock(run),
		fixedRevision,
	)
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer plan.clear()
	conditions, mutations, err := plan.composeTransaction(nil, nil)
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer testkeyvalue.ClearMutationValues(mutations)
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	if !result.Succeeded {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup run publication changed",
		)
	}
	return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{
		Record: run, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func backupRuntimePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	run testbackupruntime.BackupRunRecord,
) (TaskRecord, *agentpb.ExecutionPlan, testidempotency.IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	project, err := testhierarchy.DecodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(run.CreatedAt)
	task.ID = run.TaskID
	task.OperationID = run.OperationID
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorOperator
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.Type = testtaskjournal.TaskBackup
	task.Target = run.EnvironmentID
	task.IdempotencyKey = "backup-runtime-0001"
	sealed := backupRuntimeSealedRunPlan(t, run, task.PlanID)
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = run.EnvironmentID
	marker.Locator.Route = "/environments/{environment}/backups"
	marker.Locator.Key = task.IdempotencyKey
	initiation, err := newTaskInitiation(owner, testtaskjournal.TaskActorOperator)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}

func configureBackupRuntimeConfigRun(
	t *testing.T,
	store *memoryHierarchyStore,
	run *testbackupruntime.BackupRunRecord,
) int64 {
	t.Helper()
	source := &run.Sources[0]
	source.Kind = testbackupruntime.BackupRuntimeSourceConfig
	source.TargetID = run.EnvironmentID
	source.TargetRevision = mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID)).ModRevision
	source.Format = testbackupruntime.BackupRuntimeFormatConfig
	source.Snapshot = testbackupruntime.BackupRunSourceSnapshot{Config: &testbackupruntime.BackupConfigSourceSnapshot{
		ConfigSnapshotID: run.TaskID,
	}}
	policyValue, err := testbackuppolicy.EncodeBackupPolicyRecord(testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceValue, err := testbackuppolicy.EncodeBackupSourceRecord(backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "config", run.EnvironmentID, run.CreatedAt,
	))
	if err != nil {
		clear(policyValue)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupPolicyKey(run.EnvironmentID), Value: policyValue},
		{Type: testkeyvalue.MutationPut, Key: testbackuppolicy.BackupSourceKey(source.SourceID), Value: sourceValue},
	})
	clear(policyValue)
	clear(sourceValue)
	if err != nil || !result.Succeeded {
		t.Fatalf("replace Config publication evidence = %#v, %v", result, err)
	}
	run.PolicyRevision = result.Revision
	source.SourceRevision = result.Revision
	fixedRevision := backupRuntimeCurrentRevision(t, store, run.EnvironmentID)
	source.Snapshot.Config.ReadRevision = fixedRevision
	return fixedRevision
}
