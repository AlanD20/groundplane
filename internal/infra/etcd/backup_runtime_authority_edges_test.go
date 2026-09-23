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
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func backupRuntimeSealedRunPlan(
	t *testing.T,
	run testbackupruntime.BackupRunRecord,
	planID string,
) *agentpb.ExecutionPlan {
	t.Helper()
	plan := &agentpb.ExecutionPlan{
		Schema:    executionplan.SchemaVersion,
		PlanId:    planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		TargetId:  run.EnvironmentID,
		Steps:     make([]*agentpb.ExecutionStep, len(run.Sources)),
	}
	for index, source := range run.Sources {
		capture := &agentpb.BackupSourceCapture{
			SourceId: source.SourceID, SourceRevision: uint64(source.SourceRevision),
			TargetId: source.TargetID, TargetRevision: uint64(source.TargetRevision),
			PointId:     source.RecoveryPointID,
			ConnectorId: run.ConnectorID, ConnectorRevision: uint64(run.ConnectorRevision),
			SourceFormat: map[testbackupruntime.BackupRuntimeFormat]agentpb.BackupSourceFormat{
				testbackupruntime.BackupRuntimeFormatPostgres: agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1,
				testbackupruntime.BackupRuntimeFormatConfig:   agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1,
				testbackupruntime.BackupRuntimeFormatVolume:   agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1,
			}[source.Format],
			Encryption: map[testbackupruntime.BackupRuntimeEncryption]agentpb.BackupEncryption{
				testbackupruntime.BackupRuntimeEncryptionNone: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
				testbackupruntime.BackupRuntimeEncryptionAge:  agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			}[run.Encryption],
			KeyEra: uint64(run.KeyEra), AgeRecipient: run.Recipient,
			Upload: &agentpb.BackupUploadAuthority{
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorPrefix: run.ConnectorPrefix, ConnectorRegion: run.ConnectorRegion,
				ConnectorAddressing: backupFixtureAddressing(run.ConnectorPathStyle),
				ProtectedObjectKey:  source.ObjectKey,
				ImmutableCreate:     true, PutAfterArtifactPreparedAck: true, HeadAfterUploadCompletedAck: true,
			},
		}
		switch source.Kind {
		case testbackupruntime.BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			capture.Source = &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
				BackingServiceId:       snapshot.BackingServiceID,
				BackingServiceRevision: uint64(snapshot.BackingServiceRevision),
				Database:               snapshot.Database, Role: snapshot.Role,
			}}
		case testbackupruntime.BackupRuntimeSourceConfig:
			capture.Source = &agentpb.BackupSourceCapture_Config{Config: &agentpb.BackupConfigSource{
				SnapshotRevision: uint64(source.Snapshot.Config.ReadRevision),
			}}
		case testbackupruntime.BackupRuntimeSourceVolume:
			artifactSHA256, err := hex.DecodeString(source.Snapshot.Volume.ArtifactDigest)
			if err != nil {
				t.Fatal(err)
			}
			services := make([]*agentpb.BackupVolumeService, len(source.Snapshot.Volume.Services))
			for serviceIndex, service := range source.Snapshot.Volume.Services {
				services[serviceIndex] = &agentpb.BackupVolumeService{
					ServiceId: service.ServiceID, ServiceRevision: uint64(service.ServiceRevision),
					PriorIntent: map[testbackupruntime.BackupServiceRuntimeIntent]agentpb.BackupServiceRuntimeIntent{
						testbackupruntime.BackupServiceIntentRunning: agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
						testbackupruntime.BackupServiceIntentStopped: agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED,
						testbackupruntime.BackupServiceIntentAbsent:  agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT,
					}[service.PriorIntent],
					ComposeKey: service.ComposeKey, MountPaths: append([]string(nil), service.MountPaths...),
				}
			}
			volume := source.Snapshot.Volume
			capture.Source = &agentpb.BackupSourceCapture_Volume{Volume: &agentpb.BackupVolumeSource{
				Services: services, ArtifactId: volume.ArtifactID, ArtifactSha256: artifactSHA256,
				ArtifactRevision: uint64(volume.ArtifactRevision), ProjectionRoot: uint64(volume.ProjectionRoot),
				RenderGeneration: volume.RenderGeneration, ComposeVolumeKey: volume.ComposeVolumeKey,
				DockerVolumeName: volume.DockerVolumeName, AuthorizedVolumeDir: volume.AuthorizedVolumeDir,
			}}
		}
		plan.Steps[index] = &agentpb.ExecutionStep{
			StepId:         ids.NewAt(ids.KindStep, run.CreatedAt, int64(2600+index)),
			TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
			Payload:        &agentpb.ExecutionStep_BackupSourceCapture{BackupSourceCapture: capture},
		}
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func prepareBackupRuntimeStagedRun(
	t *testing.T,
	repository *BackupRuntimeRepository,
	run testbackupruntime.BackupRunRecord,
) (testkeyvalue.Versioned[testbackupruntime.BackupRunRecord], testbackupruntime.BackupRunRecord) {
	t.Helper()
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	staged := run
	staged.State = testbackupruntime.BackupRunRunning
	staged.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	staged.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	staged.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	staged.Sources[0].SizeBytes = 123
	staged.Sources[0].SHA256 = testBackupDigest
	staged.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, staged)
	if err != nil {
		t.Fatal(err)
	}
	return created, staged
}

func backupRuntimePointCommitRecords(
	run testbackupruntime.BackupRunRecord,
	staged testbackupruntime.BackupRunRecord,
) (testbackupruntime.BackupRunRecord, testbackupruntime.BackupRecoveryPointRecord, testbackupruntime.BackupRetentionSweepRecord) {
	committed := staged
	committed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	committed.Sources[0].State = testbackupruntime.BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = testbackupruntime.BackupSourcePhaseRetention
	committed.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], committed.UpdatedAt)
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: committed.UpdatedAt, UpdatedAt: committed.UpdatedAt,
	}
	return committed, point, sweep
}

// Rationale: a point selected through any one index is visible only when the
// primary and all three immutable memberships retain their commit revision.
func TestBackupRuntimeRepositoryPointPagesRejectIncompleteAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		key    func(testbackupruntime.BackupRecoveryPointRecord) string
		delete bool
	}{
		{
			name: "missing Connector membership",
			key: func(point testbackupruntime.BackupRecoveryPointRecord) string {
				key, _ := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
				return key
			},
			delete: true,
		},
		{
			name: "rewritten Environment membership",
			key: func(point testbackupruntime.BackupRecoveryPointRecord) string {
				key, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
				return key
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			committed, point, sweep := backupRuntimePointCommitRecords(run, staged)
			checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
			checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
			if _, _, err := repository.CommitBackupRecoveryPoint(
				context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion,
				committed, 0, point, nil, sweep,
			); err != nil {
				t.Fatal(err)
			}
			key := test.key(point)
			entry := mustOptionalKey(t, store, key)
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}
			if test.delete {
				mutation = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]testkeyvalue.Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("tamper point companion = %#v, %v", changed, err)
			}
			if _, err := repository.ListBackupRecoveryPointsBySource(
				context.Background(), point.SourceID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("ListBackupRecoveryPointsBySource(corrupt authority) error = %v", err)
			}
		})
	}
}

// Rationale: retention enumeration uses the same immutable point aggregate as
// public pages, including the selected source index and both other memberships.
func TestBackupRuntimeRepositoryRetentionRejectsMalformedPointAuthority(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		key    func(testbackupruntime.BackupRecoveryPointRecord) string
		delete bool
	}{
		{
			name: "rewritten source index",
			key: func(point testbackupruntime.BackupRecoveryPointRecord) string {
				key, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
				return key
			},
		},
		{
			name: "missing Environment index",
			key: func(point testbackupruntime.BackupRecoveryPointRecord) string {
				key, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
				return key
			},
			delete: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			committed, point, sweep := backupRuntimePointCommitRecords(run, staged)
			checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
			checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
			_, committedRun, err := repository.CommitBackupRecoveryPoint(
				context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion,
				committed, 0, point, nil, sweep,
			)
			if err != nil {
				t.Fatal(err)
			}
			currentSweep, found, err := repository.GetBackupRetentionSweep(
				context.Background(), point.SourceID, point.ID,
			)
			if err != nil || !found {
				t.Fatalf("GetBackupRetentionSweep() = %#v/%v/%v", currentSweep, found, err)
			}
			key := test.key(point)
			entry := mustOptionalKey(t, store, key)
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}
			if test.delete {
				mutation = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]testkeyvalue.Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("tamper retention point authority = %#v, %v", changed, err)
			}
			if _, _, err := repository.AdvanceBackupRetentionSweep(
				context.Background(), committedRun, currentSweep, sweep.UpdatedAt.Add(time.Second),
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("AdvanceBackupRetentionSweep(corrupt point authority) error = %v", err)
			}
		})
	}
}

// Rationale: stale prune authority is a retryable CAS conflict, while bytes
// or aggregate companions that disagree at the exact revision are corruption.
func TestBackupRuntimeRepositoryClassifiesPruneCASAndCorruption(t *testing.T) {
	t.Parallel()
	_, _, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: 71, OperationID: run.OperationID,
		State: testbackupruntime.BackupPrunePending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	sourceIndex, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	connectorIndex, _ := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	pointRevision := int64(71)
	pruneRevision := int64(72)
	values := []*testkeyvalue.KeyValue{
		{
			Key:         testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
			Value:       pruneValue,
			ModRevision: pruneRevision,
			Version:     1,
		},
		{
			Key:         testbackupruntime.BackupRecoveryPointKey(point.ID),
			Value:       pointValue,
			ModRevision: pointRevision,
			Version:     1,
		},
		{Key: environmentIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
		{Key: sourceIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
		{Key: connectorIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
	}
	expectedPrune := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
		Record:   prune,
		Revision: pruneRevision,
	}
	stalePrune := expectedPrune
	stalePrune.Revision++
	if err := validatePendingBackupPruneAuthority(values, stalePrune); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("publication prune revision drift error = %v", err)
	}
	corruptPrune := append([]*testkeyvalue.KeyValue(nil), values...)
	corruptPrunePrimary := *values[0]
	corruptPrunePrimary.Value = []byte(`{"invalid":`)
	corruptPrune[0] = &corruptPrunePrimary
	if err := validatePendingBackupPruneAuthority(corruptPrune, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication same-revision corruption error = %v", err)
	}
	missingCompanion := append([]*testkeyvalue.KeyValue(nil), values...)
	missingCompanion[4] = nil
	if err := validatePendingBackupPruneAuthority(missingCompanion, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication split point authority error = %v", err)
	}
	rewrittenPoint := append([]*testkeyvalue.KeyValue(nil), values...)
	rewrittenPointPrimary := *values[1]
	rewrittenPointPrimary.Version = 2
	rewrittenPoint[1] = &rewrittenPointPrimary
	if err := validatePendingBackupPruneAuthority(rewrittenPoint, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication rewritten point primary error = %v", err)
	}
	rewrittenIndex := append([]*testkeyvalue.KeyValue(nil), values...)
	rewrittenSourceIndex := *values[3]
	rewrittenSourceIndex.Version = 2
	rewrittenIndex[3] = &rewrittenSourceIndex
	if err := validatePendingBackupPruneAuthority(rewrittenIndex, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication rewritten point membership error = %v", err)
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: run.OperationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID}, CreatedAt: point.VerifiedAt.Add(time.Second),
	}
	dispatchValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(dispatchValue)
	dispatchRevision := int64(81)
	dispatchEntry := &testkeyvalue.KeyValue{
		Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID), Value: dispatchValue,
		ModRevision: dispatchRevision, Version: 1,
	}
	expectedDispatch := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatch, Revision: dispatchRevision,
	}
	for _, path := range []string{"checkpoint", "failure", "completion"} {
		t.Run(path+" revision drift", func(t *testing.T) {
			stale := expectedDispatch
			stale.Revision++
			if err := validateExactBackupPruneDispatchValue(dispatchEntry, stale); !errors.Is(
				err,
				errs.New(errs.KindStateConflict, ""),
			) {
				t.Fatalf("dispatch revision drift error = %v", err)
			}
		})
		t.Run(path+" same-revision corruption", func(t *testing.T) {
			corrupt := *dispatchEntry
			corrupt.Value = []byte(`{"invalid":`)
			if err := validateExactBackupPruneDispatchValue(&corrupt, expectedDispatch); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("dispatch same-revision corruption error = %v", err)
			}
		})
	}
}

// Rationale: corrupt normalized authority is durable corruption, while a
// changed desired-head/root/runtime fence is a retryable conflict.
func TestBackupRuntimeRepositoryClassifiesPinnedVolumeEvidence(t *testing.T) {
	t.Parallel()
	_, store, run := newBackupRuntimeRepositoryFixture(t)
	environmentValue := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID))
	if environmentValue == nil {
		t.Fatal("missing Environment publication fixture")
	}
	environment, err := testhierarchy.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	revisionID := ids.NewAt(ids.KindTask, run.CreatedAt, 8101)
	headValue, err := testidempotency.EncodeTaskReference(revisionID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(headValue)
	digest := sha256.Sum256([]byte("normalized Volume projection"))
	sealValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID: environment.ID, RevisionID: revisionID,
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 1, ProjectionSchema: 1,
		AuditChunks: 1, AuditBytes: 1, AuditSHA256: digest,
		ProjectionChunks: 1, ProjectionBytes: 1, ProjectionSHA256: digest,
		ProjectionResources: 1, DependencyDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sealValue)
	source := testbackupruntime.BackupRunSourceAttemptRecord{TargetID: testBackupVolumeID, TargetRevision: 71}
	rootRevision := int64(72)
	projectionSnapshot := testbackupruntime.BackupVolumeSourceSnapshot{
		EnvironmentID: environment.ID, EnvironmentRevision: environmentValue.ModRevision,
		VolumeID: source.TargetID, DesiredRevisionID: revisionID, ProjectionRoot: rootRevision,
		DependencyDigest: hex.EncodeToString(digest[:]), RenderGeneration: 1,
		ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_" + source.TargetID,
		AuthorizedVolumeDir: environment.VolumeDir,
	}
	projectionEvidence := []*testkeyvalue.KeyValue{
		{
			Key:         testhierarchy.EnvironmentKey(environment.ID),
			Value:       environmentValue.Value,
			ModRevision: environmentValue.ModRevision,
		},
		{
			Key:         testblueprints.EnvironmentBlueprintHeadKey(environment.ID),
			Value:       headValue,
			ModRevision: source.TargetRevision,
		},
		{
			Key:         testblueprints.EnvironmentBlueprintRootKey(environment.ID, revisionID),
			Value:       sealValue,
			ModRevision: rootRevision,
		},
	}
	for _, test := range []struct {
		name   string
		offset int
	}{
		{name: "Environment", offset: 0},
		{name: "desired head", offset: 1},
		{name: "projection root", offset: 2},
	} {
		t.Run(test.name+" decode corruption", func(t *testing.T) {
			values := append([]*testkeyvalue.KeyValue(nil), projectionEvidence...)
			corruptValue := *values[test.offset]
			corruptValue.Value = []byte(`{"invalid":`)
			values[test.offset] = &corruptValue
			if err := testbackupplanning.ValidateBackupVolumePublicationEvidence(
				values, source, projectionSnapshot,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("pinned %s decode corruption error = %v", test.name, err)
			}
		})
	}
	wrongProjectionRevision := append([]*testkeyvalue.KeyValue(nil), projectionEvidence...)
	wrongProjection := *projectionEvidence[2]
	wrongProjection.ModRevision++
	wrongProjection.Value = []byte(`{"invalid":`)
	wrongProjectionRevision[2] = &wrongProjection
	if err := testbackupplanning.ValidateBackupVolumePublicationEvidence(
		wrongProjectionRevision, source, projectionSnapshot,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("projection revision drift error = %v", err)
	}
	driftedEnvironmentSnapshot := projectionSnapshot
	driftedEnvironmentSnapshot.AuthorizedVolumeDir += "/changed"
	if err := testbackupplanning.ValidateBackupVolumePublicationEvidence(
		projectionEvidence, source, driftedEnvironmentSnapshot,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Environment semantic drift error = %v", err)
	}
	postgres := run.Sources[0].Snapshot.Postgres
	if postgres == nil {
		t.Fatal("missing Service publication fixture")
	}
	runtimeValue := mustOptionalKey(t, store, testservices.ServiceRuntimeKey(postgres.BackingServiceID))
	if runtimeValue == nil {
		t.Fatal("missing Service runtime sidecar fixture")
	}
	runtime, err := testservices.DecodeServiceRuntimeRecord(runtimeValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	serviceSnapshot := projectionSnapshot
	serviceSnapshot.Services = []testbackupruntime.BackupVolumeServiceSnapshot{{
		ServiceID: runtime.ServiceID, ServiceRevision: runtimeValue.ModRevision,
		ComposeKey: "database", MountPaths: []string{"/data"},
		PriorIntent: testbackupruntime.BackupServiceRuntimeIntent(runtime.Runtime.RuntimeIntent),
	}}
	corruptRuntime := append([]*testkeyvalue.KeyValue(nil), projectionEvidence...)
	corruptRuntime = append(
		corruptRuntime,
		&testkeyvalue.KeyValue{
			Key:         testservices.ServiceRuntimeKey(runtime.ServiceID),
			Value:       []byte(`{"invalid":`),
			ModRevision: runtimeValue.ModRevision,
		},
	)
	if err := testbackupplanning.ValidateBackupVolumePublicationEvidence(
		corruptRuntime, source, serviceSnapshot,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("pinned Service runtime sidecar decode corruption error = %v", err)
	}
	corruptRuntime[3].ModRevision++
	if err := testbackupplanning.ValidateBackupVolumePublicationEvidence(
		corruptRuntime, source, serviceSnapshot,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Service runtime sidecar revision drift error = %v", err)
	}
}

// Rationale: losing the orphan and every adoption target is a clean CAS race;
// any partial or malformed durable publication is corruption, not retry authority.
func TestBackupRuntimeRepositoryClassifiesReconciledOrphanAdoptionRaces(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		publication string
		want        errs.Kind
	}{
		{name: "all authority absent", publication: "absent", want: errs.KindStateConflict},
		{name: "partial adoption", publication: "partial", want: errs.KindInternal},
		{name: "malformed adoption", publication: "malformed", want: errs.KindInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, orphan := createBackupRuntimeOrphanForReadTest(t)
			current, found, err := repository.GetBackupOrphan(context.Background(), orphan.Point.ID)
			if err != nil || !found {
				t.Fatalf("GetBackupOrphan() = %#v/%v/%v", current, found, err)
			}
			point := testbackupruntime.BackupRecoveryPointRecord{
				BackupRecoveryPointSnapshot: current.Record.Point,
				VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
			}
			sweep := testbackupruntime.BackupRetentionSweepRecord{
				SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
				Keep:     current.Record.Reconciliation.RetentionKeep,
				Revision: current.Record.Reconciliation.PolicyRevision,
				State:    testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
			}
			orphanConnectorIndex, _ := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
			orphanEnvironmentIndex, _ := testbackupruntime.BackupOrphanEnvironmentIndexKey(
				point.EnvironmentID,
				point.ID,
			)
			environmentIndex, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(
				point.EnvironmentID,
				point.ID,
			)
			sourceIndex, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
			connectorIndex, _ := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
			mutations := []testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupOrphanKey(point.ID)},
				{Type: testkeyvalue.MutationDelete, Key: orphanConnectorIndex},
				{Type: testkeyvalue.MutationDelete, Key: orphanEnvironmentIndex},
			}
			pointValue, encodeErr := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			defer clear(pointValue)
			if test.publication == "partial" {
				mutations = append(mutations, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue,
				})
			}
			if test.publication == "malformed" {
				sweepValue, encodeErr := testbackupruntime.EncodeBackupRetentionSweepRecord(sweep)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				defer clear(sweepValue)
				mutations = append(
					mutations,
					testkeyvalue.Mutation{
						Type:  testkeyvalue.MutationPut,
						Key:   testbackupruntime.BackupRecoveryPointKey(point.ID),
						Value: []byte(`{"invalid":`),
					},
					testkeyvalue.Mutation{
						Type:  testkeyvalue.MutationPut,
						Key:   environmentIndex,
						Value: []byte(point.ID),
					},
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
					testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
					testkeyvalue.Mutation{
						Type:  testkeyvalue.MutationPut,
						Key:   testbackupruntime.BackupRetentionKey(point.SourceID, point.ID),
						Value: sweepValue,
					},
				)
			}
			changed, err := store.Transact(context.Background(), nil, mutations)
			testkeyvalue.ClearMutationValues(mutations)
			if err != nil || !changed.Succeeded {
				t.Fatalf("race orphan adoption = %#v, %v", changed, err)
			}
			if _, err := repository.AdoptReconciledBackupOrphan(
				context.Background(), current, point, sweep,
			); !errors.Is(err, errs.New(test.want, "")) {
				t.Fatalf("AdoptReconciledBackupOrphan(%s) error = %v", test.name, err)
			}
		})
	}
}

// Rationale: two reconcilers may verify the same immutable orphan at different
// valid instants; the winner is durable authority and the loser observes a CAS conflict.
func TestBackupRuntimeRepositoryRejectsAlternateValidReconciledOrphanAdoptionReplay(t *testing.T) {
	t.Parallel()
	repository, _, orphan := createBackupRuntimeOrphanForReadTest(t)
	current, found, err := repository.GetBackupOrphan(context.Background(), orphan.Point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", current, found, err)
	}
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: current.Record.Point,
		VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
	}
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:     current.Record.Reconciliation.RetentionKeep,
		Revision: current.Record.Reconciliation.PolicyRevision,
		State:    testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	if _, err := repository.AdoptReconciledBackupOrphan(
		context.Background(), current, point, sweep,
	); err != nil {
		t.Fatalf("AdoptReconciledBackupOrphan(winner) error = %v", err)
	}
	alternatePoint := point
	alternatePoint.VerifiedAt = point.VerifiedAt.Add(time.Second)
	alternateSweep := sweep
	alternateSweep.CreatedAt = alternatePoint.VerifiedAt
	alternateSweep.UpdatedAt = alternatePoint.VerifiedAt
	if _, err := repository.AdoptReconciledBackupOrphan(
		context.Background(), current, alternatePoint, alternateSweep,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AdoptReconciledBackupOrphan(alternate valid replay) error = %v", err)
	}
}

func backupRemoteAbsentCheckpoint(
	input testbackupruntime.BackupCheckpointInput,
	sequence uint64,
	pointID string,
) testbackupruntime.BackupCheckpointInput {
	input.Sequence = sequence
	input.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind: testbackupruntime.BackupCheckpointRemoteObjectAbsent, PointID: pointID,
	}
	return input
}
