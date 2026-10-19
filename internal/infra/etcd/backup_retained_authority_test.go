package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	"strings"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-157: retained orphan reads reject missing, rewritten, malformed or
// misbucketed membership rather than overlooking cleanup obligations.
func TestBackupRuntimeRepositoryRejectsCorruptOrphanConnectorIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*testing.T, *memoryHierarchyStore, testbackupruntime.BackupOrphanRecord, string) []testkeyvalue.Mutation
	}{
		{
			name: "missing",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, _ testbackupruntime.BackupOrphanRecord, key string) []testkeyvalue.Mutation {
				return []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}}
			},
		},
		{
			name: "malformed",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, _ testbackupruntime.BackupOrphanRecord, key string) []testkeyvalue.Mutation {
				return []testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationPut, Key: key, Value: []byte("not-a-point-id")},
				}
			},
		},
		{
			name: "mismatched",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, orphan testbackupruntime.BackupOrphanRecord, key string) []testkeyvalue.Mutation {
				other := ids.NewAt(ids.KindRecoveryPoint, orphan.CreatedAt.Add(time.Second), 611)
				return []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: []byte(other)}}
			},
		},
		{
			name: "misbucketed",
			mutate: func(t *testing.T, _ *memoryHierarchyStore, orphan testbackupruntime.BackupOrphanRecord, key string) []testkeyvalue.Mutation {
				otherConnector := ids.NewAt(ids.KindConnector, orphan.CreatedAt.Add(time.Second), 612)
				wrongKey, err := testbackupruntime.BackupOrphanConnectorIndexKey(otherConnector, orphan.Target.ID)
				if err != nil {
					t.Fatal(err)
				}
				return []testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationDelete, Key: key},
					{Type: testkeyvalue.MutationPut, Key: wrongKey, Value: []byte(orphan.Target.ID)},
				}
			},
		},
		{
			name: "identical byte replay",
			mutate: func(t *testing.T, store *memoryHierarchyStore, _ testbackupruntime.BackupOrphanRecord, key string) []testkeyvalue.Mutation {
				entry := mustOptionalKey(t, store, key)
				return []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository, store, orphan := createBackupRuntimeOrphanForReadTest(t)
			connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(
				orphan.Target.ConnectorID,
				orphan.Target.ID,
			)
			if err != nil {
				t.Fatal(err)
			}
			entry := mustOptionalKey(t, store, connectorIndex)
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: connectorIndex, ModRevision: entry.ModRevision}},
				test.mutate(t, store, orphan, connectorIndex),
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("corrupt Connector index = %#v, %v", changed, err)
			}
			if _, _, err := repository.GetBackupOrphan(
				context.Background(), orphan.Target.ID,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("GetBackupOrphan() error = %v", err)
			}
			if _, err := repository.ListBackupOrphansByEnvironment(
				context.Background(),
				orphan.Target.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("ListBackupOrphansByEnvironment() error = %v", err)
			}
		})
	}
}

// TASK-157: stale ownership is a conflict; malformed bytes and incomplete
// immutable companions are corruption, not authority for another attempt.
func TestBackupRuntimeRepositoryClassifiesPruneCASAndCorruption(t *testing.T) {
	t.Parallel()
	_, _, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	backupRuntimeCompleteSourceArtifact(run, &source)
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: 71, OperationID: run.OperationID,
		PolicyRevision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
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
	if err := testbackupruntime.ValidatePendingBackupPruneAuthority(values, stalePrune); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("publication prune revision drift error = %v", err)
	}
	corruptPrune := append([]*testkeyvalue.KeyValue(nil), values...)
	corruptPrunePrimary := *values[0]
	corruptPrunePrimary.Value = []byte(`{"invalid":`)
	corruptPrune[0] = &corruptPrunePrimary
	if err := testbackupruntime.ValidatePendingBackupPruneAuthority(corruptPrune, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication same-revision corruption error = %v", err)
	}
	missingCompanion := append([]*testkeyvalue.KeyValue(nil), values...)
	missingCompanion[4] = nil
	if err := testbackupruntime.ValidatePendingBackupPruneAuthority(missingCompanion, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication split point authority error = %v", err)
	}
	rewrittenPoint := append([]*testkeyvalue.KeyValue(nil), values...)
	rewrittenPointPrimary := *values[1]
	rewrittenPointPrimary.Version = 2
	rewrittenPoint[1] = &rewrittenPointPrimary
	if err := testbackupruntime.ValidatePendingBackupPruneAuthority(rewrittenPoint, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication rewritten point primary error = %v", err)
	}
	rewrittenIndex := append([]*testkeyvalue.KeyValue(nil), values...)
	rewrittenSourceIndex := *values[3]
	rewrittenSourceIndex.Version = 2
	rewrittenIndex[3] = &rewrittenSourceIndex
	if err := testbackupruntime.ValidatePendingBackupPruneAuthority(rewrittenIndex, expectedPrune); !errors.Is(
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
	t.Run("dispatch revision drift", func(t *testing.T) {
		stale := expectedDispatch
		stale.Revision++
		if err := testbackupruntime.ValidateExactBackupPruneDispatchValue(dispatchEntry, stale); !errors.Is(
			err,
			errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("dispatch revision drift error = %v", err)
		}
	})
	t.Run("dispatch same-revision corruption", func(t *testing.T) {
		corrupt := *dispatchEntry
		corrupt.Value = []byte(`{"invalid":`)
		if err := testbackupruntime.ValidateExactBackupPruneDispatchValue(&corrupt, expectedDispatch); !errors.Is(
			err,
			errs.New(errs.KindInternal, ""),
		) {
			t.Fatalf("dispatch same-revision corruption error = %v", err)
		}
	})
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
		SourceKind: testblueprints.EnvironmentBlueprintSourceApply, RenderGeneration: 1, ProjectionSchema: testblueprints.EnvironmentDesiredInputSchema,
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
		ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_" + strings.ToLower(source.TargetID),
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
			current, found, err := repository.GetBackupOrphan(context.Background(), orphan.Target.ID)
			if err != nil || !found {
				t.Fatalf("GetBackupOrphan() = %#v/%v/%v", current, found, err)
			}
			current = seedBackupOrphanAcknowledgedCleanup(t, store, current)
			point := testbackupruntime.BackupRecoveryPointRecord{
				BackupRecoveryPointSnapshot: backupRuntimeOrphanPoint(current.Record),
				VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
			}
			sweep := testbackupruntime.BackupRetentionSweepRecord{
				SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
				Keep:         current.Record.Reconciliation.RetentionKeep,
				Revision:     current.Record.Reconciliation.PolicyRevision,
				PolicySHA256: current.Record.Reconciliation.PolicySHA256,
				State:        testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
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
	repository, store, orphan := createBackupRuntimeOrphanForReadTest(t)
	current, found, err := repository.GetBackupOrphan(context.Background(), orphan.Target.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", current, found, err)
	}
	current = seedBackupOrphanAcknowledgedCleanup(t, store, current)
	point := testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: backupRuntimeOrphanPoint(current.Record),
		VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
	}
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:         current.Record.Reconciliation.RetentionKeep,
		Revision:     current.Record.Reconciliation.PolicyRevision,
		PolicySHA256: current.Record.Reconciliation.PolicySHA256,
		State:        testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
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
