package etcd

import (
	context "context"
	errors "errors"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: run transitions must remain fenced by the exact owned Environment
// operation authority and advance its mutation epoch atomically.
func TestBackupRuntimeRepositoryRunTransitionsUseOwnedFixedFence(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("CreateBackupRun() error = %v", err)
	}
	if created.Record.State != testbackupruntime.BackupRunQueued || created.Revision <= 0 {
		t.Fatalf("CreateBackupRun() = %#v", created)
	}
	read, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || read.Revision != created.Revision {
		t.Fatalf("GetBackupRun() = %#v, %v", read, err)
	}
	page, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.TaskID != run.TaskID {
		t.Fatalf("ListBackupRunsByEnvironment() = %#v, %v", page, err)
	}
	if _, found, err := repository.GetBackupSourceTargetExclusion(
		context.Background(), testbackupruntime.BackupSourceTargetAttach, run.Sources[0].TargetID,
	); err != nil || !found {
		t.Fatalf("GetBackupSourceTargetExclusion() = %v/%v", found, err)
	}
	epochAfterCreate := mustEnvironmentMutationEpochRevision(t, store, run.EnvironmentID)
	next := run
	next.State = testbackupruntime.BackupRunRunning
	next.UpdatedAt = run.UpdatedAt.Add(time.Second)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	transitioned, err := repository.TransitionBackupRun(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), created, next,
	)
	if err != nil {
		t.Fatalf("TransitionBackupRun() error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		store,
		run.EnvironmentID,
	); epoch <= epochAfterCreate ||
		epoch != transitioned.Revision {
		t.Fatalf(
			"transition epoch = %d, create epoch = %d, revision = %d",
			epoch,
			epochAfterCreate,
			transitioned.Revision,
		)
	}
}

// Rationale: Agent-produced artifact evidence is acknowledged only with its
// exact assignment sequence and the matching source transition in one txn.
func TestBackupRuntimeRepositoryCheckpointTransitionIsAtomicAndReplayable(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	ready := run
	ready.State = testbackupruntime.BackupRunRunning
	ready.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	ready.Sources[0].State = testbackupruntime.BackupSourceAttemptReady
	ready.Sources[0].Phase = testbackupruntime.BackupSourcePhaseStaging
	ready.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, ready)
	if err != nil {
		t.Fatal(err)
	}
	staged := ready
	staged.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), ready.Sources...)
	staged.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	staged.Sources[0].Phase = testbackupruntime.BackupSourcePhaseUpload
	staged.Sources[0].SizeBytes = 123
	staged.Sources[0].SHA256 = testBackupDigest
	staged.UpdatedAt = ready.UpdatedAt.Add(time.Second)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind: testbackupruntime.BackupCheckpointArtifactPrepared, PointID: staged.Sources[0].RecoveryPointID,
		StoredSizeBytes: uint64(staged.Sources[0].SizeBytes), StoredSHA256: staged.Sources[0].SHA256,
	}
	transitioned, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, created, staged,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun() error = %v", err)
	}
	replayed, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, created, staged,
	)
	if err != nil || replayed.Revision != transitioned.Revision {
		t.Fatalf("CheckpointBackupRun(replay) = %#v, %v", replayed, err)
	}
	headVerified := staged
	headVerified.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	headVerified.Sources[0].Phase = testbackupruntime.BackupSourcePhaseHeadVerification
	headVerified.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	if _, err := repository.TransitionBackupRun(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		transitioned,
		headVerified,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("TransitionBackupRun(upload without checkpoint) error = %v", err)
	}
	uploadCompleted := checkpoint
	uploadCompleted.Sequence++
	uploadCompleted.Payload.Kind = testbackupruntime.BackupCheckpointUploadCompleted
	headVersion, err := repository.CheckpointBackupRun(
		context.Background(),
		uploadCompleted,
		transitioned,
		headVerified,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(upload completed) error = %v", err)
	}
	pointCommitReady := headVerified
	pointCommitReady.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		headVerified.Sources...,
	)
	pointCommitReady.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	pointCommitReady.UpdatedAt = headVerified.UpdatedAt.Add(time.Second)
	uploadVerified := uploadCompleted
	uploadVerified.Sequence++
	uploadVerified.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	if _, err := repository.CheckpointBackupRun(
		context.Background(),
		uploadVerified,
		headVersion,
		pointCommitReady,
	); err != nil {
		t.Fatalf("CheckpointBackupRun(upload verified) error = %v", err)
	}
	if _, err := repository.TransitionBackupRun(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), created, staged,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("TransitionBackupRun(without checkpoint) error = %v", err)
	}
}

// Rationale: acknowledged artifact preparation is durable upload intent, so a
// Put-uncertain orphan advances deterministically through Put and HEAD evidence.
func TestBackupRuntimeRepositoryOrphanFollowsUploadCheckpoints(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	ready := run
	ready.State = testbackupruntime.BackupRunRunning
	ready.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	ready.Sources[0].State = testbackupruntime.BackupSourceAttemptReady
	ready.Sources[0].Phase = testbackupruntime.BackupSourcePhaseStaging
	ready.UpdatedAt = run.UpdatedAt.Add(time.Second)
	readyVersion, err := repository.replaceBackupRunForTest(context.Background(), created, ready)
	if err != nil {
		t.Fatal(err)
	}
	staged := ready
	staged.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), ready.Sources...)
	staged.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	staged.Sources[0].Phase = testbackupruntime.BackupSourcePhaseUpload
	staged.Sources[0].SizeBytes = 123
	staged.Sources[0].SHA256 = testBackupDigest
	staged.UpdatedAt = ready.UpdatedAt.Add(time.Second)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind:            testbackupruntime.BackupCheckpointArtifactPrepared,
		PointID:         staged.Sources[0].RecoveryPointID,
		StoredSizeBytes: uint64(staged.Sources[0].SizeBytes),
		StoredSHA256:    staged.Sources[0].SHA256,
	}
	stagedVersion, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, readyVersion, staged,
	)
	if err != nil {
		t.Fatal(err)
	}
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, orphaned.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point:     point.BackupRecoveryPointSnapshot,
		TaskID:    run.TaskID,
		State:     testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt,
		UpdatedAt: orphaned.UpdatedAt,
	}
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		orphaned,
		0,
		orphan,
	)
	if err != nil {
		t.Fatalf("CreateBackupOrphan(Put uncertain) error = %v", err)
	}
	headVerified := orphaned
	headVerified.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	headVerified.Sources[0].Phase = testbackupruntime.BackupSourcePhaseHeadVerification
	headVerified.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	uploadCompleted := checkpoint
	uploadCompleted.Sequence++
	uploadCompleted.Payload.Kind = testbackupruntime.BackupCheckpointUploadCompleted
	headVersion, err := repository.CheckpointBackupRun(
		context.Background(), uploadCompleted, orphanedVersion, headVerified,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(upload completed) error = %v", err)
	}
	pointCommit := headVerified
	pointCommit.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), headVerified.Sources...)
	pointCommit.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	pointCommit.UpdatedAt = headVerified.UpdatedAt.Add(time.Second)
	uploadVerified := uploadCompleted
	uploadVerified.Sequence++
	uploadVerified.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	pointVersion, err := repository.CheckpointBackupRun(
		context.Background(), uploadVerified, headVersion, pointCommit,
	)
	if err != nil || pointVersion.Record.Sources[0].Phase != testbackupruntime.BackupSourcePhasePointCommit {
		t.Fatalf("CheckpointBackupRun(orphan verified) = %#v, %v", pointVersion, err)
	}
}

// Rationale: a wrong lock owner or stale epoch must reject the whole runtime
// mutation without leaving partial durable evidence.
func TestBackupRuntimeRepositoryRejectsWrongLockAndStaleEpochWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		run  func(*testing.T, *memoryHierarchyStore, testbackupruntime.BackupRunRecord)
	}{
		{name: "wrong lock", run: func(t *testing.T, store *memoryHierarchyStore, run testbackupruntime.BackupRunRecord) {
			lock := testbackupruntime.BackupOperationLockRecord{
				EnvironmentID: run.EnvironmentID, OperationID: testBackupOperationID,
				TaskID: testBackupRetryTaskID, Kind: testbackupruntime.BackupOperationBackup,
				CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
			}
			putBackupRuntimeLock(t, store, lock)
		}},
		{name: "stale epoch", run: func(_ *testing.T, _ *memoryHierarchyStore, _ testbackupruntime.BackupRunRecord) {
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, store, run := newBackupRuntimeBareFixture(t)
			test.run(t, store, run)
			var boundary hierarchyStore = store
			if test.name == "stale epoch" {
				racing := &entryVolumeEpochRaceStore{hierarchyStore: store}
				racing.beforeTransact = func() {
					advanceEnvironmentMutationFenceEpoch(t, store, run.EnvironmentID)
				}
				boundary = racing
			}
			repository, err := newBackupRuntimeRepository(boundary)
			if err != nil {
				t.Fatal(err)
			}
			_, err = repository.createBackupRunForTest(
				context.Background(),
				run,
				backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
			)
			if test.name == "wrong lock" && !isKind(err, errs.KindResourceInUse) {
				t.Fatalf("CreateBackupRun(wrong lock) error = %v", err)
			}
			if test.name == "stale epoch" && !isKind(err, errs.KindStateConflict) {
				t.Fatalf("CreateBackupRun(stale epoch) error = %v", err)
			}
			stored, getErr := store.Get(context.Background(), testbackupruntime.BackupRunKey(run.TaskID))
			if getErr != nil || stored.Entry != nil {
				t.Fatalf("failed run primary = %#v/%v", stored, getErr)
			}
		})
	}
}

// Rationale: a caller-selected fixed revision must bind source construction;
// advancing one exact source primary before CAS cannot publish a fabricated run.
func TestBackupRuntimeRepositoryRunPublicationRejectsChangedSourceEvidence(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	fixedRevision := backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID)
	entry := mustOptionalKey(t, store, testbackuppolicy.BackupSourceKey(run.Sources[0].SourceID))
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: entry.Key, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: entry.Key, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("advance backup source = %#v, %v", result, err)
	}
	if _, err := repository.createBackupRunForTest(
		context.Background(), run, fixedRevision,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("CreateBackupRun(changed source) error = %v", err)
	}
	if entry := mustOptionalKey(t, store, testbackupruntime.BackupRunKey(run.TaskID)); entry != nil {
		t.Fatalf("run published with changed source = %#v", entry)
	}
}

// Rationale: config capture authority is created with the run and pins the
// original fixed Entry read revision rather than reconstructing it on retry.
func TestBackupRuntimeRepositoryPublishesPinnedConfigSnapshotAtomically(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
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
		t.Fatalf("replace config publication evidence = %#v, %v", result, err)
	}
	run.PolicyRevision = result.Revision
	source.SourceRevision = result.Revision
	fixedRevision := backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID)
	source.Snapshot.Config.ReadRevision = fixedRevision
	created, err := repository.createBackupRunForTest(context.Background(), run, fixedRevision)
	if err != nil {
		t.Fatalf("CreateBackupRun(config) error = %v", err)
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID), testbackupconfiguration.BackupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID), testbackupconfiguration.BackupConfigSnapshotReferenceTaskKey(run.TaskID, run.TaskID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != created.Revision {
			t.Fatalf("config publication companion %q = %#v", key, entry)
		}
	}
	entry := mustOptionalKey(t, store, testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID))
	snapshot, err := testbackupconfiguration.DecodeBackupConfigSnapshotRecord(entry.Value)
	if err != nil || snapshot.ReadRevision != fixedRevision ||
		snapshot.State != testbackupconfiguration.BackupConfigSnapshotBuilding {
		t.Fatalf("config publication snapshot = %#v, %v", snapshot, err)
	}
}

// Rationale: a verified point, every visibility index, retention authority,
// and the run transition must appear at exactly one revision.
func TestBackupRuntimeRepositoryCommitsPointIndexesAndRetentionAtomically(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	running := run
	running.State = testbackupruntime.BackupRunRunning
	running.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	running.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	running.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	running.Sources[0].SizeBytes = 123
	running.Sources[0].SHA256 = testBackupDigest
	running.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, running)
	if err != nil {
		t.Fatal(err)
	}
	committed := running
	committed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), running.Sources...)
	committed.Sources[0].State = testbackupruntime.BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = testbackupruntime.BackupSourcePhaseRetention
	committed.UpdatedAt = running.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, running.Sources[0], committed.UpdatedAt)
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: committed.UpdatedAt, UpdatedAt: committed.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(
		t,
		repository.store.(*memoryHierarchyStore),
		run,
	)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	storedPoint, storedRun, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), created, committed, 0, point, nil, sweep,
	)
	if err != nil {
		t.Fatalf("CommitBackupRecoveryPoint() error = %v", err)
	}
	if storedPoint.Revision != storedRun.Revision {
		t.Fatalf("point/run revisions = %d/%d", storedPoint.Revision, storedRun.Revision)
	}
	for _, list := range []func() (testbackupruntime.BackupRuntimePage[testbackupruntime.BackupRecoveryPointRecord], error){
		func() (testbackupruntime.BackupRuntimePage[testbackupruntime.BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsByEnvironment(
				context.Background(), run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			)
		},
		func() (testbackupruntime.BackupRuntimePage[testbackupruntime.BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsBySource(
				context.Background(), point.SourceID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			)
		},
		func() (testbackupruntime.BackupRuntimePage[testbackupruntime.BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsByConnector(
				context.Background(), point.ConnectorID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			)
		},
	} {
		page, listErr := list()
		if listErr != nil || len(page.Items) != 1 || page.Items[0].Record.ID != point.ID {
			t.Fatalf("Recovery Point page = %#v/%v", page, listErr)
		}
	}
	retention, found, err := repository.GetBackupRetentionSweep(
		context.Background(), point.SourceID, point.ID,
	)
	if err != nil || !found || retention.Record != sweep ||
		retention.Revision != storedPoint.Revision {
		t.Fatalf("GetBackupRetentionSweep() = %#v/%v/%v", retention, found, err)
	}
}

// Rationale: an unknown point-commit response may be replayed only when the
// run, point, every index, retention sweep, and checkpoint dedupe all match.
func TestBackupRuntimeRepositoryPointCommitUnknownOutcomeValidatesAllCompanions(t *testing.T) {
	t.Parallel()
	for _, tamperSweep := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact replay", true: "changed sweep"}[tamperSweep], func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			committed, point, sweep := backupRuntimePointCommitRecords(run, staged)
			checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
			checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
			unknown := &backupRuntimeUnknownOutcomeStore{hierarchyStore: store, failNext: true}
			unknownRepository, err := newBackupRuntimeRepository(unknown)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := unknownRepository.CommitBackupRecoveryPoint(
				context.Background(),
				backupAssignmentFromCheckpoint(checkpoint),
				stagedVersion,
				committed,
				0,
				point,
				nil,
				sweep,
			); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("CommitBackupRecoveryPoint(unknown) error = %v", err)
			}
			if tamperSweep {
				entry := mustOptionalKey(t, store, testbackupruntime.BackupRetentionKey(point.SourceID, point.ID))
				changed := sweep
				changed.Keep++
				value, encodeErr := testbackupruntime.EncodeBackupRetentionSweepRecord(changed)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				result, transactErr := store.Transact(
					context.Background(),
					[]testkeyvalue.Condition{{Key: entry.Key, ModRevision: entry.ModRevision}},
					[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: entry.Key, Value: value}},
				)
				clear(value)
				if transactErr != nil || !result.Succeeded {
					t.Fatalf("tamper retention sweep = %#v, %v", result, transactErr)
				}
			}
			storedPoint, storedRun, err := repository.CommitBackupRecoveryPoint(
				context.Background(),
				backupAssignmentFromCheckpoint(checkpoint),
				stagedVersion,
				committed,
				0,
				point,
				nil,
				sweep,
			)
			if tamperSweep {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("CommitBackupRecoveryPoint(changed companion) error = %v", err)
				}
				return
			}
			if err != nil || storedPoint.Revision != storedRun.Revision ||
				storedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptPointCommitted {
				t.Fatalf("CommitBackupRecoveryPoint(replay) = %#v/%#v/%v", storedPoint, storedRun, err)
			}
		})
	}
}

// Rationale: retention authority scans the immutable newest-first point index,
// keeps the newest visible point, and hides every selected older point in the
// same epoch-fenced transaction that advances its durable cursor.
func TestBackupRuntimeRepositoryRetentionSweepCreatesPendingPrunesNewestFirst(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	committed, newest, sweep := backupRuntimePointCommitRecords(run, staged)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	mismatchedSweep := sweep
	mismatchedSweep.Keep--
	if _, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion,
		committed, 0, newest, nil, mismatchedSweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(changed Keep) error = %v", err)
	}
	_, committedRun, err := repository.CommitBackupRecoveryPoint(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		committed,
		0,
		newest,
		nil,
		sweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	for offset := int64(1); offset <= 12; offset++ {
		older := newest
		olderAt := time.Date(2015, 1, 1, 0, 0, int(offset), 0, time.UTC)
		older.ID = ids.NewAt(ids.KindRecoveryPoint, olderAt, offset)
		older.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + older.SourceID + "/" + older.ID + "/artifact.bin"
		older.CreatedAt = olderAt
		older.VerifiedAt = olderAt.Add(time.Millisecond)
		value, encodeErr := testbackupruntime.EncodeBackupRecoveryPointRecord(older)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		environmentIndex, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(older.EnvironmentID, older.ID)
		sourceIndex, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(older.SourceID, older.ID)
		connectorIndex, _ := testbackupruntime.BackupRecoveryPointConnectorIndexKey(older.ConnectorID, older.ID)
		result, transactErr := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(older.ID), Value: value},
			{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(older.ID)},
			{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(older.ID)},
			{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(older.ID)},
		})
		clear(value)
		if transactErr != nil || !result.Succeeded {
			t.Fatalf("seed older point = %#v, %v", result, transactErr)
		}
	}
	currentSweep, found, err := repository.GetBackupRetentionSweep(
		context.Background(), newest.SourceID, newest.ID,
	)
	if err != nil || !found {
		t.Fatalf("GetBackupRetentionSweep() = %#v/%v/%v", currentSweep, found, err)
	}
	prematureCleanup := committedRun.Record
	prematureCleanup.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		committedRun.Record.Sources...,
	)
	prematureCleanup.Sources[0].State = testbackupruntime.BackupSourceAttemptCleanupPending
	prematureCleanup.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	prematureCleanup.UpdatedAt = currentSweep.Record.UpdatedAt.Add(time.Second)
	if _, err := repository.advanceBackupRunAfterRetention(
		context.Background(), committedRun, prematureCleanup, 0, currentSweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("advanceBackupRunAfterRetention(pending sweep) error = %v", err)
	}
	firstPage, prunes, err := repository.AdvanceBackupRetentionSweep(
		context.Background(), committedRun, currentSweep, sweep.UpdatedAt.Add(time.Second),
	)
	if err != nil || firstPage.Record.State != testbackupruntime.BackupRetentionScanning || len(prunes) != 8 ||
		firstPage.Record.SelectionRevision <= 0 {
		t.Fatalf("AdvanceBackupRetentionSweep(first page) = %#v/%#v/%v", firstPage, prunes, err)
	}
	hostile := newest
	hostileAt := time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)
	hostile.ID = ids.NewAt(ids.KindRecoveryPoint, hostileAt, 98)
	hostile.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + hostile.SourceID + "/" +
		hostile.ID + "/artifact.bin"
	hostile.CreatedAt = hostileAt
	hostile.VerifiedAt = hostileAt.Add(time.Millisecond)
	seedBackupRuntimePointAuthority(t, store, hostile)
	advanced, secondPagePrunes, err := repository.AdvanceBackupRetentionSweep(
		context.Background(), committedRun, firstPage, sweep.UpdatedAt.Add(2*time.Second),
	)
	if err != nil || advanced.Record.State != testbackupruntime.BackupRetentionCompleted ||
		advanced.Record.SelectionRevision != firstPage.Record.SelectionRevision ||
		len(secondPagePrunes) != 2 {
		t.Fatalf("AdvanceBackupRetentionSweep(second page) = %#v/%#v/%v", advanced, secondPagePrunes, err)
	}
	prunes = append(prunes, secondPagePrunes...)
	if mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(hostile.ID)) != nil {
		t.Fatal("interpage point mutation entered the pinned retention selection")
	}
	cleanup := committedRun.Record
	cleanup.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), committedRun.Record.Sources...)
	cleanup.Sources[0].State = testbackupruntime.BackupSourceAttemptCleanupPending
	cleanup.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	cleanup.UpdatedAt = advanced.Record.UpdatedAt.Add(time.Second)
	if _, err := repository.TransitionBackupRun(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), committedRun, cleanup,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("TransitionBackupRun(skipped retention) error = %v", err)
	}
	advancedRun, err := repository.advanceBackupRunAfterRetention(
		context.Background(), committedRun, cleanup, 0, advanced,
	)
	if err != nil || advancedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptCleanupPending {
		t.Fatalf("advanceBackupRunAfterRetention() = %#v, %v", advancedRun, err)
	}
	if _, err := repository.GetBackupRecoveryPoint(context.Background(), newest.ID); err != nil {
		t.Fatalf("newest retained point error = %v", err)
	}
	for _, prune := range prunes {
		if prune.Record.State != testbackupruntime.BackupPrunePending || prune.Record.OperationID == "" {
			t.Fatalf("pending prune = %#v", prune)
		}
		if _, err := repository.GetBackupRecoveryPoint(
			context.Background(), prune.Record.Point.ID,
		); !errors.Is(err, errs.New(errs.KindRecoveryPointNotFound, "")) {
			t.Fatalf("pruned point visibility error = %v", err)
		}
	}
}

// Rationale: when Keep and retained progress both equal the public maximum,
// retention must prune every remaining visible point without incrementing.
func TestBackupRuntimeRepositoryRetentionSweepAtPublicMaximumPrunesRemainingPoints(t *testing.T) {
	t.Parallel()
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
	maximumKeep := testbackuppolicy.MaximumBackupPolicyKeep
	changedRun := committedRun.Record
	changedRun.RetentionKeep = maximumKeep
	changedSweep := currentSweep.Record
	changedSweep.Keep = maximumKeep
	changedSweep.State = testbackupruntime.BackupRetentionScanning
	for offset := int64(1); offset <= 2; offset++ {
		older := point
		olderAt := point.CreatedAt.Add(-time.Duration(offset) * time.Second)
		older.ID = ids.NewAt(ids.KindRecoveryPoint, olderAt, 603+offset)
		older.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + older.SourceID + "/" +
			older.ID + "/artifact.bin"
		older.CreatedAt = olderAt
		older.VerifiedAt = olderAt.Add(time.Millisecond)
		seedBackupRuntimePointAuthority(t, store, older)
	}
	selection, err := repository.ReadCurrentKeys(
		context.Background(), []string{testbackupruntime.BackupRecoveryPointKey(point.ID)},
	)
	if err != nil {
		t.Fatal(err)
	}
	changedSweep.SelectionRevision = selection.ReadRevision
	testkeyvalue.ClearValues(selection.Values)
	changedSweep.Cursor = ids.NewAt(
		ids.KindRecoveryPoint,
		point.CreatedAt.Add(time.Second),
		601,
	)
	changedSweep.RetainedCount = maximumKeep
	changedSweep.PruneOperationID = ids.NewAt(
		ids.KindOperation,
		point.CreatedAt.Add(time.Second),
		602,
	)
	changedSweep.UpdatedAt = currentSweep.Record.UpdatedAt.Add(time.Second)
	runValue, err := testbackupruntime.EncodeBackupRunRecord(changedRun)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runValue)
	sweepValue, err := testbackupruntime.EncodeBackupRetentionSweepRecord(changedSweep)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sweepValue)
	changed, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRunKey(run.TaskID), ModRevision: committedRun.Revision},
		{Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), ModRevision: currentSweep.Revision},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(run.TaskID), Value: runValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupRetentionKey(point.SourceID, point.ID),
			Value: sweepValue,
		},
	})
	if err != nil || !changed.Succeeded {
		t.Fatalf("seed maximum retention progress = %#v, %v", changed, err)
	}
	committedRun = testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{
		Record: changedRun, Revision: changed.Revision, ReadRevision: changed.Revision,
	}
	currentSweep = testkeyvalue.Versioned[testbackupruntime.BackupRetentionSweepRecord]{
		Record: changedSweep, Revision: changed.Revision, ReadRevision: changed.Revision,
	}
	advanced, prunes, err := repository.AdvanceBackupRetentionSweep(
		context.Background(), committedRun, currentSweep, changedSweep.UpdatedAt.Add(time.Second),
	)
	if err != nil || advanced.Record.State != testbackupruntime.BackupRetentionCompleted || len(prunes) != 3 ||
		advanced.Record.RetainedCount != maximumKeep {
		t.Fatalf("AdvanceBackupRetentionSweep(int64 Keep) = %#v/%#v/%v", advanced, prunes, err)
	}
}
