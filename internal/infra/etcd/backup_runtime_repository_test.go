package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackupconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	testing "testing"
	time "time"
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

// Rationale: Connector deletion fences must retain the complete stable point identity in their key suffix.
func TestBackupRecoveryPointConnectorIndexUsesRawStablePointIdentity(t *testing.T) {
	t.Parallel()
	first, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(testBackupConnectorID, testBackupPointID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(testBackupConnectorID, testBackupPointIDTwo)
	if err != nil {
		t.Fatal(err)
	}
	prefix := testbackupruntime.BackupRecoveryPointConnectorPrefix + testBackupConnectorID + "/"
	if first != prefix+testBackupPointID || second != prefix+testBackupPointIDTwo {
		t.Fatalf("Connector point keys first=%q second=%q", first, second)
	}
}

// Rationale: retention may use an owned Environment lock only after its sweep,
// trigger, run snapshot, and every selected point prove one Environment/source.
func TestBackupRuntimeRepositoryRejectsCrossRunRetentionAuthority(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	committed, newest, sweep := backupRuntimePointCommitRecords(run, staged)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
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
	currentSweep, found, err := repository.GetBackupRetentionSweep(
		context.Background(), newest.SourceID, newest.ID,
	)
	if err != nil || !found {
		t.Fatalf("GetBackupRetentionSweep() = %#v/%v/%v", currentSweep, found, err)
	}
	wrongRun := committedRun
	wrongRun.Record.PolicyRevision++
	if _, _, err := repository.AdvanceBackupRetentionSweep(
		context.Background(),
		wrongRun,
		currentSweep,
		sweep.UpdatedAt.Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("AdvanceBackupRetentionSweep(cross run) error = %v", err)
	}
	wrongSweep := currentSweep
	wrongSweep.Record.TriggerRecoveryPointID = ids.NewAt(
		ids.KindRecoveryPoint,
		run.CreatedAt.Add(time.Millisecond),
		499,
	)
	if _, _, err := repository.AdvanceBackupRetentionSweep(
		context.Background(),
		committedRun,
		wrongSweep,
		sweep.UpdatedAt.Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("AdvanceBackupRetentionSweep(cross trigger) error = %v", err)
	}
	wrongSweep = currentSweep
	wrongSweep.Record.Keep++
	if _, _, err := repository.AdvanceBackupRetentionSweep(
		context.Background(),
		committedRun,
		wrongSweep,
		sweep.UpdatedAt.Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("AdvanceBackupRetentionSweep(changed Keep) error = %v", err)
	}
	otherAt := run.CreatedAt.Add(-time.Second)
	otherPoint := newest
	otherPoint.ID = ids.NewAt(ids.KindRecoveryPoint, otherAt, 500)
	otherPoint.EnvironmentID = testBackupBackingEnvironmentID
	otherPoint.CreatedAt = otherAt
	otherPoint.VerifiedAt = otherAt.Add(time.Millisecond)
	otherPoint.ObjectKey = run.ConnectorPrefix + otherPoint.EnvironmentID + "/" +
		otherPoint.SourceID + "/" + otherPoint.ID + "/artifact.bin"
	otherValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(otherPoint)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(otherValue)
	otherSourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(
		otherPoint.SourceID,
		otherPoint.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testbackupruntime.BackupRecoveryPointKey(otherPoint.ID),
			Value: otherValue,
		},
		{Type: testkeyvalue.MutationPut, Key: otherSourceIndex, Value: []byte(otherPoint.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed cross-Environment point = %#v, %v", seeded, err)
	}
	if _, _, err := repository.AdvanceBackupRetentionSweep(
		context.Background(),
		committedRun,
		currentSweep,
		sweep.UpdatedAt.Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AdvanceBackupRetentionSweep(cross Environment point) error = %v", err)
	}
	storedSweep := mustOptionalKey(
		t,
		store,
		testbackupruntime.BackupRetentionKey(currentSweep.Record.SourceID, currentSweep.Record.TriggerRecoveryPointID),
	)
	if storedSweep == nil || storedSweep.ModRevision != currentSweep.Revision ||
		mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(otherPoint.ID)) != nil {
		t.Fatalf("cross-Environment retention wrote state: sweep=%#v", storedSweep)
	}
}

// Rationale: an orphan state change must preserve both reverse authorities at
// the primary's new revision so exact deletion cannot observe split evidence.
func TestBackupRuntimeRepositoryTransitionsAndDeletesOrphanCompanionsAtomically(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion, orphaned, 0, orphan,
	)
	if err != nil {
		t.Fatalf("CreateBackupOrphan() error = %v", err)
	}
	storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
	}
	page, err := repository.ListBackupOrphansByEnvironment(
		context.Background(), run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Point.ID != point.ID {
		t.Fatalf("ListBackupOrphansByEnvironment() = %#v, %v", page, err)
	}
	deleting := orphan
	deleting.State = testbackupruntime.BackupOrphanDelete
	deleting.UpdatedAt = orphan.UpdatedAt.Add(time.Second)
	malformedRun := orphanedVersion
	malformedRun.Record.UpdatedAt = malformedRun.Record.UpdatedAt.Add(time.Nanosecond)
	if _, err := repository.TransitionBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		malformedRun, storedOrphan, deleting,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("TransitionBackupOrphan(malformed run evidence) error = %v", err)
	}
	unknown := &backupRuntimeUnknownOutcomeStore{hierarchyStore: store, failNext: true}
	unknownRepository, err := newBackupRuntimeRepository(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unknownRepository.TransitionBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		orphanedVersion, storedOrphan, deleting,
	); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("TransitionBackupOrphan(unknown outcome) error = %v", err)
	}
	transitioned, err := repository.TransitionBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		orphanedVersion, storedOrphan, deleting,
	)
	if err != nil {
		t.Fatalf("TransitionBackupOrphan(replay) = %#v, %v", transitioned, err)
	}
	alternate := deleting
	alternate.UpdatedAt = alternate.UpdatedAt.Add(time.Second)
	if _, err := repository.TransitionBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		orphanedVersion, storedOrphan, alternate,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("TransitionBackupOrphan(alternate valid caller) error = %v", err)
	}
	connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{testbackupruntime.BackupOrphanKey(point.ID), connectorIndex, environmentIndex} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != transitioned.Revision {
			t.Fatalf(
				"orphan companion %q = %#v, want revision %d",
				key,
				entry,
				transitioned.Revision,
			)
		}
	}
	failed := orphaned
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = testbackupruntime.BackupFailurePointCommit
	failed.UpdatedAt = deleting.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupOrphanAbsentTerminal(
		context.Background(), backupRemoteAbsentCheckpoint(checkpoint, 1, point.ID),
		orphanedVersion, failed, 0, transitioned,
	)
	if err != nil {
		t.Fatalf("prepareBackupOrphanAbsentTerminal() error = %v", err)
	}
	marker := "/v1/test/backup-orphan-absent-terminal/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: marker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		plan.clear()
		t.Fatal(err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	plan.clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("terminalize absent orphan = %#v, %v", result, err)
	}
	for _, key := range []string{testbackupruntime.BackupOrphanKey(point.ID), connectorIndex, environmentIndex} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("deleted orphan companion %q = %#v", key, entry)
		}
	}
}

// Rationale: an orphan is visible only while its exact Connector deletion
// authority remains paired with the primary and Environment membership.
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
				wrongKey, err := testbackupruntime.BackupOrphanConnectorIndexKey(otherConnector, orphan.Point.ID)
				if err != nil {
					t.Fatal(err)
				}
				return []testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationDelete, Key: key},
					{Type: testkeyvalue.MutationPut, Key: wrongKey, Value: []byte(orphan.Point.ID)},
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
				orphan.Point.ConnectorID,
				orphan.Point.ID,
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
				context.Background(), orphan.Point.ID,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("GetBackupOrphan() error = %v", err)
			}
			if _, err := repository.ListBackupOrphansByEnvironment(
				context.Background(),
				orphan.Point.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("ListBackupOrphansByEnvironment() error = %v", err)
			}
		})
	}
}

// Rationale: losing either orphan membership between fixed read and compare
// must leave the primary unchanged rather than manufacture a new revision pair.
func TestBackupRuntimeRepositoryRejectsOrphanCompanionLoss(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion, orphaned, 0, orphan,
	)
	if err != nil {
		t.Fatal(err)
	}
	stored, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", stored, found, err)
	}
	connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: connectorIndex, ModRevision: stored.Revision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: connectorIndex}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("delete orphan companion = %#v, %v", result, err)
	}
	next := orphan
	next.State = testbackupruntime.BackupOrphanDelete
	next.UpdatedAt = orphan.UpdatedAt.Add(time.Second)
	if _, err := repository.TransitionBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		orphanedVersion, stored, next,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("TransitionBackupOrphan(companion missing) error = %v", err)
	}
}

// Rationale: a later successful HEAD verification must atomically replace the
// exact inspect orphan and both memberships with visible Recovery Point authority.
func TestBackupRuntimeRepositoryCommitsOrphanAsRecoveryPointAtomically(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion, orphaned, 0, orphan,
	)
	if err != nil {
		t.Fatalf("CreateBackupOrphan() error = %v", err)
	}
	storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
	}
	mismatchedOrphan := storedOrphan
	mismatchedOrphan.Record.TaskID = ids.NewAt(
		ids.KindTask,
		storedOrphan.Record.CreatedAt.Add(time.Millisecond),
		614,
	)
	committed := orphaned
	committed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	committed.Sources[0].State = testbackupruntime.BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = testbackupruntime.BackupSourcePhaseRetention
	committed.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	point.VerifiedAt = committed.UpdatedAt
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: committed.UpdatedAt, UpdatedAt: committed.UpdatedAt,
	}
	checkpoint.Sequence = 2
	checkpoint.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind: testbackupruntime.BackupCheckpointUploadVerified, PointID: point.ID,
		StoredSizeBytes: uint64(point.SizeBytes), StoredSHA256: point.SHA256,
	}
	if _, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), orphanedVersion, committed, 0, point,
		&mismatchedOrphan, sweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(orphan Task mismatch) error = %v", err)
	}
	mismatchedOrphan = storedOrphan
	mismatchedOrphan.Record.Reconciliation.RetentionKeep++
	if _, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), orphanedVersion, committed, 0, point,
		&mismatchedOrphan, sweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(orphan authority mismatch) error = %v", err)
	}
	storedPoint, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), orphanedVersion, committed, 0, point,
		&storedOrphan, sweep,
	)
	if err != nil {
		t.Fatalf("CommitBackupRecoveryPoint(orphan) error = %v", err)
	}
	connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{testbackupruntime.BackupOrphanKey(point.ID), connectorIndex, environmentIndex} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("committed orphan authority %q = %#v", key, entry)
		}
	}
	if entry := mustOptionalKey(
		t,
		store, testbackupruntime.BackupRecoveryPointKey(point.ID),
	); entry == nil ||
		entry.ModRevision != storedPoint.Revision {
		t.Fatalf("committed point primary = %#v", entry)
	}
}

// Rationale: immutable point visibility requires every reverse index from the
// original commit revision; an identical-byte index rewrite is corruption.
func TestBackupRuntimeRepositoryRejectsRewrittenRecoveryPointIndex(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	committed, point, sweep := backupRuntimePointCommitRecords(run, staged)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	storedPoint, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		committed,
		0,
		point,
		nil,
		sweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, connectorIndex)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: connectorIndex, ModRevision: storedPoint.Revision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("rewrite point index = %#v, %v", result, err)
	}
	if _, err := repository.GetBackupRecoveryPoint(
		context.Background(), point.ID,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("GetBackupRecoveryPoint(rewritten index) error = %v", err)
	}
}

// Rationale: verified point visibility must compare the exact Connector and
// encrypted credential revisions captured by the immutable run snapshot.
func TestBackupRuntimeRepositoryPointCommitRejectsConnectorRevisionRace(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	committed, point, sweep := backupRuntimePointCommitRecords(run, staged)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	racing := &entryVolumeEpochRaceStore{hierarchyStore: store}
	racing.beforeTransact = func() {
		key := testconnectors.RecordKey(run.ConnectorID)
		entry := mustOptionalKey(t, store, key)
		result, mutateErr := store.Transact(
			context.Background(),
			[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
			[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}},
		)
		if mutateErr != nil || !result.Succeeded {
			t.Fatalf("advance connector revision = %#v, %v", result, mutateErr)
		}
	}
	racingRepository, err := newBackupRuntimeRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := racingRepository.CommitBackupRecoveryPoint(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		committed,
		0,
		point,
		nil,
		sweep,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(connector race) error = %v", err)
	}
	if entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointKey(point.ID)); entry != nil {
		t.Fatalf("racing point primary = %#v", entry)
	}
}

// Rationale: a changed attempt and the supplied ordinal are one identity; a
// caller cannot commit another source's otherwise-valid point.
func TestBackupRuntimeRepositoryRejectsMismatchedChangedSourceOrdinal(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	run.Sources = append(
		run.Sources,
		testBackupLaterSource(run.CreatedAt, 1, testbackupruntime.BackupSourceAttemptPending),
	)
	run.Sources[1].Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
	run.Sources[1].ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + run.Sources[1].SourceID + "/" +
		run.Sources[1].RecoveryPointID + "/artifact.bin"
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	current := run
	current.State = testbackupruntime.BackupRunRunning
	current.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	current.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	current.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	current.Sources[0].SizeBytes = 123
	current.Sources[0].SHA256 = testBackupDigest
	current.Sources[1].State = testbackupruntime.BackupSourceAttemptStaged
	current.Sources[1].Phase = testbackupruntime.BackupSourcePhaseUpload
	current.Sources[1].SizeBytes = 123
	current.Sources[1].SHA256 = testBackupDigest
	current.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, current)
	if err != nil {
		t.Fatal(err)
	}
	next := current
	next.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), current.Sources...)
	next.Sources[1].State = testbackupruntime.BackupSourceAttemptPointCommitted
	next.Sources[1].Phase = testbackupruntime.BackupSourcePhaseRetention
	next.UpdatedAt = current.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, current.Sources[0], next.UpdatedAt)
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: next.UpdatedAt, UpdatedAt: next.UpdatedAt,
	}
	if _, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), testbackupruntime.BackupAssignmentInput{}, created, next, 0, point, nil, sweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(mismatched ordinal) error = %v", err)
	}
	orphaned := current
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), current.Sources...)
	orphaned.Sources[1].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[1].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = current.UpdatedAt.Add(time.Second)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	if _, err := repository.CreateBackupOrphan(
		context.Background(), testbackupruntime.BackupAssignmentInput{}, created, orphaned, 0, orphan,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CreateBackupOrphan(mismatched ordinal) error = %v", err)
	}
	failed := orphaned
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	failed.Sources[1].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[1].FailureCode = testbackupruntime.BackupFailurePointCommit
	failed.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	orphan.State = testbackupruntime.BackupOrphanDelete
	if _, err := repository.prepareBackupOrphanAbsentTerminal(
		context.Background(), testbackupruntime.BackupCheckpointInput{}, testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{
			Record: orphaned, Revision: created.Revision, ReadRevision: created.ReadRevision,
		}, failed,
		0, testkeyvalue.Versioned[testbackupruntime.BackupOrphanRecord]{Record: orphan, Revision: 1, ReadRevision: 1},
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareBackupOrphanAbsentTerminal(mismatched ordinal) error = %v", err)
	}
}

// Rationale: an unknown transaction outcome must replay the exact run,
// membership, and complete exclusion set instead of publishing twice.
func TestBackupRuntimeRepositoryCreateRunUnknownOutcomeReplay(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	unknown := &backupRuntimeUnknownOutcomeStore{hierarchyStore: store, failNext: true}
	plan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	idempotencyPlan, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	replayPlan, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(
		context.Background(), marker, idempotencyPlan,
	); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("Apply(unknown outcome) error = %v", err)
	}
	policyKey := testbackuppolicy.BackupPolicyKey(run.EnvironmentID)
	policyEntry := mustOptionalKey(t, store, policyKey)
	policy, err := testbackuppolicy.DecodeBackupPolicyRecord(policyEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	policy.Keep++
	policyValue, err := testbackuppolicy.EncodeBackupPolicyRecord(policy)
	if err != nil {
		t.Fatal(err)
	}
	policyChanged, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: policyKey, ModRevision: policyEntry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: policyKey, Value: policyValue}},
	)
	clear(policyValue)
	if err != nil || !policyChanged.Succeeded {
		t.Fatalf("change current policy Keep = %#v, %v", policyChanged, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerEntry := mustOptionalKey(t, store, markerKey)
	for _, key := range []string{testtaskjournal.TaskStorageKey(run.TaskID), testbackupruntime.BackupRunKey(run.TaskID), testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || markerEntry == nil || entry.ModRevision != markerEntry.ModRevision {
			t.Fatalf("unknown-outcome authority %q = %#v, marker = %#v", key, entry, markerEntry)
		}
	}
	reader, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reader.Apply(context.Background(), marker, replayPlan)
	if err != nil {
		t.Fatalf("Apply(replay after policy Keep change) error = %v", err)
	}
	outcome, _, conflict, err := replayed.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("replay after policy Keep change = %v/%v/%v", outcome, conflict, err)
	}
	storedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || storedRun.Record.RetentionKeep != run.RetentionKeep {
		t.Fatalf("captured run retention after policy change = %#v, %v", storedRun, err)
	}
	evidence, err := reader.Read(context.Background(), marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	storedMarker, err := evidence.Marker()
	if err != nil || storedMarker.TaskID != run.TaskID {
		t.Fatalf("replayed marker = %#v, %v", storedMarker, err)
	}
}

// Rationale: a generic idempotency marker cannot make a Config run replay
// successful after its immutable cursor or either direction of its reference
// pair is missing or rewritten at another revision.
func TestBackupRuntimeRepositoryConfigReplayRequiresExactCompanions(t *testing.T) {
	checks := []struct {
		name   string
		key    func(testbackupruntime.BackupRunRecord) string
		remove bool
	}{
		{
			name: "missing immutable reference",
			key: func(run testbackupruntime.BackupRunRecord) string {
				return testbackupconfiguration.BackupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID)
			},
			remove: true,
		},
		{
			name: "rewritten cursor",
			key: func(run testbackupruntime.BackupRunRecord) string {
				return testbackupconfiguration.BackupConfigSnapshotKey(run.TaskID)
			},
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
			fixedRevision := configureBackupRuntimeConfigRun(t, store, &run)
			first, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run), fixedRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer first.clear()
			second, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run), fixedRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer second.clear()
			task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
			firstPlan, err := first.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			secondPlan, err := second.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			unknown := &backupRuntimeUnknownOutcomeStore{hierarchyStore: store, failNext: true}
			unknownIdempotency, err := NewIdempotencyRepository(unknown)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := unknownIdempotency.Apply(
				context.Background(), marker, firstPlan,
			); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("Apply(Config unknown outcome) error = %v", err)
			}
			key := check.key(run)
			entry := mustOptionalKey(t, store, key)
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: entry.Value}
			if check.remove {
				mutation = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]testkeyvalue.Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("change Config companion = %#v, %v", changed, err)
			}
			idempotency, err := NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := idempotency.Apply(
				context.Background(), marker, secondPlan,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Apply(Config corrupt replay) error = %v", err)
			}
		})
	}
}

// Rationale: unknown-outcome replay must reject a membership rewritten after
// publication even when its raw value still names the same run.
func TestBackupRuntimeRepositoryCreateRunReplayRejectsChangedMembershipRevision(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	if _, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	); err != nil {
		t.Fatalf("createBackupRunForTest() error = %v", err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, membership)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: membership, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: membership, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("rewrite run membership = %#v, %v", result, err)
	}
	if _, err := repository.GetBackupRun(
		context.Background(), run.TaskID,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("GetBackupRun(changed membership) error = %v", err)
	}
}

// Rationale: cursors are meaningful only with the fixed revision whose
// snapshot they continue.
func TestBackupRuntimeRepositoryRejectsCursorWithoutRevision(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeRepositoryFixture(t)
	_, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{
			Limit:          1,
			StartExclusive: testbackupruntime.BackupRunEnvironmentPrefix + run.EnvironmentID + "/" + run.TaskID,
		},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ListBackupRunsByEnvironment(cursor without revision) error = %v", err)
	}
}

// Rationale: the full twelve-source contract must remain composable after
// primary-owned derived membership removes redundant absence compares.
func TestBackupRuntimeRepositoryPreparesTwelveSourcePublicationWithinBounds(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeBareFixture(t)
	for ordinal := uint32(1); ordinal < testbackuppolicy.MaximumBackupPolicySources; ordinal++ {
		source := testBackupLaterSource(run.CreatedAt, ordinal, testbackupruntime.BackupSourceAttemptPending)
		source.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		run.Sources = append(run.Sources, source)
	}
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		lock,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("prepareBackupRunPublication(12 sources) error = %v", err)
	}
	defer plan.clear()
	if len(plan.conditions)+len(plan.mutations) > testkeyvalue.MaximumOperations {
		t.Fatalf(
			"12-source publication operations = %d, want <= %d",
			len(plan.conditions)+len(plan.mutations), testkeyvalue.MaximumOperations,
		)
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil || len(exclusions) != testbackuppolicy.MaximumBackupPolicySources {
		t.Fatalf("12-source exclusions = %d, %v", len(exclusions), err)
	}
}

// Rationale: exclusion cleanup is an all-or-none authority transition; a
// partial set cannot be accepted as replay or opportunistically repaired.
func TestBackupRuntimeRepositoryRejectsMixedExclusionRelease(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	second := testBackupLaterSource(run.CreatedAt, 1, testbackupruntime.BackupSourceAttemptPending)
	second.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
	second.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + second.SourceID + "/" +
		second.RecoveryPointID + "/artifact.bin"
	run.Sources = append(run.Sources, second)
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	created, err := repository.createBackupRunForTest(
		context.Background(),
		run,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	failed := run
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = testbackupruntime.BackupFailureCapture
	failed.Sources[1].State = testbackupruntime.BackupSourceAttemptUnstarted
	failed.Sources[1].Phase = testbackupruntime.BackupSourcePhaseCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	firstKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, firstKey)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: firstKey, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: firstKey}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("delete one exclusion = %#v, %v", result, err)
	}
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), created, failed,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(mixed) error = %v", err)
	}
	secondKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		second.TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if entry := mustOptionalKey(t, store, secondKey); entry == nil {
		t.Fatal("mixed exclusion release removed the surviving authority")
	}
}

// Rationale: terminal Task evidence, run state, exclusion release, lock release,
// and epoch advance must share one bounded transaction revision.
func TestBackupRuntimeRepositoryComposesTerminalRunAndAuthorityRelease(t *testing.T) {
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
	failed := run
	failed.State = testbackupruntime.BackupRunFailed
	failed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = testbackupruntime.BackupFailureCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupRunTerminal(context.Background(), created, failed)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminal() error = %v", err)
	}
	defer plan.clear()
	marker := "/v1/test/backup-terminal-tasks/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: marker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose terminal backup run = %v", err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("terminal backup transaction = %#v, %v", result, err)
	}
	exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
		testbackupruntime.BackupSourceTargetAttach,
		run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{exclusionKey, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), marker, testhierarchy.EnvironmentMutationEpochKey(run.EnvironmentID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != result.Revision {
			t.Fatalf("terminal companion %q = %#v, want revision %d", key, entry, result.Revision)
		}
	}
}

// Rationale: successful Task completion cannot claim cleanup that was not
// acknowledged by the exact Agent assignment and checkpoint sequence.
func TestBackupRuntimeRepositoryRequiresCleanupCheckpointBeforeSuccess(t *testing.T) {
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
	cleanupPending := run
	cleanupPending.State = testbackupruntime.BackupRunRunning
	cleanupPending.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	cleanupPending.Sources[0].State = testbackupruntime.BackupSourceAttemptCleanupPending
	cleanupPending.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	cleanupPending.Sources[0].SizeBytes = 123
	cleanupPending.Sources[0].SHA256 = testBackupDigest
	cleanupPending.UpdatedAt = run.UpdatedAt.Add(time.Second)
	cleanupVersion, err := repository.replaceBackupRunForTest(
		context.Background(), created, cleanupPending,
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := cleanupPending
	completed.State = testbackupruntime.BackupRunCompleted
	completed.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	completed.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	completed.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), cleanupVersion, completed,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareBackupRunTerminal(uncheckpointed cleanup) error = %v", err)
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = testbackupruntime.BackupCheckpointPayload{
		Kind:    testbackupruntime.BackupCheckpointSourceCleanupCompleted,
		PointID: cleanupPending.Sources[0].RecoveryPointID,
	}
	succeeded := cleanupPending
	succeeded.Sources = append(
		[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	succeeded.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	succeeded.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	succeededVersion, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, cleanupVersion, succeeded,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(cleanup) error = %v", err)
	}
	completed = succeeded
	completed.State = testbackupruntime.BackupRunCompleted
	completed.UpdatedAt = succeeded.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupRunTerminal(
		context.Background(), succeededVersion, completed,
	)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminal(checkpointed cleanup) error = %v", err)
	}
	plan.clear()
}

// Rationale: Task failure or abort after artifact preparation must publish an
// upload-intent orphan before releasing source exclusions and the owned lock.
func TestBackupRuntimeRepositoryTerminalizesUploadIntentWithOrphan(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		state   testbackupruntime.BackupRunState
		failure testbackupruntime.BackupFailureCode
		phase   testbackupruntime.BackupSourceAttemptPhase
	}{
		{
			name: "failed upload", state: testbackupruntime.BackupRunFailed, failure: testbackupruntime.BackupFailureUpload,
			phase: testbackupruntime.BackupSourcePhaseUpload,
		},
		{
			name: "aborted head verification", state: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
			phase: testbackupruntime.BackupSourcePhaseHeadVerification,
		},
		{
			name: "timed out point commit", state: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut,
			phase: testbackupruntime.BackupSourcePhasePointCommit,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
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
			staged.Sources[0].Phase = test.phase
			staged.Sources[0].SizeBytes = 123
			staged.Sources[0].SHA256 = testBackupDigest
			staged.UpdatedAt = run.UpdatedAt.Add(time.Second)
			stagedVersion, err := repository.replaceBackupRunForTest(
				context.Background(), created, staged,
			)
			if err != nil {
				t.Fatal(err)
			}
			withoutOrphan := staged
			withoutOrphan.State = test.state
			withoutOrphan.Sources = append(
				[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
				staged.Sources...,
			)
			withoutOrphan.Sources[0].State = testbackupruntime.BackupSourceAttemptFailed
			withoutOrphan.Sources[0].FailureCode = test.failure
			withoutOrphan.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			if _, err := repository.prepareBackupRunTerminal(
				context.Background(), stagedVersion, withoutOrphan,
			); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("prepareBackupRunTerminal(without orphan) error = %v", err)
			}
			terminal := withoutOrphan
			terminal.Sources = append(
				[]testbackupruntime.BackupRunSourceAttemptRecord(nil),
				withoutOrphan.Sources...,
			)
			terminal.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
			for _, mutate := range []struct {
				name string
				run  func(*testbackupruntime.BackupRunSourceAttemptRecord)
			}{
				{name: "empty", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SizeBytes = 0
					source.SHA256 = ""
				}},
				{name: "malformed", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SHA256 = "not-a-sha256"
				}},
				{name: "substituted", run: func(source *testbackupruntime.BackupRunSourceAttemptRecord) {
					source.SizeBytes++
				}},
			} {
				changed := terminal
				changed.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), terminal.Sources...)
				mutate.run(&changed.Sources[0])
				if _, err := repository.prepareBackupRunTerminal(
					context.Background(), stagedVersion, changed,
				); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("prepareBackupRunTerminal(%s artifact evidence) error = %v", mutate.name, err)
				}
			}
			plan, err := repository.prepareBackupRunTerminal(
				context.Background(), stagedVersion, terminal,
			)
			if err != nil {
				t.Fatalf("prepareBackupRunTerminal(orphan) error = %v", err)
			}
			marker := "/v1/test/backup-upload-terminal/" + run.TaskID
			conditions, mutations, err := plan.composeTransaction(
				[]testkeyvalue.Condition{{Key: marker}},
				[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
			testkeyvalue.ClearMutationValues(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize upload intent = %#v, %v", result, err)
			}
			pointID := staged.Sources[0].RecoveryPointID
			connectorIndex, err := testbackupruntime.BackupOrphanConnectorIndexKey(run.ConnectorID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			environmentIndex, err := testbackupruntime.BackupOrphanEnvironmentIndexKey(run.EnvironmentID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{testbackupruntime.BackupOrphanKey(pointID), connectorIndex, environmentIndex, marker} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != result.Revision {
					t.Fatalf("terminal orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("terminal upload intent retained its Environment lock")
			}
		})
	}
}

// Rationale: an already-persisted orphan is retained as exact cleanup
// authority while failure, abort, or timeout terminalizes the owning Task and
// releases the run lock and exclusions in the same transaction.
func TestBackupRuntimeRepositoryTerminalRetainsExactExistingOrphan(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		state       testbackupruntime.BackupRunState
		failureCode testbackupruntime.BackupFailureCode
	}{
		{name: "failed", state: testbackupruntime.BackupRunFailed, failureCode: testbackupruntime.BackupFailurePointCommit},
		{name: "aborted", state: testbackupruntime.BackupRunAborted, failureCode: testbackupruntime.BackupFailureAborted},
		{name: "timed out", state: testbackupruntime.BackupRunTimedOut, failureCode: testbackupruntime.BackupFailureTimedOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			orphaned := staged
			orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
			orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
			orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
			orphan := testbackupruntime.BackupOrphanRecord{
				Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
				CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
			}
			checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
			orphanedVersion, err := repository.CreateBackupOrphan(
				context.Background(),
				backupAssignmentFromCheckpoint(checkpoint),
				stagedVersion,
				orphaned,
				0,
				orphan,
			)
			if err != nil {
				t.Fatal(err)
			}
			storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
			if err != nil || !found {
				t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
			}
			terminal := orphaned
			terminal.State = test.state
			terminal.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
			terminal.Sources[0].FailureCode = test.failureCode
			terminal.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
			plan, err := repository.prepareBackupRunTerminal(
				context.Background(), orphanedVersion, terminal,
			)
			if err != nil {
				t.Fatalf("prepareBackupRunTerminal() error = %v", err)
			}
			marker := "/v1/test/backup-retained-orphan-terminal/" + test.name + "/" + run.TaskID
			conditions, mutations, err := plan.composeTransaction(
				[]testkeyvalue.Condition{{Key: marker}},
				[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
			testkeyvalue.ClearMutationValues(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize retained orphan = %#v, %v", result, err)
			}
			connectorIndex, _ := testbackupruntime.BackupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
			environmentIndex, _ := testbackupruntime.BackupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
			for _, key := range []string{testbackupruntime.BackupOrphanKey(point.ID), connectorIndex, environmentIndex} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != storedOrphan.Revision || entry.Version != 1 {
					t.Fatalf("retained orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("retained orphan terminal kept its Environment lock")
			}
			deleting := storedOrphan.Record
			deleting.State = testbackupruntime.BackupOrphanDelete
			deleting.UpdatedAt = deleting.UpdatedAt.Add(time.Second)
			transitioned, err := repository.TransitionReconciledBackupOrphan(
				context.Background(), storedOrphan, deleting,
			)
			if err != nil {
				t.Fatalf("TransitionReconciledBackupOrphan(terminal Task) error = %v", err)
			}
			if err := repository.DeleteReconciledBackupOrphan(
				context.Background(), transitioned,
			); err != nil {
				t.Fatalf("DeleteReconciledBackupOrphan(terminal Task) error = %v", err)
			}
		})
	}
}

// Rationale: Controller orphan reconciliation is durable authority in its own
// right, so generic Task/run pruning cannot prevent a verified artifact adoption.
func TestBackupRuntimeRepositoryAdoptsOrphanAfterOriginTaskPruned(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion, orphaned, 0, orphan,
	)
	if err != nil {
		t.Fatal(err)
	}
	storedOrphan, found, err := repository.GetBackupOrphan(context.Background(), point.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupOrphan() = %#v/%v/%v", storedOrphan, found, err)
	}
	pruned, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRunKey(run.TaskID), ModRevision: orphanedVersion.Revision},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupRunKey(run.TaskID)},
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskStorageKey(run.TaskID)},
	})
	if err != nil || !pruned.Succeeded {
		t.Fatalf("prune originating Task/run = %#v, %v", pruned, err)
	}
	point.VerifiedAt = storedOrphan.Record.UpdatedAt.Add(time.Second)
	sweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:     storedOrphan.Record.Reconciliation.RetentionKeep,
		Revision: storedOrphan.Record.Reconciliation.PolicyRevision,
		State:    testbackupruntime.BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	adopted, err := repository.AdoptReconciledBackupOrphan(
		context.Background(), storedOrphan, point, sweep,
	)
	if err != nil {
		t.Fatalf("AdoptReconciledBackupOrphan(pruned Task) error = %v", err)
	}
	if adopted.Record != point || mustOptionalKey(t, store, testbackupruntime.BackupOrphanKey(point.ID)) != nil {
		t.Fatalf("adopted orphan = %#v", adopted)
	}
	storedSweep, found, err := repository.GetBackupRetentionSweep(
		context.Background(), point.SourceID, point.ID,
	)
	if err != nil || !found || storedSweep.Record != sweep || storedSweep.Revision != adopted.Revision {
		t.Fatalf("adopted retention sweep = %#v/%v/%v", storedSweep, found, err)
	}
}

// Rationale: rewriting one existing orphan companion with identical bytes is
// corruption, not a new valid authority revision, and cannot terminalize the
// owning run.
func TestBackupRuntimeRepositoryRejectsRewrittenRetainedOrphanCompanion(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	orphanedVersion, err := repository.CreateBackupOrphan(
		context.Background(),
		backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion,
		orphaned,
		0,
		orphan,
	)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, _ := testbackupruntime.BackupOrphanEnvironmentIndexKey(run.EnvironmentID, point.ID)
	entry := mustOptionalKey(t, store, environmentIndex)
	rewritten, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: environmentIndex, ModRevision: entry.ModRevision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: entry.Value}},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite orphan companion = %#v, %v", rewritten, err)
	}
	terminal := orphaned
	terminal.State = testbackupruntime.BackupRunFailed
	terminal.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	terminal.Sources[0].FailureCode = testbackupruntime.BackupFailurePointCommit
	terminal.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), orphanedVersion, terminal,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(rewritten orphan) error = %v", err)
	}
	if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) == nil {
		t.Fatal("rewritten orphan companion released the Environment lock")
	}
}

// Rationale: future manual Task publication must be able to commit the Task,
// lock, run membership, and exclusions in one transaction with no standalone
// pre-publication write.
func TestBackupRuntimeRepositoryPreparesComposableRunPublication(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
	plan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		lock,
		backupRuntimeCurrentRevision(t, repository.store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatalf("prepareBackupRunPublication() error = %v", err)
	}
	defer plan.clear()
	taskMarker := "/v1/test/backup-publication-tasks/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]testkeyvalue.Condition{{Key: taskMarker}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: taskMarker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose backup publication = %v", err)
	}
	result, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("composed backup publication = %#v, %v", result, err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{testbackupruntime.BackupRunKey(run.TaskID), membership, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID), taskMarker} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != result.Revision {
			t.Fatalf("composed publication key %q = %#v", key, entry)
		}
	}
}

// Rationale: terminal Backup state, the exact assigned Task terminal records,
// exclusion and lock release, and the final 768-KiB envelope are composed and
// committed at one revision.
func TestBackupRuntimeRepositoryComposesRealAssignedTaskTerminal(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(),
		run,
		backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	genericTask := task
	genericTask.Params = map[string]string{"source": run.Sources[0].SourceID}
	if _, err := runPlan.taskIdempotencyPlan(
		genericTask, sealed, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(generic Backup Params) error = %v", err)
	}
	fabricatedRun := run
	fabricatedRun.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), run.Sources...)
	fabricatedRun.Sources[0].SourceRevision++
	fabricated := backupRuntimeSealedRunPlan(t, fabricatedRun, task.PlanID)
	fabricatedTask := task
	fabricatedTask.PlanHash = hex.EncodeToString(fabricated.PlanHash)
	if _, err := runPlan.taskIdempotencyPlan(
		fabricatedTask, fabricated, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(fabricated Backup source) error = %v", err)
	}
	idempotencyPlan, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idempotency.Apply(context.Background(), marker, idempotencyPlan)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 990)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	terminal, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		run.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(Backup failure) error = %v", err)
	}
	committedRevision := terminal.Revision
	for _, key := range []string{testtaskjournal.TaskStorageKey(run.TaskID), testbackupruntime.BackupRunKey(run.TaskID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != committedRevision {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	failedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || failedRun.Record.State != testbackupruntime.BackupRunFailed ||
		failedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptFailed ||
		failedRun.Record.Sources[0].FailureCode != testbackupruntime.BackupFailureCapture {
		t.Fatalf("terminal Backup run = %#v, %v", failedRun, err)
	}
	if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
		t.Fatal("terminal Backup Task retained its Environment lock")
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay) = %#v, %v", replay, err)
	}
	newerAt := run.CreatedAt.Add(4 * time.Second)
	putBackupRuntimeLock(t, store, testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   ids.NewAt(ids.KindOperation, newerAt, 1991),
		TaskID:        ids.NewAt(ids.KindTask, newerAt, 1992),
		Kind:          testbackupruntime.BackupOperationRestore,
		CreatedAt:     newerAt,
		UpdatedAt:     newerAt,
	})
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay after successor lock) = %#v, %v", replay, err)
	}
	membership, err := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tornMembership, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: membership}},
	)
	if err != nil || !tornMembership.Succeeded {
		t.Fatalf("remove Backup run membership = %#v, %v", tornMembership, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with torn membership) error = %v", err)
	}
	incompleteCascade, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentKey(run.EnvironmentID)},
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)},
			{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupRunKey(run.TaskID)},
		},
	)
	if err != nil || !incompleteCascade.Succeeded {
		t.Fatalf("simulate incomplete Environment owner cascade = %#v, %v", incompleteCascade, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with retained epoch) error = %v", err)
	}
	cascade, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationDelete, Key: testhierarchy.EnvironmentMutationEpochKey(run.EnvironmentID)},
		},
	)
	if err != nil || !cascade.Succeeded {
		t.Fatalf("complete Environment owner cascade = %#v, %v", cascade, err)
	}
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		newerAt.Add(4*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay after Environment cascade) = %#v, %v", replay, err)
	}
}

// Rationale: a public completed acknowledgement must atomically terminalize the
// Backup Task and its captured run rather than accepting a Task-only outcome.
func TestBackupRuntimeRepositoryRoutesCompletedAcknowledgement(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		context.Background(), run.TaskID, testtaskjournal.TaskStatusCompleted, run.CreatedAt.Add(time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("AcknowledgeControllerTask(Backup) error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 1993)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	current, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	running := current.Record
	running.State = testbackupruntime.BackupRunRunning
	running.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), current.Record.Sources...)
	running.Sources[0].State = testbackupruntime.BackupSourceAttemptSucceeded
	running.Sources[0].Phase = testbackupruntime.BackupSourcePhaseCleanup
	running.Sources[0].SizeBytes = 123
	running.Sources[0].SHA256 = testBackupDigest
	running.UpdatedAt = run.CreatedAt.Add(2 * time.Second)
	value, err := testbackupruntime.EncodeBackupRunRecord(running)
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRunKey(run.TaskID), ModRevision: current.Revision}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(run.TaskID), Value: value},
		},
	)
	clear(value)
	if err != nil || !replaced.Succeeded {
		t.Fatalf("seed completed source state = %#v, %v", replaced, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		run.TaskID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		completedComposeTaskResult(),
		run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(Backup completed) = %#v, %v", terminal, err)
	}
	completed, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || completed.Record.State != testbackupruntime.BackupRunCompleted ||
		completed.Revision != terminal.Revision {
		t.Fatalf("completed Backup run = %#v, %v", completed, err)
	}
}

// Rationale: individually valid domain records must not authorize terminalizing
// a Task whose operation, owner, target, creation, or retry identity differs.
func TestBackupTaskTerminalBindingRejectsRewrittenDomainIdentity(t *testing.T) {
	_, store, run := newBackupRuntimeBareFixture(t)
	task, _, _, _ := backupRuntimePublicationTask(t, store, run)
	if err := ValidateBackupRunTaskBinding(task, run); err != nil {
		t.Fatalf("validateBackupRunTaskBinding(valid) error = %v", err)
	}
	rewrittenRun := run
	rewrittenRun.OperationID = ids.NewAt(ids.KindOperation, run.CreatedAt, 1994)
	if err := ValidateBackupRunTaskBinding(task, rewrittenRun); !errors.Is(
		err, errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("validateBackupRunTaskBinding(rewritten) error = %v", err)
	}
	pruneTask := task
	pruneTask.Type = testtaskjournal.TaskBackupPrune
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: task.ID, OperationID: task.OperationID,
		EnvironmentID: run.EnvironmentID, CreatedAt: task.CreatedAt,
		RecoveryPointIDs: []string{run.Sources[0].RecoveryPointID},
	}
	if err := ValidateBackupPruneTaskBinding(pruneTask, dispatch); err != nil {
		t.Fatalf("validateBackupPruneTaskBinding(valid) error = %v", err)
	}
	dispatch.EnvironmentID = ids.NewAt(ids.KindEnvironment, run.CreatedAt, 1995)
	if err := ValidateBackupPruneTaskBinding(pruneTask, dispatch); !errors.Is(
		err, errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("validateBackupPruneTaskBinding(rewritten) error = %v", err)
	}
}

// Rationale: every abort and timeout entry point must use the same atomic Backup
// Task/run/orphan/cleanup/exclusion/lock terminal transaction.
func TestBackupRuntimeRepositoryRoutesAbortAndTimeoutThroughDomainTerminal(t *testing.T) {
	for _, test := range []struct {
		name            string
		pending         bool
		status          testtaskjournal.TaskStatus
		runState        testbackupruntime.BackupRunState
		failure         testbackupruntime.BackupFailureCode
		useTimeout      bool
		useAgentTimeout bool
	}{
		{
			name: "pending abort", pending: true, status: testtaskjournal.TaskStatusAborted,
			runState: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
		},
		{
			name: "running abort", status: testtaskjournal.TaskStatusAborted,
			runState: testbackupruntime.BackupRunAborted, failure: testbackupruntime.BackupFailureAborted,
		},
		{
			name: "running timeout", status: testtaskjournal.TaskStatusTimedOut,
			runState: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut, useTimeout: true,
		},
		{
			name: "stale Agent timeout", status: testtaskjournal.TaskStatusTimedOut,
			runState: testbackupruntime.BackupRunTimedOut, failure: testbackupruntime.BackupFailureTimedOut, useAgentTimeout: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeBareFixture(t)
			runPlan, err := repository.prepareBackupRunPublication(
				context.Background(), run, backupRuntimeOperationLock(run),
				backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer runPlan.clear()
			task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
			publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
			if err != nil {
				t.Fatal(err)
			}
			idempotency, err := NewIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			applied, err := idempotency.Apply(context.Background(), marker, publication)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := applied.Classify()
			if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}

			var terminal testkeyvalue.Versioned[TaskRecord]
			var assignmentID string
			agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 1990)
			if test.pending {
				terminal, err = tasks.AbortPendingTask(
					context.Background(), run.TaskID, run.CreatedAt.Add(time.Second),
				)
			} else {
				claim, found, claimErr := tasks.ClaimNextTask(
					context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
				)
				if claimErr != nil || !found || claim.Task.Record.ID != run.TaskID {
					t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, claimErr)
				}
				assignmentID = claim.Assignment.Record.AssignmentID
				if test.useTimeout || test.useAgentTimeout {
					var count int
					var timeoutErr error
					if test.useAgentTimeout {
						count, timeoutErr = tasks.TimeoutAgentAssignments(
							context.Background(), agentID, 1, 1,
							claim.Assignment.Record.Deadline,
						)
					} else {
						count, timeoutErr = tasks.ExpireTimedOutTasks(
							context.Background(), claim.Assignment.Record.Deadline,
						)
					}
					if timeoutErr != nil || count != 1 {
						t.Fatalf("timeout collector = %d, %v", count, timeoutErr)
					}
					terminal, err = tasks.GetTask(context.Background(), run.TaskID)
				} else {
					terminal, err = tasks.AcknowledgeTask(
						context.Background(), agentID, 1, run.TaskID, assignmentID,
						test.status, testtaskjournal.TaskResultRecord{
							Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
							ReconciliationRequired: true, ExecutionEpoch: 1,
						}, run.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal Backup Task = %#v, %v", terminal, err)
			}
			storedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
			if err != nil || storedRun.Record.State != test.runState ||
				storedRun.Record.Sources[0].State != testbackupruntime.BackupSourceAttemptFailed ||
				storedRun.Record.Sources[0].FailureCode != test.failure ||
				storedRun.Revision != terminal.Revision {
				t.Fatalf("terminal Backup run = %#v, %v", storedRun, err)
			}
			if mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("terminal Backup retained its Environment lock")
			}
			if test.pending {
				replay, replayErr := tasks.AbortPendingTask(
					context.Background(), run.TaskID, run.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AbortPendingTask(replay) = %#v, %v", replay, replayErr)
				}
			} else {
				result := testtaskjournal.TaskResultRecord{
					Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: 1,
				}
				replay, replayErr := tasks.AcknowledgeTask(
					context.Background(), agentID, 1, run.TaskID, assignmentID,
					test.status, result, run.CreatedAt.Add(4*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, replayErr)
				}
			}
		})
	}
}

// Rationale: one Backup terminalization must not prevent a timeout collector
// from processing later expired Tasks in the same bounded scan.
func TestTaskRepositoryTimeoutCollectorContinuesAfterBackupTerminal(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	backupTask, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(backupTask, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := applied.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish Backup Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := validTaskRecord(run.CreatedAt.Add(time.Second))
	ordinary.TimeoutSeconds = backupTaskTimeoutSeconds + 10
	createLifecycleTask(t, tasks, ordinary)
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 2990)
	backupClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(2*time.Second),
	)
	if err != nil || !found || backupClaim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask(Backup) = %#v/%v/%v", backupClaim, found, err)
	}
	ordinaryClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
		t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
	}
	if !ordinaryClaim.Assignment.Record.Deadline.After(backupClaim.Assignment.Record.Deadline) {
		t.Fatal("test fixture did not order Backup timeout before ordinary timeout")
	}
	count, err := tasks.ExpireTimedOutTasks(
		context.Background(), ordinaryClaim.Assignment.Record.Deadline,
	)
	if err != nil || count != 2 {
		t.Fatalf("ExpireTimedOutTasks(batch) = %d, %v", count, err)
	}
	for _, taskID := range []string{run.TaskID, ordinary.ID} {
		terminal, getErr := tasks.GetTask(context.Background(), taskID)
		if getErr != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
			t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
		}
	}
}

// Rationale: an expired Backup Agent assignment must not stop the collector from
// terminalizing later assignments in the same bounded scan.
func TestTaskRepositoryAgentTimeoutContinuesAfterBackupTerminal(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	backupTask, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(backupTask, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := validTaskRecord(run.CreatedAt.Add(time.Second))
	createLifecycleTask(t, tasks, ordinary)
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 2991)
	backupClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(2*time.Second),
	)
	if err != nil || !found || backupClaim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask(Backup) = %#v/%v/%v", backupClaim, found, err)
	}
	ordinaryClaim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
		t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
	}
	terminalAt := backupClaim.Assignment.Record.Deadline.Add(time.Second)
	count, err := tasks.TimeoutAgentAssignments(
		context.Background(), agentID, 1, 2, terminalAt,
	)
	if err != nil || count != 2 {
		t.Fatalf("TimeoutAgentAssignments(batch) = %d, %v", count, err)
	}
	for _, taskID := range []string{run.TaskID, ordinary.ID} {
		terminal, getErr := tasks.GetTask(context.Background(), taskID)
		if getErr != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
			t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
		}
	}
}

// Rationale: prune pending abort, assigned abort, and timeout must atomically
// release dispatch authority while preserving every surviving tombstone.
func TestBackupRuntimeRepositoryRoutesPruneAbortAndTimeoutThroughDomainTerminal(t *testing.T) {
	for _, test := range []struct {
		name       string
		pending    bool
		status     testtaskjournal.TaskStatus
		useTimeout bool
	}{
		{name: "pending abort", pending: true, status: testtaskjournal.TaskStatusAborted},
		{name: "running abort", status: testtaskjournal.TaskStatusAborted},
		{name: "running timeout", status: testtaskjournal.TaskStatusTimedOut, useTimeout: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, tasks, dispatch, pointID := publishBackupPruneLifecycleTask(t)
			agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 3990)
			var terminal testkeyvalue.Versioned[TaskRecord]
			var assignmentID string
			var err error
			if test.pending {
				terminal, err = tasks.AbortPendingTask(
					context.Background(), dispatch.TaskID, dispatch.CreatedAt.Add(time.Second),
				)
			} else {
				claim, found, claimErr := tasks.ClaimNextTask(
					context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
				)
				if claimErr != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
					t.Fatalf("ClaimNextTask(prune) = %#v/%v/%v", claim, found, claimErr)
				}
				assignmentID = claim.Assignment.Record.AssignmentID
				if test.useTimeout {
					count, timeoutErr := tasks.ExpireTimedOutTasks(
						context.Background(), claim.Assignment.Record.Deadline,
					)
					if timeoutErr != nil || count != 1 {
						t.Fatalf("ExpireTimedOutTasks(prune) = %d, %v", count, timeoutErr)
					}
					terminal, err = tasks.GetTask(context.Background(), dispatch.TaskID)
				} else {
					terminal, err = tasks.AcknowledgeTask(
						context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
						test.status, testtaskjournal.TaskResultRecord{
							Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
							ReconciliationRequired: true, ExecutionEpoch: 1,
						}, dispatch.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal prune Task = %#v, %v", terminal, err)
			}
			if mustOptionalKey(
				t,
				store,
				testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			) != nil ||
				mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID)) != nil {
				t.Fatal("terminal prune retained dispatch or Environment lock")
			}
			entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
			if entry == nil {
				t.Fatal("terminal prune lost its surviving tombstone")
			}
			prune, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
			if decodeErr != nil || prune.State != testbackupruntime.BackupPrunePending || prune.TaskID != "" ||
				prune.OperationID != dispatch.OperationID || entry.ModRevision != terminal.Revision {
				t.Fatalf("terminal prune survivor = %#v/%#v/%v", entry, prune, decodeErr)
			}
			if test.pending {
				replay, replayErr := tasks.AbortPendingTask(
					context.Background(), dispatch.TaskID, dispatch.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AbortPendingTask(prune replay) = %#v, %v", replay, replayErr)
				}
			} else {
				result := testtaskjournal.TaskResultRecord{
					Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
					ReconciliationRequired: true, ExecutionEpoch: 1,
				}
				replay, replayErr := tasks.AcknowledgeTask(
					context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
					test.status, result, dispatch.CreatedAt.Add(3*time.Second),
				)
				if replayErr != nil || replay.Revision != terminal.Revision {
					t.Fatalf("AcknowledgeTask(prune replay) = %#v, %v", replay, replayErr)
				}
				if test.name == "running abort" {
					torn, deleteErr := store.Transact(
						context.Background(),
						nil,
						[]testkeyvalue.Mutation{{
							Type: testkeyvalue.MutationDelete,
							Key:  testhierarchy.EnvironmentMutationEpochKey(dispatch.EnvironmentID),
						}},
					)
					if deleteErr != nil || !torn.Succeeded {
						t.Fatalf("remove prune Environment epoch = %#v, %v", torn, deleteErr)
					}
					if _, replayErr := tasks.AcknowledgeTask(
						context.Background(), agentID, 1, dispatch.TaskID, assignmentID,
						test.status, result, dispatch.CreatedAt.Add(4*time.Second),
					); !errors.Is(replayErr, errs.New(errs.KindStateConflict, "")) {
						t.Fatalf("AcknowledgeTask(prune replay without epoch) error = %v", replayErr)
					}
				}
			}
			_ = repository
		})
	}
}

func publishBackupPruneLifecycleTask(
	t *testing.T,
) (
	*BackupRuntimeRepository,
	*memoryHierarchyStore,
	*TaskRepository, testbackupruntime.BackupRecoveryPointPruneDispatchRecord,

	string,
) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt.Add(time.Second), 3991)
	keys := []string{
		testbackupruntime.BackupRecoveryPointKey(point.ID),
		environmentIndex,
		sourceIndex,
		connectorIndex,
		testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
	}
	seeded, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: keys[0]}, {Key: keys[1]}, {Key: keys[2]}, {Key: keys[3]}, {Key: keys[4]},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: keys[0], Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: keys[1], Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: keys[2], Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: keys[3], Value: []byte(point.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed prune point = %#v, %v", seeded, err)
	}
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: seeded.Revision,
		OperationID: operationID, State: testbackupruntime.BackupPrunePending,
		CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneSeeded, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: keys[0], ModRevision: seeded.Revision}, {Key: keys[4]}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: keys[4], Value: pruneValue}},
	)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed prune authority = %#v, %v", pruneSeeded, err)
	}
	pending := []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{{
		Record: prune, Revision: pruneSeeded.Revision, ReadRevision: pruneSeeded.Revision,
	}}
	createdAt := run.CreatedAt.Add(2 * time.Second)
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: ids.NewAt(ids.KindTask, createdAt, 3992), OperationID: operationID,
		EnvironmentID: run.EnvironmentID, RecoveryPointIDs: []string{point.ID}, CreatedAt: createdAt,
	}
	plan, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, testbackupruntime.BackupOperationLockRecord{
			EnvironmentID: dispatch.EnvironmentID, OperationID: dispatch.OperationID,
			TaskID: dispatch.TaskID, Kind: testbackupruntime.BackupOperationPrune,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	task, sealed, marker, initiation := backupRuntimePrunePublicationTask(
		t, store, dispatch, pending, plan.conditions,
	)
	publication, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := applied.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publish prune Task outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return repository, store, tasks, dispatch, point.ID
}

// Rationale: a non-success terminal after remote absence retains no local
// point or prune authority, while deleting the dispatch and releasing the
// exact lock in the same domain plan.
func TestBackupRuntimeRepositoryPruneVerifiedAbsentFailureFinalization(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	verifiedAt := run.CreatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, source, verifiedAt)
	pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	pointCommit, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointCommit.Succeeded {
		t.Fatalf("seed point = %#v, %v", pointCommit, err)
	}
	pendingSweep := testbackupruntime.BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: testbackupruntime.BackupRetentionPending,
		CreatedAt: verifiedAt, UpdatedAt: verifiedAt,
	}
	pendingSweepValue, err := testbackupruntime.EncodeBackupRetentionSweepRecord(pendingSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCreated, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: pendingSweepValue,
		}},
	)
	clear(pendingSweepValue)
	if err != nil || !sweepCreated.Succeeded {
		t.Fatalf("seed pending retention sweep = %#v, %v", sweepCreated, err)
	}
	completedSweep := pendingSweep
	completedSweep.State = testbackupruntime.BackupRetentionCompleted
	completedSweep.SelectionRevision = pointCommit.Revision
	completedSweep.PruneOperationID = run.OperationID
	completedSweep.UpdatedAt = verifiedAt.Add(time.Second)
	completedSweepValue, err := testbackupruntime.EncodeBackupRetentionSweepRecord(completedSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCompleted, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{
			Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), ModRevision: sweepCreated.Revision,
		}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRetentionKey(point.SourceID, point.ID), Value: completedSweepValue,
		}},
	)
	clear(completedSweepValue)
	if err != nil || !sweepCompleted.Succeeded {
		t.Fatalf("complete retention sweep = %#v, %v", sweepCompleted, err)
	}
	prune := testbackupruntime.BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: pointCommit.Revision,
		OperationID: run.OperationID,
		State:       testbackupruntime.BackupPrunePending, CreatedAt: verifiedAt.Add(time.Second),
		UpdatedAt: verifiedAt.Add(time.Second),
	}
	pruneValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneCreate, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRecoveryPointPruneKey(point.ID)}},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointPruneKey(point.ID),
				Value: pruneValue,
			},
		},
	)
	if err != nil || !pruneCreate.Succeeded {
		t.Fatalf("seed prune = %#v, %v", pruneCreate, err)
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: run.OperationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        prune.UpdatedAt.Add(time.Second),
	}
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: testbackupruntime.BackupOperationPrune, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(),
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			{Record: prune, Revision: pruneCreate.Revision, ReadRevision: pruneCreate.Revision},
		},
		dispatch,
		lock,
	)
	if err != nil {
		t.Fatalf("prepareBackupPrunePublication() error = %v", err)
	}
	publicationMarker := "/v1/test/backup-prune-tasks/" + dispatch.TaskID
	conditions, mutations, err := publication.composeTransaction(
		[]testkeyvalue.Condition{{Key: publicationMarker}},
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: publicationMarker, Value: []byte(dispatch.TaskID)},
		},
	)
	if err != nil {
		t.Fatalf("compose prune publication = %v", err)
	}
	published, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	publication.clear()
	if err != nil || !published.Succeeded {
		t.Fatalf("publish prune = %#v, %v", published, err)
	}
	assignedEntry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(point.ID))
	assigned, err := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(assignedEntry.Value)
	if err != nil || assigned.State != testbackupruntime.BackupPruneAssigned {
		t.Fatalf("assigned prune = %#v, %v", assigned, err)
	}
	dispatchVersion := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatch, Revision: published.Revision, ReadRevision: published.Revision,
	}
	assignedVersion := testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
		Record: assigned, Revision: assignedEntry.ModRevision, ReadRevision: published.Revision,
	}
	verified := assigned
	verified.State = testbackupruntime.BackupPruneVerifiedAbsent
	verified.UpdatedAt = assigned.UpdatedAt.Add(time.Second)
	checkpointInput, _ := seedBackupPruneCheckpointAssignment(
		t, store, run, dispatch.RecoveryPointIDs,
	)
	checkpointInput = backupRemoteAbsentCheckpoint(checkpointInput, 1, point.ID)
	checkpoint, err := repository.MarkBackupRecoveryPointPruneVerifiedAbsent(
		context.Background(), checkpointInput, dispatchVersion, assignedVersion, verified, true,
	)
	if err != nil {
		t.Fatalf("MarkBackupRecoveryPointPruneVerifiedAbsent() error = %v", err)
	}
	for _, key := range []string{testbackupruntime.BackupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex, testbackupruntime.BackupRetentionKey(point.SourceID, point.ID)} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("verified-absent point authority %q = %#v", key, entry)
		}
	}
	completion, err := repository.prepareBackupPruneFailure(
		context.Background(),
		dispatchVersion,
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{checkpoint},
		checkpoint.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneFailure() error = %v", err)
	}
	conditions, mutations, err = completion.composeTransaction(
		[]testkeyvalue.Condition{{Key: publicationMarker, ModRevision: published.Revision}},
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: publicationMarker}},
	)
	if err != nil {
		t.Fatalf("compose prune completion = %v", err)
	}
	completed, err := repository.TransactRuntime(context.Background(), conditions, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	completion.clear()
	if err != nil || !completed.Succeeded {
		t.Fatalf("complete prune = %#v, %v", completed, err)
	}
	for _, key := range []string{testbackupruntime.BackupRecoveryPointPruneKey(point.ID), testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID), testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID), publicationMarker} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("completed prune authority %q = %#v", key, entry)
		}
	}
}

// Rationale: the durable eleven-point prune maximum must leave exact room for
// complete Task publication, failure release, retry reacquisition, and
// running-terminal parents.
func TestBackupRuntimeRepositoryComposesElevenPointPruneTaskBoundaries(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt, 2100)
	secondaryConnector := testConnectorRecord(
		t, run.EnvironmentID, run.CreatedAt, 2101, "archive-store",
	)
	secondaryConnectorValue, err := testconnectors.EncodeRecord(secondaryConnector)
	if err != nil {
		t.Fatal(err)
	}
	secondaryConnectorCreated, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testconnectors.RecordKey(secondaryConnector.Connector.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testconnectors.RecordKey(secondaryConnector.Connector.ID),
			Value: secondaryConnectorValue,
		}},
	)
	clear(secondaryConnectorValue)
	if err != nil || !secondaryConnectorCreated.Succeeded {
		t.Fatalf("seed secondary prune Connector = %#v, %v", secondaryConnectorCreated, err)
	}
	pending := make(
		[]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
		testbackupruntime.MaximumBackupPruneBatch,
	)
	pointIDs := make([]string, testbackupruntime.MaximumBackupPruneBatch)
	mutations := make([]testkeyvalue.Mutation, 0, testbackupruntime.MaximumBackupPruneBatch*4)
	for index := range pending {
		allocatedAt := run.CreatedAt.Add(time.Duration(index) * time.Millisecond)
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, allocatedAt, int64(2200+index))
		source.RecoveryPointCreatedAt = allocatedAt
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		source.State = testbackupruntime.BackupSourceAttemptStaged
		source.Phase = testbackupruntime.BackupSourcePhasePointCommit
		source.SizeBytes = 123
		source.SHA256 = testBackupDigest
		pointRun := run
		if index%2 == 1 {
			pointRun.ConnectorID = secondaryConnector.Connector.ID
			pointRun.ConnectorPrefix = secondaryConnector.Connector.Prefix
			source.ObjectKey = pointRun.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
				source.RecoveryPointID + "/artifact.bin"
		}
		point := backupRuntimeTestPoint(pointRun, source, allocatedAt.Add(time.Second))
		record := testbackupruntime.BackupRecoveryPointPruneRecord{
			Point:       point.BackupRecoveryPointSnapshot,
			OperationID: operationID,
			State:       testbackupruntime.BackupPrunePending,
			CreatedAt:   allocatedAt.Add(2 * time.Second),
			UpdatedAt:   allocatedAt.Add(2 * time.Second),
		}
		pointIDs[index] = point.ID
		pending[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{Record: record}
		pointValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
		if err != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if err != nil {
			clear(pointValue)
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(err)
		}
		mutations = append(
			mutations,
			testkeyvalue.Mutation{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointKey(point.ID),
				Value: pointValue,
			},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
		)
	}
	pointSeeded, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !pointSeeded.Succeeded {
		t.Fatalf("seed eleven prune points = %#v, %v", pointSeeded, err)
	}
	mutations = make([]testkeyvalue.Mutation, 0, testbackupruntime.MaximumBackupPruneBatch)
	for index := range pending {
		pending[index].Record.PointRevision = pointSeeded.Revision
		value, encodeErr := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(pending[index].Record)
		if encodeErr != nil {
			testkeyvalue.ClearMutationValues(mutations)
			t.Fatal(encodeErr)
		}
		mutations = append(mutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneKey(pointIDs[index]), Value: value,
		})
	}
	pruneSeeded, err := store.Transact(context.Background(), nil, mutations)
	testkeyvalue.ClearMutationValues(mutations)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed eleven prune authorities = %#v, %v", pruneSeeded, err)
	}
	for index := range pending {
		pending[index].Revision = pruneSeeded.Revision
		pending[index].ReadRevision = pruneSeeded.Revision
	}
	dispatch := testbackupruntime.BackupRecoveryPointPruneDispatchRecord{
		TaskID:           run.TaskID,
		OperationID:      operationID,
		EnvironmentID:    run.EnvironmentID,
		RecoveryPointIDs: pointIDs,
		CreatedAt:        run.CreatedAt.Add(3 * time.Second),
	}
	lock := testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   operationID,
		TaskID:        run.TaskID,
		Kind:          testbackupruntime.BackupOperationPrune,
		CreatedAt:     dispatch.CreatedAt,
		UpdatedAt:     dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, lock,
	)
	if err != nil {
		t.Fatal(err)
	}
	task, sealed, marker, initiation := backupRuntimePrunePublicationTask(
		t, store, dispatch, pending, publication.conditions,
	)
	genericTask := task
	genericTask.Params = map[string]string{"object_key": pending[0].Record.Point.ObjectKey}
	if _, err := publication.taskIdempotencyPlan(
		genericTask, sealed, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(generic prune Params) error = %v", err)
	}
	fabricated := proto.Clone(sealed).(*agentpb.ExecutionPlan)
	fabricated.PlanHash = nil
	fabricated.Steps[0].GetBackupArtifactPrune().ConnectorRevision++
	fabricated, err = executionplan.Seal(fabricated)
	if err != nil {
		t.Fatal(err)
	}
	fabricatedTask := task
	fabricatedTask.PlanHash = hex.EncodeToString(fabricated.PlanHash)
	if _, err := publication.taskIdempotencyPlan(
		fabricatedTask, fabricated, marker, initiation,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskIdempotencyPlan(fabricated prune Connector) error = %v", err)
	}
	idempotencyPlan, err := publication.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	publicationResult, err := idempotency.Apply(
		context.Background(), marker, idempotencyPlan,
	)
	publication.clear()
	if err != nil {
		t.Fatalf("publish eleven-point prune Task error = %v", err)
	}
	outcome, _, conflict, err := publicationResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"publish eleven-point prune Task outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			err,
		)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 2400)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	assigned := make([]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord], len(pointIDs))
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	failedTask, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		dispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		dispatch.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failedTask.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeTask(prune failure) = %#v, %v", failedTask, err)
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, dispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, failedResult,
		dispatch.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != failedTask.Revision {
		t.Fatalf("AcknowledgeTask(prune replay) = %#v, %v", replay, err)
	}
	if dispatchEntry := mustOptionalKey(
		t,
		store, testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
	); dispatchEntry != nil {
		t.Fatalf("failed prune dispatch = %#v", dispatchEntry)
	}
	if lockEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID)); lockEntry != nil {
		t.Fatalf("failed prune lock = %#v", lockEntry)
	}
	pending = make([]testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord], len(pointIDs))
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
		record, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != testbackupruntime.BackupPrunePending || record.TaskID != "" ||
			record.OperationID != dispatch.OperationID {
			t.Fatalf("failed prune pending authority = %#v, %v", record, decodeErr)
		}
		pending[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			Record: record, Revision: entry.ModRevision, ReadRevision: failedTask.Revision,
		}
	}
	sourceTask, err := tasks.GetTask(context.Background(), dispatch.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	nextDispatch := dispatch
	nextDispatch.TaskID = testBackupRetryTaskID
	nextDispatch.CreatedAt = dispatch.CreatedAt.Add(4 * time.Second)
	transferredAt := dispatch.CreatedAt.Add(4 * time.Second)
	retryPublication, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, nextDispatch, testbackupruntime.BackupOperationLockRecord{
			EnvironmentID: nextDispatch.EnvironmentID, OperationID: nextDispatch.OperationID,
			TaskID: nextDispatch.TaskID, Kind: testbackupruntime.BackupOperationPrune,
			CreatedAt: transferredAt, UpdatedAt: transferredAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask, err := CloneRetryTask(
		sourceTask.Record, nextDispatch.TaskID, testtaskjournal.TaskActorSystem, transferredAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask.PlanID = ids.NewAt(ids.KindPlan, transferredAt, 2450)
	retrySealed := backupRuntimeSealedPrunePlan(t, store, nextDispatch, pending, retryTask.PlanID)
	retryTask.PlanHash = hex.EncodeToString(retrySealed.PlanHash)
	retryTask.Steps = make([]testtaskjournal.TaskStepRecord, len(retrySealed.Steps))
	for index, step := range retrySealed.Steps {
		retryTask.Steps[index] = testtaskjournal.TaskStepRecord{
			Kind: testtaskjournal.TaskStepOperation,
			ID:   step.StepId,
		}
	}
	retryMarker := pendingTaskMarker(retryTask)
	retryMarker.Locator.ScopeID = dispatch.EnvironmentID
	retryMarker.Locator.Route = "/internal/backup-prunes/{task}/retry"
	retryMarker.Locator.Key = "backup-prune-retry-runtime-0001"
	staleSource := sourceTask
	staleSource.ReadRevision++
	if _, err := retryPublication.taskRetryIdempotencyPlan(
		staleSource, retryTask, retrySealed, retryMarker,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("taskRetryIdempotencyPlan(stale fixed read) error = %v", err)
	}
	if retryEntry := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID)); retryEntry != nil {
		t.Fatalf("rejected stale prune retry Task = %#v", retryEntry)
	}
	retryPlan, err := retryPublication.taskRetryIdempotencyPlan(
		sourceTask, retryTask, retrySealed, retryMarker,
	)
	if err != nil {
		t.Fatal(err)
	}
	transferResult, err := idempotency.Apply(context.Background(), retryMarker, retryPlan)
	retryPublication.clear()
	if err != nil {
		t.Fatalf("transfer eleven-point prune Task error = %v", err)
	}
	outcome, _, conflict, err = transferResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"transfer eleven-point prune Task outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			err,
		)
	}
	transferredRevision := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID)).ModRevision
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointPruneKey(pointID))
		record, decodeErr := testbackupruntime.DecodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != testbackupruntime.BackupPruneAssigned ||
			record.TaskID != nextDispatch.TaskID {
			t.Fatalf("retry assigned prune = %#v, %v", record, decodeErr)
		}
		assigned[index] = testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord]{
			Record: record, Revision: entry.ModRevision, ReadRevision: transferredRevision,
		}
	}
	claim, found, err = tasks.ClaimNextTask(
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(5*time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != nextDispatch.TaskID {
		t.Fatalf("ClaimNextTask(retry) = %#v/%v/%v", claim, found, err)
	}
	verifiedMutations := make([]testkeyvalue.Mutation, 0, len(assigned))
	verifiedConditions := make([]testkeyvalue.Condition, 0, len(assigned))
	for index := range assigned {
		record := assigned[index].Record
		record.State = testbackupruntime.BackupPruneVerifiedAbsent
		record.UpdatedAt = dispatch.CreatedAt.Add(6 * time.Second)
		value, encodeErr := testbackupruntime.EncodeBackupRecoveryPointPruneRecord(record)
		if encodeErr != nil {
			testkeyvalue.ClearMutationValues(verifiedMutations)
			t.Fatal(encodeErr)
		}
		verifiedConditions = append(verifiedConditions, testkeyvalue.Condition{
			Key:         testbackupruntime.BackupRecoveryPointPruneKey(record.Point.ID),
			ModRevision: assigned[index].Revision,
		})
		verifiedMutations = append(verifiedMutations, testkeyvalue.Mutation{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneKey(record.Point.ID), Value: value,
		})
		authorityKeys, keyErr := backupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			testkeyvalue.ClearMutationValues(verifiedMutations)
			t.Fatal(keyErr)
		}
		for _, key := range authorityKeys[1:] {
			verifiedMutations = append(
				verifiedMutations,
				testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key},
			)
		}
		assigned[index].Record = record
	}
	verified, err := store.Transact(
		context.Background(), verifiedConditions, verifiedMutations,
	)
	testkeyvalue.ClearMutationValues(verifiedMutations)
	if err != nil || !verified.Succeeded {
		t.Fatalf("verify eleven prunes = %#v, %v", verified, err)
	}
	for index := range assigned {
		assigned[index].Revision = verified.Revision
		assigned[index].ReadRevision = verified.Revision
	}
	completedTask, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		nextDispatch.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		dispatch.CreatedAt.Add(7*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(prune completion) error = %v", err)
	}
	terminalTask := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(nextDispatch.TaskID))
	if terminalTask == nil || terminalTask.ModRevision != completedTask.Revision {
		t.Fatalf("terminal prune Task = %#v", terminalTask)
	}
	if lockEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentOperationLockKey(run.EnvironmentID)); lockEntry != nil {
		t.Fatalf("completed eleven-point prune lock = %#v", lockEntry)
	}
	sourceTask, err = tasks.GetTask(context.Background(), dispatch.TaskID)
	if err != nil || sourceTask.Record.RetainUntil == nil {
		t.Fatalf("terminal source prune Task = %#v, %v", sourceTask, err)
	}
	pruneAt := sourceTask.Record.RetainUntil.Add(time.Nanosecond)
	for {
		count, pruneErr := idempotency.PruneExpired(context.Background(), pruneAt)
		if pruneErr != nil {
			t.Fatalf("PruneExpired(prune Task marker) error = %v", pruneErr)
		}
		if count == 0 {
			break
		}
	}
	staleDispatchValue, err := testbackupruntime.EncodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		t.Fatal(err)
	}
	staleDispatch, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			Value: staleDispatchValue,
		}},
	)
	clear(staleDispatchValue)
	if err != nil || !staleDispatch.Succeeded {
		t.Fatalf("seed stale terminal prune dispatch = %#v, %v", staleDispatch, err)
	}
	if count, pruneErr := tasks.PruneExpiredTasks(
		context.Background(), pruneAt,
	); !errors.Is(pruneErr, errs.New(errs.KindInternal, "")) || count != 0 {
		t.Fatalf("PruneExpiredTasks(stale prune dispatch) = %d, %v", count, pruneErr)
	}
	removedDispatch, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{
			Key:         testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			ModRevision: staleDispatch.Revision,
		}},
		[]testkeyvalue.Mutation{
			{
				Type: testkeyvalue.MutationDelete,
				Key:  testbackupruntime.BackupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			},
		},
	)
	if err != nil || !removedDispatch.Succeeded {
		t.Fatalf("remove stale terminal prune dispatch = %#v, %v", removedDispatch, err)
	}
	if count, pruneErr := tasks.PruneExpiredTasks(
		context.Background(), pruneAt,
	); pruneErr != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(prune source) = %d, %v", count, pruneErr)
	}
	if sourceEntry := mustOptionalKey(t, store, testtaskjournal.TaskStorageKey(dispatch.TaskID)); sourceEntry != nil {
		t.Fatalf("retained source prune Task = %#v", sourceEntry)
	}
}

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

func seedBackupRuntimePointAuthority(
	t *testing.T,
	store *memoryHierarchyStore,
	point testbackupruntime.BackupRecoveryPointRecord,
) int64 {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	environmentIndex, err := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := testbackupruntime.BackupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRecoveryPointKey(point.ID), Value: value},
		{Type: testkeyvalue.MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: testkeyvalue.MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Recovery Point authority = %#v, %v", result, err)
	}
	return result.Revision
}

type backupRuntimeAuthorityAuditStore struct {
	hierarchyStore
	maximumKeys       int
	revisions         []int64
	truncateNextChunk bool
}

func (store *backupRuntimeAuthorityAuditStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) > store.maximumKeys {
		store.maximumKeys = len(request.Keys)
	}
	store.revisions = append(store.revisions, request.Revision)
	result, err := store.hierarchyStore.GetMany(ctx, request)
	if err == nil && result != nil && store.truncateNextChunk && request.Revision > 0 {
		store.truncateNextChunk = false
		if len(result.Values) > 0 {
			result.Values = result.Values[:len(result.Values)-1]
		}
	}
	return result, err
}

type backupRuntimeUnknownOutcomeStore struct {
	hierarchyStore
	failNext bool
}

func (store *backupRuntimeUnknownOutcomeStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	result, err := store.hierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext {
		store.failNext = false
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindStorageUnavailable,
			"unknown backup runtime outcome",
		)
	}
	return result, err
}

func newBackupRuntimeID(kind ids.Kind, at time.Time, seed int64) string {
	return ids.NewAt(kind, at, seed)
}

func backupRuntimeSourceRecord(
	t *testing.T,
	sourceID string,
	environmentID string,
	kind string,
	targetID string,
	createdAt time.Time,
) testbackuppolicy.BackupSourceRecord {
	t.Helper()
	value, err := testrecordcodec.Encode("backup-source", map[string]any{
		"id": sourceID, "environment_id": environmentID, "kind": kind,
		"target_id": targetID, "created_at": createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	record, err := testbackuppolicy.DecodeBackupSourceRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func putBackupRuntimeLock(
	t *testing.T,
	store *memoryHierarchyStore,
	lock testbackupruntime.BackupOperationLockRecord,
) {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID)}},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID),
				Value: value,
			},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("put backup runtime lock = %#v/%v", result, err)
	}
}

func backupRuntimeTestPoint(
	run testbackupruntime.BackupRunRecord,
	source testbackupruntime.BackupRunSourceAttemptRecord,
	verifiedAt time.Time,
) testbackupruntime.BackupRecoveryPointRecord {
	return testbackupruntime.BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: testbackupruntime.BackupRecoveryPointSnapshot{
			ID: source.RecoveryPointID, EnvironmentID: run.EnvironmentID,
			SourceID: source.SourceID, SourceKind: source.Kind, TargetID: source.TargetID,
			ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
			ObjectKey: source.ObjectKey, SourceFormat: source.Format,
			Encryption: run.Encryption, KeyEra: run.KeyEra, Recipient: run.Recipient,
			SizeBytes: source.SizeBytes, SHA256: source.SHA256,
			CreatedAt: source.RecoveryPointCreatedAt,
		},
		VerifiedAt: verifiedAt,
	}
}

func createBackupRuntimeOrphanForReadTest(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupOrphanRecord) {
	t.Helper()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]testbackupruntime.BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = testbackupruntime.BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = testbackupruntime.BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := testbackupruntime.BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: testbackupruntime.BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = testbackupruntime.BackupCheckpointUploadVerified
	if _, err := repository.CreateBackupOrphan(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint),
		stagedVersion, orphaned, 0, orphan,
	); err != nil {
		t.Fatalf("CreateBackupOrphan() error = %v", err)
	}
	return repository, store, orphan
}

func (repository *BackupRuntimeRepository) replaceBackupRunForTest(
	ctx context.Context,
	current testkeyvalue.Versioned[testbackupruntime.BackupRunRecord],
	next testbackupruntime.BackupRunRecord,
) (testkeyvalue.Versioned[testbackupruntime.BackupRunRecord], error) {
	value, err := testbackupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer clear(value)
	anchor, err := repository.ReadCurrentKeys(ctx, []string{testbackupruntime.BackupRunKey(current.Record.TaskID)})
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer testkeyvalue.ClearValues(anchor.Values)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, current.Record.EnvironmentID,
		anchor.ReadRevision, environmentfence.Owner{
			Kind:        testbackupruntime.BackupOperationBackup,
			OperationID: current.Record.OperationID, TaskID: current.Record.TaskID,
		})
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	conditions := []testkeyvalue.Condition{
		{Key: testbackupruntime.BackupRunKey(current.Record.TaskID), ModRevision: current.Revision},
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	defer clear(epoch.Value)
	result, err := repository.TransactRuntime(ctx, conditions, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testbackupruntime.BackupRunKey(next.TaskID), Value: value}, epoch,
	})
	if err != nil || !result.Succeeded {
		return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{}, err
	}
	return testkeyvalue.Versioned[testbackupruntime.BackupRunRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
