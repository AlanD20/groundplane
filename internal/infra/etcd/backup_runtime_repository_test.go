package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
	if created.Record.State != BackupRunQueued || created.Revision <= 0 {
		t.Fatalf("CreateBackupRun() = %#v", created)
	}
	read, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || read.Revision != created.Revision {
		t.Fatalf("GetBackupRun() = %#v, %v", read, err)
	}
	page, err := repository.ListBackupRunsByEnvironment(
		context.Background(),
		run.EnvironmentID,
		BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.TaskID != run.TaskID {
		t.Fatalf("ListBackupRunsByEnvironment() = %#v, %v", page, err)
	}
	if _, found, err := repository.GetBackupSourceTargetExclusion(
		context.Background(), BackupSourceTargetAttach, run.Sources[0].TargetID,
	); err != nil || !found {
		t.Fatalf("GetBackupSourceTargetExclusion() = %v/%v", found, err)
	}
	epochAfterCreate := mustEnvironmentMutationEpochRevision(t, store, run.EnvironmentID)
	next := run
	next.State = BackupRunRunning
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
	ready.State = BackupRunRunning
	ready.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	ready.Sources[0].State = BackupSourceAttemptReady
	ready.Sources[0].Phase = BackupSourcePhaseStaging
	ready.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, ready)
	if err != nil {
		t.Fatal(err)
	}
	staged := ready
	staged.Sources = append([]BackupRunSourceAttemptRecord(nil), ready.Sources...)
	staged.Sources[0].State = BackupSourceAttemptStaged
	staged.Sources[0].Phase = BackupSourcePhaseUpload
	staged.Sources[0].SizeBytes = 123
	staged.Sources[0].SHA256 = testBackupDigest
	staged.UpdatedAt = ready.UpdatedAt.Add(time.Second)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = BackupCheckpointPayload{
		Kind: BackupCheckpointArtifactPrepared, PointID: staged.Sources[0].RecoveryPointID,
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
	headVerified.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	headVerified.Sources[0].Phase = BackupSourcePhaseHeadVerification
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
	uploadCompleted.Payload.Kind = BackupCheckpointUploadCompleted
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
		[]BackupRunSourceAttemptRecord(nil),
		headVerified.Sources...,
	)
	pointCommitReady.Sources[0].Phase = BackupSourcePhasePointCommit
	pointCommitReady.UpdatedAt = headVerified.UpdatedAt.Add(time.Second)
	uploadVerified := uploadCompleted
	uploadVerified.Sequence++
	uploadVerified.Payload.Kind = BackupCheckpointUploadVerified
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
	ready.State = BackupRunRunning
	ready.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	ready.Sources[0].State = BackupSourceAttemptReady
	ready.Sources[0].Phase = BackupSourcePhaseStaging
	ready.UpdatedAt = run.UpdatedAt.Add(time.Second)
	readyVersion, err := repository.replaceBackupRunForTest(context.Background(), created, ready)
	if err != nil {
		t.Fatal(err)
	}
	staged := ready
	staged.Sources = append([]BackupRunSourceAttemptRecord(nil), ready.Sources...)
	staged.Sources[0].State = BackupSourceAttemptStaged
	staged.Sources[0].Phase = BackupSourcePhaseUpload
	staged.Sources[0].SizeBytes = 123
	staged.Sources[0].SHA256 = testBackupDigest
	staged.UpdatedAt = ready.UpdatedAt.Add(time.Second)
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = BackupCheckpointPayload{
		Kind:            BackupCheckpointArtifactPrepared,
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, orphaned.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point:     point.BackupRecoveryPointSnapshot,
		TaskID:    run.TaskID,
		State:     BackupOrphanInspect,
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
	headVerified.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	headVerified.Sources[0].Phase = BackupSourcePhaseHeadVerification
	headVerified.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	uploadCompleted := checkpoint
	uploadCompleted.Sequence++
	uploadCompleted.Payload.Kind = BackupCheckpointUploadCompleted
	headVersion, err := repository.CheckpointBackupRun(
		context.Background(), uploadCompleted, orphanedVersion, headVerified,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(upload completed) error = %v", err)
	}
	pointCommit := headVerified
	pointCommit.Sources = append([]BackupRunSourceAttemptRecord(nil), headVerified.Sources...)
	pointCommit.Sources[0].Phase = BackupSourcePhasePointCommit
	pointCommit.UpdatedAt = headVerified.UpdatedAt.Add(time.Second)
	uploadVerified := uploadCompleted
	uploadVerified.Sequence++
	uploadVerified.Payload.Kind = BackupCheckpointUploadVerified
	pointVersion, err := repository.CheckpointBackupRun(
		context.Background(), uploadVerified, headVersion, pointCommit,
	)
	if err != nil || pointVersion.Record.Sources[0].Phase != BackupSourcePhasePointCommit {
		t.Fatalf("CheckpointBackupRun(orphan verified) = %#v, %v", pointVersion, err)
	}
}

// Rationale: a wrong lock owner or stale epoch must reject the whole runtime
// mutation without leaving partial durable evidence.
func TestBackupRuntimeRepositoryRejectsWrongLockAndStaleEpochWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		run  func(*testing.T, *memoryHierarchyStore, BackupRunRecord)
	}{
		{name: "wrong lock", run: func(t *testing.T, store *memoryHierarchyStore, run BackupRunRecord) {
			lock := BackupOperationLockRecord{
				EnvironmentID: run.EnvironmentID, OperationID: testBackupOperationID,
				TaskID: testBackupRetryTaskID, Kind: BackupOperationBackup,
				CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
			}
			putBackupRuntimeLock(t, store, lock)
		}},
		{name: "stale epoch", run: func(_ *testing.T, _ *memoryHierarchyStore, _ BackupRunRecord) {
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
			stored, getErr := store.Get(context.Background(), backupRunKey(run.TaskID))
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
	entry := mustOptionalKey(t, store, backupSourceKey(run.Sources[0].SourceID))
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: entry.Key, ModRevision: entry.ModRevision}},
		[]Mutation{{Type: MutationPut, Key: entry.Key, Value: entry.Value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("advance backup source = %#v, %v", result, err)
	}
	if _, err := repository.createBackupRunForTest(
		context.Background(), run, fixedRevision,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("CreateBackupRun(changed source) error = %v", err)
	}
	if entry := mustOptionalKey(t, store, backupRunKey(run.TaskID)); entry != nil {
		t.Fatalf("run published with changed source = %#v", entry)
	}
}

// Rationale: config capture authority is created with the run and pins the
// original fixed Entry read revision rather than reconstructing it on retry.
func TestBackupRuntimeRepositoryPublishesPinnedConfigSnapshotAtomically(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	source := &run.Sources[0]
	source.Kind = BackupRuntimeSourceConfig
	source.TargetID = run.EnvironmentID
	source.TargetRevision = mustOptionalKey(t, store, environmentKey(run.EnvironmentID)).ModRevision
	source.Format = BackupRuntimeFormatConfig
	source.Snapshot = BackupRunSourceSnapshot{Config: &BackupConfigSourceSnapshot{
		ConfigSnapshotID: run.TaskID,
	}}
	policyValue, err := encodeBackupPolicyRecord(BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceValue, err := encodeBackupSourceRecord(backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "config", run.EnvironmentID, run.CreatedAt,
	))
	if err != nil {
		clear(policyValue)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: backupPolicyKey(run.EnvironmentID), Value: policyValue},
		{Type: MutationPut, Key: backupSourceKey(source.SourceID), Value: sourceValue},
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
	for _, key := range []string{
		backupRunKey(run.TaskID), backupConfigSnapshotKey(run.TaskID),
		backupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID),
		backupConfigSnapshotReferenceTaskKey(run.TaskID, run.TaskID),
	} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != created.Revision {
			t.Fatalf("config publication companion %q = %#v", key, entry)
		}
	}
	entry := mustOptionalKey(t, store, backupConfigSnapshotKey(run.TaskID))
	snapshot, err := decodeBackupConfigSnapshotRecord(entry.Value)
	if err != nil || snapshot.ReadRevision != fixedRevision ||
		snapshot.State != BackupConfigSnapshotBuilding {
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
	running.State = BackupRunRunning
	running.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	running.Sources[0].State = BackupSourceAttemptStaged
	running.Sources[0].Phase = BackupSourcePhasePointCommit
	running.Sources[0].SizeBytes = 123
	running.Sources[0].SHA256 = testBackupDigest
	running.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, running)
	if err != nil {
		t.Fatal(err)
	}
	committed := running
	committed.Sources = append([]BackupRunSourceAttemptRecord(nil), running.Sources...)
	committed.Sources[0].State = BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = BackupSourcePhaseRetention
	committed.UpdatedAt = running.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, running.Sources[0], committed.UpdatedAt)
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: BackupRetentionPending,
		CreatedAt: committed.UpdatedAt, UpdatedAt: committed.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(
		t,
		repository.store.(*memoryHierarchyStore),
		run,
	)
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
	storedPoint, storedRun, err := repository.CommitBackupRecoveryPoint(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), created, committed, 0, point, nil, sweep,
	)
	if err != nil {
		t.Fatalf("CommitBackupRecoveryPoint() error = %v", err)
	}
	if storedPoint.Revision != storedRun.Revision {
		t.Fatalf("point/run revisions = %d/%d", storedPoint.Revision, storedRun.Revision)
	}
	for _, list := range []func() (BackupRuntimePage[BackupRecoveryPointRecord], error){
		func() (BackupRuntimePage[BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsByEnvironment(
				context.Background(), run.EnvironmentID, BackupRuntimeListRequest{Limit: 1},
			)
		},
		func() (BackupRuntimePage[BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsBySource(
				context.Background(), point.SourceID, BackupRuntimeListRequest{Limit: 1},
			)
		},
		func() (BackupRuntimePage[BackupRecoveryPointRecord], error) {
			return repository.ListBackupRecoveryPointsByConnector(
				context.Background(), point.ConnectorID, BackupRuntimeListRequest{Limit: 1},
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
			checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
				entry := mustOptionalKey(t, store, backupRetentionKey(point.SourceID, point.ID))
				changed := sweep
				changed.Keep++
				value, encodeErr := encodeBackupRetentionSweepRecord(changed)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				result, transactErr := store.Transact(
					context.Background(),
					[]Condition{{Key: entry.Key, ModRevision: entry.ModRevision}},
					[]Mutation{{Type: MutationPut, Key: entry.Key, Value: value}},
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
				storedRun.Record.Sources[0].State != BackupSourceAttemptPointCommitted {
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
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
		value, encodeErr := encodeBackupRecoveryPointRecord(older)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		environmentIndex, _ := backupRecoveryPointEnvironmentIndexKey(older.EnvironmentID, older.ID)
		sourceIndex, _ := backupRecoveryPointSourceIndexKey(older.SourceID, older.ID)
		connectorIndex, _ := backupRecoveryPointConnectorIndexKey(older.ConnectorID, older.ID)
		result, transactErr := store.Transact(context.Background(), nil, []Mutation{
			{Type: MutationPut, Key: backupRecoveryPointKey(older.ID), Value: value},
			{Type: MutationPut, Key: environmentIndex, Value: []byte(older.ID)},
			{Type: MutationPut, Key: sourceIndex, Value: []byte(older.ID)},
			{Type: MutationPut, Key: connectorIndex, Value: []byte(older.ID)},
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
		[]BackupRunSourceAttemptRecord(nil),
		committedRun.Record.Sources...,
	)
	prematureCleanup.Sources[0].State = BackupSourceAttemptCleanupPending
	prematureCleanup.Sources[0].Phase = BackupSourcePhaseCleanup
	prematureCleanup.UpdatedAt = currentSweep.Record.UpdatedAt.Add(time.Second)
	if _, err := repository.advanceBackupRunAfterRetention(
		context.Background(), committedRun, prematureCleanup, 0, currentSweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("advanceBackupRunAfterRetention(pending sweep) error = %v", err)
	}
	firstPage, prunes, err := repository.AdvanceBackupRetentionSweep(
		context.Background(), committedRun, currentSweep, sweep.UpdatedAt.Add(time.Second),
	)
	if err != nil || firstPage.Record.State != BackupRetentionScanning || len(prunes) != 8 ||
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
	if err != nil || advanced.Record.State != BackupRetentionCompleted ||
		advanced.Record.SelectionRevision != firstPage.Record.SelectionRevision ||
		len(secondPagePrunes) != 2 {
		t.Fatalf("AdvanceBackupRetentionSweep(second page) = %#v/%#v/%v", advanced, secondPagePrunes, err)
	}
	prunes = append(prunes, secondPagePrunes...)
	if mustOptionalKey(t, store, backupRecoveryPointPruneKey(hostile.ID)) != nil {
		t.Fatal("interpage point mutation entered the pinned retention selection")
	}
	cleanup := committedRun.Record
	cleanup.Sources = append([]BackupRunSourceAttemptRecord(nil), committedRun.Record.Sources...)
	cleanup.Sources[0].State = BackupSourceAttemptCleanupPending
	cleanup.Sources[0].Phase = BackupSourcePhaseCleanup
	cleanup.UpdatedAt = advanced.Record.UpdatedAt.Add(time.Second)
	if _, err := repository.TransitionBackupRun(
		context.Background(), backupAssignmentFromCheckpoint(checkpoint), committedRun, cleanup,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("TransitionBackupRun(skipped retention) error = %v", err)
	}
	advancedRun, err := repository.advanceBackupRunAfterRetention(
		context.Background(), committedRun, cleanup, 0, advanced,
	)
	if err != nil || advancedRun.Record.Sources[0].State != BackupSourceAttemptCleanupPending {
		t.Fatalf("advanceBackupRunAfterRetention() = %#v, %v", advancedRun, err)
	}
	if _, err := repository.GetBackupRecoveryPoint(context.Background(), newest.ID); err != nil {
		t.Fatalf("newest retained point error = %v", err)
	}
	for _, prune := range prunes {
		if prune.Record.State != BackupPrunePending || prune.Record.OperationID == "" {
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
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	maximumKeep := MaximumBackupPolicyKeep
	changedRun := committedRun.Record
	changedRun.RetentionKeep = maximumKeep
	changedSweep := currentSweep.Record
	changedSweep.Keep = maximumKeep
	changedSweep.State = BackupRetentionScanning
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
	selection, err := repository.readCurrentKeys(
		context.Background(), []string{backupRecoveryPointKey(point.ID)},
	)
	if err != nil {
		t.Fatal(err)
	}
	changedSweep.SelectionRevision = selection.ReadRevision
	clearKeyValues(selection.Values)
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
	runValue, err := encodeBackupRunRecord(changedRun)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runValue)
	sweepValue, err := encodeBackupRetentionSweepRecord(changedSweep)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sweepValue)
	changed, err := store.Transact(context.Background(), []Condition{
		{Key: backupRunKey(run.TaskID), ModRevision: committedRun.Revision},
		{Key: backupRetentionKey(point.SourceID, point.ID), ModRevision: currentSweep.Revision},
	}, []Mutation{
		{Type: MutationPut, Key: backupRunKey(run.TaskID), Value: runValue},
		{Type: MutationPut, Key: backupRetentionKey(point.SourceID, point.ID), Value: sweepValue},
	})
	if err != nil || !changed.Succeeded {
		t.Fatalf("seed maximum retention progress = %#v, %v", changed, err)
	}
	committedRun = Versioned[BackupRunRecord]{
		Record: changedRun, Revision: changed.Revision, ReadRevision: changed.Revision,
	}
	currentSweep = Versioned[BackupRetentionSweepRecord]{
		Record: changedSweep, Revision: changed.Revision, ReadRevision: changed.Revision,
	}
	advanced, prunes, err := repository.AdvanceBackupRetentionSweep(
		context.Background(), committedRun, currentSweep, changedSweep.UpdatedAt.Add(time.Second),
	)
	if err != nil || advanced.Record.State != BackupRetentionCompleted || len(prunes) != 3 ||
		advanced.Record.RetainedCount != maximumKeep {
		t.Fatalf("AdvanceBackupRetentionSweep(int64 Keep) = %#v/%#v/%v", advanced, prunes, err)
	}
}

// Rationale: Connector deletion fences must retain the complete stable point identity in their key suffix.
func TestBackupRecoveryPointConnectorIndexUsesRawStablePointIdentity(t *testing.T) {
	t.Parallel()
	first, err := backupRecoveryPointConnectorIndexKey(testBackupConnectorID, testBackupPointID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := backupRecoveryPointConnectorIndexKey(testBackupConnectorID, testBackupPointIDTwo)
	if err != nil {
		t.Fatal(err)
	}
	prefix := backupRecoveryPointConnectorPrefix + testBackupConnectorID + "/"
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
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	otherValue, err := encodeBackupRecoveryPointRecord(otherPoint)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(otherValue)
	otherSourceIndex, err := backupRecoveryPointSourceIndexKey(
		otherPoint.SourceID,
		otherPoint.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: backupRecoveryPointKey(otherPoint.ID), Value: otherValue},
		{Type: MutationPut, Key: otherSourceIndex, Value: []byte(otherPoint.ID)},
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
		backupRetentionKey(currentSweep.Record.SourceID, currentSweep.Record.TriggerRecoveryPointID),
	)
	if storedSweep == nil || storedSweep.ModRevision != currentSweep.Revision ||
		mustOptionalKey(t, store, backupRecoveryPointPruneKey(otherPoint.ID)) != nil {
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
		context.Background(), run.EnvironmentID, BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Point.ID != point.ID {
		t.Fatalf("ListBackupOrphansByEnvironment() = %#v, %v", page, err)
	}
	deleting := orphan
	deleting.State = BackupOrphanDelete
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
	connectorIndex, err := backupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := backupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{backupOrphanKey(point.ID), connectorIndex, environmentIndex} {
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
	failed.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	failed.State = BackupRunFailed
	failed.Sources[0].State = BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = BackupFailurePointCommit
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
		[]Condition{{Key: marker}},
		[]Mutation{{Type: MutationPut, Key: marker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		plan.clear()
		t.Fatal(err)
	}
	result, err := repository.transact(context.Background(), conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	plan.clear()
	if err != nil || !result.Succeeded {
		t.Fatalf("terminalize absent orphan = %#v, %v", result, err)
	}
	for _, key := range []string{backupOrphanKey(point.ID), connectorIndex, environmentIndex} {
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
		mutate func(*testing.T, *memoryHierarchyStore, BackupOrphanRecord, string) []Mutation
	}{
		{
			name: "missing",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, _ BackupOrphanRecord, key string) []Mutation {
				return []Mutation{{Type: MutationDelete, Key: key}}
			},
		},
		{
			name: "malformed",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, _ BackupOrphanRecord, key string) []Mutation {
				return []Mutation{{Type: MutationPut, Key: key, Value: []byte("not-a-point-id")}}
			},
		},
		{
			name: "mismatched",
			mutate: func(_ *testing.T, _ *memoryHierarchyStore, orphan BackupOrphanRecord, key string) []Mutation {
				other := ids.NewAt(ids.KindRecoveryPoint, orphan.CreatedAt.Add(time.Second), 611)
				return []Mutation{{Type: MutationPut, Key: key, Value: []byte(other)}}
			},
		},
		{
			name: "misbucketed",
			mutate: func(t *testing.T, _ *memoryHierarchyStore, orphan BackupOrphanRecord, key string) []Mutation {
				otherConnector := ids.NewAt(ids.KindConnector, orphan.CreatedAt.Add(time.Second), 612)
				wrongKey, err := backupOrphanConnectorIndexKey(otherConnector, orphan.Point.ID)
				if err != nil {
					t.Fatal(err)
				}
				return []Mutation{
					{Type: MutationDelete, Key: key},
					{Type: MutationPut, Key: wrongKey, Value: []byte(orphan.Point.ID)},
				}
			},
		},
		{
			name: "identical byte replay",
			mutate: func(t *testing.T, store *memoryHierarchyStore, _ BackupOrphanRecord, key string) []Mutation {
				entry := mustOptionalKey(t, store, key)
				return []Mutation{{Type: MutationPut, Key: key, Value: entry.Value}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository, store, orphan := createBackupRuntimeOrphanForReadTest(t)
			connectorIndex, err := backupOrphanConnectorIndexKey(
				orphan.Point.ConnectorID,
				orphan.Point.ID,
			)
			if err != nil {
				t.Fatal(err)
			}
			entry := mustOptionalKey(t, store, connectorIndex)
			changed, err := store.Transact(
				context.Background(),
				[]Condition{{Key: connectorIndex, ModRevision: entry.ModRevision}},
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
				orphan.Point.EnvironmentID,
				BackupRuntimeListRequest{Limit: 1},
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	connectorIndex, err := backupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: connectorIndex, ModRevision: stored.Revision}},
		[]Mutation{{Type: MutationDelete, Key: connectorIndex}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("delete orphan companion = %#v, %v", result, err)
	}
	next := orphan
	next.State = BackupOrphanDelete
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	committed.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	committed.Sources[0].State = BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = BackupSourcePhaseRetention
	committed.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	point.VerifiedAt = committed.UpdatedAt
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: BackupRetentionPending,
		CreatedAt: committed.UpdatedAt, UpdatedAt: committed.UpdatedAt,
	}
	checkpoint.Sequence = 2
	checkpoint.Payload = BackupCheckpointPayload{
		Kind: BackupCheckpointUploadVerified, PointID: point.ID,
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
	connectorIndex, err := backupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	environmentIndex, err := backupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{backupOrphanKey(point.ID), connectorIndex, environmentIndex} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("committed orphan authority %q = %#v", key, entry)
		}
	}
	if entry := mustOptionalKey(
		t,
		store,
		backupRecoveryPointKey(point.ID),
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
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, connectorIndex)
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: connectorIndex, ModRevision: storedPoint.Revision}},
		[]Mutation{{Type: MutationPut, Key: connectorIndex, Value: entry.Value}},
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
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
	racing := &entryVolumeEpochRaceStore{hierarchyStore: store}
	racing.beforeTransact = func() {
		key := connectorRecordKey(run.ConnectorID)
		entry := mustOptionalKey(t, store, key)
		result, mutateErr := store.Transact(
			context.Background(),
			[]Condition{{Key: key, ModRevision: entry.ModRevision}},
			[]Mutation{{Type: MutationPut, Key: key, Value: entry.Value}},
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
	if entry := mustOptionalKey(t, store, backupRecoveryPointKey(point.ID)); entry != nil {
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
		testBackupLaterSource(run.CreatedAt, 1, BackupSourceAttemptPending),
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
	current.State = BackupRunRunning
	current.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	current.Sources[0].State = BackupSourceAttemptSucceeded
	current.Sources[0].Phase = BackupSourcePhaseCleanup
	current.Sources[0].SizeBytes = 123
	current.Sources[0].SHA256 = testBackupDigest
	current.Sources[1].State = BackupSourceAttemptStaged
	current.Sources[1].Phase = BackupSourcePhaseUpload
	current.Sources[1].SizeBytes = 123
	current.Sources[1].SHA256 = testBackupDigest
	current.UpdatedAt = run.UpdatedAt.Add(time.Second)
	created, err = repository.replaceBackupRunForTest(context.Background(), created, current)
	if err != nil {
		t.Fatal(err)
	}
	next := current
	next.Sources = append([]BackupRunSourceAttemptRecord(nil), current.Sources...)
	next.Sources[1].State = BackupSourceAttemptPointCommitted
	next.Sources[1].Phase = BackupSourcePhaseRetention
	next.UpdatedAt = current.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, current.Sources[0], next.UpdatedAt)
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: BackupRetentionPending,
		CreatedAt: next.UpdatedAt, UpdatedAt: next.UpdatedAt,
	}
	if _, _, err := repository.CommitBackupRecoveryPoint(
		context.Background(), BackupAssignmentInput{}, created, next, 0, point, nil, sweep,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CommitBackupRecoveryPoint(mismatched ordinal) error = %v", err)
	}
	orphaned := current
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), current.Sources...)
	orphaned.Sources[1].State = BackupSourceAttemptOrphaned
	orphaned.Sources[1].Phase = BackupSourcePhasePointCommit
	orphaned.UpdatedAt = current.UpdatedAt.Add(time.Second)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	if _, err := repository.CreateBackupOrphan(
		context.Background(), BackupAssignmentInput{}, created, orphaned, 0, orphan,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("CreateBackupOrphan(mismatched ordinal) error = %v", err)
	}
	failed := orphaned
	failed.State = BackupRunFailed
	failed.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	failed.Sources[1].State = BackupSourceAttemptFailed
	failed.Sources[1].FailureCode = BackupFailurePointCommit
	failed.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	orphan.State = BackupOrphanDelete
	if _, err := repository.prepareBackupOrphanAbsentTerminal(
		context.Background(),
		BackupCheckpointInput{},
		Versioned[BackupRunRecord]{
			Record: orphaned, Revision: created.Revision, ReadRevision: created.ReadRevision,
		},
		failed,
		0,
		Versioned[BackupOrphanRecord]{Record: orphan, Revision: 1, ReadRevision: 1},
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
	idempotency, err := newIdempotencyRepository(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(
		context.Background(), marker, idempotencyPlan,
	); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("Apply(unknown outcome) error = %v", err)
	}
	policyKey := backupPolicyKey(run.EnvironmentID)
	policyEntry := mustOptionalKey(t, store, policyKey)
	policy, err := decodeBackupPolicyRecord(policyEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	policy.Keep++
	policyValue, err := encodeBackupPolicyRecord(policy)
	if err != nil {
		t.Fatal(err)
	}
	policyChanged, err := store.Transact(
		context.Background(),
		[]Condition{{Key: policyKey, ModRevision: policyEntry.ModRevision}},
		[]Mutation{{Type: MutationPut, Key: policyKey, Value: policyValue}},
	)
	clear(policyValue)
	if err != nil || !policyChanged.Succeeded {
		t.Fatalf("change current policy Keep = %#v, %v", policyChanged, err)
	}
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerEntry := mustOptionalKey(t, store, markerKey)
	for _, key := range []string{
		taskKey(run.TaskID), backupRunKey(run.TaskID), environmentOperationLockKey(run.EnvironmentID),
	} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || markerEntry == nil || entry.ModRevision != markerEntry.ModRevision {
			t.Fatalf("unknown-outcome authority %q = %#v, marker = %#v", key, entry, markerEntry)
		}
	}
	reader, err := newIdempotencyRepository(store)
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
		key    func(BackupRunRecord) string
		remove bool
	}{
		{
			name: "missing immutable reference",
			key: func(run BackupRunRecord) string {
				return backupConfigSnapshotTaskReferenceKey(run.TaskID, run.TaskID)
			},
			remove: true,
		},
		{
			name: "rewritten cursor",
			key:  func(run BackupRunRecord) string { return backupConfigSnapshotKey(run.TaskID) },
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
			unknownIdempotency, err := newIdempotencyRepository(unknown)
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
			mutation := Mutation{Type: MutationPut, Key: key, Value: entry.Value}
			if check.remove {
				mutation = Mutation{Type: MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("change Config companion = %#v, %v", changed, err)
			}
			idempotency, err := newIdempotencyRepository(store)
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
	membership, err := backupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, membership)
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: membership, ModRevision: entry.ModRevision}},
		[]Mutation{{Type: MutationPut, Key: membership, Value: entry.Value}},
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
		run.EnvironmentID,
		BackupRuntimeListRequest{
			Limit:          1,
			StartExclusive: backupRunEnvironmentPrefix + run.EnvironmentID + "/" + run.TaskID,
		},
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ListBackupRunsByEnvironment(cursor without revision) error = %v", err)
	}
}

// Rationale: internal Connector-fence enumeration must preserve raw identity order and the caller's fixed revision.
func TestBackupRuntimeRepositoryPaginatesConnectorPointsAtFixedRevision(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = BackupSourceAttemptStaged
	source.Phase = BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	older := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	newer := older
	newer.CreatedAt = older.CreatedAt.Add(time.Second)
	newer.ID = ids.NewAt(ids.KindRecoveryPoint, newer.CreatedAt, 902)
	newer.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + newer.SourceID + "/" + newer.ID + "/artifact.bin"
	newer.VerifiedAt = older.VerifiedAt.Add(time.Second)
	olderValue, err := encodeBackupRecoveryPointRecord(older)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(olderValue)
	newerValue, err := encodeBackupRecoveryPointRecord(newer)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(newerValue)
	olderIndex, err := backupRecoveryPointConnectorIndexKey(older.ConnectorID, older.ID)
	if err != nil {
		t.Fatal(err)
	}
	newerIndex, err := backupRecoveryPointConnectorIndexKey(newer.ConnectorID, newer.ID)
	if err != nil {
		t.Fatal(err)
	}
	olderEnvironmentIndex, _ := backupRecoveryPointEnvironmentIndexKey(older.EnvironmentID, older.ID)
	olderSourceIndex, _ := backupRecoveryPointSourceIndexKey(older.SourceID, older.ID)
	newerEnvironmentIndex, _ := backupRecoveryPointEnvironmentIndexKey(newer.EnvironmentID, newer.ID)
	newerSourceIndex, _ := backupRecoveryPointSourceIndexKey(newer.SourceID, newer.ID)
	seeded, err := store.Transact(
		context.Background(),
		[]Condition{
			{Key: backupRecoveryPointKey(older.ID)}, {Key: olderIndex}, {Key: olderEnvironmentIndex},
			{Key: olderSourceIndex}, {Key: backupRecoveryPointKey(newer.ID)}, {Key: newerIndex},
			{Key: newerEnvironmentIndex}, {Key: newerSourceIndex},
		},
		[]Mutation{
			{Type: MutationPut, Key: backupRecoveryPointKey(older.ID), Value: olderValue},
			{Type: MutationPut, Key: olderIndex, Value: []byte(older.ID)},
			{Type: MutationPut, Key: olderEnvironmentIndex, Value: []byte(older.ID)},
			{Type: MutationPut, Key: olderSourceIndex, Value: []byte(older.ID)},
			{Type: MutationPut, Key: backupRecoveryPointKey(newer.ID), Value: newerValue},
			{Type: MutationPut, Key: newerIndex, Value: []byte(newer.ID)},
			{Type: MutationPut, Key: newerEnvironmentIndex, Value: []byte(newer.ID)},
			{Type: MutationPut, Key: newerSourceIndex, Value: []byte(newer.ID)},
		},
	)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed connector points = %#v, %v", seeded, err)
	}
	first, err := repository.ListBackupRecoveryPointsByConnector(
		context.Background(), run.ConnectorID, BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.ID != older.ID ||
		first.Next == "" {
		t.Fatalf("first connector point page = %#v, %v", first, err)
	}
	rewritten, err := store.Transact(
		context.Background(),
		[]Condition{{Key: backupRecoveryPointKey(newer.ID), ModRevision: seeded.Revision}},
		[]Mutation{{Type: MutationPut, Key: backupRecoveryPointKey(newer.ID), Value: newerValue}},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite newer point = %#v, %v", rewritten, err)
	}
	second, err := repository.ListBackupRecoveryPointsByConnector(
		context.Background(),
		run.ConnectorID,
		BackupRuntimeListRequest{
			Limit: 1, StartExclusive: first.Next, Revision: first.Revision,
		},
	)
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.ID != newer.ID ||
		second.Items[0].Revision != seeded.Revision || second.Revision != first.Revision {
		t.Fatalf("second connector point page = %#v, %v", second, err)
	}
}

// Rationale: the public 96-item point page validates complete authority at one
// fixed revision without any GetMany call crossing the operation ceiling.
func TestBackupRuntimeRepositoryListsNinetySixPointsAtFixedRevision(t *testing.T) {
	t.Parallel()
	_, store, run := newBackupRuntimeBareFixture(t)
	for index := range maximumBackupRuntimeListLimit {
		createdAt := run.CreatedAt.Add(time.Duration(index+1) * time.Millisecond)
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(950+index))
		source.RecoveryPointCreatedAt = createdAt
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		source.SizeBytes = 123
		source.SHA256 = testBackupDigest
		point := backupRuntimeTestPoint(run, source, createdAt.Add(time.Millisecond))
		value, err := encodeBackupRecoveryPointRecord(point)
		if err != nil {
			t.Fatal(err)
		}
		clear(value)
		seedBackupRuntimePointAuthority(t, store, point)
	}
	audited := &backupRuntimeAuthorityAuditStore{hierarchyStore: store}
	repository, err := newBackupRuntimeRepository(audited)
	if err != nil {
		t.Fatal(err)
	}
	page, err := repository.ListBackupRecoveryPointsByEnvironment(
		context.Background(), run.EnvironmentID,
		BackupRuntimeListRequest{Limit: maximumBackupRuntimeListLimit},
	)
	if err != nil || len(page.Items) != maximumBackupRuntimeListLimit ||
		audited.maximumKeys > maximumTransactionOperations {
		t.Fatalf("ListBackupRecoveryPointsByEnvironment(96) = %#v, max keys %d, %v",
			page, audited.maximumKeys, err)
	}
	for _, revision := range audited.revisions {
		if revision != page.Revision {
			t.Fatalf("fixed GetMany revisions = %v, page revision %d", audited.revisions, page.Revision)
		}
	}
}

// Rationale: a continuation cursor and every fixed-revision authority chunk
// are durable evidence; malformed shapes fail closed instead of widening reads.
func TestBackupRuntimeRepositoryRejectsMalformedPointCursorAndChunk(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	if _, err := repository.ListBackupRecoveryPointsByEnvironment(
		context.Background(), run.EnvironmentID, BackupRuntimeListRequest{
			Limit: 1, Revision: 1,
			StartExclusive: backupRecoveryPointEnvironmentPrefix + run.EnvironmentID + "/bad/cursor",
		},
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("malformed point cursor error = %v", err)
	}
	source := run.Sources[0]
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	seedBackupRuntimePointAuthority(t, store, point)
	malformed := &backupRuntimeAuthorityAuditStore{hierarchyStore: store, truncateNextChunk: true}
	malformedRepository, err := newBackupRuntimeRepository(malformed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := malformedRepository.ListBackupRecoveryPointsByEnvironment(
		context.Background(), run.EnvironmentID, BackupRuntimeListRequest{Limit: 1},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("malformed fixed-revision chunk error = %v", err)
	}
}

// Rationale: transaction preflight accepts the exact byte boundary; exceeding
// a locked internal composition ceiling is an implementation defect, not bad input.
func TestBackupRuntimeRepositoryTransactionBounds(t *testing.T) {
	t.Parallel()
	conditions := make([]Condition, maximumTransactionOperations)
	if err := validateBackupRuntimeTransactionBounds(conditions, nil); err != nil {
		t.Fatalf("exact operation boundary error = %v", err)
	}
	conditions = make([]Condition, maximumTransactionOperations+1)
	if err := validateBackupRuntimeTransactionBounds(conditions, nil); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("operation overflow error = %v", err)
	}
	key := "/v1/runtime/boundary"
	value := make([]byte, maximumBackupRuntimeTransactionBytes-len(key)-64)
	if err := validateBackupRuntimeTransactionBounds(
		nil,
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	); err != nil {
		t.Fatalf("exact byte boundary error = %v", err)
	}
	value = append(value, 0)
	if err := validateBackupRuntimeTransactionBounds(
		nil,
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("byte overflow error = %v", err)
	}
}

// Rationale: the full twelve-source contract must remain composable after
// primary-owned derived membership removes redundant absence compares.
func TestBackupRuntimeRepositoryPreparesTwelveSourcePublicationWithinBounds(t *testing.T) {
	t.Parallel()
	repository, _, run := newBackupRuntimeBareFixture(t)
	for ordinal := uint32(1); ordinal < MaximumBackupPolicySources; ordinal++ {
		source := testBackupLaterSource(run.CreatedAt, ordinal, BackupSourceAttemptPending)
		source.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		run.Sources = append(run.Sources, source)
	}
	extendBackupRuntimePublicationSources(t, repository.store, &run)
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
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
	if len(plan.conditions)+len(plan.mutations) > maximumTransactionOperations {
		t.Fatalf(
			"12-source publication operations = %d, want <= %d",
			len(plan.conditions)+len(plan.mutations),
			maximumTransactionOperations,
		)
	}
	exclusions, err := backupRunExclusionRecords(run, run.CreatedAt)
	if err != nil || len(exclusions) != MaximumBackupPolicySources {
		t.Fatalf("12-source exclusions = %d, %v", len(exclusions), err)
	}
}

// Rationale: exclusion cleanup is an all-or-none authority transition; a
// partial set cannot be accepted as replay or opportunistically repaired.
func TestBackupRuntimeRepositoryRejectsMixedExclusionRelease(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	second := testBackupLaterSource(run.CreatedAt, 1, BackupSourceAttemptPending)
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
	failed.State = BackupRunFailed
	failed.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = BackupFailureCapture
	failed.Sources[1].State = BackupSourceAttemptUnstarted
	failed.Sources[1].Phase = BackupSourcePhaseCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	firstKey, err := backupSourceTargetExclusionKey(
		BackupSourceTargetAttach,
		run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustOptionalKey(t, store, firstKey)
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: firstKey, ModRevision: entry.ModRevision}},
		[]Mutation{{Type: MutationDelete, Key: firstKey}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("delete one exclusion = %#v, %v", result, err)
	}
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), created, failed,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(mixed) error = %v", err)
	}
	secondKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, second.TargetID)
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
	failed.State = BackupRunFailed
	failed.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	failed.Sources[0].State = BackupSourceAttemptFailed
	failed.Sources[0].FailureCode = BackupFailureCapture
	failed.UpdatedAt = run.UpdatedAt.Add(time.Second)
	plan, err := repository.prepareBackupRunTerminal(context.Background(), created, failed)
	if err != nil {
		t.Fatalf("prepareBackupRunTerminal() error = %v", err)
	}
	defer plan.clear()
	marker := "/v1/test/backup-terminal-tasks/" + run.TaskID
	conditions, mutations, err := plan.composeTransaction(
		[]Condition{{Key: marker}},
		[]Mutation{{Type: MutationPut, Key: marker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose terminal backup run = %v", err)
	}
	result, err := repository.transact(context.Background(), conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("terminal backup transaction = %#v, %v", result, err)
	}
	exclusionKey, err := backupSourceTargetExclusionKey(
		BackupSourceTargetAttach, run.Sources[0].TargetID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{exclusionKey, environmentOperationLockKey(run.EnvironmentID)} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	for _, key := range []string{backupRunKey(run.TaskID), marker, environmentMutationEpochKey(run.EnvironmentID)} {
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
	cleanupPending.State = BackupRunRunning
	cleanupPending.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	cleanupPending.Sources[0].State = BackupSourceAttemptCleanupPending
	cleanupPending.Sources[0].Phase = BackupSourcePhaseCleanup
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
	completed.State = BackupRunCompleted
	completed.Sources = append(
		[]BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	completed.Sources[0].State = BackupSourceAttemptSucceeded
	completed.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), cleanupVersion, completed,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("prepareBackupRunTerminal(uncheckpointed cleanup) error = %v", err)
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload = BackupCheckpointPayload{
		Kind:    BackupCheckpointSourceCleanupCompleted,
		PointID: cleanupPending.Sources[0].RecoveryPointID,
	}
	succeeded := cleanupPending
	succeeded.Sources = append(
		[]BackupRunSourceAttemptRecord(nil),
		cleanupPending.Sources...,
	)
	succeeded.Sources[0].State = BackupSourceAttemptSucceeded
	succeeded.UpdatedAt = cleanupPending.UpdatedAt.Add(time.Second)
	succeededVersion, err := repository.CheckpointBackupRun(
		context.Background(), checkpoint, cleanupVersion, succeeded,
	)
	if err != nil {
		t.Fatalf("CheckpointBackupRun(cleanup) error = %v", err)
	}
	completed = succeeded
	completed.State = BackupRunCompleted
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
		state   BackupRunState
		failure BackupFailureCode
		phase   BackupSourceAttemptPhase
	}{
		{
			name: "failed upload", state: BackupRunFailed, failure: BackupFailureUpload,
			phase: BackupSourcePhaseUpload,
		},
		{
			name: "aborted head verification", state: BackupRunAborted, failure: BackupFailureAborted,
			phase: BackupSourcePhaseHeadVerification,
		},
		{
			name: "timed out point commit", state: BackupRunTimedOut, failure: BackupFailureTimedOut,
			phase: BackupSourcePhasePointCommit,
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
			staged.State = BackupRunRunning
			staged.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
			staged.Sources[0].State = BackupSourceAttemptStaged
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
				[]BackupRunSourceAttemptRecord(nil),
				staged.Sources...,
			)
			withoutOrphan.Sources[0].State = BackupSourceAttemptFailed
			withoutOrphan.Sources[0].FailureCode = test.failure
			withoutOrphan.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			if _, err := repository.prepareBackupRunTerminal(
				context.Background(), stagedVersion, withoutOrphan,
			); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("prepareBackupRunTerminal(without orphan) error = %v", err)
			}
			terminal := withoutOrphan
			terminal.Sources = append(
				[]BackupRunSourceAttemptRecord(nil),
				withoutOrphan.Sources...,
			)
			terminal.Sources[0].State = BackupSourceAttemptOrphaned
			for _, mutate := range []struct {
				name string
				run  func(*BackupRunSourceAttemptRecord)
			}{
				{name: "empty", run: func(source *BackupRunSourceAttemptRecord) {
					source.SizeBytes = 0
					source.SHA256 = ""
				}},
				{name: "malformed", run: func(source *BackupRunSourceAttemptRecord) {
					source.SHA256 = "not-a-sha256"
				}},
				{name: "substituted", run: func(source *BackupRunSourceAttemptRecord) {
					source.SizeBytes++
				}},
			} {
				changed := terminal
				changed.Sources = append([]BackupRunSourceAttemptRecord(nil), terminal.Sources...)
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
				[]Condition{{Key: marker}},
				[]Mutation{{Type: MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.transact(context.Background(), conditions, mutations)
			clearBackupRuntimeMutations(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize upload intent = %#v, %v", result, err)
			}
			pointID := staged.Sources[0].RecoveryPointID
			connectorIndex, err := backupOrphanConnectorIndexKey(run.ConnectorID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			environmentIndex, err := backupOrphanEnvironmentIndexKey(run.EnvironmentID, pointID)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{
				backupOrphanKey(pointID), connectorIndex, environmentIndex, marker,
			} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != result.Revision {
					t.Fatalf("terminal orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)) != nil {
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
		state       BackupRunState
		failureCode BackupFailureCode
	}{
		{name: "failed", state: BackupRunFailed, failureCode: BackupFailurePointCommit},
		{name: "aborted", state: BackupRunAborted, failureCode: BackupFailureAborted},
		{name: "timed out", state: BackupRunTimedOut, failureCode: BackupFailureTimedOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run := newBackupRuntimeRepositoryFixture(t)
			stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
			orphaned := staged
			orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
			orphaned.Sources[0].State = BackupSourceAttemptOrphaned
			orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
			point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
			orphan := BackupOrphanRecord{
				Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
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
			terminal.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
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
				[]Condition{{Key: marker}},
				[]Mutation{{Type: MutationPut, Key: marker, Value: []byte(run.TaskID)}},
			)
			if err != nil {
				plan.clear()
				t.Fatal(err)
			}
			result, err := repository.transact(context.Background(), conditions, mutations)
			clearBackupRuntimeMutations(mutations)
			plan.clear()
			if err != nil || !result.Succeeded {
				t.Fatalf("terminalize retained orphan = %#v, %v", result, err)
			}
			connectorIndex, _ := backupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
			environmentIndex, _ := backupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
			for _, key := range []string{backupOrphanKey(point.ID), connectorIndex, environmentIndex} {
				entry := mustOptionalKey(t, store, key)
				if entry == nil || entry.ModRevision != storedOrphan.Revision || entry.Version != 1 {
					t.Fatalf("retained orphan companion %q = %#v", key, entry)
				}
			}
			if mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)) != nil {
				t.Fatal("retained orphan terminal kept its Environment lock")
			}
			deleting := storedOrphan.Record
			deleting.State = BackupOrphanDelete
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
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
	pruned, err := store.Transact(context.Background(), []Condition{
		{Key: backupRunKey(run.TaskID), ModRevision: orphanedVersion.Revision},
	}, []Mutation{
		{Type: MutationDelete, Key: backupRunKey(run.TaskID)},
		{Type: MutationDelete, Key: taskKey(run.TaskID)},
	})
	if err != nil || !pruned.Succeeded {
		t.Fatalf("prune originating Task/run = %#v, %v", pruned, err)
	}
	point.VerifiedAt = storedOrphan.Record.UpdatedAt.Add(time.Second)
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:     storedOrphan.Record.Reconciliation.RetentionKeep,
		Revision: storedOrphan.Record.Reconciliation.PolicyRevision,
		State:    BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	adopted, err := repository.AdoptReconciledBackupOrphan(
		context.Background(), storedOrphan, point, sweep,
	)
	if err != nil {
		t.Fatalf("AdoptReconciledBackupOrphan(pruned Task) error = %v", err)
	}
	if adopted.Record != point || mustOptionalKey(t, store, backupOrphanKey(point.ID)) != nil {
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
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
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
	environmentIndex, _ := backupOrphanEnvironmentIndexKey(run.EnvironmentID, point.ID)
	entry := mustOptionalKey(t, store, environmentIndex)
	rewritten, err := store.Transact(
		context.Background(),
		[]Condition{{Key: environmentIndex, ModRevision: entry.ModRevision}},
		[]Mutation{{Type: MutationPut, Key: environmentIndex, Value: entry.Value}},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite orphan companion = %#v, %v", rewritten, err)
	}
	terminal := orphaned
	terminal.State = BackupRunFailed
	terminal.Sources = append([]BackupRunSourceAttemptRecord(nil), orphaned.Sources...)
	terminal.Sources[0].FailureCode = BackupFailurePointCommit
	terminal.UpdatedAt = orphaned.UpdatedAt.Add(time.Second)
	if _, err := repository.prepareBackupRunTerminal(
		context.Background(), orphanedVersion, terminal,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("prepareBackupRunTerminal(rewritten orphan) error = %v", err)
	}
	if mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)) == nil {
		t.Fatal("rewritten orphan companion released the Environment lock")
	}
}

// Rationale: future manual Task publication must be able to commit the Task,
// lock, run membership, and exclusions in one transaction with no standalone
// pre-publication write.
func TestBackupRuntimeRepositoryPreparesComposableRunPublication(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
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
		[]Condition{{Key: taskMarker}},
		[]Mutation{{Type: MutationPut, Key: taskMarker, Value: []byte(run.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose backup publication = %v", err)
	}
	result, err := repository.transact(context.Background(), conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("composed backup publication = %#v, %v", result, err)
	}
	membership, err := backupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		backupRunKey(run.TaskID), membership, environmentOperationLockKey(run.EnvironmentID), taskMarker,
	} {
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
	fabricatedRun.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
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
	idempotency, err := newIdempotencyRepository(store)
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
		claim.Assignment.Record.AssignmentID,
		TaskStatusFailed,
		failedResult,
		run.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(Backup failure) error = %v", err)
	}
	committedRevision := terminal.Revision
	for _, key := range []string{taskKey(run.TaskID), backupRunKey(run.TaskID)} {
		entry := mustOptionalKey(t, store, key)
		if entry == nil || entry.ModRevision != committedRevision {
			t.Fatalf("terminal authority %q = %#v", key, entry)
		}
	}
	failedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || failedRun.Record.State != BackupRunFailed ||
		failedRun.Record.Sources[0].State != BackupSourceAttemptFailed ||
		failedRun.Record.Sources[0].FailureCode != BackupFailureCapture {
		t.Fatalf("terminal Backup run = %#v, %v", failedRun, err)
	}
	if mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)) != nil {
		t.Fatal("terminal Backup Task retained its Environment lock")
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
		run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay) = %#v, %v", replay, err)
	}
	newerAt := run.CreatedAt.Add(4 * time.Second)
	putBackupRuntimeLock(t, store, BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   ids.NewAt(ids.KindOperation, newerAt, 1991),
		TaskID:        ids.NewAt(ids.KindTask, newerAt, 1992),
		Kind:          BackupOperationRestore,
		CreatedAt:     newerAt,
		UpdatedAt:     newerAt,
	})
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
		newerAt.Add(time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(Backup replay after successor lock) = %#v, %v", replay, err)
	}
	membership, err := backupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tornMembership, err := store.Transact(
		context.Background(),
		nil,
		[]Mutation{{Type: MutationDelete, Key: membership}},
	)
	if err != nil || !tornMembership.Succeeded {
		t.Fatalf("remove Backup run membership = %#v, %v", tornMembership, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
		newerAt.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with torn membership) error = %v", err)
	}
	incompleteCascade, err := store.Transact(
		context.Background(),
		nil,
		[]Mutation{
			{Type: MutationDelete, Key: environmentKey(run.EnvironmentID)},
			{Type: MutationDelete, Key: environmentOperationLockKey(run.EnvironmentID)},
			{Type: MutationDelete, Key: backupRunKey(run.TaskID)},
		},
	)
	if err != nil || !incompleteCascade.Succeeded {
		t.Fatalf("simulate incomplete Environment owner cascade = %#v, %v", incompleteCascade, err)
	}
	if _, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
		newerAt.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(Backup replay with retained epoch) error = %v", err)
	}
	cascade, err := store.Transact(
		context.Background(),
		nil,
		[]Mutation{{Type: MutationDelete, Key: environmentMutationEpochKey(run.EnvironmentID)}},
	)
	if err != nil || !cascade.Succeeded {
		t.Fatalf("complete Environment owner cascade = %#v, %v", cascade, err)
	}
	replay, err = tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
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
	idempotency, err := newIdempotencyRepository(store)
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
		context.Background(), run.TaskID, TaskStatusCompleted, run.CreatedAt.Add(time.Second),
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
	running.State = BackupRunRunning
	running.Sources = append([]BackupRunSourceAttemptRecord(nil), current.Record.Sources...)
	running.Sources[0].State = BackupSourceAttemptSucceeded
	running.Sources[0].Phase = BackupSourcePhaseCleanup
	running.Sources[0].SizeBytes = 123
	running.Sources[0].SHA256 = testBackupDigest
	running.UpdatedAt = run.CreatedAt.Add(2 * time.Second)
	value, err := encodeBackupRunRecord(running)
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := store.Transact(
		context.Background(),
		[]Condition{{Key: backupRunKey(run.TaskID), ModRevision: current.Revision}},
		[]Mutation{{Type: MutationPut, Key: backupRunKey(run.TaskID), Value: value}},
	)
	clear(value)
	if err != nil || !replaced.Succeeded {
		t.Fatalf("seed completed source state = %#v, %v", replaced, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, run.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusCompleted,
		completedComposeTaskResult(), run.CreatedAt.Add(3*time.Second),
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(Backup completed) = %#v, %v", terminal, err)
	}
	completed, err := repository.GetBackupRun(context.Background(), run.TaskID)
	if err != nil || completed.Record.State != BackupRunCompleted || completed.Revision != terminal.Revision {
		t.Fatalf("completed Backup run = %#v, %v", completed, err)
	}
}

// Rationale: individually valid domain records must not authorize terminalizing
// a Task whose operation, owner, target, creation, or retry identity differs.
func TestBackupTaskTerminalBindingRejectsRewrittenDomainIdentity(t *testing.T) {
	_, store, run := newBackupRuntimeBareFixture(t)
	task, _, _, _ := backupRuntimePublicationTask(t, store, run)
	if err := validateBackupRunTaskBinding(task, run); err != nil {
		t.Fatalf("validateBackupRunTaskBinding(valid) error = %v", err)
	}
	rewrittenRun := run
	rewrittenRun.OperationID = ids.NewAt(ids.KindOperation, run.CreatedAt, 1994)
	if err := validateBackupRunTaskBinding(task, rewrittenRun); !errors.Is(
		err, errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("validateBackupRunTaskBinding(rewritten) error = %v", err)
	}
	pruneTask := task
	pruneTask.Type = TaskBackupPrune
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID: task.ID, OperationID: task.OperationID,
		EnvironmentID: run.EnvironmentID, CreatedAt: task.CreatedAt,
		RecoveryPointIDs: []string{run.Sources[0].RecoveryPointID},
	}
	if err := validateBackupPruneTaskBinding(pruneTask, dispatch); err != nil {
		t.Fatalf("validateBackupPruneTaskBinding(valid) error = %v", err)
	}
	dispatch.EnvironmentID = ids.NewAt(ids.KindEnvironment, run.CreatedAt, 1995)
	if err := validateBackupPruneTaskBinding(pruneTask, dispatch); !errors.Is(
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
		status          TaskStatus
		runState        BackupRunState
		failure         BackupFailureCode
		useTimeout      bool
		useAgentTimeout bool
	}{
		{
			name: "pending abort", pending: true, status: TaskStatusAborted,
			runState: BackupRunAborted, failure: BackupFailureAborted,
		},
		{
			name: "running abort", status: TaskStatusAborted,
			runState: BackupRunAborted, failure: BackupFailureAborted,
		},
		{
			name: "running timeout", status: TaskStatusTimedOut,
			runState: BackupRunTimedOut, failure: BackupFailureTimedOut, useTimeout: true,
		},
		{
			name: "stale Agent timeout", status: TaskStatusTimedOut,
			runState: BackupRunTimedOut, failure: BackupFailureTimedOut, useAgentTimeout: true,
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
			idempotency, err := newIdempotencyRepository(store)
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

			var terminal Versioned[TaskRecord]
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
						test.status,
						TaskResultRecord{
							Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
							ReconciliationRequired: true,
						},
						run.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal Backup Task = %#v, %v", terminal, err)
			}
			storedRun, err := repository.GetBackupRun(context.Background(), run.TaskID)
			if err != nil || storedRun.Record.State != test.runState ||
				storedRun.Record.Sources[0].State != BackupSourceAttemptFailed ||
				storedRun.Record.Sources[0].FailureCode != test.failure ||
				storedRun.Revision != terminal.Revision {
				t.Fatalf("terminal Backup run = %#v, %v", storedRun, err)
			}
			if mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)) != nil {
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
				result := TaskResultRecord{
					Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
					ReconciliationRequired: true,
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
	idempotency, err := newIdempotencyRepository(store)
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
		if getErr != nil || terminal.Record.Status != TaskStatusTimedOut {
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
	idempotency, err := newIdempotencyRepository(store)
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
		if getErr != nil || terminal.Record.Status != TaskStatusTimedOut {
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
		status     TaskStatus
		useTimeout bool
	}{
		{name: "pending abort", pending: true, status: TaskStatusAborted},
		{name: "running abort", status: TaskStatusAborted},
		{name: "running timeout", status: TaskStatusTimedOut, useTimeout: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, store, tasks, dispatch, pointID := publishBackupPruneLifecycleTask(t)
			agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 3990)
			var terminal Versioned[TaskRecord]
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
						test.status,
						TaskResultRecord{
							Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
							ReconciliationRequired: true,
						},
						dispatch.CreatedAt.Add(2*time.Second),
					)
				}
			}
			if err != nil || terminal.Record.Status != test.status {
				t.Fatalf("terminal prune Task = %#v, %v", terminal, err)
			}
			if mustOptionalKey(t, store, backupRecoveryPointPruneDispatchKey(dispatch.TaskID)) != nil ||
				mustOptionalKey(t, store, environmentOperationLockKey(dispatch.EnvironmentID)) != nil {
				t.Fatal("terminal prune retained dispatch or Environment lock")
			}
			entry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
			if entry == nil {
				t.Fatal("terminal prune lost its surviving tombstone")
			}
			prune, decodeErr := decodeBackupRecoveryPointPruneRecord(entry.Value)
			if decodeErr != nil || prune.State != BackupPrunePending || prune.TaskID != "" ||
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
				result := TaskResultRecord{
					Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
					ReconciliationRequired: true,
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
						[]Mutation{{
							Type: MutationDelete,
							Key:  environmentMutationEpochKey(dispatch.EnvironmentID),
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
	*TaskRepository,
	BackupRecoveryPointPruneDispatchRecord,
	string,
) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = BackupSourceAttemptStaged
	source.Phase = BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	point := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.NewAt(ids.KindOperation, run.CreatedAt.Add(time.Second), 3991)
	keys := []string{
		backupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex,
		backupRecoveryPointPruneKey(point.ID),
	}
	seeded, err := store.Transact(context.Background(), []Condition{
		{Key: keys[0]}, {Key: keys[1]}, {Key: keys[2]}, {Key: keys[3]}, {Key: keys[4]},
	}, []Mutation{
		{Type: MutationPut, Key: keys[0], Value: pointValue},
		{Type: MutationPut, Key: keys[1], Value: []byte(point.ID)},
		{Type: MutationPut, Key: keys[2], Value: []byte(point.ID)},
		{Type: MutationPut, Key: keys[3], Value: []byte(point.ID)},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed prune point = %#v, %v", seeded, err)
	}
	prune := BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: seeded.Revision,
		OperationID: operationID, State: BackupPrunePending,
		CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	pruneValue, err := encodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneSeeded, err := store.Transact(
		context.Background(),
		[]Condition{{Key: keys[0], ModRevision: seeded.Revision}, {Key: keys[4]}},
		[]Mutation{{Type: MutationPut, Key: keys[4], Value: pruneValue}},
	)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed prune authority = %#v, %v", pruneSeeded, err)
	}
	pending := []Versioned[BackupRecoveryPointPruneRecord]{{
		Record: prune, Revision: pruneSeeded.Revision, ReadRevision: pruneSeeded.Revision,
	}}
	createdAt := run.CreatedAt.Add(2 * time.Second)
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID: ids.NewAt(ids.KindTask, createdAt, 3992), OperationID: operationID,
		EnvironmentID: run.EnvironmentID, RecoveryPointIDs: []string{point.ID}, CreatedAt: createdAt,
	}
	plan, err := repository.prepareBackupPrunePublication(
		context.Background(), pending, dispatch, BackupOperationLockRecord{
			EnvironmentID: dispatch.EnvironmentID, OperationID: dispatch.OperationID,
			TaskID: dispatch.TaskID, Kind: BackupOperationPrune,
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
	idempotency, err := newIdempotencyRepository(store)
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
	source.State = BackupSourceAttemptStaged
	source.Phase = BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	verifiedAt := run.CreatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, source, verifiedAt)
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	pointCommit, err := store.Transact(context.Background(), []Condition{
		{Key: backupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []Mutation{
		{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: pointValue},
		{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
	})
	if err != nil || !pointCommit.Succeeded {
		t.Fatalf("seed point = %#v, %v", pointCommit, err)
	}
	pendingSweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: BackupRetentionPending,
		CreatedAt: verifiedAt, UpdatedAt: verifiedAt,
	}
	pendingSweepValue, err := encodeBackupRetentionSweepRecord(pendingSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCreated, err := store.Transact(
		context.Background(),
		[]Condition{{Key: backupRetentionKey(point.SourceID, point.ID)}},
		[]Mutation{{
			Type: MutationPut, Key: backupRetentionKey(point.SourceID, point.ID), Value: pendingSweepValue,
		}},
	)
	clear(pendingSweepValue)
	if err != nil || !sweepCreated.Succeeded {
		t.Fatalf("seed pending retention sweep = %#v, %v", sweepCreated, err)
	}
	completedSweep := pendingSweep
	completedSweep.State = BackupRetentionCompleted
	completedSweep.SelectionRevision = pointCommit.Revision
	completedSweep.PruneOperationID = run.OperationID
	completedSweep.UpdatedAt = verifiedAt.Add(time.Second)
	completedSweepValue, err := encodeBackupRetentionSweepRecord(completedSweep)
	if err != nil {
		t.Fatal(err)
	}
	sweepCompleted, err := store.Transact(
		context.Background(),
		[]Condition{{
			Key: backupRetentionKey(point.SourceID, point.ID), ModRevision: sweepCreated.Revision,
		}},
		[]Mutation{{
			Type: MutationPut, Key: backupRetentionKey(point.SourceID, point.ID), Value: completedSweepValue,
		}},
	)
	clear(completedSweepValue)
	if err != nil || !sweepCompleted.Succeeded {
		t.Fatalf("complete retention sweep = %#v, %v", sweepCompleted, err)
	}
	prune := BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: pointCommit.Revision,
		OperationID: run.OperationID,
		State:       BackupPrunePending, CreatedAt: verifiedAt.Add(time.Second),
		UpdatedAt: verifiedAt.Add(time.Second),
	}
	pruneValue, err := encodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pruneCreate, err := store.Transact(
		context.Background(),
		[]Condition{{Key: backupRecoveryPointPruneKey(point.ID)}},
		[]Mutation{
			{Type: MutationPut, Key: backupRecoveryPointPruneKey(point.ID), Value: pruneValue},
		},
	)
	if err != nil || !pruneCreate.Succeeded {
		t.Fatalf("seed prune = %#v, %v", pruneCreate, err)
	}
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: run.OperationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID},
		CreatedAt:        prune.UpdatedAt.Add(time.Second),
	}
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: BackupOperationPrune, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt,
	}
	publication, err := repository.prepareBackupPrunePublication(
		context.Background(),
		[]Versioned[BackupRecoveryPointPruneRecord]{
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
		[]Condition{{Key: publicationMarker}},
		[]Mutation{{Type: MutationPut, Key: publicationMarker, Value: []byte(dispatch.TaskID)}},
	)
	if err != nil {
		t.Fatalf("compose prune publication = %v", err)
	}
	published, err := repository.transact(context.Background(), conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	publication.clear()
	if err != nil || !published.Succeeded {
		t.Fatalf("publish prune = %#v, %v", published, err)
	}
	assignedEntry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(point.ID))
	assigned, err := decodeBackupRecoveryPointPruneRecord(assignedEntry.Value)
	if err != nil || assigned.State != BackupPruneAssigned {
		t.Fatalf("assigned prune = %#v, %v", assigned, err)
	}
	dispatchVersion := Versioned[BackupRecoveryPointPruneDispatchRecord]{
		Record: dispatch, Revision: published.Revision, ReadRevision: published.Revision,
	}
	assignedVersion := Versioned[BackupRecoveryPointPruneRecord]{
		Record: assigned, Revision: assignedEntry.ModRevision, ReadRevision: published.Revision,
	}
	verified := assigned
	verified.State = BackupPruneVerifiedAbsent
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
	for _, key := range []string{
		backupRecoveryPointKey(point.ID), environmentIndex, sourceIndex, connectorIndex,
		backupRetentionKey(point.SourceID, point.ID),
	} {
		if entry := mustOptionalKey(t, store, key); entry != nil {
			t.Fatalf("verified-absent point authority %q = %#v", key, entry)
		}
	}
	completion, err := repository.prepareBackupPruneFailure(
		context.Background(),
		dispatchVersion,
		[]Versioned[BackupRecoveryPointPruneRecord]{checkpoint},
		checkpoint.Record.UpdatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("prepareBackupPruneFailure() error = %v", err)
	}
	conditions, mutations, err = completion.composeTransaction(
		[]Condition{{Key: publicationMarker, ModRevision: published.Revision}},
		[]Mutation{{Type: MutationDelete, Key: publicationMarker}},
	)
	if err != nil {
		t.Fatalf("compose prune completion = %v", err)
	}
	completed, err := repository.transact(context.Background(), conditions, mutations)
	clearBackupRuntimeMutations(mutations)
	completion.clear()
	if err != nil || !completed.Succeeded {
		t.Fatalf("complete prune = %#v, %v", completed, err)
	}
	for _, key := range []string{
		backupRecoveryPointPruneKey(point.ID),
		backupRecoveryPointPruneDispatchKey(dispatch.TaskID),
		environmentOperationLockKey(run.EnvironmentID), publicationMarker,
	} {
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
	secondaryConnectorValue, err := encodeConnectorRecord(secondaryConnector)
	if err != nil {
		t.Fatal(err)
	}
	secondaryConnectorCreated, err := store.Transact(
		context.Background(),
		[]Condition{{Key: connectorRecordKey(secondaryConnector.Connector.ID)}},
		[]Mutation{{
			Type: MutationPut, Key: connectorRecordKey(secondaryConnector.Connector.ID),
			Value: secondaryConnectorValue,
		}},
	)
	clear(secondaryConnectorValue)
	if err != nil || !secondaryConnectorCreated.Succeeded {
		t.Fatalf("seed secondary prune Connector = %#v, %v", secondaryConnectorCreated, err)
	}
	pending := make([]Versioned[BackupRecoveryPointPruneRecord], maximumBackupPruneBatch)
	pointIDs := make([]string, maximumBackupPruneBatch)
	mutations := make([]Mutation, 0, maximumBackupPruneBatch*4)
	for index := range pending {
		allocatedAt := run.CreatedAt.Add(time.Duration(index) * time.Millisecond)
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, allocatedAt, int64(2200+index))
		source.RecoveryPointCreatedAt = allocatedAt
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		source.State = BackupSourceAttemptStaged
		source.Phase = BackupSourcePhasePointCommit
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
		record := BackupRecoveryPointPruneRecord{
			Point:       point.BackupRecoveryPointSnapshot,
			OperationID: operationID,
			State:       BackupPrunePending,
			CreatedAt:   allocatedAt.Add(2 * time.Second),
			UpdatedAt:   allocatedAt.Add(2 * time.Second),
		}
		pointIDs[index] = point.ID
		pending[index] = Versioned[BackupRecoveryPointPruneRecord]{Record: record}
		pointValue, err := encodeBackupRecoveryPointRecord(point)
		if err != nil {
			clearBackupRuntimeMutations(mutations)
			t.Fatal(err)
		}
		environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
		if err != nil {
			clear(pointValue)
			clearBackupRuntimeMutations(mutations)
			t.Fatal(err)
		}
		sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
		if err != nil {
			clear(pointValue)
			clearBackupRuntimeMutations(mutations)
			t.Fatal(err)
		}
		connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
		if err != nil {
			clear(pointValue)
			clearBackupRuntimeMutations(mutations)
			t.Fatal(err)
		}
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: pointValue},
			Mutation{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
			Mutation{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
			Mutation{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)})
	}
	pointSeeded, err := store.Transact(context.Background(), nil, mutations)
	clearBackupRuntimeMutations(mutations)
	if err != nil || !pointSeeded.Succeeded {
		t.Fatalf("seed eleven prune points = %#v, %v", pointSeeded, err)
	}
	mutations = make([]Mutation, 0, maximumBackupPruneBatch)
	for index := range pending {
		pending[index].Record.PointRevision = pointSeeded.Revision
		value, encodeErr := encodeBackupRecoveryPointPruneRecord(pending[index].Record)
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			t.Fatal(encodeErr)
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: backupRecoveryPointPruneKey(pointIDs[index]), Value: value,
		})
	}
	pruneSeeded, err := store.Transact(context.Background(), nil, mutations)
	clearBackupRuntimeMutations(mutations)
	if err != nil || !pruneSeeded.Succeeded {
		t.Fatalf("seed eleven prune authorities = %#v, %v", pruneSeeded, err)
	}
	for index := range pending {
		pending[index].Revision = pruneSeeded.Revision
		pending[index].ReadRevision = pruneSeeded.Revision
	}
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID:           run.TaskID,
		OperationID:      operationID,
		EnvironmentID:    run.EnvironmentID,
		RecoveryPointIDs: pointIDs,
		CreatedAt:        run.CreatedAt.Add(3 * time.Second),
	}
	lock := BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID,
		OperationID:   operationID,
		TaskID:        run.TaskID,
		Kind:          BackupOperationPrune,
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
	idempotency, err := newIdempotencyRepository(store)
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
	assigned := make([]Versioned[BackupRecoveryPointPruneRecord], len(pointIDs))
	failedResult := completedComposeTaskResult()
	failedResult.ExitCode = 1
	failedTask, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		dispatch.TaskID,
		claim.Assignment.Record.AssignmentID,
		TaskStatusFailed,
		failedResult,
		dispatch.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failedTask.Record.Status != TaskStatusFailed {
		t.Fatalf("AcknowledgeTask(prune failure) = %#v, %v", failedTask, err)
	}
	replay, err := tasks.AcknowledgeTask(
		context.Background(), agentID, 1, dispatch.TaskID,
		claim.Assignment.Record.AssignmentID, TaskStatusFailed, failedResult,
		dispatch.CreatedAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != failedTask.Revision {
		t.Fatalf("AcknowledgeTask(prune replay) = %#v, %v", replay, err)
	}
	if dispatchEntry := mustOptionalKey(
		t,
		store,
		backupRecoveryPointPruneDispatchKey(dispatch.TaskID),
	); dispatchEntry != nil {
		t.Fatalf("failed prune dispatch = %#v", dispatchEntry)
	}
	if lockEntry := mustOptionalKey(t, store, environmentOperationLockKey(dispatch.EnvironmentID)); lockEntry != nil {
		t.Fatalf("failed prune lock = %#v", lockEntry)
	}
	pending = make([]Versioned[BackupRecoveryPointPruneRecord], len(pointIDs))
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
		record, decodeErr := decodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != BackupPrunePending || record.TaskID != "" ||
			record.OperationID != dispatch.OperationID {
			t.Fatalf("failed prune pending authority = %#v, %v", record, decodeErr)
		}
		pending[index] = Versioned[BackupRecoveryPointPruneRecord]{
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
		context.Background(), pending, nextDispatch, BackupOperationLockRecord{
			EnvironmentID: nextDispatch.EnvironmentID, OperationID: nextDispatch.OperationID,
			TaskID: nextDispatch.TaskID, Kind: BackupOperationPrune,
			CreatedAt: transferredAt, UpdatedAt: transferredAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask, err := cloneRetryTask(
		sourceTask.Record, nextDispatch.TaskID, TaskActorSystem, transferredAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryTask.PlanID = ids.NewAt(ids.KindPlan, transferredAt, 2450)
	retrySealed := backupRuntimeSealedPrunePlan(t, store, nextDispatch, pending, retryTask.PlanID)
	retryTask.PlanHash = hex.EncodeToString(retrySealed.PlanHash)
	retryTask.Steps = make([]TaskStepRecord, len(retrySealed.Steps))
	for index, step := range retrySealed.Steps {
		retryTask.Steps[index] = TaskStepRecord{Kind: TaskStepOperation, ID: step.StepId}
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
	if retryEntry := mustOptionalKey(t, store, taskKey(nextDispatch.TaskID)); retryEntry != nil {
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
	transferredRevision := mustOptionalKey(t, store, taskKey(nextDispatch.TaskID)).ModRevision
	for index, pointID := range pointIDs {
		entry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
		record, decodeErr := decodeBackupRecoveryPointPruneRecord(entry.Value)
		if decodeErr != nil || record.State != BackupPruneAssigned || record.TaskID != nextDispatch.TaskID {
			t.Fatalf("retry assigned prune = %#v, %v", record, decodeErr)
		}
		assigned[index] = Versioned[BackupRecoveryPointPruneRecord]{
			Record: record, Revision: entry.ModRevision, ReadRevision: transferredRevision,
		}
	}
	claim, found, err = tasks.ClaimNextTask(
		context.Background(), agentID, 1, dispatch.CreatedAt.Add(5*time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != nextDispatch.TaskID {
		t.Fatalf("ClaimNextTask(retry) = %#v/%v/%v", claim, found, err)
	}
	verifiedMutations := make([]Mutation, 0, len(assigned))
	verifiedConditions := make([]Condition, 0, len(assigned))
	for index := range assigned {
		record := assigned[index].Record
		record.State = BackupPruneVerifiedAbsent
		record.UpdatedAt = dispatch.CreatedAt.Add(6 * time.Second)
		value, encodeErr := encodeBackupRecoveryPointPruneRecord(record)
		if encodeErr != nil {
			clearBackupRuntimeMutations(verifiedMutations)
			t.Fatal(encodeErr)
		}
		verifiedConditions = append(verifiedConditions, Condition{
			Key:         backupRecoveryPointPruneKey(record.Point.ID),
			ModRevision: assigned[index].Revision,
		})
		verifiedMutations = append(verifiedMutations, Mutation{
			Type: MutationPut, Key: backupRecoveryPointPruneKey(record.Point.ID), Value: value,
		})
		authorityKeys, keyErr := backupPruneAuthorityKeys(record.Point)
		if keyErr != nil {
			clearBackupRuntimeMutations(verifiedMutations)
			t.Fatal(keyErr)
		}
		for _, key := range authorityKeys[1:] {
			verifiedMutations = append(verifiedMutations, Mutation{Type: MutationDelete, Key: key})
		}
		assigned[index].Record = record
	}
	verified, err := store.Transact(
		context.Background(), verifiedConditions, verifiedMutations,
	)
	clearBackupRuntimeMutations(verifiedMutations)
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
		claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		dispatch.CreatedAt.Add(7*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(prune completion) error = %v", err)
	}
	terminalTask := mustOptionalKey(t, store, taskKey(nextDispatch.TaskID))
	if terminalTask == nil || terminalTask.ModRevision != completedTask.Revision {
		t.Fatalf("terminal prune Task = %#v", terminalTask)
	}
	if lockEntry := mustOptionalKey(t, store, environmentOperationLockKey(run.EnvironmentID)); lockEntry != nil {
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
	staleDispatchValue, err := encodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		t.Fatal(err)
	}
	staleDispatch, err := store.Transact(
		context.Background(),
		[]Condition{{Key: backupRecoveryPointPruneDispatchKey(dispatch.TaskID)}},
		[]Mutation{{
			Type: MutationPut, Key: backupRecoveryPointPruneDispatchKey(dispatch.TaskID),
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
		[]Condition{{
			Key:         backupRecoveryPointPruneDispatchKey(dispatch.TaskID),
			ModRevision: staleDispatch.Revision,
		}},
		[]Mutation{{Type: MutationDelete, Key: backupRecoveryPointPruneDispatchKey(dispatch.TaskID)}},
	)
	if err != nil || !removedDispatch.Succeeded {
		t.Fatalf("remove stale terminal prune dispatch = %#v, %v", removedDispatch, err)
	}
	if count, pruneErr := tasks.PruneExpiredTasks(
		context.Background(), pruneAt,
	); pruneErr != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(prune source) = %d, %v", count, pruneErr)
	}
	if sourceEntry := mustOptionalKey(t, store, taskKey(dispatch.TaskID)); sourceEntry != nil {
		t.Fatalf("retained source prune Task = %#v", sourceEntry)
	}
}

func backupRuntimePrunePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch BackupRecoveryPointPruneDispatchRecord,
	pending []Versioned[BackupRecoveryPointPruneRecord],
	publicationConditions []Condition,
) (TaskRecord, *agentpb.ExecutionPlan, IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, environmentKey(dispatch.EnvironmentID))
	environment, err := decodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, projectKey(environment.ProjectID))
	project, err := decodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(dispatch.CreatedAt)
	task.ID = dispatch.TaskID
	task.OperationID = dispatch.OperationID
	task.Owner = owner
	task.Actor = TaskActorSystem
	task.Executor = TaskExecutorAgent
	task.Type = TaskBackupPrune
	task.Target = dispatch.EnvironmentID
	task.IdempotencyKey = "backup-prune-runtime-0001"
	sealed := backupRuntimeSealedPrunePlan(t, store, dispatch, pending, task.PlanID)
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = TaskStepRecord{Kind: TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = dispatch.EnvironmentID
	marker.Locator.Route = "/internal/backup-prunes"
	marker.Locator.Key = task.IdempotencyKey
	var initiationFence Condition
	for _, condition := range publicationConditions {
		if condition.ModRevision > 0 {
			initiationFence = condition
			break
		}
	}
	if initiationFence.Key == "" {
		t.Fatal("prune Task publication has no durable initiation fence")
	}
	initiation, err := newTaskInitiation(owner, TaskActorSystem, initiationFence)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}

func backupRuntimeSealedPrunePlan(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch BackupRecoveryPointPruneDispatchRecord,
	pending []Versioned[BackupRecoveryPointPruneRecord],
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
		pointEntry := mustOptionalKey(t, store, backupRecoveryPointKey(point.ID))
		sourceEntry := mustOptionalKey(t, store, backupSourceKey(point.SourceID))
		environmentEntry := mustOptionalKey(t, store, environmentKey(point.EnvironmentID))
		connectorEntry := mustOptionalKey(t, store, connectorRecordKey(point.ConnectorID))
		connector, err := decodeConnectorRecord(connectorEntry.Value)
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
				ConnectorAddressing: backupPlanAddressing(connector.Connector.PathStyle),
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
) (*BackupRuntimeRepository, *memoryHierarchyStore, BackupRunRecord) {
	t.Helper()
	return newBackupRuntimeBareFixture(t)
}

func newBackupRuntimeBareFixture(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, BackupRunRecord) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	connectorRepository, err := newConnectorRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	connector := testConnectorRecord(t, environment.Record.ID, now, 900, "backup-store")
	credentials, err := NewConnectorEncryptedCredentials(
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
	run.State = BackupRunQueued
	run.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	run.Sources[0].Snapshot.Postgres.ConsumerEnvironmentID = environment.Record.ID
	run.Sources[0].State = BackupSourceAttemptPending
	run.Sources[0].Phase = BackupSourcePhaseCapture
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
	run *BackupRunRecord,
) {
	t.Helper()
	source := &run.Sources[0]
	snapshot := source.Snapshot.Postgres
	backingProject := ProjectRecord{
		ID: snapshot.BackingProjectID, Slug: "postgres", Name: "Postgres",
		Kind: ProjectKindBacking,
	}
	backingEnvironment, err := NewProvisioningEnvironment(
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
	backingServiceProjection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    backingEnvironment.ID,
		RevisionID:       backingServiceRevisionID,
		RenderGeneration: 1,
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID:    backingEnvironment.ID,
			BackingNetworkID: backingNetworkID,
			Desired: core.Service{
				ID: backingServiceID, Name: "postgres", Image: "postgres:16-alpine", Adapter: "postgres:16",
			},
		}},
	})
	backingServiceProjectionValue, err := encodeEnvironmentComposeProjection(backingServiceProjection)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(backingServiceProjectionValue)
	backingServiceProjectionDigest := sha256.Sum256(backingServiceProjectionValue)
	backingServiceAudit := []byte("backup-runtime-backing-service-audit")
	backingServiceAuditDigest := sha256.Sum256(backingServiceAudit)
	backingServiceSeal := EnvironmentBlueprintSeal{
		EnvironmentID: backingEnvironment.ID, RevisionID: backingServiceRevisionID,
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1, ProjectionSchema: 1,
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
	attach, err := NewPendingAttachRecord(
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
		[]AttachFactSetMetadata{{Facts: []AttachFactDefinition{{Key: "pg16_URL", Secret: true}}}},
		newBackupRuntimeID(ids.KindTask, run.CreatedAt, 913),
		run.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	attach, err = MarkAttachProvisioning(attach, attach.TaskID)
	if err == nil {
		attach, err = CompleteAttachProvisioning(attach, attach.TaskID, true)
	}
	if err != nil {
		t.Fatal(err)
	}
	facts, err := NewAttachEncryptedFacts(
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
	policy := BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	}
	digest, err := backupPolicyScheduleDigest(policy)
	if err != nil {
		t.Fatal(err)
	}
	coordination := EnvironmentCoordinationRecord{
		EnvironmentID: run.EnvironmentID, ScheduleClockFloor: run.CreatedAt,
		CurrentBackupScheduleState: &CurrentBackupScheduleState{
			PolicyDigest: digest, Frequency: policy.Frequency, EnabledAt: run.CreatedAt,
			LastEvaluatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
		},
	}
	storedSource := backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "attach", source.TargetID, run.CreatedAt,
	)
	keyRecord := BackupKeyRecord{
		EnvironmentID: run.EnvironmentID, Recipient: run.Recipient, KeyEra: run.KeyEra,
		CreatedAt: run.CreatedAt, RotatedAt: run.CreatedAt,
	}
	keyValue := BackupKeyEncryptedValue{
		EnvironmentID: run.EnvironmentID, KeyEra: run.KeyEra,
		Ciphertext: []byte("wrapped-age-identity"),
	}
	encoders := []struct {
		key    string
		encode func() ([]byte, error)
	}{
		{backupPolicyKey(run.EnvironmentID), func() ([]byte, error) { return encodeBackupPolicyRecord(policy) }},
		{environmentCoordinationKey(run.EnvironmentID), func() ([]byte, error) {
			return encodeEnvironmentCoordinationRecord(coordination)
		}},
		{backupSourceKey(source.SourceID), func() ([]byte, error) { return encodeBackupSourceRecord(storedSource) }},
		{attachKey(attach.ID), func() ([]byte, error) { return encodeAttachRecord(attach) }},
		{attachFactsKey(attach.ID), func() ([]byte, error) { return encodeAttachEncryptedFacts(facts) }},
		{projectKey(backingProject.ID), func() ([]byte, error) { return encodeProject(backingProject) }},
		{
			environmentKey(backingEnvironment.ID),
			func() ([]byte, error) { return encodeEnvironment(backingEnvironment) },
		},
		{
			environmentBlueprintHeadKey(backingEnvironment.ID),
			func() ([]byte, error) { return encodeTaskReference(backingServiceRevisionID) },
		},
		{
			environmentBlueprintRootKey(backingEnvironment.ID, backingServiceRevisionID),
			func() ([]byte, error) { return encodeEnvironmentBlueprintSeal(backingServiceSeal) },
		},
		{
			environmentBlueprintChunkKeyFor(
				backingEnvironment.ID, backingServiceRevisionID, EnvironmentBlueprintChunkProjection, 0,
			),
			func() ([]byte, error) {
				return encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
					Family: EnvironmentBlueprintChunkProjection, LogicalLength: uint32(len(backingServiceProjectionValue)),
					Digest: backingServiceProjectionDigest, Data: backingServiceProjectionValue,
				})
			},
		},
		{
			serviceRuntimeKey(backingServiceID),
			func() ([]byte, error) {
				return encodeServiceRuntimeRecord(ServiceRuntimeRecord{
					EnvironmentID: backingEnvironment.ID, ServiceID: backingServiceID,
					BackingNetworkID: backingNetworkID,
					Runtime: core.ServiceRuntime{
						ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
					},
				})
			},
		},
		{backupKeyKey(run.EnvironmentID), func() ([]byte, error) { return encodeBackupKeyRecord(keyRecord) }},
		{
			backupKeyValueKey(run.EnvironmentID),
			func() ([]byte, error) { return encodeBackupKeyEncryptedValue(keyValue) },
		},
	}
	mutations := make([]Mutation, 0, len(encoders))
	for _, item := range encoders {
		value, encodeErr := item.encode()
		if encodeErr != nil {
			clearBackupRuntimeMutations(mutations)
			t.Fatal(encodeErr)
		}
		mutations = append(mutations, Mutation{Type: MutationPut, Key: item.key, Value: value})
	}
	result, err := store.Transact(context.Background(), nil, mutations)
	clearBackupRuntimeMutations(mutations)
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
	run *BackupRunRecord,
) {
	t.Helper()
	baseAttachRead, err := store.Get(context.Background(), attachKey(run.Sources[0].TargetID))
	if err != nil || baseAttachRead == nil || baseAttachRead.Entry == nil {
		t.Fatalf("read base backup Attach = %#v, %v", baseAttachRead, err)
	}
	baseAttach, err := decodeAttachRecord(baseAttachRead.Entry.Value)
	clear(baseAttachRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	baseFactsRead, err := store.Get(context.Background(), attachFactsKey(run.Sources[0].TargetID))
	if err != nil || baseFactsRead == nil || baseFactsRead.Entry == nil {
		t.Fatalf("read base backup Attach facts = %#v, %v", baseFactsRead, err)
	}
	baseFacts, err := decodeAttachEncryptedFacts(baseFactsRead.Entry.Value)
	clear(baseFactsRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(baseFacts.Ciphertext)
	sourceIDs := make([]string, len(run.Sources))
	mutations := make([]Mutation, 0, len(run.Sources)*3)
	for index := range run.Sources {
		source := &run.Sources[index]
		sourceIDs[index] = source.SourceID
		value, err := encodeBackupSourceRecord(backupRuntimeSourceRecord(
			t, source.SourceID, run.EnvironmentID, "attach", source.TargetID, run.CreatedAt,
		))
		if err != nil {
			clearBackupRuntimeMutations(mutations)
			t.Fatal(err)
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: backupSourceKey(source.SourceID), Value: value,
		})
		if index > 0 {
			attach := baseAttach
			attach.ID = source.TargetID
			attach.TaskID = newBackupRuntimeID(ids.KindTask, run.CreatedAt, int64(920+index))
			facts := baseFacts
			facts.AttachID = source.TargetID
			attachValue, encodeErr := encodeAttachRecord(attach)
			if encodeErr != nil {
				clearBackupRuntimeMutations(mutations)
				t.Fatal(encodeErr)
			}
			factsValue, encodeErr := encodeAttachEncryptedFacts(facts)
			if encodeErr != nil {
				clear(attachValue)
				clearBackupRuntimeMutations(mutations)
				t.Fatal(encodeErr)
			}
			mutations = append(mutations,
				Mutation{Type: MutationPut, Key: attachKey(source.TargetID), Value: attachValue},
				Mutation{Type: MutationPut, Key: attachFactsKey(source.TargetID), Value: factsValue},
			)
		}
	}
	policyValue, err := encodeBackupPolicyRecord(BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: sourceIDs, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		clearBackupRuntimeMutations(mutations)
		t.Fatal(err)
	}
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: backupPolicyKey(run.EnvironmentID), Value: policyValue,
	})
	result, err := store.Transact(context.Background(), nil, mutations)
	clearBackupRuntimeMutations(mutations)
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
	result, err := store.Get(context.Background(), environmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.ReadRevision <= 0 {
		t.Fatalf("read backup publication revision = %#v, %v", result, err)
	}
	return result.ReadRevision
}

func backupRuntimeOperationLock(run BackupRunRecord) BackupOperationLockRecord {
	return BackupOperationLockRecord{
		EnvironmentID: run.EnvironmentID, OperationID: run.OperationID, TaskID: run.TaskID,
		Kind: BackupOperationBackup, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
	}
}

func (repository *BackupRuntimeRepository) createBackupRunForTest(
	ctx context.Context,
	run BackupRunRecord,
	fixedRevision int64,
) (Versioned[BackupRunRecord], error) {
	plan, err := repository.prepareBackupRunPublication(
		ctx,
		run,
		backupRuntimeOperationLock(run),
		fixedRevision,
	)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer plan.clear()
	conditions, mutations, err := plan.composeTransaction(nil, nil)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clearBackupRuntimeMutations(mutations)
	result, err := repository.transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[BackupRunRecord]{}, errs.New(
			errs.KindStateConflict,
			"backup run publication changed",
		)
	}
	return Versioned[BackupRunRecord]{
		Record: run, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func backupRuntimePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	run BackupRunRecord,
) (TaskRecord, *agentpb.ExecutionPlan, IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, environmentKey(run.EnvironmentID))
	environment, err := decodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, projectKey(environment.ProjectID))
	project, err := decodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(run.CreatedAt)
	task.ID = run.TaskID
	task.OperationID = run.OperationID
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.Executor = TaskExecutorAgent
	task.Type = TaskBackup
	task.Target = run.EnvironmentID
	task.IdempotencyKey = "backup-runtime-0001"
	sealed := backupRuntimeSealedRunPlan(t, run, task.PlanID)
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = TaskStepRecord{Kind: TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = run.EnvironmentID
	marker.Locator.Route = "/environments/{environment}/backups"
	marker.Locator.Key = task.IdempotencyKey
	initiation, err := newTaskInitiation(owner, TaskActorOperator)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}

func configureBackupRuntimeConfigRun(
	t *testing.T,
	store *memoryHierarchyStore,
	run *BackupRunRecord,
) int64 {
	t.Helper()
	source := &run.Sources[0]
	source.Kind = BackupRuntimeSourceConfig
	source.TargetID = run.EnvironmentID
	source.TargetRevision = mustOptionalKey(t, store, environmentKey(run.EnvironmentID)).ModRevision
	source.Format = BackupRuntimeFormatConfig
	source.Snapshot = BackupRunSourceSnapshot{Config: &BackupConfigSourceSnapshot{
		ConfigSnapshotID: run.TaskID,
	}}
	policyValue, err := encodeBackupPolicyRecord(BackupPolicyRecord{
		EnvironmentID: run.EnvironmentID, Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 3,
		Encryption: string(run.Encryption), ConnectorID: run.ConnectorID,
		SourceIDs: []string{source.SourceID}, UpdatedAt: run.CreatedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceValue, err := encodeBackupSourceRecord(backupRuntimeSourceRecord(
		t, source.SourceID, run.EnvironmentID, "config", run.EnvironmentID, run.CreatedAt,
	))
	if err != nil {
		clear(policyValue)
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: backupPolicyKey(run.EnvironmentID), Value: policyValue},
		{Type: MutationPut, Key: backupSourceKey(source.SourceID), Value: sourceValue},
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
	run BackupRunRecord,
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
			SourceFormat: backupPlanSourceFormat(source.Format),
			Encryption:   backupPlanEncryption(run.Encryption),
			KeyEra:       uint64(run.KeyEra), AgeRecipient: run.Recipient,
			Upload: &agentpb.BackupUploadAuthority{
				ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket,
				ConnectorPrefix: run.ConnectorPrefix, ConnectorRegion: run.ConnectorRegion,
				ConnectorAddressing: backupPlanAddressing(run.ConnectorPathStyle),
				ProtectedObjectKey:  source.ObjectKey,
				ImmutableCreate:     true, PutAfterArtifactPreparedAck: true, HeadAfterUploadCompletedAck: true,
			},
		}
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			capture.Source = &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
				BackingServiceId:       snapshot.BackingServiceID,
				BackingServiceRevision: uint64(snapshot.BackingServiceRevision),
				Database:               snapshot.Database, Role: snapshot.Role,
			}}
		case BackupRuntimeSourceConfig:
			capture.Source = &agentpb.BackupSourceCapture_Config{Config: &agentpb.BackupConfigSource{
				SnapshotRevision: uint64(source.Snapshot.Config.ReadRevision),
			}}
		case BackupRuntimeSourceVolume:
			artifactSHA256, err := hex.DecodeString(source.Snapshot.Volume.ArtifactDigest)
			if err != nil {
				t.Fatal(err)
			}
			services := make([]*agentpb.BackupVolumeService, len(source.Snapshot.Volume.Services))
			for serviceIndex, service := range source.Snapshot.Volume.Services {
				services[serviceIndex] = &agentpb.BackupVolumeService{
					ServiceId: service.ServiceID, ServiceRevision: uint64(service.ServiceRevision),
					PriorIntent: backupPlanServiceIntent(service.PriorIntent),
					ComposeKey:  service.ComposeKey, MountPaths: append([]string(nil), service.MountPaths...),
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
	run BackupRunRecord,
) (Versioned[BackupRunRecord], BackupRunRecord) {
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
	staged.State = BackupRunRunning
	staged.Sources = append([]BackupRunSourceAttemptRecord(nil), run.Sources...)
	staged.Sources[0].State = BackupSourceAttemptStaged
	staged.Sources[0].Phase = BackupSourcePhasePointCommit
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
	run BackupRunRecord,
	staged BackupRunRecord,
) (BackupRunRecord, BackupRecoveryPointRecord, BackupRetentionSweepRecord) {
	committed := staged
	committed.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	committed.Sources[0].State = BackupSourceAttemptPointCommitted
	committed.Sources[0].Phase = BackupSourcePhaseRetention
	committed.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], committed.UpdatedAt)
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID, Keep: 3,
		Revision: run.PolicyRevision, State: BackupRetentionPending,
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
		key    func(BackupRecoveryPointRecord) string
		delete bool
	}{
		{
			name: "missing Connector membership",
			key: func(point BackupRecoveryPointRecord) string {
				key, _ := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
				return key
			},
			delete: true,
		},
		{
			name: "rewritten Environment membership",
			key: func(point BackupRecoveryPointRecord) string {
				key, _ := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
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
			checkpoint.Payload.Kind = BackupCheckpointUploadVerified
			if _, _, err := repository.CommitBackupRecoveryPoint(
				context.Background(), backupAssignmentFromCheckpoint(checkpoint), stagedVersion,
				committed, 0, point, nil, sweep,
			); err != nil {
				t.Fatal(err)
			}
			key := test.key(point)
			entry := mustOptionalKey(t, store, key)
			mutation := Mutation{Type: MutationPut, Key: key, Value: entry.Value}
			if test.delete {
				mutation = Mutation{Type: MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("tamper point companion = %#v, %v", changed, err)
			}
			if _, err := repository.ListBackupRecoveryPointsBySource(
				context.Background(), point.SourceID, BackupRuntimeListRequest{Limit: 1},
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
		key    func(BackupRecoveryPointRecord) string
		delete bool
	}{
		{
			name: "rewritten source index",
			key: func(point BackupRecoveryPointRecord) string {
				key, _ := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
				return key
			},
		},
		{
			name: "missing Environment index",
			key: func(point BackupRecoveryPointRecord) string {
				key, _ := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
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
			checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
			mutation := Mutation{Type: MutationPut, Key: key, Value: entry.Value}
			if test.delete {
				mutation = Mutation{Type: MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]Mutation{mutation},
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
	prune := BackupRecoveryPointPruneRecord{
		Point: point.BackupRecoveryPointSnapshot, PointRevision: 71, OperationID: run.OperationID,
		State: BackupPrunePending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
	}
	pruneValue, err := encodeBackupRecoveryPointPruneRecord(prune)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pruneValue)
	pointValue, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pointValue)
	environmentIndex, _ := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	sourceIndex, _ := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	connectorIndex, _ := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	pointRevision := int64(71)
	pruneRevision := int64(72)
	values := []*KeyValue{
		{Key: backupRecoveryPointPruneKey(point.ID), Value: pruneValue, ModRevision: pruneRevision, Version: 1},
		{Key: backupRecoveryPointKey(point.ID), Value: pointValue, ModRevision: pointRevision, Version: 1},
		{Key: environmentIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
		{Key: sourceIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
		{Key: connectorIndex, Value: []byte(point.ID), ModRevision: pointRevision, Version: 1},
	}
	expectedPrune := Versioned[BackupRecoveryPointPruneRecord]{Record: prune, Revision: pruneRevision}
	stalePrune := expectedPrune
	stalePrune.Revision++
	if err := validatePendingBackupPruneAuthority(values, stalePrune); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("publication prune revision drift error = %v", err)
	}
	corruptPrune := append([]*KeyValue(nil), values...)
	corruptPrunePrimary := *values[0]
	corruptPrunePrimary.Value = []byte(`{"invalid":`)
	corruptPrune[0] = &corruptPrunePrimary
	if err := validatePendingBackupPruneAuthority(corruptPrune, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication same-revision corruption error = %v", err)
	}
	missingCompanion := append([]*KeyValue(nil), values...)
	missingCompanion[4] = nil
	if err := validatePendingBackupPruneAuthority(missingCompanion, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication split point authority error = %v", err)
	}
	rewrittenPoint := append([]*KeyValue(nil), values...)
	rewrittenPointPrimary := *values[1]
	rewrittenPointPrimary.Version = 2
	rewrittenPoint[1] = &rewrittenPointPrimary
	if err := validatePendingBackupPruneAuthority(rewrittenPoint, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication rewritten point primary error = %v", err)
	}
	rewrittenIndex := append([]*KeyValue(nil), values...)
	rewrittenSourceIndex := *values[3]
	rewrittenSourceIndex.Version = 2
	rewrittenIndex[3] = &rewrittenSourceIndex
	if err := validatePendingBackupPruneAuthority(rewrittenIndex, expectedPrune); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("publication rewritten point membership error = %v", err)
	}
	dispatch := BackupRecoveryPointPruneDispatchRecord{
		TaskID: run.TaskID, OperationID: run.OperationID, EnvironmentID: run.EnvironmentID,
		RecoveryPointIDs: []string{point.ID}, CreatedAt: point.VerifiedAt.Add(time.Second),
	}
	dispatchValue, err := encodeBackupRecoveryPointPruneDispatchRecord(dispatch)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(dispatchValue)
	dispatchRevision := int64(81)
	dispatchEntry := &KeyValue{
		Key: backupRecoveryPointPruneDispatchKey(dispatch.TaskID), Value: dispatchValue,
		ModRevision: dispatchRevision, Version: 1,
	}
	expectedDispatch := Versioned[BackupRecoveryPointPruneDispatchRecord]{
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
	environmentValue := mustOptionalKey(t, store, environmentKey(run.EnvironmentID))
	if environmentValue == nil {
		t.Fatal("missing Environment publication fixture")
	}
	environment, err := decodeEnvironment(environmentValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	revisionID := ids.NewAt(ids.KindTask, run.CreatedAt, 8101)
	headValue, err := encodeTaskReference(revisionID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(headValue)
	digest := sha256.Sum256([]byte("normalized Volume projection"))
	sealValue, err := encodeEnvironmentBlueprintSeal(EnvironmentBlueprintSeal{
		EnvironmentID: environment.ID, RevisionID: revisionID,
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1, ProjectionSchema: 1,
		AuditChunks: 1, AuditBytes: 1, AuditSHA256: digest,
		ProjectionChunks: 1, ProjectionBytes: 1, ProjectionSHA256: digest,
		ProjectionResources: 1, DependencyDigest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(sealValue)
	source := BackupRunSourceAttemptRecord{TargetID: testBackupVolumeID, TargetRevision: 71}
	rootRevision := int64(72)
	projectionSnapshot := BackupVolumeSourceSnapshot{
		EnvironmentID: environment.ID, EnvironmentRevision: environmentValue.ModRevision,
		VolumeID: source.TargetID, DesiredRevisionID: revisionID, ProjectionRoot: rootRevision,
		DependencyDigest: hex.EncodeToString(digest[:]), RenderGeneration: 1,
		ComposeVolumeKey: "data", DockerVolumeName: "gp_vol_" + source.TargetID,
		AuthorizedVolumeDir: environment.VolumeDir,
	}
	projectionEvidence := []*KeyValue{
		{Key: environmentKey(environment.ID), Value: environmentValue.Value, ModRevision: environmentValue.ModRevision},
		{Key: environmentBlueprintHeadKey(environment.ID), Value: headValue, ModRevision: source.TargetRevision},
		{Key: environmentBlueprintRootKey(environment.ID, revisionID), Value: sealValue, ModRevision: rootRevision},
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
			values := append([]*KeyValue(nil), projectionEvidence...)
			corruptValue := *values[test.offset]
			corruptValue.Value = []byte(`{"invalid":`)
			values[test.offset] = &corruptValue
			if err := validateBackupVolumePublicationEvidence(
				values, source, projectionSnapshot,
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("pinned %s decode corruption error = %v", test.name, err)
			}
		})
	}
	wrongProjectionRevision := append([]*KeyValue(nil), projectionEvidence...)
	wrongProjection := *projectionEvidence[2]
	wrongProjection.ModRevision++
	wrongProjection.Value = []byte(`{"invalid":`)
	wrongProjectionRevision[2] = &wrongProjection
	if err := validateBackupVolumePublicationEvidence(
		wrongProjectionRevision, source, projectionSnapshot,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("projection revision drift error = %v", err)
	}
	driftedEnvironmentSnapshot := projectionSnapshot
	driftedEnvironmentSnapshot.AuthorizedVolumeDir += "/changed"
	if err := validateBackupVolumePublicationEvidence(
		projectionEvidence, source, driftedEnvironmentSnapshot,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Environment semantic drift error = %v", err)
	}
	postgres := run.Sources[0].Snapshot.Postgres
	if postgres == nil {
		t.Fatal("missing Service publication fixture")
	}
	runtimeValue := mustOptionalKey(t, store, serviceRuntimeKey(postgres.BackingServiceID))
	if runtimeValue == nil {
		t.Fatal("missing Service runtime sidecar fixture")
	}
	runtime, err := decodeServiceRuntimeRecord(runtimeValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	serviceSnapshot := projectionSnapshot
	serviceSnapshot.Services = []BackupVolumeServiceSnapshot{{
		ServiceID: runtime.ServiceID, ServiceRevision: runtimeValue.ModRevision,
		ComposeKey: "database", MountPaths: []string{"/data"},
		PriorIntent: BackupServiceRuntimeIntent(runtime.Runtime.RuntimeIntent),
	}}
	corruptRuntime := append([]*KeyValue(nil), projectionEvidence...)
	corruptRuntime = append(
		corruptRuntime,
		&KeyValue{
			Key:         serviceRuntimeKey(runtime.ServiceID),
			Value:       []byte(`{"invalid":`),
			ModRevision: runtimeValue.ModRevision,
		},
	)
	if err := validateBackupVolumePublicationEvidence(
		corruptRuntime, source, serviceSnapshot,
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("pinned Service runtime sidecar decode corruption error = %v", err)
	}
	corruptRuntime[3].ModRevision++
	if err := validateBackupVolumePublicationEvidence(
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
			point := BackupRecoveryPointRecord{
				BackupRecoveryPointSnapshot: current.Record.Point,
				VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
			}
			sweep := BackupRetentionSweepRecord{
				SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
				Keep:     current.Record.Reconciliation.RetentionKeep,
				Revision: current.Record.Reconciliation.PolicyRevision,
				State:    BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
			}
			orphanConnectorIndex, _ := backupOrphanConnectorIndexKey(point.ConnectorID, point.ID)
			orphanEnvironmentIndex, _ := backupOrphanEnvironmentIndexKey(point.EnvironmentID, point.ID)
			environmentIndex, _ := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
			sourceIndex, _ := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
			connectorIndex, _ := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
			mutations := []Mutation{
				{Type: MutationDelete, Key: backupOrphanKey(point.ID)},
				{Type: MutationDelete, Key: orphanConnectorIndex},
				{Type: MutationDelete, Key: orphanEnvironmentIndex},
			}
			pointValue, encodeErr := encodeBackupRecoveryPointRecord(point)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			defer clear(pointValue)
			if test.publication == "partial" {
				mutations = append(mutations, Mutation{
					Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: pointValue,
				})
			}
			if test.publication == "malformed" {
				sweepValue, encodeErr := encodeBackupRetentionSweepRecord(sweep)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				defer clear(sweepValue)
				mutations = append(mutations,
					Mutation{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: []byte(`{"invalid":`)},
					Mutation{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
					Mutation{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
					Mutation{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
					Mutation{Type: MutationPut, Key: backupRetentionKey(point.SourceID, point.ID), Value: sweepValue},
				)
			}
			changed, err := store.Transact(context.Background(), nil, mutations)
			clearBackupRuntimeMutations(mutations)
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
	point := BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: current.Record.Point,
		VerifiedAt:                  current.Record.UpdatedAt.Add(time.Second),
	}
	sweep := BackupRetentionSweepRecord{
		SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
		Keep:     current.Record.Reconciliation.RetentionKeep,
		Revision: current.Record.Reconciliation.PolicyRevision,
		State:    BackupRetentionPending, CreatedAt: point.VerifiedAt, UpdatedAt: point.VerifiedAt,
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
	input BackupCheckpointInput,
	sequence uint64,
	pointID string,
) BackupCheckpointInput {
	input.Sequence = sequence
	input.Payload = BackupCheckpointPayload{
		Kind: BackupCheckpointRemoteObjectAbsent, PointID: pointID,
	}
	return input
}

func seedBackupRuntimePointAuthority(
	t *testing.T,
	store *memoryHierarchyStore,
	point BackupRecoveryPointRecord,
) int64 {
	t.Helper()
	value, err := encodeBackupRecoveryPointRecord(point)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	environmentIndex, err := backupRecoveryPointEnvironmentIndexKey(point.EnvironmentID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	sourceIndex, err := backupRecoveryPointSourceIndexKey(point.SourceID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	connectorIndex, err := backupRecoveryPointConnectorIndexKey(point.ConnectorID, point.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(context.Background(), []Condition{
		{Key: backupRecoveryPointKey(point.ID)}, {Key: environmentIndex},
		{Key: sourceIndex}, {Key: connectorIndex},
	}, []Mutation{
		{Type: MutationPut, Key: backupRecoveryPointKey(point.ID), Value: value},
		{Type: MutationPut, Key: environmentIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: sourceIndex, Value: []byte(point.ID)},
		{Type: MutationPut, Key: connectorIndex, Value: []byte(point.ID)},
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
	request GetManyRequest,
) (*GetManyResult, error) {
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
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := store.hierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext {
		store.failNext = false
		return TransactionResult{}, errs.New(
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
) BackupSourceRecord {
	t.Helper()
	value, err := encodeEnvelope("backup-source", map[string]any{
		"id": sourceID, "environment_id": environmentID, "kind": kind,
		"target_id": targetID, "created_at": createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	record, err := decodeBackupSourceRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func putBackupRuntimeLock(
	t *testing.T,
	store *memoryHierarchyStore,
	lock BackupOperationLockRecord,
) {
	t.Helper()
	value, err := encodeBackupOperationLockRecord(lock)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: environmentOperationLockKey(lock.EnvironmentID)}},
		[]Mutation{
			{Type: MutationPut, Key: environmentOperationLockKey(lock.EnvironmentID), Value: value},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("put backup runtime lock = %#v/%v", result, err)
	}
}

func backupRuntimeTestPoint(
	run BackupRunRecord,
	source BackupRunSourceAttemptRecord,
	verifiedAt time.Time,
) BackupRecoveryPointRecord {
	return BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: BackupRecoveryPointSnapshot{
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
) (*BackupRuntimeRepository, *memoryHierarchyStore, BackupOrphanRecord) {
	t.Helper()
	repository, store, run := newBackupRuntimeRepositoryFixture(t)
	stagedVersion, staged := prepareBackupRuntimeStagedRun(t, repository, run)
	orphaned := staged
	orphaned.Sources = append([]BackupRunSourceAttemptRecord(nil), staged.Sources...)
	orphaned.Sources[0].State = BackupSourceAttemptOrphaned
	orphaned.Sources[0].Phase = BackupSourcePhasePointCommit
	orphaned.UpdatedAt = staged.UpdatedAt.Add(time.Second)
	point := backupRuntimeTestPoint(run, staged.Sources[0], orphaned.UpdatedAt)
	orphan := BackupOrphanRecord{
		Point: point.BackupRecoveryPointSnapshot, TaskID: run.TaskID, State: BackupOrphanInspect,
		CreatedAt: orphaned.UpdatedAt, UpdatedAt: orphaned.UpdatedAt,
	}
	checkpoint, _ := seedBackupCheckpointAssignment(t, store, run)
	checkpoint.Payload.Kind = BackupCheckpointUploadVerified
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
	current Versioned[BackupRunRecord],
	next BackupRunRecord,
) (Versioned[BackupRunRecord], error) {
	value, err := encodeBackupRunRecord(next)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clear(value)
	anchor, err := repository.readCurrentKeys(ctx, []string{backupRunKey(current.Record.TaskID)})
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clearKeyValues(anchor.Values)
	evidence, err := repository.loadOwnedEvidence(ctx, current.Record, anchor.ReadRevision)
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	conditions := []Condition{
		{Key: backupRunKey(current.Record.TaskID), ModRevision: current.Revision},
	}
	conditions = append(conditions, evidence.fence.transactionConditions()...)
	epoch, err := evidence.fence.epochRewriteMutation()
	if err != nil {
		return Versioned[BackupRunRecord]{}, err
	}
	defer clear(epoch.Value)
	result, err := repository.transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: backupRunKey(next.TaskID), Value: value}, epoch,
	})
	if err != nil || !result.Succeeded {
		return Versioned[BackupRunRecord]{}, err
	}
	return Versioned[BackupRunRecord]{
		Record:       next,
		Revision:     result.Revision,
		ReadRevision: result.Revision,
	}, nil
}
