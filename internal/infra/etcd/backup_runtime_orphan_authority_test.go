package etcd

import (
	context "context"
	errors "errors"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

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
