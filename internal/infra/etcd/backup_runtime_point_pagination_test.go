package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

// Rationale: internal Connector-fence enumeration must preserve raw identity order and the caller's fixed revision.
func TestBackupRuntimeRepositoryPaginatesConnectorPointsAtFixedRevision(t *testing.T) {
	t.Parallel()
	repository, store, run := newBackupRuntimeBareFixture(t)
	source := run.Sources[0]
	source.State = testbackupruntime.BackupSourceAttemptStaged
	source.Phase = testbackupruntime.BackupSourcePhaseUpload
	source.SizeBytes = 123
	source.SHA256 = testBackupDigest
	older := backupRuntimeTestPoint(run, source, run.CreatedAt.Add(time.Second))
	newer := older
	newer.CreatedAt = older.CreatedAt.Add(time.Second)
	newer.ID = ids.NewAt(ids.KindRecoveryPoint, newer.CreatedAt, 902)
	newer.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + newer.SourceID + "/" + newer.ID + "/artifact.bin"
	newer.VerifiedAt = older.VerifiedAt.Add(time.Second)
	olderValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(older)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(olderValue)
	newerValue, err := testbackupruntime.EncodeBackupRecoveryPointRecord(newer)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(newerValue)
	olderIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(older.ConnectorID, older.ID)
	if err != nil {
		t.Fatal(err)
	}
	newerIndex, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(newer.ConnectorID, newer.ID)
	if err != nil {
		t.Fatal(err)
	}
	olderEnvironmentIndex, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(older.EnvironmentID, older.ID)
	olderSourceIndex, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(older.SourceID, older.ID)
	newerEnvironmentIndex, _ := testbackupruntime.BackupRecoveryPointEnvironmentIndexKey(newer.EnvironmentID, newer.ID)
	newerSourceIndex, _ := testbackupruntime.BackupRecoveryPointSourceIndexKey(newer.SourceID, newer.ID)
	seeded, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{
			{Key: testbackupruntime.BackupRecoveryPointKey(older.ID)}, {Key: olderIndex}, {Key: olderEnvironmentIndex},
			{Key: olderSourceIndex}, {Key: testbackupruntime.BackupRecoveryPointKey(newer.ID)}, {Key: newerIndex},
			{Key: newerEnvironmentIndex}, {Key: newerSourceIndex},
		},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointKey(older.ID),
				Value: olderValue,
			},
			{Type: testkeyvalue.MutationPut, Key: olderIndex, Value: []byte(older.ID)},
			{Type: testkeyvalue.MutationPut, Key: olderEnvironmentIndex, Value: []byte(older.ID)},
			{Type: testkeyvalue.MutationPut, Key: olderSourceIndex, Value: []byte(older.ID)},
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointKey(newer.ID),
				Value: newerValue,
			},
			{Type: testkeyvalue.MutationPut, Key: newerIndex, Value: []byte(newer.ID)},
			{Type: testkeyvalue.MutationPut, Key: newerEnvironmentIndex, Value: []byte(newer.ID)},
			{Type: testkeyvalue.MutationPut, Key: newerSourceIndex, Value: []byte(newer.ID)},
		},
	)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed connector points = %#v, %v", seeded, err)
	}
	first, err := repository.ListBackupRecoveryPointsByConnector(
		context.Background(), run.ConnectorID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
	)
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.ID != older.ID ||
		first.Next == "" {
		t.Fatalf("first connector point page = %#v, %v", first, err)
	}
	rewritten, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{
			{Key: testbackupruntime.BackupRecoveryPointKey(newer.ID), ModRevision: seeded.Revision},
		},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testbackupruntime.BackupRecoveryPointKey(newer.ID),
				Value: newerValue,
			},
		},
	)
	if err != nil || !rewritten.Succeeded {
		t.Fatalf("rewrite newer point = %#v, %v", rewritten, err)
	}
	second, err := repository.ListBackupRecoveryPointsByConnector(
		context.Background(),
		run.ConnectorID, testbackupruntime.BackupRuntimeListRequest{
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
	const pageLimit = 96
	for index := range pageLimit {
		createdAt := run.CreatedAt.Add(time.Duration(index+1) * time.Millisecond)
		source := run.Sources[0]
		source.RecoveryPointID = ids.NewAt(ids.KindRecoveryPoint, createdAt, int64(950+index))
		source.RecoveryPointCreatedAt = createdAt
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		source.SizeBytes = 123
		source.SHA256 = testBackupDigest
		point := backupRuntimeTestPoint(run, source, createdAt.Add(time.Millisecond))
		value, err := testbackupruntime.EncodeBackupRecoveryPointRecord(point)
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
		context.Background(), run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: pageLimit},
	)
	if err != nil || len(page.Items) != pageLimit ||
		audited.maximumKeys > testkeyvalue.MaximumOperations {
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
		context.Background(), run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{
			Limit: 1, Revision: 1,
			StartExclusive: testbackupruntime.BackupRecoveryPointEnvironmentPrefix + run.EnvironmentID + "/bad/cursor",
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
		context.Background(), run.EnvironmentID, testbackupruntime.BackupRuntimeListRequest{Limit: 1},
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("malformed fixed-revision chunk error = %v", err)
	}
}
