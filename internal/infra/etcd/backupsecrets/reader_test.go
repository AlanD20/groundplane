package backupsecrets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: evidence ownership ends explicitly, so every returned encrypted
// credential buffer must be released even after partial consumption.
func TestBackupSecretResolutionEvidenceClearZeroesCiphertext(t *testing.T) {
	// Rationale: the reader transfers encrypted bytes to the Controller, which
	// must be able to release every partial buffer on success or failure.
	connectorCiphertext := bytes.Repeat([]byte{0xA1}, 16)
	secretCiphertext := bytes.Repeat([]byte{0xB2}, 16)
	evidence := Evidence{
		Credentials: testconnectors.EncryptedCredentials{Ciphertext: connectorCiphertext},
		SecretValues: []SecretValueEvidence{{
			Value: testsecrets.EncryptedValue{Ciphertext: secretCiphertext},
		}},
	}
	evidence.Clear()
	if evidence.Credentials.Ciphertext != nil || evidence.SecretValues != nil {
		t.Fatal("BackupSecretResolutionEvidence.Clear() retained ciphertext")
	}
	for index, value := range connectorCiphertext {
		if value != 0 {
			t.Fatalf("connector ciphertext byte %d = %d, want zero", index, value)
		}
	}
	for index, value := range secretCiphertext {
		if value != 0 {
			t.Fatalf("Secret ciphertext byte %d = %d, want zero", index, value)
		}
	}
}

// Rationale: deletion fences are typed durable authority; a mismatched target
// kind is corruption rather than evidence that resolution may proceed.
func TestRequireNoDeletionFenceRejectsWrongTargetKind(t *testing.T) {
	// Rationale: a tombstone for another resource cannot be treated as a valid
	// fence for the selected Environment or Connector.
	value, err := testdeletions.EncodeDeletionTombstone(testdeletions.DeletionTombstoneRecord{
		TargetKind:     testdeletions.DeletionTargetProject,
		TargetID:       "prj_01H00000000000000000000000",
		TargetRevision: 1,
		TaskID:         "task_01H00000000000000000000000",
		Phase:          testdeletions.DeletionPhaseFinalizing,
		CreatedAt:      time.Unix(1, 0).UTC(),
		UpdatedAt:      time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	defer clear(value)
	if err := requireNoDeletionFence(
		&testkeyvalue.KeyValue{Value: value}, testdeletions.DeletionTargetEnvironment, "env_01H00000000000000000000000",
	); err == nil {
		t.Fatal("requireNoDeletionFence() error = nil")
	}
}

// Rationale: Volume snapshots have no arbitrary Service-count cap; their
// fixed-revision reads must be split only by the accepted etcd op ceiling.
func TestBackupSecretFixedReadBatchesArbitraryVolumeServiceEvidence(t *testing.T) {
	store := &boundedBackupSecretGetManyStore{values: map[string][]byte{}}
	keys := make([]string, testkeyvalue.MaximumOperations*2+7)

	for index := range keys {
		keys[index] = "/test/backup-volume-services/" + ids.NewAt(
			ids.KindService, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), int64(index+1),
		)
		store.values[keys[index]] = []byte{byte(index)}
	}
	reader := &Reader{store: store}
	read, err := reader.readFixed(context.Background(), keys, 7)
	if err != nil {
		t.Fatalf("readFixed() error = %v", err)
	}
	defer testkeyvalue.ClearValues(read.Values)
	if len(read.Values) != len(keys) || store.calls != 3 {
		t.Fatalf("read values/calls = %d/%d, want %d/3", len(read.Values), store.calls, len(keys))
	}
}

type boundedBackupSecretGetManyStore struct {
	values map[string][]byte
	calls  int
}

func (store *boundedBackupSecretGetManyStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if len(request.Keys) > testkeyvalue.MaximumOperations || request.Revision != 7 {
		return nil, errs.New(errs.KindInternal, "fixed read exceeded its operation bound or lost its pinned revision")
	}
	store.calls++
	result := &testkeyvalue.GetManyResult{ReadRevision: 7, ResponseRevision: 7}
	for _, key := range request.Keys {
		result.Values = append(
			result.Values,
			&testkeyvalue.KeyValue{Key: key, Value: append([]byte(nil), store.values[key]...), ModRevision: 7},
		)
	}
	return result, nil
}

type secretOwnedGetManyStoreFunc func(context.Context, testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error)

func (fn secretOwnedGetManyStoreFunc) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	return fn(ctx, request)
}

// Rationale: a malformed later etcd batch must clear both that response and
// every sensitive buffer already accumulated from earlier batches.
func TestBackupSecretFixedReadClearsEarlierAndMalformedLaterResponses(t *testing.T) {
	t.Parallel()

	keys := make([]string, testkeyvalue.MaximumOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/secret-owned/%03d", index)
	}
	var returned [][]byte
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error) {
			calls++
			values := make([]*testkeyvalue.KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				value := []byte{byte(calls), byte(index + 1), 0x7f}
				returned = append(returned, value)
				values[index] = &testkeyvalue.KeyValue{Key: key, Value: value, ModRevision: int64(index + 1)}
			}
			if calls == 2 {
				values[0].Key = "/secret-owned/malformed"
			}
			return &testkeyvalue.GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: request.Revision,
			}, nil
		},
	)

	reader := &Reader{store: store}
	if _, err := reader.readFixed(context.Background(), keys, 41); err == nil {
		t.Fatal("expected malformed later batch to fail")
	}
	if calls != 2 {
		t.Fatalf("get many calls = %d, want 2", calls)
	}
	for index, value := range returned {
		for offset, octet := range value {
			if octet != 0 {
				t.Fatalf("returned buffer %d byte %d was not cleared", index, offset)
			}
		}
	}
}

// Rationale: batching is an implementation detail; every batch must retain
// the one immutable assignment revision even when current state mutates.
func TestBackupSecretFixedReadPinsEveryBatchAcrossMutation(t *testing.T) {
	t.Parallel()

	keys := make([]string, testkeyvalue.MaximumOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/fixed-secret/%03d", index)
	}
	const fixedRevision int64 = 77
	snapshot := []byte("sealed-at-77")
	current := []byte("sealed-at-77")
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request testkeyvalue.GetManyRequest) (*testkeyvalue.GetManyResult, error) {
			calls++
			if request.Revision != fixedRevision {
				t.Fatalf("batch revision = %d, want %d", request.Revision, fixedRevision)
			}
			values := make([]*testkeyvalue.KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				selected := current
				if request.Revision == fixedRevision {
					selected = snapshot
				}
				values[index] = &testkeyvalue.KeyValue{
					Key:         key,
					Value:       append([]byte(nil), selected...),
					ModRevision: fixedRevision,
				}
			}
			if calls == 1 {
				current = []byte("mutated-between-batches")
			}
			return &testkeyvalue.GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: fixedRevision + 1,
			}, nil
		},
	)

	reader := &Reader{store: store}
	result, err := reader.readFixed(context.Background(), keys, fixedRevision)
	if err != nil {
		t.Fatalf("readFixed: %v", err)
	}
	defer testkeyvalue.ClearValues(result.Values)
	if calls != 2 {
		t.Fatalf("get many calls = %d, want 2", calls)
	}
	for index, value := range result.Values {
		if value == nil || string(value.Value) != string(snapshot) {
			t.Fatalf("value %d did not come from fixed revision", index)
		}
	}
}

// Rationale: Config capture seals the run's snapshot revision while current
// target evidence is independently validated at the later assignment view.
func TestBackupSecretConfigCaptureBindsSealedSnapshotAtLaterAssignmentRevision(t *testing.T) {
	t.Parallel()

	const (
		taskID             = "task-config-capture"
		environmentID      = "environment-config-capture"
		snapshotRevision   = int64(101)
		assignmentRevision = int64(303)
		targetRevision     = int64(29)
	)
	dynamic := newBackupSecretDynamicRead()
	dynamic.add(testhierarchy.EnvironmentKey(environmentID))
	environmentValue := []byte("sealed-config-capture-environment")
	result := &testkeyvalue.GetManyResult{
		ReadRevision: assignmentRevision,
		Values: []*testkeyvalue.KeyValue{{
			Key: testhierarchy.EnvironmentKey(environmentID), Value: environmentValue, ModRevision: targetRevision,
		}},
	}
	run := &testbackupruntime.BackupRunRecord{TaskID: taskID, EnvironmentID: environmentID}
	source := testbackupruntime.BackupRunSourceAttemptRecord{
		Kind: testbackupruntime.BackupRuntimeSourceConfig, TargetRevision: targetRevision,
		Snapshot: testbackupruntime.BackupRunSourceSnapshot{Config: &testbackupruntime.BackupConfigSourceSnapshot{
			ConfigSnapshotID: taskID, ReadRevision: snapshotRevision,
		}},
	}
	step := &agentpb.BackupCaptureAuthority{
		Resource: &agentpb.BackupResourceIdentity{
			Kind:       agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
			ResourceId: environmentID,
			Resource:   backupSecretFixtureRevision(targetRevision, environmentValue),
		},
		Source: &agentpb.BackupCaptureAuthority_Config{Config: &agentpb.BackupConfigCaptureAuthority{
			MetadataSnapshotRevision: snapshotRevision,
		}},
	}
	reader := &Reader{}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err != nil {
		t.Fatalf("later assignment fixed revision rejected immutable config snapshot: %v", err)
	}

	step.GetConfig().MetadataSnapshotRevision++
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err == nil {
		t.Fatal("mismatched sealed config snapshot revision was accepted")
	}
}

func backupSecretFixtureRevision(revision int64, value []byte) *agentpb.RevisionDigest {
	digest := sha256.Sum256(value)
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: digest[:]}
}
