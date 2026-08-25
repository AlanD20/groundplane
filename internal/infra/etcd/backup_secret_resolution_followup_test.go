package etcd

import (
	"context"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type secretOwnedGetManyStoreFunc func(context.Context, GetManyRequest) (*GetManyResult, error)

func (fn secretOwnedGetManyStoreFunc) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	return fn(ctx, request)
}

// Rationale: a malformed later etcd batch must clear both that response and
// every sensitive buffer already accumulated from earlier batches.
func TestBackupSecretFixedReadClearsEarlierAndMalformedLaterResponses(t *testing.T) {
	t.Parallel()

	keys := make([]string, maximumTransactionOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/secret-owned/%03d", index)
	}
	var returned [][]byte
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request GetManyRequest) (*GetManyResult, error) {
			calls++
			values := make([]*KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				value := []byte{byte(calls), byte(index + 1), 0x7f}
				returned = append(returned, value)
				values[index] = &KeyValue{Key: key, Value: value, ModRevision: int64(index + 1)}
			}
			if calls == 2 {
				values[0].Key = "/secret-owned/malformed"
			}
			return &GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: request.Revision,
			}, nil
		},
	)

	reader := &BackupSecretResolutionReader{store: store}
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

	keys := make([]string, maximumTransactionOperations+1)
	for index := range keys {
		keys[index] = fmt.Sprintf("/fixed-secret/%03d", index)
	}
	const fixedRevision int64 = 77
	snapshot := []byte("sealed-at-77")
	current := []byte("sealed-at-77")
	calls := 0
	store := secretOwnedGetManyStoreFunc(
		func(_ context.Context, request GetManyRequest) (*GetManyResult, error) {
			calls++
			if request.Revision != fixedRevision {
				t.Fatalf("batch revision = %d, want %d", request.Revision, fixedRevision)
			}
			values := make([]*KeyValue, len(request.Keys))
			for index, key := range request.Keys {
				selected := current
				if request.Revision == fixedRevision {
					selected = snapshot
				}
				values[index] = &KeyValue{
					Key:         key,
					Value:       append([]byte(nil), selected...),
					ModRevision: fixedRevision,
				}
			}
			if calls == 1 {
				current = []byte("mutated-between-batches")
			}
			return &GetManyResult{
				Values: values, ReadRevision: request.Revision, ResponseRevision: fixedRevision + 1,
			}, nil
		},
	)

	reader := &BackupSecretResolutionReader{store: store}
	result, err := reader.readFixed(context.Background(), keys, fixedRevision)
	if err != nil {
		t.Fatalf("readFixed: %v", err)
	}
	defer clearKeyValues(result.Values)
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
	dynamic.add(environmentKey(environmentID))
	result := &GetManyResult{
		ReadRevision: assignmentRevision,
		Values: []*KeyValue{{
			Key: environmentKey(environmentID), ModRevision: targetRevision,
		}},
	}
	run := &BackupRunRecord{TaskID: taskID, EnvironmentID: environmentID}
	source := BackupRunSourceAttemptRecord{
		Kind: BackupRuntimeSourceConfig, TargetRevision: targetRevision,
		Snapshot: BackupRunSourceSnapshot{Config: &BackupConfigSourceSnapshot{
			ConfigSnapshotID: taskID, ReadRevision: snapshotRevision,
		}},
	}
	step := &agentpb.BackupSourceCapture{Source: &agentpb.BackupSourceCapture_Config{
		Config: &agentpb.BackupConfigSource{SnapshotRevision: uint64(snapshotRevision)},
	}}
	reader := &BackupSecretResolutionReader{}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err != nil {
		t.Fatalf("later assignment fixed revision rejected immutable config snapshot: %v", err)
	}

	step.GetConfig().SnapshotRevision++
	if err := reader.validateCaptureTargetEvidence(result, dynamic, run, source, step); err == nil {
		t.Fatal("mismatched sealed config snapshot revision was accepted")
	}
}
