package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every published Environment must have a decodable canonical
// mutation epoch before a resource transaction can claim fixed evidence.
func TestEnvironmentMutationFenceRejectsMissingAndCorruptEpoch(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value []byte
		kind  MutationType
	}{
		{name: "missing", kind: MutationDelete},
		{name: "corrupt", kind: MutationPut, value: []byte("not-json")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
			result, err := store.Transact(context.Background(), nil, []Mutation{{
				Type: test.kind, Key: environmentMutationEpochKey(environment.Record.ID), Value: test.value,
			}})
			if err != nil || !result.Succeeded {
				t.Fatalf("seed epoch state = %#v, %v", result, err)
			}
			_, err = loadOrdinaryEnvironmentMutationFence(
				context.Background(), store, environment.Record.ID, result.Revision,
			)
			if !isKind(err, errs.KindInternal) {
				t.Fatalf("loadOrdinaryEnvironmentMutationFence() error = %v", err)
			}
		})
	}
}

// Rationale: ordinary Environment mutation must never enter while a durable
// persistence operation owns the canonical lock.
func TestEnvironmentMutationFenceRejectsHeldOrdinaryLock(t *testing.T) {
	t.Parallel()
	_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	owner := environmentMutationFenceTestOwner(time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC), 100)
	revision := putEnvironmentMutationFenceTestLock(t, store, environment.Record.ID, owner)
	_, err := loadOrdinaryEnvironmentMutationFence(
		context.Background(), store, environment.Record.ID, revision,
	)
	if !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("loadOrdinaryEnvironmentMutationFence() error = %v", err)
	}
}

// Rationale: restartable persistence work may reuse the fence only when the
// decoded lock identifies its exact operation and Task owner.
func TestEnvironmentMutationFenceValidatesOwnedLock(t *testing.T) {
	t.Parallel()
	_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	at := time.Date(2026, 8, 24, 20, 10, 0, 0, time.UTC)
	owner := environmentMutationFenceTestOwner(at, 200)
	revision := putEnvironmentMutationFenceTestLock(t, store, environment.Record.ID, owner)
	evidence, err := loadOwnedEnvironmentMutationFence(
		context.Background(), store, environment.Record.ID, revision, owner,
	)
	if err != nil {
		t.Fatalf("loadOwnedEnvironmentMutationFence() error = %v", err)
	}
	if evidence.readAtRevision() != revision {
		t.Fatalf("readAtRevision() = %d, want %d", evidence.readAtRevision(), revision)
	}
	wrongOwner := owner
	wrongOwner.TaskID = ids.NewAt(ids.KindTask, at, 203)
	if _, err := loadOwnedEnvironmentMutationFence(
		context.Background(), store, environment.Record.ID, revision, wrongOwner,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("loadOwnedEnvironmentMutationFence(wrong owner) error = %v", err)
	}
}

// Rationale: ancestry discovery may require multiple reads, but every read
// must use the caller's single selected MVCC revision even after later writes.
func TestEnvironmentMutationFenceUsesOneFixedRevision(t *testing.T) {
	t.Parallel()
	_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	selectedRevision := store.revision
	advanceEnvironmentMutationFenceEpoch(t, store, environment.Record.ID)
	audited := &environmentMutationFenceAuditStore{memoryHierarchyStore: store}
	evidence, err := loadOrdinaryEnvironmentMutationFence(
		context.Background(), audited, environment.Record.ID, selectedRevision,
	)
	if err != nil {
		t.Fatalf("loadOrdinaryEnvironmentMutationFence() error = %v", err)
	}
	if evidence.readAtRevision() != selectedRevision || len(audited.revisions) != 3 {
		t.Fatalf("fixed read = %d, calls = %v", evidence.readAtRevision(), audited.revisions)
	}
	for _, revision := range audited.revisions {
		if revision != selectedRevision {
			t.Fatalf("GetMany revision = %d, want %d", revision, selectedRevision)
		}
	}
}

// Rationale: a successful resource transaction must advance the Environment
// epoch in the same commit while retaining the helper's bounded condition set.
func TestEnvironmentMutationFenceAdvancesEpochAtomically(t *testing.T) {
	t.Parallel()
	_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	evidence, err := loadOrdinaryEnvironmentMutationFence(
		context.Background(), store, environment.Record.ID, store.revision,
	)
	if err != nil {
		t.Fatalf("loadOrdinaryEnvironmentMutationFence() error = %v", err)
	}
	conditions := evidence.transactionConditions()
	if len(conditions)+1 > maximumTransactionOperations {
		t.Fatalf("transaction operations = %d", len(conditions)+1)
	}
	mutation, err := evidence.epochRewriteMutation()
	if err != nil {
		t.Fatalf("epochRewriteMutation() error = %v", err)
	}
	defer clear(mutation.Value)
	result, err := store.Transact(context.Background(), conditions, []Mutation{mutation})
	if err != nil || !result.Succeeded {
		t.Fatalf("Transact() = %#v, %v", result, err)
	}
	stored, err := store.Get(context.Background(), environmentMutationEpochKey(environment.Record.ID))
	if err != nil || stored.Entry == nil || stored.Entry.ModRevision != result.Revision {
		t.Fatalf("stored epoch = %#v, %v", stored, err)
	}
}

// Rationale: epoch or lock races must fail the compare, report a state
// conflict, and perform none of the caller's resource or epoch writes.
func TestEnvironmentMutationFenceCASConflictsPerformNoWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		race func(*testing.T, *memoryHierarchyStore, string)
	}{
		{name: "epoch advanced", race: advanceEnvironmentMutationFenceEpoch},
		{name: "lock acquired", race: func(t *testing.T, store *memoryHierarchyStore, environmentID string) {
			putEnvironmentMutationFenceTestLock(
				t,
				store,
				environmentID,
				environmentMutationFenceTestOwner(time.Date(2026, 8, 24, 20, 30, 0, 0, time.UTC), 300),
			)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
			evidence, err := loadOrdinaryEnvironmentMutationFence(
				context.Background(), store, environment.Record.ID, store.revision,
			)
			if err != nil {
				t.Fatalf("loadOrdinaryEnvironmentMutationFence() error = %v", err)
			}
			test.race(t, store, environment.Record.ID)
			before := store.revision
			mutation, err := evidence.epochRewriteMutation()
			if err != nil {
				t.Fatalf("epochRewriteMutation() error = %v", err)
			}
			defer clear(mutation.Value)
			sentinel := "/v1/test/environment-mutation-fence/no-write/" + test.name
			result, err := store.Transact(
				context.Background(),
				evidence.transactionConditions(),
				[]Mutation{mutation, {Type: MutationPut, Key: sentinel, Value: []byte("written")}},
			)
			if err != nil || result.Succeeded {
				t.Fatalf("Transact() = %#v, %v", result, err)
			}
			conflict := evidence.classifyCAS(result.FailureReads)
			clearKeyValues(result.FailureReads)
			if !isKind(conflict, errs.KindStateConflict) {
				t.Fatalf("classifyCAS() error = %v", conflict)
			}
			if store.revision != before || store.valueAt(sentinel, store.revision) != nil {
				t.Fatalf("failed transaction wrote state at revision %d", store.revision)
			}
		})
	}
}

type environmentMutationFenceAuditStore struct {
	*memoryHierarchyStore
	revisions []int64
}

func (store *environmentMutationFenceAuditStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	store.revisions = append(store.revisions, request.Revision)
	return store.memoryHierarchyStore.GetMany(ctx, request)
}

func environmentMutationFenceTestOwner(at time.Time, seed int64) environmentMutationFenceOwner {
	return environmentMutationFenceOwner{
		Kind:        BackupOperationBackup,
		OperationID: ids.NewAt(ids.KindOperation, at, seed),
		TaskID:      ids.NewAt(ids.KindTask, at, seed+1),
	}
}

func putEnvironmentMutationFenceTestLock(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
	owner environmentMutationFenceOwner,
) int64 {
	t.Helper()
	at := time.Date(2026, 8, 24, 20, 20, 0, 0, time.UTC)
	value, err := encodeBackupOperationLockRecord(BackupOperationLockRecord{
		EnvironmentID: environmentID,
		OperationID:   owner.OperationID,
		TaskID:        owner.TaskID,
		Kind:          owner.Kind,
		CreatedAt:     at,
		UpdatedAt:     at,
	})
	if err != nil {
		t.Fatalf("encodeBackupOperationLockRecord() error = %v", err)
	}
	defer clear(value)
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: environmentOperationLockKey(environmentID), Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("put operation lock = %#v, %v", result, err)
	}
	return result.Revision
}

func advanceEnvironmentMutationFenceEpoch(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) {
	t.Helper()
	value, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{EnvironmentID: environmentID})
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	defer clear(value)
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("advance mutation epoch = %#v, %v", result, err)
	}
}
