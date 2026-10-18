package app

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-50: plaintext delivery requires one atomic native grant over real source,
// credential and Agent ownership reads. A successful read alone cannot authorize it.
func TestNativeBackupCredentialGrantFencesConcurrentChanges(t *testing.T) {
	for _, mutation := range []string{"secret revision", "source removal", "Task revision", "Agent process", "unknown commit"} {
		t.Run(mutation, func(t *testing.T) {
			f := newNativeBackupCredentialFixture(t)
			switch mutation {
			case "secret revision":
				f.store.before = func() { f.rewrite(t, f.secretKey) }
			case "source removal":
				f.store.before = func() {
					if _, err := f.store.memoryHierarchyStore.Delete(t.Context(), f.sourceKey); err != nil {
						t.Fatal(err)
					}
				}
			case "Task revision":
				f.store.before = func() { f.rewrite(t, workqueue.TaskKey(f.attempt.TaskId)) }
			case "Agent process":
				f.store.before = func() {
					if _, err := f.agents.RegisterWorkProcess(t.Context(), f.agentID, 1, [16]byte{2}, 1, f.operations); err != nil {
						t.Fatal(err)
					}
				}
			case "unknown commit":
				f.store.loseResponse = true
			}
			input, slots, err := f.delivery.ResolveWorkSecretSlots(t.Context(), f.authorization, f.attempt, 0)
			if f.store.before != nil || f.store.loseResponse {
				t.Fatalf("credential delivery never reached the injected grant boundary: %v", err)
			}
			if err == nil || input != nil || slots != nil || len(f.crypt.opened) != 0 {
				t.Fatalf("changed ownership opened credentials: input=%t slots=%d opens=%d err=%v",
					input != nil, len(slots), len(f.crypt.opened), err)
			}
			grants, rangeErr := f.store.Range(t.Context(), keyvalue.RangeRequest{
				Prefix: "/v1/runtime/backup-work-credential-grants/", Limit: 10})
			if rangeErr != nil {
				t.Fatal(rangeErr)
			}
			if mutation == "unknown commit" {
				if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || len(grants.Values) != 1 {
					t.Fatal("lost transaction response did not retain its single committed grant")
				}
				f.resolveAndCheck(t)
			} else if len(grants.Values) != 0 || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("failed ownership comparison did not reject the grant: %v", err)
			}
		})
	}
}

// TASK-50: fixed-revision mixed Connector/Secret evidence must yield owned slots
// only after native admission; reconnect revalidates without accumulating grants.
func TestNativeBackupCredentialGrantDeliversOwnedSlotsAndRevalidates(t *testing.T) {
	f := newNativeBackupCredentialFixture(t)
	f.resolveAndCheck(t)
	f.resolveAndCheck(t)
	grants, err := f.store.Range(t.Context(), keyvalue.RangeRequest{
		Prefix: "/v1/runtime/backup-work-credential-grants/", Limit: 10})
	if err != nil || len(grants.Values) != 1 {
		t.Fatalf("repeat delivery accumulated grants: %v", err)
	}
}

func (f *nativeBackupCredentialFixture) resolveAndCheck(t *testing.T) {
	t.Helper()
	input, slots, err := f.delivery.ResolveWorkSecretSlots(t.Context(), f.authorization, f.attempt, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, value := range slots {
			clear(value)
		}
	}()
	if input == nil || input.EnvironmentId != f.environmentID || len(slots) != 2 ||
		!bytes.Equal(
			slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY],
			[]byte("app-access"),
		) ||
		!bytes.Equal(
			slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY],
			[]byte("app-secret"),
		) {
		t.Fatal("native delivery returned different credentials or scope")
	}
	for _, buffer := range append(append([][]byte(nil), f.crypt.ciphertexts...), f.crypt.opened...) {
		for _, value := range buffer {
			if value != 0 {
				t.Fatal("native delivery retained provider plaintext or ciphertext")
			}
		}
	}
}

func (f *nativeBackupCredentialFixture) rewrite(t *testing.T, key string) {
	t.Helper()
	current, err := f.store.Get(t.Context(), key)
	if err != nil || current.Entry == nil {
		t.Fatalf("missing concurrent-change target %s: %v", key, err)
	}
	if _, err := f.store.memoryHierarchyStore.Put(t.Context(), key, current.Entry.Value); err != nil {
		t.Fatal(err)
	}
}

type backupCredentialCASStore struct {
	*memoryHierarchyStore
	before       func()
	loseResponse bool
}

func (store *backupCredentialCASStore) Transact(ctx context.Context,
	conditions []keyvalue.Condition, mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if before := store.before; before != nil {
		store.before = nil
		before()
	}
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if store.loseResponse {
		store.loseResponse = false
		return keyvalue.TransactionResult{}, errs.New(errs.KindStorageUnavailable, "injected lost grant response")
	}
	return result, err
}

type nativeBackupCredentialCrypt struct{ ciphertexts, opened [][]byte }

func (*nativeBackupCredentialCrypt) Seal(context.Context, []byte) ([]byte, error) {
	return nil, errs.New(errs.KindInternal, "unexpected credential fixture encryption")
}

func (crypt *nativeBackupCredentialCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	crypt.ciphertexts = append(crypt.ciphertexts, ciphertext)
	var value []byte
	switch string(ciphertext) {
	case "native-direct-envelope":
		value = []byte(`{"access_key":"app-access"}`)
	case "native-secret-envelope":
		value = []byte("app-secret")
	default:
		return nil, errs.New(errs.KindInternal, "unexpected fixture ciphertext")
	}
	crypt.opened = append(crypt.opened, value)
	return value, nil
}
