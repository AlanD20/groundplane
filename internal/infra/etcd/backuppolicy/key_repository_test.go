package backuppolicy

import (
	"context"
	"testing"
	"time"

	"filippo.io/age"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

const keyRepositoryTestEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestKeyRepositoryReadsMetadataAndEncryptedIdentityAtOneRevision(t *testing.T) {
	t.Parallel()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	record := BackupKeyRecord{
		EnvironmentID: keyRepositoryTestEnvironmentID, Recipient: identity.Recipient().String(),
		KeyEra: 1, CreatedAt: at, RotatedAt: at,
	}
	encrypted := BackupKeyEncryptedValue{
		EnvironmentID: keyRepositoryTestEnvironmentID, KeyEra: 1,
		Ciphertext: []byte("controller-key-wrapped-age-identity"),
	}
	recordValue, err := EncodeBackupKeyRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	encryptedValue, err := EncodeBackupKeyEncryptedValue(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	store := &keyRepositoryTestStore{revision: 17, values: map[string][]byte{
		BackupKeyKey(keyRepositoryTestEnvironmentID):      recordValue,
		BackupKeyValueKey(keyRepositoryTestEnvironmentID): encryptedValue,
	}}
	reader := &KeyRepository{store: store}
	stored, found, err := reader.GetBackupKey(context.Background(), keyRepositoryTestEnvironmentID)
	if err != nil || !found {
		t.Fatalf("GetBackupKey() = %#v, %t, %v", stored, found, err)
	}
	defer clear(stored.Encrypted.Ciphertext)
	if stored.Record != record || string(stored.Encrypted.Ciphertext) != string(encrypted.Ciphertext) ||
		stored.RecordRevision != store.revision || stored.EncryptedRevision != store.revision ||
		stored.ReadRevision != store.revision {
		t.Fatalf("GetBackupKey() = %#v, want matching fixed-revision state", stored)
	}
	if err := ValidateVersionedBackupKey(stored); err != nil {
		t.Fatalf("ValidateVersionedBackupKey() error = %v", err)
	}
}

func TestKeyRepositoryDistinguishesAbsentFromSplitKeyState(t *testing.T) {
	t.Parallel()
	store := &keyRepositoryTestStore{revision: 9, values: map[string][]byte{}}
	reader := &KeyRepository{store: store}
	missing, found, err := reader.GetBackupKey(context.Background(), keyRepositoryTestEnvironmentID)
	if err != nil || found || missing.ReadRevision != store.revision {
		t.Fatalf("GetBackupKey(absent) = %#v, %t, %v", missing, found, err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	store.values[BackupKeyKey(keyRepositoryTestEnvironmentID)], err = EncodeBackupKeyRecord(BackupKeyRecord{
		EnvironmentID: keyRepositoryTestEnvironmentID, Recipient: identity.Recipient().String(),
		KeyEra: 1, CreatedAt: at, RotatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.GetBackupKey(context.Background(), keyRepositoryTestEnvironmentID); err == nil {
		t.Fatal("GetBackupKey(split state) succeeded")
	}
}

type keyRepositoryTestStore struct {
	revision int64
	values   map[string][]byte
}

func (store *keyRepositoryTestStore) GetMany(
	_ context.Context,
	request etcdstore.GetManyRequest,
) (*etcdstore.GetManyResult, error) {
	values := make([]*etcdstore.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		if value, ok := store.values[key]; ok {
			values[index] = &etcdstore.KeyValue{
				Key: key, Value: append([]byte(nil), value...), ModRevision: store.revision,
			}
		}
	}
	return &etcdstore.GetManyResult{
		Values: values, ReadRevision: store.revision, ResponseRevision: store.revision,
	}, nil
}
