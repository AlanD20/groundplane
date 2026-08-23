package etcd

import (
	"context"
	"testing"
	"time"

	"filippo.io/age"
)

func TestBackupPolicyRepositoryReadsKeyMetadataAndEncryptedIdentityAtOneRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	at := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	record := BackupKeyRecord{
		EnvironmentID: environment.Record.ID, Recipient: identity.Recipient().String(),
		KeyEra: 1, CreatedAt: at, RotatedAt: at,
	}
	encrypted := BackupKeyEncryptedValue{
		EnvironmentID: environment.Record.ID, KeyEra: 1,
		Ciphertext: []byte("controller-key-wrapped-age-identity"),
	}
	recordValue, err := encodeBackupKeyRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupKeyRecord() error = %v", err)
	}
	defer clear(recordValue)
	encryptedValue, err := encodeBackupKeyEncryptedValue(encrypted)
	if err != nil {
		t.Fatalf("encodeBackupKeyEncryptedValue() error = %v", err)
	}
	defer clear(encryptedValue)
	transaction, err := store.Transact(ctx, []Condition{
		{Key: backupKeyKey(environment.Record.ID)},
		{Key: backupKeyValueKey(environment.Record.ID)},
	}, []Mutation{
		{Type: MutationPut, Key: backupKeyKey(environment.Record.ID), Value: recordValue},
		{Type: MutationPut, Key: backupKeyValueKey(environment.Record.ID), Value: encryptedValue},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed Backup key = %#v, %v", transaction, err)
	}

	stored, found, err := repository.GetBackupKey(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetBackupKey() = %#v, %t, %v", stored, found, err)
	}
	defer clear(stored.Encrypted.Ciphertext)
	if stored.Record != record || string(stored.Encrypted.Ciphertext) != string(encrypted.Ciphertext) ||
		stored.RecordRevision != transaction.Revision || stored.EncryptedRevision != transaction.Revision ||
		stored.ReadRevision < transaction.Revision {
		t.Fatalf("GetBackupKey() = %#v, want matching fixed-revision state", stored)
	}
	if err := validateVersionedBackupKey(stored); err != nil {
		t.Fatalf("validateVersionedBackupKey() error = %v", err)
	}
}

func TestBackupPolicyRepositoryDistinguishesAbsentFromSplitKeyState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := backupPolicyRepositoryTestHierarchy(t)
	missing, found, err := repository.GetBackupKey(ctx, environment.Record.ID)
	if err != nil || found || missing.ReadRevision <= 0 {
		t.Fatalf("GetBackupKey(absent) = %#v, %t, %v", missing, found, err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	at := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	value, err := encodeBackupKeyRecord(BackupKeyRecord{
		EnvironmentID: environment.Record.ID, Recipient: identity.Recipient().String(),
		KeyEra: 1, CreatedAt: at, RotatedAt: at,
	})
	if err != nil {
		t.Fatalf("encodeBackupKeyRecord() error = %v", err)
	}
	defer clear(value)
	transaction, err := store.Transact(
		ctx,
		[]Condition{{Key: backupKeyKey(environment.Record.ID)}},
		[]Mutation{{Type: MutationPut, Key: backupKeyKey(environment.Record.ID), Value: value}},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed split Backup key = %#v, %v", transaction, err)
	}
	if _, _, err := repository.GetBackupKey(ctx, environment.Record.ID); err == nil {
		t.Fatal("GetBackupKey(split state) succeeded")
	}
}
