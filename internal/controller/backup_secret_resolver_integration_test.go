package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: mixed Connector mode must decrypt only its direct member, late-
// bind the referenced member, and return exactly the two capture S3 purposes.
func TestBackupSecretResolverResolvesMixedDirectAndReferencedCredentials(t *testing.T) {
	evidence := mixedBackupSecretEvidence()
	directCiphertext := evidence.Credentials.Ciphertext
	secretCiphertext := evidence.SecretValues[0].Value.Ciphertext
	reader := &staticBackupSecretEvidenceReader{evidence: evidence}
	crypt := &observingBackupSecretCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewBackupSecretResolver(reader, protector)
	if err != nil {
		t.Fatal(err)
	}
	slots, err := resolver.ResolveBackupSecretSlots(context.Background(), backupsecret.Request{})
	if err != nil {
		t.Fatalf("resolve backup secret slots: %v", err)
	}
	defer clearBackupSecretSlotMap(slots)
	if len(slots) != 2 ||
		string(
			slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY],
		) != "direct-access" ||
		string(
			slots[agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY],
		) != "project-secret" {
		t.Fatalf("resolved slots = %#v", slots)
	}
	for _, forbidden := range []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY,
	} {
		if _, found := slots[forbidden]; found {
			t.Fatalf("resolver returned forbidden purpose %s", forbidden)
		}
	}
	if !allZeroBytes(directCiphertext) || !allZeroBytes(secretCiphertext) {
		t.Fatal("resolver retained durable ciphertext")
	}
}

// Rationale: a later credential decryption failure must clear the earlier
// direct plaintext, provider partial output, and every ciphertext buffer.
func TestBackupSecretResolverClearsPartialMixedResolution(t *testing.T) {
	evidence := mixedBackupSecretEvidence()
	directCiphertext := evidence.Credentials.Ciphertext
	secretCiphertext := evidence.SecretValues[0].Value.Ciphertext
	reader := &staticBackupSecretEvidenceReader{evidence: evidence}
	crypt := &observingBackupSecretCrypt{failOpenAt: 2}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewBackupSecretResolver(reader, protector)
	if err != nil {
		t.Fatal(err)
	}
	if slots, err := resolver.ResolveBackupSecretSlots(
		context.Background(), backupsecret.Request{},
	); err == nil || slots != nil {
		t.Fatalf("resolve backup secret slots = %#v, %v", slots, err)
	}
	if !allZeroBytes(directCiphertext) || !allZeroBytes(secretCiphertext) {
		t.Fatal("failed resolution retained durable ciphertext")
	}
	for _, returned := range crypt.returned {
		if !allZeroBytes(returned) {
			t.Fatal("failed resolution retained provider plaintext")
		}
	}
}

type staticBackupSecretEvidenceReader struct {
	evidence etcd.BackupSecretResolutionEvidence
}

func (reader *staticBackupSecretEvidenceReader) ResolveBackupSecretEvidence(
	context.Context,
	backupsecret.Request,
) (etcd.BackupSecretResolutionEvidence, error) {
	return reader.evidence, nil
}

type observingBackupSecretCrypt struct {
	openCalls  int
	failOpenAt int
	returned   [][]byte
}

func (crypt *observingBackupSecretCrypt) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (crypt *observingBackupSecretCrypt) Open(_ context.Context, value []byte) ([]byte, error) {
	crypt.openCalls++
	plaintext := append([]byte(nil), value...)
	crypt.returned = append(crypt.returned, plaintext)
	if crypt.openCalls == crypt.failOpenAt {
		return plaintext, errors.New("injected open failure")
	}
	return plaintext, nil
}

func mixedBackupSecretEvidence() etcd.BackupSecretResolutionEvidence {
	direct := []byte(`{"access_key":"direct-access"}`)
	secret := []byte("project-secret")
	return etcd.BackupSecretResolutionEvidence{
		Connector: etcd.ConnectorRecord{
			Connector: core.Connector{Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
				backupsecret.CredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
				backupsecret.CredentialSecretKey: {
					Kind: core.ConnectorCredentialSecretRef, SecretRef: "BACKUP_SECRET_KEY",
				},
			}},
		},
		Credentials:    connectorCredentialEvidence(direct),
		HasCredentials: true,
		SecretValues: []etcd.BackupSecretValueEvidence{{
			Name: backupsecret.CredentialSecretKey, Reference: "BACKUP_SECRET_KEY",
			Value: secretCredentialEvidence(secret),
		}},
	}
}

func connectorCredentialEvidence(ciphertext []byte) etcd.ConnectorEncryptedCredentials {
	digest := sha256.Sum256(ciphertext)
	return etcd.ConnectorEncryptedCredentials{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}
}

func secretCredentialEvidence(ciphertext []byte) etcd.SecretEncryptedValue {
	digest := sha256.Sum256(ciphertext)
	return etcd.SecretEncryptedValue{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}
}

func allZeroBytes(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
