package etcd

import (
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func newbackupPolicyInitialKey(
	environmentID string,
	now time.Time,
	material *BackupPolicyInitialKeyMaterial,
) (*backupPolicyInitialKey, error) {
	if material == nil {
		return nil, errs.New(errs.KindValidationFailed, "initial age key material is required")
	}
	initial := &backupPolicyInitialKey{
		Record: backuppolicy.BackupKeyRecord{
			EnvironmentID: environmentID,
			Recipient:     material.Recipient,
			KeyEra:        1,
			CreatedAt:     now,
			RotatedAt:     now,
		},
		Encrypted: backuppolicy.BackupKeyEncryptedValue{
			EnvironmentID: environmentID,
			KeyEra:        1,
			Ciphertext:    append([]byte(nil), material.Ciphertext...),
		},
	}
	if err := backuppolicy.ValidateBackupKeyRecord(initial.Record); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return nil, err
	}
	if err := backuppolicy.ValidateBackupKeyEncryptedValue(initial.Encrypted); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return nil, err
	}
	return initial, nil
}

func backupPolicyProjectionFromCandidate(candidate backupPolicyReplacementCandidate) BackupPolicyProjection {
	projection := BackupPolicyProjection{
		EnvironmentID: candidate.Replacement.EnvironmentID,
		Enabled:       candidate.Replacement.Enabled,
		Frequency:     candidate.Replacement.Frequency,
		Keep:          candidate.Replacement.Keep,
		Encryption:    candidate.Replacement.Encryption,
		ConnectorID:   candidate.Replacement.ConnectorID,
		Sources:       make([]BackupPolicySourceProjection, len(candidate.Sources)),
		NextRunAt:     backupPolicyNextRunAt(candidate.NextRunAt),
	}
	for index, source := range candidate.Sources {
		projection.Sources[index] = BackupPolicySourceProjection{
			ID: source.Source.Record.ID, Kind: source.Source.Record.Kind,
			TargetID: source.Source.Record.TargetID,
		}
	}
	if candidate.ExistingKey != nil {
		projection.AgeRecipient = candidate.ExistingKey.Record.Recipient
		projection.KeyEra = candidate.ExistingKey.Record.KeyEra
		projection.KeyCreatedAt = candidate.ExistingKey.Record.CreatedAt
		projection.KeyRotatedAt = candidate.ExistingKey.Record.RotatedAt
	} else if candidate.InitialKey != nil {
		projection.AgeRecipient = candidate.InitialKey.Record.Recipient
		projection.KeyEra = candidate.InitialKey.Record.KeyEra
		projection.KeyCreatedAt = candidate.InitialKey.Record.CreatedAt
		projection.KeyRotatedAt = candidate.InitialKey.Record.RotatedAt
	}
	return projection
}

func cloneBackupPolicyEvidenceKeyValue(value *etcdstore.KeyValue) *etcdstore.KeyValue {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Value = append([]byte(nil), value.Value...)
	return &clone
}
