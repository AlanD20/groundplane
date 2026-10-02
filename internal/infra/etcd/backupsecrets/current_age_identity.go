package backupsecrets

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CurrentAgeIdentityEvidence owns only encrypted private material selected
// by the sealed slot. Evidence.Clear releases that ciphertext buffer.
type CurrentAgeIdentityEvidence struct {
	Record            backuppolicy.BackupKeyRecord
	Encrypted         backuppolicy.BackupKeyEncryptedValue
	RecordRevision    int64
	EncryptedRevision int64
}

func planCurrentAgeIdentityKeys(
	dynamic *backupSecretDynamicRead,
	step *agentpb.ExecutionStep,
	environmentID string,
) error {
	purposes, err := backupsecret.Purposes(step)
	if err != nil {
		return err
	}
	for _, purpose := range purposes {
		switch purpose {
		case agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY:
			dynamic.currentAgeKey = dynamic.add(backuppolicy.BackupKeyKey(environmentID))
			dynamic.currentAgeValue = dynamic.add(backuppolicy.BackupKeyValueKey(environmentID))
		}
	}
	return nil
}

func decodeCurrentAgeIdentityEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *Evidence,
	step *agentpb.ExecutionStep,
) error {
	purposes, err := backupsecret.Purposes(step)
	if err != nil {
		return err
	}
	required := false
	for _, purpose := range purposes {
		if purpose == agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY {
			required = true
		}
	}
	if !required {
		return nil
	}
	environmentID, recipient, era := "", "", 0
	recordRevision, valueRevision := int64(0), int64(0)
	encryption := step.GetBackupStep().GetCapture().GetEncryption()
	if run := evidence.Run; run != nil {
		environmentID, recipient, era = run.EnvironmentID, run.Recipient, run.KeyEra
		recordRevision, valueRevision = run.BackupKeyRecordRevision, run.BackupKeyValueRevision
	} else if restored := evidence.Restore; restored != nil && !restored.UsesOldIdentity {
		environmentID, recipient, era = restored.EnvironmentID, restored.Point.Recipient, restored.Point.KeyEra
		recordRevision, valueRevision = restored.ExpectedKeyRecordRevision, restored.ExpectedKeyValueRevision
		encryption = step.GetBackupStep().GetRestore().GetEncryption()
	}
	if environmentID == "" || encryption == nil || recordRevision <= 0 || valueRevision <= 0 {
		return errs.New(errs.KindStateConflict, "current age identity lacks sealed operation evidence")
	}
	if dynamic.currentAgeKey < 0 || dynamic.currentAgeValue < 0 ||
		dynamic.currentAgeKey >= len(result.Values) || dynamic.currentAgeValue >= len(result.Values) {
		return errs.New(errs.KindStateConflict, "backup current age identity evidence is unavailable")
	}
	keyValue := result.Values[dynamic.currentAgeKey]
	encryptedValue := result.Values[dynamic.currentAgeValue]
	if keyValue == nil || keyValue.ModRevision != recordRevision ||
		encryptedValue == nil || encryptedValue.ModRevision != valueRevision ||
		!backupSecretRecordMatches(encryptedValue, encryption.SecretSlot) {
		return errs.New(errs.KindStateConflict, "backup sealed current age identity changed")
	}
	key, err := backuppolicy.DecodeBackupKeyRecord(keyValue.Value)
	if err != nil || key.EnvironmentID != environmentID || key.KeyEra != era || key.Recipient != recipient {
		return errs.New(errs.KindStateConflict, "backup current age recipient or key era changed")
	}
	recipientDigest := sha256.Sum256([]byte(key.Recipient))
	if !bytes.Equal(recipientDigest[:], encryption.RecipientSha256) {
		return errs.New(errs.KindStateConflict, "backup sealed current age recipient digest changed")
	}
	encrypted, err := backuppolicy.DecodeBackupKeyEncryptedValue(encryptedValue.Value)
	if err != nil {
		return errs.New(errs.KindStateConflict, "backup encrypted current age identity is invalid")
	}
	if encrypted.EnvironmentID != key.EnvironmentID || encrypted.KeyEra != key.KeyEra {
		clear(encrypted.Ciphertext)
		return errs.New(errs.KindStateConflict, "backup encrypted current age identity ownership changed")
	}
	evidence.CurrentAgeIdentity = &CurrentAgeIdentityEvidence{
		Record: key, Encrypted: encrypted, RecordRevision: keyValue.ModRevision, EncryptedRevision: encryptedValue.ModRevision,
	}
	return nil
}
