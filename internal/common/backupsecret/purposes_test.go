package backupsecret

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-49: captured encryption, not the credential resolver, selects whether a
// private age identity may be delivered. Old-era identity is Restore-only.
func TestSourcePurposesRestrictsIdentityToCapturedOperation(t *testing.T) {
	access := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY
	secret := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY
	current := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY
	old := agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY
	for _, test := range []struct {
		name       string
		restore    bool
		prune      bool
		encryption agentpb.BackupEncryption
		slot       string
		want       []agentpb.BackupSecretSlotPurpose
	}{
		{name: "unencrypted capture", encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE,
			want: []agentpb.BackupSecretSlotPurpose{access, secret}},
		{name: "age capture", encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			slot: CurrentAgeIdentitySlotID, want: []agentpb.BackupSecretSlotPurpose{access, secret, current}},
		{name: "prune", prune: true, want: []agentpb.BackupSecretSlotPurpose{access, secret}},
		{name: "current-era restore", restore: true, encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			slot: CurrentAgeIdentitySlotID, want: []agentpb.BackupSecretSlotPurpose{access, secret, current}},
		{name: "old-era restore", restore: true, encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			slot: OperatorOldAgeIdentitySlotID, want: []agentpb.BackupSecretSlotPurpose{access, secret, old}},
		{name: "old-era capture rejected", encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			slot: OperatorOldAgeIdentitySlotID},
		{name: "unknown identity rejected", encryption: agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
			slot: "not-a-captured-slot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			encryption := &agentpb.BackupEncryptionAuthority{Kind: test.encryption, SecretSlotId: test.slot}
			step := &agentpb.BackupStepAuthority{}
			switch {
			case test.prune:
				step.Operation = &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{}}
			case test.restore:
				step.Operation = &agentpb.BackupStepAuthority_Restore{
					Restore: &agentpb.BackupRestoreAuthority{Encryption: encryption},
				}
			default:
				step.Operation = &agentpb.BackupStepAuthority_Capture{
					Capture: &agentpb.BackupCaptureAuthority{Encryption: encryption},
				}
			}
			got, err := SourcePurposes(step)
			if test.want == nil {
				if err == nil || got != nil {
					t.Fatal("uncaptured identity purpose was admitted")
				}
			} else if err != nil || !slices.Equal(got, test.want) {
				t.Fatalf("purposes=%v, want %v: %v", got, test.want, err)
			}
		})
	}
}
