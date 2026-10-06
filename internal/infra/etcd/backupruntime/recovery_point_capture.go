package backupruntime

import etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

// Capture metadata is separate from restore authority: introducing presentation
// metadata must not alter immutable snapshots or invalidate older assignments.
func BackupRecoveryPointCaptureKey(pointID string) string {
	return "/v1/records/recovery-point-captures/" + pointID
}

func EncodeRecoveryPointCapture(capture BackupRecoveryPointCapture) ([]byte, error) {
	return EncodeBackupRuntimeRecord("recovery-point-capture", capture, validateRecoveryPointCapture)
}

func readRecoveryPointCapture(value *etcdstore.KeyValue, revision int64) (BackupRecoveryPointCapture, error) {
	if value == nil {
		return BackupRecoveryPointCapture{}, nil
	}
	if value.Version != 1 || value.ModRevision != revision {
		return BackupRecoveryPointCapture{}, CorruptBackupRuntimeRecord()
	}
	return DecodeBackupRuntimeRecord(value.Value, "recovery-point-capture", validateRecoveryPointCapture)
}
