package backupruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BackupTerminalResultDigest(result *taskjournal.TaskResultRecord) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.result.v1\x00", result)
}

func BackupTerminalDomainDigest(value any) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.domain.v1\x00", value)
}

func BackupTerminalEnvironmentEpochDigest(environmentID string) (string, error) {
	value, err := EncodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environmentID,
	})
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.epoch.v1\x00"), value...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalCanonicalDigest(domain string, value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(append([]byte(domain), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func BackupTerminalReceiptDigest(record BackupTerminalReceiptRecord) (string, error) {
	record.ReceiptDigest = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.receipt.v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}
