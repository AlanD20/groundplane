package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backupTerminalResultDigest(result *taskjournal.TaskResultRecord) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.result.v1\x00", result)
}

func backupTerminalTaskDigest(task TaskRecord) (string, error) {
	value, err := encodeTaskRecord(task)
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.task.v1\x00"), value...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalDomainDigest(value any) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.domain.v1\x00", value)
}

func backupTerminalEnvironmentEpochDigest(environmentID string) (string, error) {
	value, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
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

func backupTerminalReceiptDigest(record BackupTerminalReceiptRecord) (string, error) {
	record.ReceiptDigest = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.receipt.v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}
