package etcd

import (
	"crypto/sha256"
	"encoding/hex"
)

func backupTerminalTaskDigest(task TaskRecord) (string, error) {
	value, err := EncodeTaskRecord(task)
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.task.v1\x00"), value...))
	return hex.EncodeToString(digest[:]), nil
}
