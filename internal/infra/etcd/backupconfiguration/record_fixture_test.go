package backupconfiguration

import (
	sha256 "crypto/sha256"
	hex "encoding/hex"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	time "time"
)

func testBackupConfigSnapshotRecord() BackupConfigSnapshotRecord {
	now := testBackupConfigTime()
	return BackupConfigSnapshotRecord{
		SnapshotID: ids.NewAt(ids.KindTask, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		SourceID: ids.NewAt(ids.KindBackupSource, now, 3), State: BackupConfigSnapshotSealed,
		ReadRevision: 100, NextEntryOrdinal: 2, EntryCount: 2, DescriptorChunkCount: 2,
		ValueChunkCount: 2, PlainValueBytes: 16,
		DescriptorChainSHA256:  testBackupConfigSHA256([]byte("descriptor-chain")),
		StoredValueChainSHA256: testBackupConfigSHA256([]byte("stored-value-chain")),
		StoredManifestSHA256:   testBackupConfigSHA256([]byte("stored-manifest")),
		CreatedAt:              now, UpdatedAt: now.Add(time.Second),
	}
}

func testBackupConfigTime() time.Time {
	return time.Date(2026, 8, 24, 12, 0, 0, 123456789, time.UTC)
}

func testBackupConfigSHA256(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}
