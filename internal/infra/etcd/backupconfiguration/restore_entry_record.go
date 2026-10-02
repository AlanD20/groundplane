package backupconfiguration

import (
	"bytes"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// A receipt pins the original immutable value, including its exact ciphertext.
// Reconnect may verify it, but cannot reseal the same generation differently.
type ConfigRestoreEntryReceipt struct {
	Owner          ConfigRestoreTransferOwner `json:"owner"`
	Ordinal        uint32                     `json:"ordinal"`
	EntryID        string                     `json:"entry_id"`
	Secret         bool                       `json:"secret"`
	MetadataSHA256 string                     `json:"metadata_sha256"`
	ValueSizeBytes uint64                     `json:"value_size_bytes"`
	ValueSHA256    string                     `json:"value_sha256"`
	StoredSHA256   string                     `json:"stored_sha256"`
}

type ConfigRestoreEntryCursor struct {
	Owner       ConfigRestoreTransferOwner `json:"owner"`
	LastOrdinal uint32                     `json:"last_ordinal"`
	LastEntryID string                     `json:"last_entry_id"`
}

func ConfigRestoreEntryReceiptKey(owner ConfigRestoreTransferOwner, ordinal uint32) string {
	return configTransferPrefix(owner.Transfer.Binding) + fmt.Sprintf("restore-entry/%08d", ordinal)
}

func ConfigRestoreEntryCursorKey(owner ConfigRestoreTransferOwner) string {
	return configTransferPrefix(owner.Transfer.Binding) + "restore-entry-cursor"
}

func (receipt ConfigRestoreEntryReceipt) ValueKey() string {
	if receipt.Secret {
		return entryvalues.SecretKey(receipt.EntryID, receipt.Owner.GenerationID)
	}
	return entryvalues.PlainKey(receipt.EntryID, receipt.Owner.GenerationID)
}

func encodeConfigRestoreEntryReceipt(receipt ConfigRestoreEntryReceipt) ([]byte, error) {
	if ValidateConfigRestoreTransferOwner(receipt.Owner) != nil || receipt.Ordinal == 0 ||
		receipt.Ordinal > backupconfig.MaxEntries ||
		ids.Validate(ids.KindEnvEntry, receipt.EntryID) != nil ||
		receipt.ValueSizeBytes > backupconfig.MaxSelectedValueBytes ||
		!recordcodec.ValidSHA256(receipt.MetadataSHA256) ||
		!recordcodec.ValidSHA256(receipt.StoredSHA256) {
		return nil, captureSnapshotConflict()
	}
	if receipt.Secret && (receipt.ValueSizeBytes != 0 || receipt.ValueSHA256 != "") ||
		!receipt.Secret && !recordcodec.ValidSHA256(receipt.ValueSHA256) {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode("backup-config-restore-entry", receipt)
}

func decodeConfigRestoreEntryReceipt(raw []byte) (ConfigRestoreEntryReceipt, error) {
	receipt, err := recordcodec.Decode[ConfigRestoreEntryReceipt](raw, "backup-config-restore-entry")
	if err != nil || len(raw) > 8192 {
		return ConfigRestoreEntryReceipt{}, captureSnapshotConflict()
	}
	canonical, err := encodeConfigRestoreEntryReceipt(receipt)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ConfigRestoreEntryReceipt{}, captureSnapshotConflict()
	}
	return receipt, nil
}

func encodeConfigRestoreEntryCursor(cursor ConfigRestoreEntryCursor) ([]byte, error) {
	if ValidateConfigRestoreTransferOwner(cursor.Owner) != nil || cursor.LastOrdinal == 0 ||
		cursor.LastOrdinal > backupconfig.MaxEntries ||
		ids.Validate(ids.KindEnvEntry, cursor.LastEntryID) != nil {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode("backup-config-restore-entry-cursor", cursor)
}

func decodeConfigRestoreEntryCursor(raw []byte) (ConfigRestoreEntryCursor, error) {
	cursor, err := recordcodec.Decode[ConfigRestoreEntryCursor](raw, "backup-config-restore-entry-cursor")
	if err != nil || len(raw) > 8192 {
		return ConfigRestoreEntryCursor{}, captureSnapshotConflict()
	}
	canonical, err := encodeConfigRestoreEntryCursor(cursor)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ConfigRestoreEntryCursor{}, captureSnapshotConflict()
	}
	return cursor, nil
}
