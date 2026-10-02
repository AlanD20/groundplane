package backupconfiguration

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConfigTransferOwner binds progress to the exact native assignment. The
// digest authenticates the sealed source; records contain no selected values.
type ConfigTransferOwner struct {
	Binding              backupconfigtransfer.Binding `json:"binding"`
	AgentID              string                       `json:"agent_id"`
	AgentGeneration      uint64                       `json:"agent_generation"`
	AssignmentGeneration uint64                       `json:"assignment_generation"`
	AuthoritySHA256      string                       `json:"authority_sha256"`
}

type ConfigTransferCursor struct {
	Owner                          ConfigTransferOwner `json:"owner"`
	LastCreditSequence             uint64              `json:"last_credit_sequence"`
	MetadataAcceptedCreditSequence uint64              `json:"metadata_accepted_credit_sequence"`
}

type ConfigTransferCredit struct {
	Owner  ConfigTransferOwner
	Credit *agentpb.BackupConfigCredit
}

type configTransferCreditData struct {
	Owner  ConfigTransferOwner `json:"owner"`
	Credit []byte              `json:"credit"`
}

func ConfigTransferTaskPrefix(taskID string) string {
	return "/v1/runtime/backup-config-transfer/" + taskID + "/"
}

func configTransferPrefix(binding backupconfigtransfer.Binding) string {
	return ConfigTransferTaskPrefix(
		binding.TaskID,
	) + binding.AssignmentID + "/" + binding.StepID + "/" + binding.ExecutionID + "/"
}

func ConfigTransferCursorKey(binding backupconfigtransfer.Binding) string {
	return configTransferPrefix(binding) + "cursor"
}

func ConfigTransferCreditPrefix(binding backupconfigtransfer.Binding) string {
	return configTransferPrefix(binding) + "credit/"
}

func ConfigTransferCreditKey(binding backupconfigtransfer.Binding, sequence uint64) string {
	return ConfigTransferCreditPrefix(binding) + fmt.Sprintf("%020d", sequence)
}

func ValidateConfigTransferOwner(owner ConfigTransferOwner) error {
	if owner.Binding.Validate() != nil || ids.Validate(ids.KindAgent, owner.AgentID) != nil ||
		owner.AgentGeneration == 0 || owner.AssignmentGeneration == 0 || !recordcodec.ValidSHA256(owner.AuthoritySHA256) {
		return captureSnapshotConflict()
	}
	return nil
}

func EncodeConfigTransferCursor(cursor ConfigTransferCursor) ([]byte, error) {
	if ValidateConfigTransferOwner(cursor.Owner) != nil || cursor.LastCreditSequence == 0 ||
		cursor.MetadataAcceptedCreditSequence > cursor.LastCreditSequence {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode("backup-config-transfer-cursor", cursor)
}

func DecodeConfigTransferCursor(value []byte) (ConfigTransferCursor, error) {
	if len(value) == 0 || len(value) > 4096 {
		return ConfigTransferCursor{}, captureSnapshotConflict()
	}
	cursor, err := recordcodec.Decode[ConfigTransferCursor](value, "backup-config-transfer-cursor")
	if err != nil {
		return ConfigTransferCursor{}, err
	}
	encoded, err := EncodeConfigTransferCursor(cursor)
	if err != nil || !bytes.Equal(encoded, value) {
		return ConfigTransferCursor{}, captureSnapshotConflict()
	}
	return cursor, nil
}

func EncodeConfigTransferCredit(record ConfigTransferCredit) ([]byte, error) {
	if ValidateConfigTransferOwner(record.Owner) != nil {
		return nil, captureSnapshotConflict()
	}
	credit, err := backupconfigtransfer.ValidateCredit(record.Owner.Binding, record.Credit)
	if err != nil {
		return nil, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(credit)
	if err != nil {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode(
		"backup-config-transfer-credit",
		configTransferCreditData{Owner: record.Owner, Credit: encoded},
	)
}

func DecodeConfigTransferCredit(value []byte) (ConfigTransferCredit, error) {
	if len(value) == 0 || len(value) > 8192 {
		return ConfigTransferCredit{}, captureSnapshotConflict()
	}
	data, err := recordcodec.Decode[configTransferCreditData](value, "backup-config-transfer-credit")
	if err != nil {
		return ConfigTransferCredit{}, err
	}
	record := ConfigTransferCredit{Owner: data.Owner, Credit: &agentpb.BackupConfigCredit{}}
	if proto.Unmarshal(data.Credit, record.Credit) != nil {
		return ConfigTransferCredit{}, captureSnapshotConflict()
	}
	encoded, err := EncodeConfigTransferCredit(record)
	if err != nil || !bytes.Equal(encoded, value) {
		return ConfigTransferCredit{}, captureSnapshotConflict()
	}
	return record, nil
}

func ValidateConfigTransferPruneEntry(taskID, key string, value []byte) error {
	if strings.HasSuffix(key, "/restore-entry-cursor") {
		cursor, err := decodeConfigRestoreEntryCursor(value)
		if err != nil || cursor.Owner.Transfer.Binding.TaskID != taskID ||
			ConfigRestoreEntryCursorKey(cursor.Owner) != key {
			return captureSnapshotConflict()
		}
		return nil
	}
	if strings.Contains(key, "/restore-entry/") {
		receipt, err := decodeConfigRestoreEntryReceipt(value)
		if err != nil || receipt.Owner.Transfer.Binding.TaskID != taskID ||
			ConfigRestoreEntryReceiptKey(receipt.Owner, receipt.Ordinal) != key {
			return captureSnapshotConflict()
		}
		return nil
	}
	if strings.HasSuffix(key, "/restore-generation") {
		record, err := DecodeConfigRestoreGeneration(value)
		if err != nil || record.Owner.Transfer.Binding.TaskID != taskID ||
			ConfigRestoreGenerationKey(record.Owner) != key {
			return captureSnapshotConflict()
		}
		return nil
	}
	if strings.Contains(key, "/restore-record/") {
		return validateConfigRestoreTransferPruneEntry(taskID, key, value)
	}
	if strings.HasSuffix(key, "/cursor") {
		cursor, err := DecodeConfigTransferCursor(value)
		if err != nil || cursor.Owner.Binding.TaskID != taskID || ConfigTransferCursorKey(cursor.Owner.Binding) != key {
			return captureSnapshotConflict()
		}
		return nil
	}
	credit, err := DecodeConfigTransferCredit(value)
	if err != nil || credit.Owner.Binding.TaskID != taskID ||
		ConfigTransferCreditKey(credit.Owner.Binding, credit.Credit.CreditSequence) != key {
		return captureSnapshotConflict()
	}
	return nil
}
