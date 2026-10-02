package backupconfiguration

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConfigRestoreGenerationRecord seals a fully received, protected generation.
// Its immutable journal is the source for publication; Ready is not a claim
// that live Entries or configuration files have already changed.
type ConfigRestoreGenerationRecord struct {
	Owner     ConfigRestoreTransferOwner
	Completed *agentpb.BackupConfigTransferCompleted
}

type configRestoreGenerationData struct {
	Owner     ConfigRestoreTransferOwner `json:"owner"`
	Completed []byte                     `json:"completed"`
}

func ConfigRestoreGenerationKey(owner ConfigRestoreTransferOwner) string {
	return configTransferPrefix(owner.Transfer.Binding) + "restore-generation"
}

func EncodeConfigRestoreGeneration(record ConfigRestoreGenerationRecord) ([]byte, error) {
	completed := record.Completed
	if ValidateConfigRestoreTransferOwner(record.Owner) != nil || completed == nil ||
		completed.RestoreGenerationId != record.Owner.GenerationID || completed.RenderGeneration != record.Owner.RenderGeneration ||
		completed.CommittedRecordCount == 0 || len(completed.ValueChainSha256) != 32 ||
		len(completed.TransferTranscriptSha256) != 32 || proto.Size(completed) > 4096 {
		return nil, captureSnapshotConflict()
	}
	digest, err := backupconfigtransfer.ContentSHA256(completed.Content)
	if err != nil || hex.EncodeToString(digest) != record.Owner.ContentSHA256 ||
		completed.Content.SourceSizeBytes != record.Owner.SourceSizeBytes || len(completed.ProtoReflect().GetUnknown()) != 0 {
		return nil, captureSnapshotConflict()
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(completed)
	if err != nil {
		return nil, captureSnapshotConflict()
	}
	return recordcodec.Encode(
		"backup-config-restore-generation",
		configRestoreGenerationData{Owner: record.Owner, Completed: encoded},
	)
}

func DecodeConfigRestoreGeneration(raw []byte) (ConfigRestoreGenerationRecord, error) {
	if len(raw) == 0 || len(raw) > 8192 {
		return ConfigRestoreGenerationRecord{}, captureSnapshotConflict()
	}
	data, err := recordcodec.Decode[configRestoreGenerationData](raw, "backup-config-restore-generation")
	if err != nil {
		return ConfigRestoreGenerationRecord{}, err
	}
	record := ConfigRestoreGenerationRecord{Owner: data.Owner, Completed: &agentpb.BackupConfigTransferCompleted{}}
	if proto.Unmarshal(data.Completed, record.Completed) != nil {
		return ConfigRestoreGenerationRecord{}, captureSnapshotConflict()
	}
	canonical, err := EncodeConfigRestoreGeneration(record)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ConfigRestoreGenerationRecord{}, captureSnapshotConflict()
	}
	return record, nil
}
