// Package backupvolumemanifest owns the native, assignment-fenced Volume
// manifest ledger. Frames and their credits are immutable and contiguous.
package backupvolumemanifest

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Owner struct {
	Binding              backupvolumetransfer.Binding `json:"binding"`
	AgentID              string                       `json:"agent_id"`
	AgentGeneration      uint64                       `json:"agent_generation"`
	AssignmentGeneration uint64                       `json:"assignment_generation"`
}

func (owner Owner) Validate() error {
	if owner.Binding.Validate() != nil || ids.Validate(ids.KindAgent, owner.AgentID) != nil ||
		owner.AgentGeneration == 0 || owner.AssignmentGeneration == 0 {
		return invalidLedger()
	}
	return nil
}

func TaskPrefix(taskID string) string { return "/v1/runtime/backup-volume-manifest/" + taskID + "/" }

func prefix(binding backupvolumetransfer.Binding) string {
	return TaskPrefix(binding.TaskID) + binding.AssignmentID + "/" + binding.StepID + "/" + binding.TransferID + "/"
}

func Prefix(binding backupvolumetransfer.Binding) string { return prefix(binding) }

func CursorKey(binding backupvolumetransfer.Binding) string { return prefix(binding) + "cursor" }

func FrameKey(binding backupvolumetransfer.Binding, sequence uint64) string {
	return prefix(binding) + "frame/" + fmt.Sprintf("%020d", sequence)
}

func CreditKey(binding backupvolumetransfer.Binding, sequence uint64) string {
	return prefix(binding) + "credit/" + fmt.Sprintf("%020d", sequence)
}

type Cursor struct {
	Owner       Owner  `json:"owner"`
	Credit      []byte `json:"credit"`
	EntryCount  uint64 `json:"entry_count"`
	ContentSHA  []byte `json:"content_sha256"`
	FullTreeSHA []byte `json:"full_tree_sha256"`
	PointID     string `json:"point_id"`
	Generation  string `json:"generation"`
	Complete    bool   `json:"complete"`
}

func (cursor Cursor) LastCredit() (*agentpb.BackupVolumeManifestAckCredit, error) {
	if cursor.Owner.Validate() != nil || len(cursor.Credit) == 0 || len(cursor.Credit) > 2048 {
		return nil, invalidLedger()
	}
	var credit agentpb.BackupVolumeManifestAckCredit
	if proto.Unmarshal(cursor.Credit, &credit) != nil ||
		backupvolumetransfer.ValidateCredit(cursor.Owner.Binding, &credit) != nil {
		return nil, invalidLedger()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&credit)
	if err != nil || !bytes.Equal(canonical, cursor.Credit) {
		return nil, invalidLedger()
	}
	return &credit, nil
}

func encodeCursor(cursor Cursor) ([]byte, error) {
	if _, err := cursor.LastCredit(); err != nil {
		return nil, err
	}
	if cursor.EntryCount > 0 &&
		(len(cursor.ContentSHA) != sha256.Size || len(cursor.FullTreeSHA) != sha256.Size || cursor.PointID == "") {
		return nil, invalidLedger()
	}
	return recordcodec.Encode("backup-volume-manifest-cursor", cursor)
}

func decodeCursor(raw []byte) (Cursor, error) {
	if len(raw) == 0 || len(raw) > 8192 {
		return Cursor{}, invalidLedger()
	}
	value, err := recordcodec.Decode[Cursor](raw, "backup-volume-manifest-cursor")
	if err != nil {
		return Cursor{}, err
	}
	canonical, err := encodeCursor(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Cursor{}, invalidLedger()
	}
	return value, nil
}

func DecodeCursor(raw []byte) (Cursor, error) { return decodeCursor(raw) }

type frameRecord struct {
	Owner Owner  `json:"owner"`
	Frame []byte `json:"frame"`
}

func encodeFrame(owner Owner, frame *agentpb.BackupVolumeManifestTransfer) ([]byte, error) {
	if owner.Validate() != nil {
		return nil, invalidLedger()
	}
	validated, err := backupvolumetransfer.ValidateFrame(owner.Binding, frame)
	if err != nil {
		return nil, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(validated)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return recordcodec.Encode("backup-volume-manifest-frame", frameRecord{Owner: owner, Frame: raw})
}

func decodeFrame(raw []byte) (Owner, *agentpb.BackupVolumeManifestTransfer, error) {
	if len(raw) == 0 || len(raw) > backupvolumetransfer.MaxFrameBytes+8192 {
		return Owner{}, nil, invalidLedger()
	}
	record, err := recordcodec.Decode[frameRecord](raw, "backup-volume-manifest-frame")
	if err != nil {
		return Owner{}, nil, err
	}
	var frame agentpb.BackupVolumeManifestTransfer
	if proto.Unmarshal(record.Frame, &frame) != nil {
		return Owner{}, nil, invalidLedger()
	}
	canonical, err := encodeFrame(record.Owner, &frame)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Owner{}, nil, invalidLedger()
	}
	return record.Owner, &frame, nil
}

func invalidLedger() error {
	return errs.New(errs.KindStateConflict, "Volume manifest native cursor or frame is invalid")
}
