package backupvolumetransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

const (
	MaxBatchEntries uint64 = 8
	MaxFrameBytes          = 64 << 10
	GrantRecords    uint32 = 8
	GrantBytes      uint64 = 256 << 10
	chainDomain            = "groundplane.backup.volume-manifest-transfer.schema-one.v1\x00"
)

type Binding struct {
	TaskID, AssignmentID, StepID, TransferID string
	AuthorityDigest                          [sha256.Size]byte
	Direction                                agentpb.BackupVolumeManifestDirection
}

func (binding Binding) Validate() error {
	if ids.Validate(ids.KindTask, binding.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, binding.AssignmentID) != nil ||
		ids.Validate(ids.KindStep, binding.StepID) != nil ||
		!validULID(binding.TransferID) || binding.AuthorityDigest == ([sha256.Size]byte{}) ||
		(binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE &&
			binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW &&
			binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD) {
		return invalidManifest()
	}
	return nil
}

func validULID(value string) bool {
	_, err := ulid.ParseStrict(value)
	return err == nil
}

// TransferID gives each direction its own stable cursor without adding a
// mutable identifier to the sealed step. It is derived only from that step.
func TransferID(executionID string, direction agentpb.BackupVolumeManifestDirection) (string, error) {
	if !validULID(executionID) ||
		direction < agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE ||
		direction > agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD {
		return "", invalidManifest()
	}
	digest := sha256.Sum256(
		[]byte("groundplane.backup.volume-transfer-id.schema-one.v1\x00" + executionID + "/" + direction.String()),
	)
	var id ulid.ULID
	copy(id[:], digest[:16])
	return id.String(), nil
}

func (binding Binding) frame(sequence uint64) *agentpb.BackupVolumeManifestTransfer {
	return &agentpb.BackupVolumeManifestTransfer{TaskId: binding.TaskID, AssignmentId: binding.AssignmentID,
		StepId: binding.StepID, AuthorityDigest: append([]byte(nil), binding.AuthorityDigest[:]...),
		TransferId: binding.TransferID, Direction: binding.Direction, RecordSequence: sequence}
}

func (binding Binding) credit(sequence, committed, next uint64, chain []byte) *agentpb.BackupVolumeManifestAckCredit {
	return &agentpb.BackupVolumeManifestAckCredit{TaskId: binding.TaskID, AssignmentId: binding.AssignmentID,
		StepId: binding.StepID, AuthorityDigest: append([]byte(nil), binding.AuthorityDigest[:]...),
		TransferId: binding.TransferID, AckSequence: sequence, CommittedRecordSequence: committed,
		NextOrdinal: next, TransferChainSha256: append([]byte(nil), chain...),
		RecordCredit: GrantRecords, ByteCredit: GrantBytes}
}

func InitialChain(binding Binding) ([sha256.Size]byte, error) {
	if err := binding.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	seed := binding.frame(0)
	encoded, err := deterministic.Marshal(seed)
	if err != nil {
		return [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	return sha256.Sum256(append([]byte(chainDomain), encoded...)), nil
}

func InitialCredit(binding Binding) (*agentpb.BackupVolumeManifestAckCredit, error) {
	seed, err := InitialChain(binding)
	if err != nil {
		return nil, err
	}
	return binding.credit(1, 0, 1, seed[:]), nil
}

// ValidateFrame makes a private copy before a channel owner retains a frame.
func ValidateFrame(
	binding Binding,
	frame *agentpb.BackupVolumeManifestTransfer,
) (*agentpb.BackupVolumeManifestTransfer, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	if frame == nil || frame.RecordSequence == 0 || proto.Size(frame) > MaxFrameBytes ||
		frame.ProtoReflect().GetUnknown() != nil || frame.TaskId != binding.TaskID ||
		frame.AssignmentId != binding.AssignmentID || frame.StepId != binding.StepID ||
		frame.TransferId != binding.TransferID || frame.Direction != binding.Direction ||
		!bytes.Equal(frame.AuthorityDigest, binding.AuthorityDigest[:]) {
		return nil, invalidManifest()
	}
	switch value := frame.Record.(type) {
	case *agentpb.BackupVolumeManifestTransfer_Start:
		if value == nil || value.Start == nil || value.Start.EntryCount == 0 ||
			value.Start.EntryCount > backupvolume.MaxEntries || len(value.Start.ContentManifestSha256) != sha256.Size ||
			len(value.Start.FullTreeSha256) != sha256.Size || !roleMatches(binding.Direction, value.Start.Role) ||
			(value.Start.GetSourceArchive() == nil) == (value.Start.GetNoSourceArchive() == nil) {
			return nil, invalidManifest()
		}
	case *agentpb.BackupVolumeManifestTransfer_Batch:
		if value == nil || value.Batch == nil || value.Batch.FirstOrdinal == 0 ||
			len(value.Batch.Entries) == 0 || len(value.Batch.Entries) > int(MaxBatchEntries) ||
			len(value.Batch.PrecedingTransferChainSha256) != sha256.Size ||
			len(value.Batch.ResultingTransferChainSha256) != sha256.Size {
			return nil, invalidManifest()
		}
		for index, entry := range value.Batch.Entries {
			if entry == nil || entry.Ordinal != value.Batch.FirstOrdinal+uint64(index) ||
				entry.ProtoReflect().GetUnknown() != nil || proto.Size(entry) > backupvolume.MaxManifestEntryBytes {
				return nil, invalidManifest()
			}
		}
	case *agentpb.BackupVolumeManifestTransfer_End:
		if value == nil || value.End == nil || value.End.EntryCount == 0 ||
			value.End.EntryCount > backupvolume.MaxEntries || len(value.End.ContentManifestSha256) != sha256.Size ||
			len(value.End.FullTreeSha256) != sha256.Size || len(value.End.FinalTransferChainSha256) != sha256.Size {
			return nil, invalidManifest()
		}
	case *agentpb.BackupVolumeManifestTransfer_Resume:
		if value == nil || value.Resume == nil || value.Resume.NextRecordSequence == 0 ||
			value.Resume.NextOrdinal == 0 || value.Resume.NextOrdinal > backupvolume.MaxEntries+1 ||
			len(value.Resume.PrecedingTransferChainSha256) != sha256.Size {
			return nil, invalidManifest()
		}
	default:
		return nil, invalidManifest()
	}
	return proto.CloneOf(frame), nil
}

func roleMatches(direction agentpb.BackupVolumeManifestDirection, role agentpb.BackupVolumeManifestRole) bool {
	switch direction {
	case agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE:
		return role == agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_CAPTURED
	case agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW:
		return role == agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_RESTORE_NEW
	case agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD:
		return role == agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_STOPPED_LIVE_OLD
	}
	return false
}

// NextChain commits exactly one wire record. A batch authenticates its own
// predecessor and result; the result field is zeroed for the hash preimage.
func NextChain(
	binding Binding,
	preceding [sha256.Size]byte,
	frame *agentpb.BackupVolumeManifestTransfer,
) ([sha256.Size]byte, error) {
	owned, err := ValidateFrame(binding, frame)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if batch := owned.GetBatch(); batch != nil {
		if !bytes.Equal(batch.PrecedingTransferChainSha256, preceding[:]) {
			return [sha256.Size]byte{}, invalidManifest()
		}
		batch.ResultingTransferChainSha256 = nil
	}
	if end := owned.GetEnd(); end != nil {
		if !bytes.Equal(end.FinalTransferChainSha256, preceding[:]) {
			return [sha256.Size]byte{}, invalidManifest()
		}
		end.FinalTransferChainSha256 = nil
	}
	result, err := chainValue(preceding, owned)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if batch := frame.GetBatch(); batch != nil && !bytes.Equal(batch.ResultingTransferChainSha256, result[:]) {
		return [sha256.Size]byte{}, invalidManifest()
	}
	return result, nil
}

func chainValue(preceding [sha256.Size]byte, frame *agentpb.BackupVolumeManifestTransfer) ([sha256.Size]byte, error) {
	encoded, err := deterministic.Marshal(frame)
	if err != nil {
		return [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(chainDomain))
	_, _ = hash.Write(preceding[:])
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(encoded)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write(encoded)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

func ValidateCredit(binding Binding, credit *agentpb.BackupVolumeManifestAckCredit) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if credit == nil || credit.ProtoReflect().GetUnknown() != nil ||
		credit.TaskId != binding.TaskID || credit.AssignmentId != binding.AssignmentID ||
		credit.StepId != binding.StepID || credit.TransferId != binding.TransferID ||
		!bytes.Equal(credit.AuthorityDigest, binding.AuthorityDigest[:]) ||
		credit.AckSequence == 0 || credit.NextOrdinal == 0 || credit.NextOrdinal > backupvolume.MaxEntries+1 ||
		len(credit.TransferChainSha256) != sha256.Size || credit.RecordCredit == 0 ||
		credit.RecordCredit > GrantRecords || credit.ByteCredit == 0 || credit.ByteCredit > GrantBytes {
		return invalidManifest()
	}
	if credit.CommittedRecordSequence == 0 {
		seed, err := InitialChain(binding)
		if err != nil || credit.AckSequence != 1 || credit.NextOrdinal != 1 ||
			!bytes.Equal(credit.TransferChainSha256, seed[:]) {
			return invalidManifest()
		}
	}
	return nil
}
