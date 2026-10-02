package backupvolumetransfer

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// FrameSet is deterministic for a sealed manifest. The caller may regenerate
// it from a retained source after reconnect and resume at a native credit.
type FrameSet struct {
	Frames []*agentpb.BackupVolumeManifestTransfer
	Chains [][sha256.Size]byte
	Next   []uint64
}

func BuildFrames(binding Binding, pointID, generationID string, role agentpb.BackupVolumeManifestRole,
	entries []backupvolume.Entry, source *backupvolume.ArtifactEvidence,
) (FrameSet, error) {
	if err := binding.Validate(); err != nil {
		return FrameSet{}, err
	}
	manifest, err := EncodeEntries(entries)
	if err != nil {
		return FrameSet{}, err
	}
	content, err := backupvolume.ContentManifestSHA256(manifest)
	if err != nil {
		return FrameSet{}, err
	}
	tree, err := backupvolume.FullTreeSHA256(entries)
	if err != nil {
		return FrameSet{}, err
	}
	if pointID == "" || !roleMatches(binding.Direction, role) ||
		(binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE && generationID != "") ||
		(binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE && generationID == "") {
		return FrameSet{}, invalidManifest()
	}
	start := &agentpb.BackupVolumeManifestStart{PointId: pointID, RestoreGenerationId: generationID,
		Role: role, EntryCount: uint64(len(entries)), ContentManifestSha256: content[:], FullTreeSha256: tree[:]}
	if source == nil {
		if binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD {
			return FrameSet{}, invalidManifest()
		}
		start.Archive = &agentpb.BackupVolumeManifestStart_NoSourceArchive{
			NoSourceArchive: &agentpb.BackupVolumeNoSourceArchiveEvidence{},
		}
	} else {
		if binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD ||
			source.Archive.EntryCount != uint64(len(entries)) || source.Archive.ContentManifestSHA256 != content ||
			source.Archive.FullTreeSHA256 != tree || source.Archive.SourceSizeBytes != source.Source.SizeBytes {
			return FrameSet{}, invalidManifest()
		}
		start.Archive = &agentpb.BackupVolumeManifestStart_SourceArchive{SourceArchive: &agentpb.BackupVolumeSourceArchiveEvidence{
			SourceSizeBytes: source.Source.SizeBytes, SourceSha256: append([]byte(nil), source.Source.SHA256[:]...),
		}}
	}
	seed, err := InitialChain(binding)
	if err != nil {
		return FrameSet{}, err
	}
	set := FrameSet{Frames: make([]*agentpb.BackupVolumeManifestTransfer, 0, 2+len(entries)/int(MaxBatchEntries)),
		Chains: make([][sha256.Size]byte, 0, 2+len(entries)/int(MaxBatchEntries)),
		Next:   make([]uint64, 0, 2+len(entries)/int(MaxBatchEntries))}
	appendFrame := func(frame *agentpb.BackupVolumeManifestTransfer, next uint64) error {
		if protoSize := uint64(frameSize(frame)); protoSize > MaxFrameBytes {
			return invalidManifest()
		}
		chain, err := NextChain(binding, seed, frame)
		if err != nil {
			return err
		}
		set.Frames = append(set.Frames, frame)
		set.Chains = append(set.Chains, chain)
		set.Next = append(set.Next, next)
		seed = chain
		return nil
	}
	first := binding.frame(1)
	first.Record = &agentpb.BackupVolumeManifestTransfer_Start{Start: start}
	if err := appendFrame(first, 1); err != nil {
		return FrameSet{}, err
	}
	wires, err := WireEntries(entries)
	if err != nil {
		return FrameSet{}, err
	}
	for offset := 0; offset < len(wires); offset += int(MaxBatchEntries) {
		limit := min(offset+int(MaxBatchEntries), len(wires))
		frame := binding.frame(uint64(len(set.Frames) + 1))
		batch := &agentpb.BackupVolumeManifestBatch{FirstOrdinal: uint64(offset + 1),
			Entries: wires[offset:limit], PrecedingTransferChainSha256: append([]byte(nil), seed[:]...)}
		frame.Record = &agentpb.BackupVolumeManifestTransfer_Batch{Batch: batch}
		preimage := binding.frame(frame.RecordSequence)
		preimage.Record = &agentpb.BackupVolumeManifestTransfer_Batch{Batch: &agentpb.BackupVolumeManifestBatch{
			FirstOrdinal: batch.FirstOrdinal, Entries: batch.Entries,
			PrecedingTransferChainSha256: append([]byte(nil), seed[:]...),
		}}
		result, err := chainValue(seed, preimage)
		if err != nil {
			return FrameSet{}, err
		}
		batch.ResultingTransferChainSha256 = result[:]
		if err := appendFrame(frame, uint64(limit+1)); err != nil {
			return FrameSet{}, err
		}
	}
	last := binding.frame(uint64(len(set.Frames) + 1))
	last.Record = &agentpb.BackupVolumeManifestTransfer_End{End: &agentpb.BackupVolumeManifestEnd{
		EntryCount: uint64(len(entries)), ContentManifestSha256: content[:], FullTreeSha256: tree[:],
		FinalTransferChainSha256: append([]byte(nil), seed[:]...),
	}}
	if err := appendFrame(last, uint64(len(entries)+1)); err != nil {
		return FrameSet{}, err
	}
	return set, nil
}

func frameSize(frame *agentpb.BackupVolumeManifestTransfer) int {
	encoded, err := deterministic.Marshal(frame)
	if err != nil {
		return int(MaxFrameBytes + 1)
	}
	return len(encoded)
}

// ResumeIndex validates a durable credit against the deterministic replayed
// frame prefix. It never treats an unacknowledged send as a committed record.
func (set FrameSet) ResumeIndex(binding Binding, credit *agentpb.BackupVolumeManifestAckCredit) (int, error) {
	if err := ValidateCredit(binding, credit); err != nil {
		return 0, err
	}
	committed := credit.CommittedRecordSequence
	if committed == 0 {
		return 0, nil
	}
	if committed > uint64(len(set.Frames)) {
		return 0, invalidManifest()
	}
	index := int(committed - 1)
	if credit.NextOrdinal != set.Next[index] || !bytes.Equal(credit.TransferChainSha256, set.Chains[index][:]) {
		return 0, invalidManifest()
	}
	return int(committed), nil
}

func NextCredit(binding Binding, previous *agentpb.BackupVolumeManifestAckCredit,
	frame *agentpb.BackupVolumeManifestTransfer, chain [sha256.Size]byte, next uint64,
) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if err := ValidateCredit(binding, previous); err != nil {
		return nil, err
	}
	if frame == nil || frame.RecordSequence != previous.CommittedRecordSequence+1 || next < previous.NextOrdinal ||
		next > backupvolume.MaxEntries+1 || frameSize(frame) > int(previous.ByteCredit) {
		return nil, invalidManifest()
	}
	actual, err := NextChain(binding, digestArray(previous.TransferChainSha256), frame)
	if err != nil {
		return nil, err
	}
	if actual != chain {
		return nil, invalidManifest()
	}
	return binding.credit(previous.AckSequence+1, frame.RecordSequence, next, chain[:]), nil
}

func digestArray(value []byte) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], value)
	return result
}

func invalidFrameState() error {
	return errs.New(errs.KindStateConflict, "Volume manifest frame differs from its native cursor")
}
