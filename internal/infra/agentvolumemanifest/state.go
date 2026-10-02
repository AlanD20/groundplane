package agentvolumemanifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type receiverState struct {
	frames   [][]byte
	credits  []*agentpb.BackupVolumeManifestAckCredit
	wires    []*agentpb.BackupVolumeManifestEntry
	entries  []backupvolume.Entry
	manifest []backupvolume.ManifestEntryBytes
	started  bool
	complete bool
	chain    [sha256.Size]byte
	logHash  [sha256.Size]byte
	partial  bool
}

func parseLog(raw []byte, config Config, authority []byte) (receiverState, []byte, error) {
	if len(raw) > MaximumJournalBytes {
		return receiverState{}, nil, invalid("Volume manifest receiver log exceeds its bound")
	}
	state := receiverState{logHash: initialHash(authority)}
	position := 0
	for position < len(raw) {
		if len(raw)-position < 4 {
			break
		}
		length := binary.BigEndian.Uint32(raw[position : position+4])
		if length == 0 || length > maximumTransaction-4 {
			return receiverState{}, nil, invalid("Volume manifest receiver transaction length is invalid")
		}
		if int(length)+4 > len(raw)-position {
			break
		}
		value, err := decodeTransaction(raw[position : position+int(length)+4])
		if err != nil {
			return receiverState{}, nil, err
		}
		if value.prior != state.logHash || len(state.frames) > backupvolume.MaxEntries+2 {
			return receiverState{}, nil, invalid("Volume manifest receiver transaction chain is invalid")
		}
		if len(state.credits) == 0 {
			if value.sequence != 0 || len(value.frame) != 0 {
				return receiverState{}, nil, invalid("Volume manifest receiver initial credit is missing")
			}
			credit, err := decodeCredit(value.credit)
			if err != nil {
				return receiverState{}, nil, err
			}
			expected, err := backupvolumetransfer.InitialCredit(config.Binding)
			if err != nil || !proto.Equal(credit, expected) {
				return receiverState{}, nil, invalid("Volume manifest receiver initial credit differs")
			}
			state.credits = append(state.credits, credit)
		} else {
			if value.sequence == 0 || value.sequence != uint64(len(state.frames)+1) {
				return receiverState{}, nil, invalid("Volume manifest receiver frame sequence is invalid")
			}
			frame, err := decodeFrame(value.frame)
			if err != nil {
				return receiverState{}, nil, err
			}
			candidate, credit, err := state.advance(config, frame)
			if err != nil {
				return receiverState{}, nil, err
			}
			stored, err := decodeCredit(value.credit)
			if err != nil || !proto.Equal(stored, credit) {
				return receiverState{}, nil, invalid("Volume manifest receiver credit differs from frame")
			}
			candidate.frames = append(candidate.frames, value.frame)
			candidate.credits = append(candidate.credits, stored)
			state = candidate
		}
		state.logHash = value.hash
		position += int(length) + 4
	}
	tail := append([]byte(nil), raw[position:]...)
	if len(tail) > maximumTransaction {
		return receiverState{}, nil, invalid("Volume manifest receiver partial transaction exceeds its bound")
	}
	state.partial = len(tail) != 0
	return state, tail, nil
}

func (state receiverState) advance(config Config, frame *agentpb.BackupVolumeManifestTransfer) (receiverState,
	*agentpb.BackupVolumeManifestAckCredit, error,
) {
	owned, err := backupvolumetransfer.ValidateFrame(config.Binding, frame)
	if err != nil {
		return receiverState{}, nil, err
	}
	if len(state.credits) == 0 || state.complete || owned.RecordSequence != uint64(len(state.frames)+1) ||
		owned.RecordSequence > backupvolume.MaxEntries+2 {
		return receiverState{}, nil, conflict("Volume manifest receiver frame is out of order")
	}
	previous := state.credits[len(state.credits)-1]
	chain, err := backupvolumetransfer.NextChain(config.Binding, digest(previous.TransferChainSha256), owned)
	if err != nil {
		return receiverState{}, nil, err
	}
	candidate := state
	next := previous.NextOrdinal
	switch record := owned.Record.(type) {
	case *agentpb.BackupVolumeManifestTransfer_Start:
		start := record.Start
		if state.started || owned.RecordSequence != 1 || start.ProtoReflect().GetUnknown() != nil ||
			start.GetSourceArchive() == nil || start.GetSourceArchive().ProtoReflect().GetUnknown() != nil ||
			start.PointId != config.PointID || start.RestoreGenerationId != config.RestoreGenerationID ||
			start.EntryCount != config.ExpectedArchive.EntryCount ||
			!bytes.Equal(start.ContentManifestSha256, config.ExpectedArchive.ContentManifestSHA256[:]) ||
			!bytes.Equal(start.FullTreeSha256, config.ExpectedArchive.FullTreeSHA256[:]) ||
			start.GetSourceArchive().SourceSizeBytes != config.ExpectedSource.SizeBytes ||
			!bytes.Equal(start.GetSourceArchive().SourceSha256, config.ExpectedSource.SHA256[:]) {
			return receiverState{}, nil, conflict("Volume manifest receiver Start differs from selected Point")
		}
		candidate.started = true
	case *agentpb.BackupVolumeManifestTransfer_Batch:
		batch := record.Batch
		if !state.started || batch.ProtoReflect().GetUnknown() != nil ||
			batch.FirstOrdinal != next || uint64(len(batch.Entries)) > config.ExpectedArchive.EntryCount-next+1 ||
			uint64(len(state.wires))+uint64(len(batch.Entries)) > config.ExpectedArchive.EntryCount {
			return receiverState{}, nil, conflict("Volume manifest receiver Batch is out of order")
		}
		candidate.wires = append(append([]*agentpb.BackupVolumeManifestEntry(nil), state.wires...), batch.Entries...)
		next += uint64(len(batch.Entries))
	case *agentpb.BackupVolumeManifestTransfer_End:
		end := record.End
		if !state.started || end.ProtoReflect().GetUnknown() != nil ||
			uint64(len(state.wires)) != config.ExpectedArchive.EntryCount ||
			end.EntryCount != config.ExpectedArchive.EntryCount ||
			!bytes.Equal(end.ContentManifestSha256, config.ExpectedArchive.ContentManifestSHA256[:]) ||
			!bytes.Equal(end.FullTreeSha256, config.ExpectedArchive.FullTreeSHA256[:]) {
			return receiverState{}, nil, conflict("Volume manifest receiver End differs from selected Point")
		}
		entries, manifest, err := backupvolumetransfer.DecodeEntries(state.wires)
		if err != nil || backupvolumetransfer.VerifyEntries(entries, manifest,
			backupvolumetransfer.ArchiveEvidenceToWire(config.ExpectedArchive)) != nil {
			return receiverState{}, nil, invalid("Volume manifest receiver complete tree differs from selected archive")
		}
		candidate.entries, candidate.manifest, candidate.complete = entries, manifest, true
	case *agentpb.BackupVolumeManifestTransfer_Resume:
		return receiverState{}, nil, conflict("Volume manifest receiver does not persist reconnect controls")
	default:
		return receiverState{}, nil, invalid("Volume manifest receiver frame kind is invalid")
	}
	credit, err := backupvolumetransfer.NextCredit(config.Binding, previous, owned, chain, next)
	if err != nil {
		return receiverState{}, nil, err
	}
	candidate.chain = chain
	return candidate, credit, nil
}

func digest(value []byte) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], value)
	return result
}
