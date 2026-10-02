package agentconfigjournal

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type transferEntry struct {
	id         string
	sizeBytes  uint64
	sha256     [sha256.Size]byte
	chunkCount uint32
}

type transferState struct {
	entries       []transferEntry
	valueOrdinal  uint32
	valueOffset   uint64
	valueChunks   uint32
	selectedBytes uint64
	started       bool
	complete      bool
}

func (state *transferState) accept(
	direction agentpb.BackupConfigDirection,
	expected *agentpb.BackupConfigContentAuthority,
	descriptor *backupconfigtransfer.RecordDescriptor,
) error {
	if state == nil || descriptor == nil || descriptor.Frame == nil || state.complete {
		return invalidRecord()
	}
	frame := descriptor.Frame
	switch record := frame.Record.(type) {
	case *agentpb.BackupConfigTransfer_Start:
		if state.started || frame.RecordSequence != 1 || record == nil || record.Start == nil ||
			!proto.Equal(record.Start.Content, expected) {
			return conflict("Agent Config journal transfer start is not canonical")
		}
		state.started = true
		if expected.EntryCount == 0 {
			state.valueOrdinal = 1
		}
	case *agentpb.BackupConfigTransfer_EntryHeader:
		if !state.started || state.valueOrdinal != 0 || record == nil || record.EntryHeader == nil ||
			uint32(len(state.entries)) >= expected.EntryCount {
			return conflict("Agent Config journal transfer header is out of order")
		}
		var entry backupconfig.Entry
		var err error
		switch direction {
		case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
			capture := record.EntryHeader.GetCaptureEntry()
			entry, err = backupconfigtransfer.CaptureEntry(capture)
			if capture.GetOrdinal() != uint32(len(state.entries)+1) {
				return invalidRecord()
			}
		case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
			entry, err = backupconfigtransfer.RestoreEntry(record.EntryHeader.GetRestoreEntry())
		default:
			return invalidRecord()
		}
		if err != nil ||
			(len(state.entries) != 0 && state.entries[len(state.entries)-1].id >= entry.ID) {
			return invalidRecord()
		}
		chunks := uint32((entry.Value.SizeBytes + backupconfig.TransferChunkBytes - 1) / backupconfig.TransferChunkBytes)
		if record.EntryHeader.ChunkCount != chunks ||
			entry.Value.SizeBytes > expected.TotalSelectedValueBytes ||
			state.selectedBytes > expected.TotalSelectedValueBytes-entry.Value.SizeBytes {
			return conflict("Agent Config journal transfer header geometry conflicts")
		}
		retained := transferEntry{
			id: entry.ID, sizeBytes: entry.Value.SizeBytes,
			sha256: entry.Value.SHA256, chunkCount: chunks,
		}
		state.entries = append(state.entries, retained)
		state.selectedBytes += entry.Value.SizeBytes
		if uint32(len(state.entries)) == expected.EntryCount {
			if state.selectedBytes != expected.TotalSelectedValueBytes {
				return conflict("Agent Config journal transfer value total conflicts")
			}
			state.valueOrdinal = 1
		}
	case *agentpb.BackupConfigTransfer_ValueChunk:
		if !state.valuesReady(expected) || record == nil || record.ValueChunk == nil ||
			descriptor.Value == nil || record.ValueChunk.Ordinal != state.valueOrdinal {
			return conflict("Agent Config journal transfer value is out of order")
		}
		entry := state.entries[state.valueOrdinal-1]
		remaining := entry.sizeBytes - state.valueOffset
		want := uint64(backupconfig.TransferChunkBytes)
		if remaining < want {
			want = remaining
		}
		if want == 0 || record.ValueChunk.Offset != state.valueOffset ||
			uint64(descriptor.Value.SizeBytes) != want {
			return conflict("Agent Config journal transfer value geometry conflicts")
		}
		state.valueOffset += want
		state.valueChunks++
	case *agentpb.BackupConfigTransfer_EntryEnd:
		if !state.valuesReady(expected) || record == nil || record.EntryEnd == nil ||
			record.EntryEnd.Ordinal != state.valueOrdinal {
			return conflict("Agent Config journal transfer Entry end is out of order")
		}
		entry := state.entries[state.valueOrdinal-1]
		if state.valueOffset != entry.sizeBytes || state.valueChunks != entry.chunkCount ||
			record.EntryEnd.ValueSizeBytes != entry.sizeBytes ||
			!bytes.Equal(record.EntryEnd.ValueSha256, entry.sha256[:]) {
			return conflict("Agent Config journal transfer Entry end conflicts")
		}
		state.valueOrdinal++
		state.valueOffset = 0
		state.valueChunks = 0
	case *agentpb.BackupConfigTransfer_End:
		if !state.started || record == nil || record.End == nil ||
			state.valueOrdinal != expected.EntryCount+1 || !proto.Equal(record.End.Content, expected) {
			return conflict("Agent Config journal transfer end is not canonical")
		}
		state.complete = true
	case *agentpb.BackupConfigTransfer_Resume:
		return conflict("Agent Config journal does not persist reconnect controls")
	default:
		return invalidRecord()
	}
	return nil
}

func (state *transferState) valuesReady(expected *agentpb.BackupConfigContentAuthority) bool {
	return state.started && uint32(len(state.entries)) == expected.EntryCount &&
		state.valueOrdinal > 0 && state.valueOrdinal <= expected.EntryCount
}
