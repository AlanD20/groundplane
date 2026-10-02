package backupconfigtransfer

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RecordDescriptor retains exact control/metadata frames, but replaces value
// plaintext with a size and digest. Replay obtains those bytes from the owned
// source prefix, never from a hidden copy of Secret values.
type RecordDescriptor struct {
	Frame *agentpb.BackupConfigTransfer
	Value *ValueChunkDescriptor
}

type ValueChunkDescriptor struct {
	SizeBytes uint32
	SHA256    [sha256.Size]byte
}

// DescribeRecord is the descriptor producer for a validated, actually accepted
// frame. A journal must persist this descriptor before acknowledging its cursor.
func DescribeRecord(binding Binding, frame *agentpb.BackupConfigTransfer) (*RecordDescriptor, error) {
	owned, err := ValidateFrame(binding, frame)
	if err != nil {
		return nil, err
	}
	descriptor := &RecordDescriptor{Frame: owned}
	if chunk := owned.GetValueChunk(); chunk != nil {
		descriptor.Value = &ValueChunkDescriptor{
			SizeBytes: uint32(len(chunk.Content)),
			SHA256:    sha256.Sum256(chunk.Content),
		}
		ClearFrame(owned)
	}
	return descriptor, nil
}

func RestoreRecord(
	ctx context.Context,
	binding Binding,
	descriptor *RecordDescriptor,
	prefix io.ReaderAt,
	layout backupconfig.Layout,
) (*agentpb.BackupConfigTransfer, error) {
	if ctx == nil || descriptor == nil || descriptor.Frame == nil {
		return nil, invalid("Config replay descriptor is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if descriptor.Value == nil {
		if descriptor.Frame.GetValueChunk() != nil {
			return nil, invalid("Config value descriptor is missing")
		}
		return ValidateFrame(binding, descriptor.Frame)
	}
	frame := proto.CloneOf(descriptor.Frame)
	chunk := frame.GetValueChunk()
	value := descriptor.Value
	if rejectUnknown(frame) != nil || !frameMatches(binding, frame) || frame.RecordSequence == 0 || chunk == nil ||
		len(chunk.Content) != 0 || chunk.Ordinal == 0 || int(chunk.Ordinal) > len(layout.Entries) ||
		len(layout.ValuePayloadOffsets) != len(layout.Entries) || prefix == nil ||
		value.SizeBytes == 0 || value.SizeBytes > backupconfig.TransferChunkBytes {
		return nil, invalid("Config retained value descriptor is invalid")
	}
	entry := layout.Entries[chunk.Ordinal-1]
	if chunk.Offset > entry.Value.SizeBytes || uint64(value.SizeBytes) > entry.Value.SizeBytes-chunk.Offset {
		return nil, invalid("Config retained value exceeds its sealed Entry")
	}
	offset := layout.ValuePayloadOffsets[chunk.Ordinal-1] + chunk.Offset
	if offset > backupconfig.MaxSourceBytes || uint64(value.SizeBytes) > backupconfig.MaxSourceBytes-offset {
		return nil, invalid("Config retained value offset exceeds the archive bound")
	}
	chunk.Content = make([]byte, int(value.SizeBytes))
	count, err := prefix.ReadAt(chunk.Content, int64(offset))
	if count != len(chunk.Content) || (err != nil && err != io.EOF) || sha256.Sum256(chunk.Content) != value.SHA256 {
		ClearFrame(frame)
		return nil, invalid("Config retained value does not match the acknowledged descriptor")
	}
	if err := ctx.Err(); err != nil {
		ClearFrame(frame)
		return nil, err
	}
	validated, err := ValidateFrame(binding, frame)
	ClearFrame(frame)
	return validated, err
}
