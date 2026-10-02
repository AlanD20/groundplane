package backupconfigtransfer

import (
	"bytes"
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	recordDescriptorSchema      = uint8(1)
	recordDescriptorHeaderBytes = 6
	recordDescriptorValueBytes  = 4 + 32
	maximumDescriptorBytes      = recordDescriptorHeaderBytes + recordDescriptorValueBytes +
		backupconfig.MaxEntryHeaderEnvelopeBytes
)

// MaximumRecordDescriptorBytes bounds one durable schema-one descriptor.
const MaximumRecordDescriptorBytes = maximumDescriptorBytes

// MarshalRecordDescriptor encodes one canonical schema-one descriptor. Value
// frames must already have had their plaintext removed by DescribeRecord.
func MarshalRecordDescriptor(binding Binding, descriptor *RecordDescriptor) ([]byte, error) {
	frame, value, err := validateRecordDescriptor(binding, descriptor)
	if err != nil {
		return nil, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return nil, invalid("Config record descriptor cannot be encoded")
	}
	if len(encoded) > backupconfig.MaxEntryHeaderEnvelopeBytes {
		return nil, invalid("Config record descriptor exceeds its canonical size")
	}
	flags := byte(0)
	valueBytes := 0
	if value != nil {
		flags = 1
		valueBytes = recordDescriptorValueBytes
	}
	raw := make([]byte, recordDescriptorHeaderBytes+valueBytes+len(encoded))
	raw[0] = recordDescriptorSchema
	raw[1] = flags
	binary.BigEndian.PutUint32(raw[2:6], uint32(len(encoded)))
	offset := recordDescriptorHeaderBytes
	if value != nil {
		binary.BigEndian.PutUint32(raw[offset:offset+4], value.SizeBytes)
		copy(raw[offset+4:offset+recordDescriptorValueBytes], value.SHA256[:])
		offset += recordDescriptorValueBytes
	}
	copy(raw[offset:], encoded)
	return raw, nil
}

// UnmarshalRecordDescriptor accepts only the one canonical schema-one
// representation. It has no compatibility or plaintext reconstruction path.
func UnmarshalRecordDescriptor(binding Binding, raw []byte) (*RecordDescriptor, error) {
	if len(raw) < recordDescriptorHeaderBytes || len(raw) > maximumDescriptorBytes ||
		raw[0] != recordDescriptorSchema || raw[1] > 1 {
		return nil, invalid("Config record descriptor encoding is invalid")
	}
	valueBytes := 0
	if raw[1] == 1 {
		valueBytes = recordDescriptorValueBytes
	}
	frameSize := int(binary.BigEndian.Uint32(raw[2:6]))
	offset := recordDescriptorHeaderBytes + valueBytes
	if frameSize == 0 || frameSize > backupconfig.MaxEntryHeaderEnvelopeBytes || offset+frameSize != len(raw) {
		return nil, invalid("Config record descriptor length is invalid")
	}
	descriptor := &RecordDescriptor{Frame: &agentpb.BackupConfigTransfer{}}
	if valueBytes != 0 {
		descriptor.Value = &ValueChunkDescriptor{SizeBytes: binary.BigEndian.Uint32(raw[6:10])}
		copy(descriptor.Value.SHA256[:], raw[10:10+32])
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw[offset:], descriptor.Frame); err != nil {
		return nil, invalid("Config record descriptor frame is invalid")
	}
	canonical, err := MarshalRecordDescriptor(binding, descriptor)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, invalid("Config record descriptor is not canonical")
	}
	return descriptor, nil
}

func validateRecordDescriptor(
	binding Binding,
	descriptor *RecordDescriptor,
) (*agentpb.BackupConfigTransfer, *ValueChunkDescriptor, error) {
	if err := binding.Validate(); err != nil {
		return nil, nil, err
	}
	if descriptor == nil || descriptor.Frame == nil || rejectUnknown(descriptor.Frame) != nil ||
		!frameMatches(binding, descriptor.Frame) || descriptor.Frame.RecordSequence == 0 {
		return nil, nil, invalid("Config record descriptor identity is invalid")
	}
	chunk := descriptor.Frame.GetValueChunk()
	if chunk == nil {
		if descriptor.Value != nil {
			return nil, nil, invalid("Config value descriptor has no value frame")
		}
		validated, err := ValidateFrame(binding, descriptor.Frame)
		if err != nil {
			return nil, nil, err
		}
		return validated, nil, nil
	}
	value := descriptor.Value
	if value == nil || len(chunk.Content) != 0 || chunk.Ordinal == 0 ||
		chunk.Ordinal > backupconfig.MaxEntries || value.SizeBytes == 0 ||
		value.SizeBytes > backupconfig.TransferChunkBytes ||
		chunk.Offset > backupconfig.MaxSelectedValueBytes-uint64(value.SizeBytes) {
		return nil, nil, invalid("Config retained value descriptor is invalid")
	}
	switch record := descriptor.Frame.Record.(type) {
	case *agentpb.BackupConfigTransfer_ValueChunk:
		if record == nil || record.ValueChunk == nil {
			return nil, nil, invalid("Config retained value descriptor kind is invalid")
		}
	default:
		return nil, nil, invalid("Config retained value descriptor kind is invalid")
	}
	frame := proto.CloneOf(descriptor.Frame)
	if proto.Size(frame) > backupconfig.MaxEntryHeaderEnvelopeBytes {
		return nil, nil, invalid("Config retained value descriptor exceeds its canonical size")
	}
	ownedValue := *value
	return frame, &ownedValue, nil
}
