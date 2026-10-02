package agentvolumejournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
)

const (
	maximumAuthorityBytes      = 4096
	maximumFrameBytes          = 4352
	recordDomain               = "groundplane.agent.volume-journal.record.v1"
	authorityDomain            = "groundplane.agent.volume-journal.authority.v1"
	phaseIntent           byte = 1
	phaseCompleted        byte = 2
)

type record struct {
	sequence uint64
	phase    byte
	mutation backupvolumefs.Mutation
	prior    [sha256.Size]byte
	hash     [sha256.Size]byte
}

func encodeAuthority(config Config) ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return nil, storage(err)
	}
	if len(raw) == 0 || len(raw) > maximumAuthorityBytes {
		return nil, invalid("Volume journal authority exceeds its bound")
	}
	return raw, nil
}

func decodeAuthority(raw []byte) (Config, error) {
	if len(raw) == 0 || len(raw) > maximumAuthorityBytes {
		return Config{}, invalid("Volume journal authority length is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config Config
	if decoder.Decode(&config) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return Config{}, invalid("Volume journal authority encoding is invalid")
	}
	encoded, err := encodeAuthority(config)
	if err != nil || !bytes.Equal(encoded, raw) {
		return Config{}, invalid("Volume journal authority is not canonical")
	}
	return config, nil
}

func initialChain(raw []byte) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte(authorityDomain))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(raw)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func encodeRecord(value record) ([]byte, error) {
	if value.sequence == 0 || value.phase != phaseIntent && value.phase != phaseCompleted ||
		len(value.mutation.Entry.Path) > backupvolume.MaxPathBytes {
		return nil, invalid("Volume journal record header is invalid")
	}
	kind := kindByte(value.mutation.Kind)
	if kind == 0 {
		return nil, invalid("Volume journal mutation kind is invalid")
	}
	raw := make([]byte, 4, maximumFrameBytes)
	raw = append(raw, 1)
	raw = binary.BigEndian.AppendUint64(raw, value.sequence)
	raw = append(raw, value.phase, kind)
	raw = binary.BigEndian.AppendUint64(raw, value.mutation.Ordinal)
	raw = binary.BigEndian.AppendUint16(raw, uint16(len(value.mutation.Entry.Path)))
	raw = append(raw, value.mutation.Entry.Path...)
	raw = append(raw, byte(value.mutation.Entry.Kind))
	raw = binary.BigEndian.AppendUint32(raw, value.mutation.Entry.Mode)
	raw = binary.BigEndian.AppendUint32(raw, value.mutation.Entry.UID)
	raw = binary.BigEndian.AppendUint32(raw, value.mutation.Entry.GID)
	raw = binary.BigEndian.AppendUint64(raw, value.mutation.Entry.SizeBytes)
	raw = append(raw, value.mutation.Entry.ContentSHA256[:]...)
	raw = append(raw, value.mutation.OldTreeSHA[:]...)
	raw = append(raw, value.mutation.NewTreeSHA[:]...)
	raw = binary.BigEndian.AppendUint64(raw, value.mutation.OldInode)
	raw = binary.BigEndian.AppendUint64(raw, value.mutation.NewInode)
	raw = append(raw, value.prior[:]...)
	digest := sha256.New()
	_, _ = digest.Write([]byte(recordDomain))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(raw[4:])
	raw = append(raw, digest.Sum(nil)...)
	if len(raw) > maximumFrameBytes {
		return nil, invalid("Volume journal record exceeds its bound")
	}
	binary.BigEndian.PutUint32(raw[:4], uint32(len(raw)-4))
	return raw, nil
}

func decodeRecord(raw []byte) (record, error) {
	if len(raw) < 4 || len(raw) > maximumFrameBytes || binary.BigEndian.Uint32(raw[:4]) != uint32(len(raw)-4) {
		return record{}, invalid("Volume journal record framing is invalid")
	}
	reader := fieldReader{raw: raw[4:]}
	if reader.byte() != 1 {
		return record{}, invalid("Volume journal record schema is invalid")
	}
	value := record{sequence: reader.uint64(), phase: reader.byte()}
	value.mutation.Kind = byteKind(reader.byte())
	value.mutation.Ordinal = reader.uint64()
	pathLength := reader.uint16()
	value.mutation.Entry.Path = append([]byte(nil), reader.bytes(int(pathLength))...)
	value.mutation.Entry.Kind = backupvolume.EntryKind(reader.byte())
	value.mutation.Entry.Mode = reader.uint32()
	value.mutation.Entry.UID = reader.uint32()
	value.mutation.Entry.GID = reader.uint32()
	value.mutation.Entry.SizeBytes = reader.uint64()
	copy(value.mutation.Entry.ContentSHA256[:], reader.bytes(sha256.Size))
	copy(value.mutation.OldTreeSHA[:], reader.bytes(sha256.Size))
	copy(value.mutation.NewTreeSHA[:], reader.bytes(sha256.Size))
	value.mutation.OldInode = reader.uint64()
	value.mutation.NewInode = reader.uint64()
	copy(value.prior[:], reader.bytes(sha256.Size))
	copy(value.hash[:], reader.bytes(sha256.Size))
	if reader.invalid || reader.offset != len(reader.raw) {
		return record{}, invalid("Volume journal record length is invalid")
	}
	canonical, err := encodeRecord(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return record{}, invalid("Volume journal record hash or encoding is invalid")
	}
	return value, nil
}

type fieldReader struct {
	raw     []byte
	offset  int
	invalid bool
}

func (reader *fieldReader) bytes(size int) []byte {
	if reader.invalid || size < 0 || size > len(reader.raw)-reader.offset {
		reader.invalid = true
		return nil
	}
	value := reader.raw[reader.offset : reader.offset+size]
	reader.offset += size
	return value
}
func (reader *fieldReader) byte() byte {
	value := reader.bytes(1)
	if len(value) != 1 {
		return 0
	}
	return value[0]
}
func (reader *fieldReader) uint16() uint16 {
	value := reader.bytes(2)
	if len(value) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(value)
}
func (reader *fieldReader) uint32() uint32 {
	value := reader.bytes(4)
	if len(value) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}
func (reader *fieldReader) uint64() uint64 {
	value := reader.bytes(8)
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

func kindByte(kind backupvolumefs.MutationKind) byte {
	switch kind {
	case backupvolumefs.MutationConstruction:
		return 1
	case backupvolumefs.MutationFinalization:
		return 2
	case backupvolumefs.MutationExchange:
		return 3
	case backupvolumefs.MutationDelete:
		return 4
	default:
		return 0
	}
}
func byteKind(value byte) backupvolumefs.MutationKind {
	switch value {
	case 1:
		return backupvolumefs.MutationConstruction
	case 2:
		return backupvolumefs.MutationFinalization
	case 3:
		return backupvolumefs.MutationExchange
	case 4:
		return backupvolumefs.MutationDelete
	default:
		return ""
	}
}
