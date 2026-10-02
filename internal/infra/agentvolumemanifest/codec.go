package agentvolumemanifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	authorityDomain   = "groundplane.agent.volume-manifest.authority.v1\x00"
	transactionDomain = "groundplane.agent.volume-manifest.transaction.v1\x00"
)

var deterministic = proto.MarshalOptions{Deterministic: true}

type transaction struct {
	sequence uint64
	frame    []byte
	credit   []byte
	prior    [sha256.Size]byte
	hash     [sha256.Size]byte
}

func encodeAuthority(config Config) ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(config)
	if err != nil || len(raw) == 0 || len(raw) > maximumAuthority {
		return nil, invalid("Volume manifest receiver authority exceeds its bound")
	}
	return raw, nil
}

func decodeAuthority(raw []byte) (Config, error) {
	if len(raw) == 0 || len(raw) > maximumAuthority {
		return Config{}, invalid("Volume manifest receiver authority length is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config Config
	if decoder.Decode(&config) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return Config{}, invalid("Volume manifest receiver authority encoding is invalid")
	}
	encoded, err := encodeAuthority(config)
	if err != nil || !bytes.Equal(encoded, raw) {
		return Config{}, invalid("Volume manifest receiver authority is not canonical")
	}
	return config, nil
}

func initialHash(authority []byte) [sha256.Size]byte {
	return sha256.Sum256(append([]byte(authorityDomain), authority...))
}

func encodeTransaction(value transaction) ([]byte, error) {
	if len(value.frame) > backupFrameMaximum || len(value.credit) == 0 || len(value.credit) > 1024 ||
		(value.sequence == 0) != (len(value.frame) == 0) {
		return nil, invalid("Volume manifest receiver transaction shape is invalid")
	}
	raw := make([]byte, 4, maximumTransaction)
	raw = append(raw, 1)
	raw = binary.BigEndian.AppendUint64(raw, value.sequence)
	raw = binary.BigEndian.AppendUint32(raw, uint32(len(value.frame)))
	raw = append(raw, value.frame...)
	raw = binary.BigEndian.AppendUint32(raw, uint32(len(value.credit)))
	raw = append(raw, value.credit...)
	raw = append(raw, value.prior[:]...)
	hash := sha256.New()
	_, _ = hash.Write([]byte(transactionDomain))
	_, _ = hash.Write(raw[4:])
	raw = append(raw, hash.Sum(nil)...)
	if len(raw) > maximumTransaction {
		return nil, invalid("Volume manifest receiver transaction exceeds its bound")
	}
	binary.BigEndian.PutUint32(raw[:4], uint32(len(raw)-4))
	return raw, nil
}

const backupFrameMaximum = backupvolumetransfer.MaxFrameBytes

func decodeTransaction(raw []byte) (transaction, error) {
	if len(raw) < 4 || len(raw) > maximumTransaction || binary.BigEndian.Uint32(raw[:4]) != uint32(len(raw)-4) {
		return transaction{}, invalid("Volume manifest receiver transaction framing is invalid")
	}
	data := raw[4:]
	if len(data) < 1+8+4+4+sha256.Size+sha256.Size || data[0] != 1 {
		return transaction{}, invalid("Volume manifest receiver transaction schema is invalid")
	}
	position := 1
	value := transaction{sequence: binary.BigEndian.Uint64(data[position : position+8])}
	position += 8
	frameLength := int(binary.BigEndian.Uint32(data[position : position+4]))
	position += 4
	if frameLength < 0 || frameLength > backupFrameMaximum || frameLength > len(data)-position-4-2*sha256.Size {
		return transaction{}, invalid("Volume manifest receiver frame length is invalid")
	}
	value.frame = append([]byte(nil), data[position:position+frameLength]...)
	position += frameLength
	creditLength := int(binary.BigEndian.Uint32(data[position : position+4]))
	position += 4
	if creditLength <= 0 || creditLength > 1024 || creditLength != len(data)-position-2*sha256.Size {
		return transaction{}, invalid("Volume manifest receiver credit length is invalid")
	}
	value.credit = append([]byte(nil), data[position:position+creditLength]...)
	position += creditLength
	copy(value.prior[:], data[position:position+sha256.Size])
	position += sha256.Size
	copy(value.hash[:], data[position:position+sha256.Size])
	canonical, err := encodeTransaction(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return transaction{}, invalid("Volume manifest receiver transaction hash or encoding is invalid")
	}
	return value, nil
}

func encodeFrame(frame *agentpb.BackupVolumeManifestTransfer) ([]byte, error) {
	raw, err := deterministic.Marshal(frame)
	if err != nil || len(raw) == 0 || len(raw) > backupFrameMaximum {
		return nil, invalid("Volume manifest receiver frame encoding is invalid")
	}
	return raw, nil
}

func encodeCredit(credit *agentpb.BackupVolumeManifestAckCredit) ([]byte, error) {
	raw, err := deterministic.Marshal(credit)
	if err != nil || len(raw) == 0 || len(raw) > 1024 {
		return nil, invalid("Volume manifest receiver credit encoding is invalid")
	}
	return raw, nil
}

func decodeFrame(raw []byte) (*agentpb.BackupVolumeManifestTransfer, error) {
	frame := &agentpb.BackupVolumeManifestTransfer{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, frame) != nil {
		return nil, invalid("Volume manifest receiver frame decoding failed")
	}
	canonical, err := encodeFrame(frame)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, invalid("Volume manifest receiver frame is not canonical")
	}
	return frame, nil
}

func decodeCredit(raw []byte) (*agentpb.BackupVolumeManifestAckCredit, error) {
	credit := &agentpb.BackupVolumeManifestAckCredit{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, credit) != nil {
		return nil, invalid("Volume manifest receiver credit decoding failed")
	}
	canonical, err := encodeCredit(credit)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, invalid("Volume manifest receiver credit is not canonical")
	}
	return credit, nil
}
