// Package recordcodec implements the binary envelope used by final-schema
// Groundplane authority records. Payload schemas remain owned by their
// persistence modules.
package recordcodec

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	HeaderBytes = 44
	Version     = uint8(1)
)

var magic = [4]byte{'G', 'D', 'R', '1'}

// Kind is a closed record discriminator within one persistence package.
// Zero is reserved so an uninitialized kind can never be durable.
type Kind uint8

// Encode wraps payload in the exact GDR1 envelope. The returned slice owns a
// copy of payload.
func Encode(kind Kind, payload []byte) ([]byte, error) {
	if kind == 0 {
		return nil, errs.New(errs.KindValidationFailed, "durable record kind is required")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return nil, errs.New(errs.KindValidationFailed, "durable record payload is too large")
	}
	value := make([]byte, HeaderBytes+len(payload))
	copy(value[:4], magic[:])
	value[4] = byte(kind)
	value[5] = Version
	// Bytes 6 and 7 are the version-1 flags and must remain zero.
	binary.BigEndian.PutUint32(value[8:12], uint32(len(payload)))
	digest := sha256.Sum256(payload)
	copy(value[12:HeaderBytes], digest[:])
	copy(value[HeaderBytes:], payload)
	return value, nil
}

// Decode verifies the complete envelope and returns a payload copy. A maximum
// of zero means no package-local payload ceiling beyond the uint32 envelope.
func Decode(value []byte, kind Kind, maximum int) ([]byte, error) {
	if kind == 0 || len(value) < HeaderBytes || string(value[:4]) != string(magic[:]) ||
		value[4] != byte(kind) || value[5] != Version || value[6] != 0 || value[7] != 0 {
		return nil, corrupt()
	}
	payloadLength := uint64(binary.BigEndian.Uint32(value[8:12]))
	if payloadLength != uint64(len(value)-HeaderBytes) || (maximum > 0 && payloadLength > uint64(maximum)) {
		return nil, corrupt()
	}
	payload := value[HeaderBytes:]
	digest := sha256.Sum256(payload)
	if !equalDigest(value[12:HeaderBytes], digest[:]) {
		return nil, corrupt()
	}
	return append([]byte(nil), payload...), nil
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for index := range left {
		different |= left[index] ^ right[index]
	}
	return different == 0
}

func corrupt() error {
	return errs.New(errs.KindInternal, "durable record envelope is corrupt")
}
