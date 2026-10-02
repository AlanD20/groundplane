package postgres16helper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

// The gate wire is private to this release. It has one canonical encoding,
// no extension fields, and a four-byte length prefix on pipes. The intent is
// written to a sealed descriptor before launch; a gate cannot select another
// operation or client from its own command line.
const (
	privateIntentLimit = 32 * 1024
	privateStatusLimit = 4 * 1024
	privateStateLimit  = 64 * 1024
)

type privateGateLaunch struct {
	Schema    uint32                                         `json:"schema"`
	Authority postgres16protocol.ConfinementReleaseAuthority `json:"authority"`
	Intent    postgres16protocol.ConfinementLaunchIntent     `json:"intent"`
}

func marshalGateLaunch(value privateGateLaunch) ([]byte, postgres16protocol.Digest, error) {
	if value.Schema != 1 {
		return nil, postgres16protocol.Digest{}, privateWireError()
	}
	if err := value.Intent.Validate(value.Authority); err != nil {
		return nil, postgres16protocol.Digest{}, err
	}
	encoded, err := marshalPrivate(value, privateIntentLimit)
	if err != nil {
		return nil, postgres16protocol.Digest{}, err
	}
	return encoded, postgres16protocol.Digest(sha256.Sum256(encoded)), nil
}

func unmarshalGateLaunch(encoded []byte) (privateGateLaunch, postgres16protocol.Digest, error) {
	var value privateGateLaunch
	if err := unmarshalPrivate(encoded, privateIntentLimit, &value); err != nil {
		return privateGateLaunch{}, postgres16protocol.Digest{}, err
	}
	canonical, digest, err := marshalGateLaunch(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return privateGateLaunch{}, postgres16protocol.Digest{}, privateWireError()
	}
	return value, digest, nil
}

func marshalGateStatus(value postgres16protocol.ConfinementGateStatus) ([]byte, error) {
	return marshalPrivate(value, privateStatusLimit)
}

func unmarshalGateStatus(
	encoded []byte,
	launch privateGateLaunch,
	intentSHA256 postgres16protocol.Digest,
	parent postgres16protocol.ConfinementProcessIdentity,
) (postgres16protocol.ConfinementGateStatus, error) {
	var value postgres16protocol.ConfinementGateStatus
	if err := unmarshalPrivate(encoded, privateStatusLimit, &value); err != nil {
		return value, err
	}
	if err := value.Validate(launch.Intent, launch.Authority, intentSHA256, parent); err != nil {
		return postgres16protocol.ConfinementGateStatus{}, err
	}
	canonical, err := marshalGateStatus(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return postgres16protocol.ConfinementGateStatus{}, privateWireError()
	}
	return value, nil
}

func marshalState(value postgres16protocol.ConfinementStateShape) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return marshalPrivate(value, privateStateLimit)
}

func unmarshalState(encoded []byte) (postgres16protocol.ConfinementStateShape, error) {
	var value postgres16protocol.ConfinementStateShape
	if err := unmarshalPrivate(encoded, privateStateLimit, &value); err != nil {
		return value, err
	}
	if err := value.Validate(); err != nil {
		return postgres16protocol.ConfinementStateShape{}, err
	}
	canonical, err := marshalState(value)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return postgres16protocol.ConfinementStateShape{}, privateWireError()
	}
	return value, nil
}

func marshalPrivate[T any](value T, limit int) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) == 0 || len(encoded) > limit {
		return nil, privateWireError()
	}
	return encoded, nil
}

func unmarshalPrivate[T any](encoded []byte, limit int, value *T) error {
	if len(encoded) == 0 || len(encoded) > limit {
		return privateWireError()
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return privateWireError()
	}
	return nil
}

func writePrivateFrame(writer io.Writer, encoded []byte, limit int) error {
	if len(encoded) == 0 || len(encoded) > limit {
		return privateWireError()
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(encoded)))
	if err := writeAll(writer, header[:]); err != nil {
		return err
	}
	return writeAll(writer, encoded)
}

func readPrivateFrame(reader io.Reader, limit int) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, privateWireError()
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > uint32(limit) {
		return nil, privateWireError()
	}
	encoded := make([]byte, int(size))
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return nil, privateWireError()
	}
	return encoded, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil || n <= 0 {
			return privateWireError()
		}
		data = data[n:]
	}
	return nil
}

func privateWireError() error {
	return grounderrs.New(grounderrs.KindInternal, "postgres helper private wire is invalid")
}
