package postgres16helper

import (
	"bytes"
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

const gateCapsuleMagic = "GPPG16G1"

// encodeGateCapsule is the one private binary handoff to the single-threaded
// gate. The authoritative Go intent is validated first; this smaller capsule
// contains only the kernel identities and fixed argv the gate must enforce.
func encodeGateCapsule(
	launch privateGateLaunch, intentSHA256 postgres16protocol.Digest,
) ([]byte, error) {
	if err := launch.Intent.Validate(launch.Authority); err != nil {
		return nil, err
	}
	intent := launch.Intent
	if len(intent.Arguments) == 0 || len(intent.Arguments) > 32 {
		return nil, privateWireError()
	}
	var buffer bytes.Buffer
	buffer.Grow(privateIntentLimit)
	buffer.WriteString(gateCapsuleMagic)
	writeGateUint32(&buffer, 1)
	buffer.Write(intent.Nonce[:])
	buffer.Write(intentSHA256[:])
	writeGateUint32(&buffer, intent.Supervisor.PID)
	writeGateUint64(&buffer, intent.Supervisor.StartTicks)
	buffer.Write(intent.Supervisor.BootID[:])
	writeGateUint64(&buffer, intent.DeadlineUnixNano)
	writeGateUint64(&buffer, intent.GateFile.Device)
	writeGateUint64(&buffer, intent.GateFile.Inode)
	writeGateUint64(&buffer, intent.ClientFile.Device)
	writeGateUint64(&buffer, intent.ClientFile.Inode)
	writeGateUint64(&buffer, intent.ClientFile.SizeBytes)
	buffer.Write(intent.ClientFile.SHA256[:])
	buffer.WriteByte(byte(intent.Operation))
	buffer.WriteByte(byte(len(intent.Arguments)))
	for _, argument := range intent.Arguments {
		if len(argument) == 0 || len(argument) > 4096 {
			return nil, privateWireError()
		}
		writeGateUint16(&buffer, uint16(len(argument)))
		buffer.Write(argument)
	}
	if buffer.Len() > privateIntentLimit {
		return nil, privateWireError()
	}
	return buffer.Bytes(), nil
}

func writeGateUint16(buffer *bytes.Buffer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeGateUint32(buffer *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeGateUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}
