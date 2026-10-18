package backingcommand

import (
	"encoding/binary"
	"io"
)

// Docker frame lengths do not control allocation. Callers either discard stdout
// or supply a bounded private result sink. Stderr is never retained or exposed.
func drainOutput(reader io.Reader, ready chan<- struct{}, output io.Writer) error {
	var header [8]byte
	var buffer [32 * 1024]byte
	matched := 0
	for {
		_, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return unavailable("backing command Docker stream frame is invalid")
		}
		remaining := uint64(binary.BigEndian.Uint32(header[4:]))
		for remaining > 0 {
			count := min(remaining, uint64(len(buffer)))
			if _, err := io.ReadFull(reader, buffer[:count]); err != nil {
				clear(buffer[:])
				return err
			}
			offset := 0
			if header[0] == 1 && matched < len(readyMarker) {
				for _, value := range buffer[:count] {
					if value != readyMarker[matched] {
						clear(buffer[:])
						return unavailable("backing command readiness handshake is invalid")
					}
					matched++
					offset++
					if matched == len(readyMarker) {
						close(ready)
						break
					}
				}
			}
			if header[0] == 1 && matched == len(readyMarker) && uint64(offset) < count {
				written, err := output.Write(buffer[offset:count])
				if err != nil || written != int(count)-offset {
					clear(buffer[:])
					return unavailable("backing command private result could not be captured")
				}
			}
			clear(buffer[:count])
			remaining -= count
		}
	}
}

func writeInput(connection io.Writer, input []byte) error {
	closer, ok := connection.(interface{ CloseWrite() error })
	if !ok {
		return unavailable("backing command input cannot be closed independently")
	}
	for len(input) > 0 {
		written, err := connection.Write(input)
		if err != nil || written == 0 {
			return unavailable("backing command input delivery is uncertain")
		}
		input = input[written:]
	}
	if err := closer.CloseWrite(); err != nil {
		return unavailable("backing command input completion is uncertain")
	}
	return nil
}
