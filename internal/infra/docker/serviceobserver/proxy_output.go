package serviceobserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Envoy's compact-file/HTTP evidence must not pass through a TTY's newline
// conversion. Bound Docker framing before copying; an untrusted declared frame
// length cannot allocate memory or bypass the combined stdout/stderr budget.
func readProxyObservation(reader io.Reader, tty bool, maximumBytes int) ([]byte, error) {
	if tty {
		return io.ReadAll(io.LimitReader(reader, int64(maximumBytes)+1))
	}
	var output bytes.Buffer
	remaining := int64(maximumBytes)
	frames := 0
	for {
		var header [8]byte
		count, err := io.ReadFull(reader, header[:])
		if errors.Is(err, io.EOF) && count == 0 {
			return output.Bytes(), nil
		}
		frames++
		length := int64(binary.BigEndian.Uint32(header[4:]))
		if err != nil || header[1] != 0 || header[2] != 0 || header[3] != 0 ||
			(header[0] != 1 && header[0] != 2) || length > remaining || frames > 4096 {
			return nil, errs.New(errs.KindRequestFailed, "proxy observation Docker stream is incomplete or oversized")
		}
		remaining -= length
		var target io.Writer = io.Discard
		if header[0] == 1 {
			target = &output
		}
		if _, err := io.CopyN(target, reader, length); err != nil {
			return nil, errs.New(errs.KindRequestFailed, "proxy observation Docker stream ended within a frame")
		}
	}
}
