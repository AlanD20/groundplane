package dnsresolverobserver

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"io"

	"github.com/moby/moby/api/pkg/stdcopy"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Read the current process's complete stream, not a query-count-dependent tail.
// Memory is bounded by one line; the Docker request shares the proof deadline.
// CoreDNS emits this fingerprint on successful instance startup, whereas its
// reload metric is absent initially and may describe a failed reload attempt.
func readServingConfiguration(logs io.ReadCloser, tty bool) (*[sha512.Size]byte, error) {
	if logs == nil {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver process logs are unavailable")
	}
	defer logs.Close()
	writer := &configurationLogWriter{}
	var err error
	if tty {
		_, err = io.Copy(writer, logs)
	} else {
		_, err = stdcopy.StdCopy(writer, writer, logs)
	}
	if err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, err)
	}
	if err := writer.line(writer.pending); err != nil {
		return nil, err
	}
	return writer.serving, nil
}

type configurationLogWriter struct {
	pending []byte
	serving *[sha512.Size]byte
}

func (writer *configurationLogWriter) Write(value []byte) (int, error) {
	length := len(value)
	for len(value) > 0 {
		end := bytes.IndexByte(value, '\n')
		if end < 0 {
			end = len(value)
		}
		if len(writer.pending)+end > maximumArtifactBytes {
			return 0, errs.New(errs.KindStateConflict, "DNS resolver process log line is too large")
		}
		writer.pending = append(writer.pending, value[:end]...)
		if end == len(value) {
			break
		}
		if err := writer.line(writer.pending); err != nil {
			return 0, err
		}
		writer.pending = writer.pending[:0]
		value = value[end+1:]
	}
	return length, nil
}

func (writer *configurationLogWriter) line(line []byte) error {
	prefix := []byte("[INFO] plugin/reload: Running configuration SHA512 = ")
	if !bytes.HasPrefix(line, prefix) {
		return nil
	}
	encoded := bytes.TrimSpace(line[len(prefix):])
	var digest [sha512.Size]byte
	if len(encoded) != hex.EncodedLen(len(digest)) {
		return errs.New(errs.KindStateConflict, "DNS resolver serving fingerprint is invalid")
	}
	if _, err := hex.Decode(digest[:], encoded); err != nil {
		return errs.New(errs.KindStateConflict, "DNS resolver serving fingerprint is invalid")
	}
	writer.serving = &digest
	return nil
}
