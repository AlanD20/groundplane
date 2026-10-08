package agent

import (
	"bytes"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// databaseDumpRecoveryWriter verifies the retained staging prefix against the
// original daemon-owned dump, then appends only its missing suffix. It never
// starts another database dump or overwrites previously observed bytes.
type databaseDumpRecoveryWriter struct {
	prefix          io.Reader
	remainingPrefix uint64
	output          io.Writer
	limit           uint64
}

func (writer *databaseDumpRecoveryWriter) Write(content []byte) (int, error) {
	if uint64(len(content)) > writer.limit {
		return 0, errs.New(errs.KindStateConflict, "original database dump exceeds its sealed limit")
	}
	length := len(content)
	consumed := 0
	var comparison [32 * 1024]byte
	for writer.remainingPrefix > 0 && len(content) > 0 {
		count := min(len(content), len(comparison), int(writer.remainingPrefix))
		if _, err := io.ReadFull(writer.prefix, comparison[:count]); err != nil {
			return consumed, errs.Wrap(errs.KindStateConflict, err)
		}
		if !bytes.Equal(comparison[:count], content[:count]) {
			return consumed, errs.New(
				errs.KindStateConflict,
				"retained database dump prefix differs from original execution",
			)
		}
		writer.remainingPrefix -= uint64(count)
		writer.limit -= uint64(count)
		content = content[count:]
		consumed += count
	}
	if len(content) != 0 {
		count, err := writer.output.Write(content)
		consumed += count
		writer.limit -= uint64(count)
		return consumed, err
	}
	return length, nil
}

func (*databaseDumpRecoveryWriter) Close() error { return nil }
