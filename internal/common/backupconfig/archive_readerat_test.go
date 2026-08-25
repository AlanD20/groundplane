package backupconfig

import (
	"context"
	"io"
	"testing"
)

// Rationale: io.ReaderAt permits a full final read to return io.EOF, and the
// archive reader must accept that valid result without weakening short-read checks.
func TestReadAtContextAcceptsFullReadWithEOF(t *testing.T) {
	t.Parallel()

	source := eofReaderAt([]byte("payload"))
	destination := make([]byte, len(source))
	if err := readAtContext(context.Background(), source, destination, 0); err != nil {
		t.Fatalf("readAtContext() error = %v", err)
	}
	if string(destination) != string(source) {
		t.Fatalf("destination = %q, want %q", destination, source)
	}
}

type eofReaderAt []byte

func (source eofReaderAt) ReadAt(destination []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(source)) {
		return 0, io.EOF
	}
	count := copy(destination, source[offset:])
	return count, io.EOF
}
