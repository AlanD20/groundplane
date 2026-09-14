package materializationcontent

import (
	"bytes"
	"testing"
)

// SVC-15: a source snapshot can preserve a file written by an earlier
// generation. The immutable record and bytes, not the latest renderer, own it.
func TestRetainedFileLoadsAcrossLaterGenerationsButNotEarlierOnes(t *testing.T) {
	t.Parallel()
	repository := mustRepository(t, newMemoryStore())
	content := []byte("original file")
	record := componentRecord(content)
	if err := repository.Stage(t.Context(), record, contentGeneration, content); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.Load(t.Context(), record, contentGeneration+2)
	if err != nil || !bytes.Equal(loaded, content) {
		t.Fatalf("retained file unavailable: %v", err)
	}
	clear(loaded)
	if _, err := repository.Load(t.Context(), record, contentGeneration-1); err == nil {
		t.Fatal("future generation supplied earlier execution")
	}
	record.Mode = 0o600
	if _, err := repository.Load(t.Context(), record, contentGeneration+2); err == nil {
		t.Fatal("later generation bypassed exact record identity")
	}
}
