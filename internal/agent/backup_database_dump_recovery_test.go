package agent

import (
	"bytes"
	"testing"
)

// Agent loss can leave a partial dump in staging. Recovery must compare that
// prefix, append only missing original bytes, and reject altered or oversized
// output instead of accepting corruption or duplicating the prefix.
func TestDatabaseDumpRecoveryPreservesPrefixAndRejectsChangedOutput(t *testing.T) {
	for _, prefix := range []string{"", "ori", "original-dump"} {
		t.Run("prefix-"+prefix, func(t *testing.T) {
			var output bytes.Buffer
			writer := &databaseDumpRecoveryWriter{prefix: bytes.NewBufferString(prefix),
				remainingPrefix: uint64(len(prefix)), output: &output, limit: 13}
			for _, part := range []string{"or", "igina", "l-dump"} {
				if count, err := writer.Write([]byte(part)); err != nil || count != len(part) {
					t.Fatalf("Write() = %d, %v", count, err)
				}
			}
			if writer.remainingPrefix != 0 || prefix+output.String() != "original-dump" {
				t.Fatalf("recovered output = %q, remaining prefix = %d", prefix+output.String(), writer.remainingPrefix)
			}
		})
	}
	var output bytes.Buffer
	changed := &databaseDumpRecoveryWriter{prefix: bytes.NewBufferString("bad"),
		remainingPrefix: 3, output: &output, limit: 13}
	if _, err := changed.Write([]byte("original-dump")); err == nil || output.Len() != 0 {
		t.Fatal("changed prefix accepted or appended")
	}
	oversized := &databaseDumpRecoveryWriter{prefix: &bytes.Buffer{}, output: &output, limit: 2}
	if _, err := oversized.Write([]byte("original-dump")); err == nil || output.Len() != 0 {
		t.Fatal("oversized original output accepted or appended")
	}
}
