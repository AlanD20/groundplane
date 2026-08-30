package backuppostgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
)

// Rationale: opaque PostgreSQL bytes still require the fixed producer major,
// adapter contract, exact length, exact digest, and exact EOF.
func TestVerifySourceAcceptsOnlyExactOpaqueArtifact(t *testing.T) {
	t.Parallel()
	content := []byte("PGDMP\x01opaque-custom-dump")
	evidence := ArtifactEvidence{
		Source: backupformat.Evidence{SizeBytes: uint64(len(content)), SHA256: sha256.Sum256(content)},
		Archive: ArchiveEvidence{
			PGDumpMajor:            16,
			AdapterContractVersion: 1,
		},
	}
	if err := VerifySource(context.Background(), bytes.NewReader(content), evidence); err != nil {
		t.Fatal(err)
	}
	for _, invalidContent := range [][]byte{content[:len(content)-1], append(append([]byte(nil), content...), 0)} {
		if err := VerifySource(context.Background(), bytes.NewReader(invalidContent), evidence); err == nil {
			t.Fatal("VerifySource accepted a length mismatch")
		}
	}
}

// Rationale: no mutable producer identity or future adapter version may be
// interpreted as postgres-custom-v1 evidence.
func TestArchiveEvidenceRejectsOtherVersions(t *testing.T) {
	t.Parallel()
	for _, evidence := range []ArchiveEvidence{
		{},
		{PGDumpMajor: 15, AdapterContractVersion: 1},
		{PGDumpMajor: 16, AdapterContractVersion: 2},
	} {
		if err := evidence.Validate(); err == nil {
			t.Fatalf("Validate accepted %#v", evidence)
		}
	}
}
