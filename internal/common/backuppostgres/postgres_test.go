package backuppostgres

import "testing"

// Rationale: no mutable producer identity or future adapter version may be
// interpreted as postgres-custom-v1 evidence.
func TestArchiveEvidenceRejectsOtherVersions(t *testing.T) {
	t.Parallel()
	for _, evidence := range []ArchiveEvidence{
		{},
		{PGDumpMajor: 15, AdapterContractVersion: 1, SourceServerVersion: "16.9", BackupToolVersion: "16.9"},
		{PGDumpMajor: 16, AdapterContractVersion: 2, SourceServerVersion: "16.9", BackupToolVersion: "16.9"},
	} {
		if err := evidence.Validate(); err == nil {
			t.Fatalf("Validate accepted %#v", evidence)
		}
	}
}
