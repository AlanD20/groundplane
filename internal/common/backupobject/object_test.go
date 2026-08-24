package backupobject

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: artifact metadata is durable immutable authority, so version 1
// must have one exact four-part representation and no legacy digest key.
func TestMetadataIsTheExactVersionOneSet(t *testing.T) {
	era := uint64(7)
	artifact := Artifact{
		Key:             "backups/env/source/point/artifact.bin",
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, testTime, 1),
		SourceID:        ids.NewAt(ids.KindBackupSource, testTime, 2),
		RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, testTime, 3),
		SourceFormat:    SourceFormatVolumeTar,
		Encryption:      EncryptionAge,
		KeyEra:          &era,
		Evidence: Evidence{
			SourceSizeBytes: 17,
			SourceSHA256:    sha256.Sum256([]byte("source")),
			StoredSizeBytes: 41,
			StoredSHA256:    sha256.Sum256([]byte("stored")),
		},
	}
	if err := artifact.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	metadata := artifact.Metadata()
	if len(metadata) != 11 {
		t.Fatalf("metadata count = %d, want 11: %#v", len(metadata), metadata)
	}
	if metadata[metadataSourceSize] != "17" || metadata[metadataStoredSize] != "41" ||
		metadata[metadataKeyEra] != "7" {
		t.Fatalf("numeric metadata = %#v", metadata)
	}
	if metadata[metadataSourceSHA256] != "41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d" ||
		metadata[metadataStoredSHA256] != "87b04e58961f9a99d853d4046a0b5b793e7c3e4bbd21f5aca8fb17c20cdb1d8b" {
		t.Fatalf("digest metadata = %#v", metadata)
	}
	if _, exists := metadata["groundplane-sha256"]; exists {
		t.Fatal("legacy groundplane-sha256 metadata must not exist")
	}
}

// Rationale: unencrypted artifacts reuse the source bytes, so unequal source
// and stored evidence would make object verification internally contradictory.
func TestArtifactRejectsUnequalUnencryptedEvidence(t *testing.T) {
	digest := sha256.Sum256([]byte("same"))
	artifact := Artifact{
		Key:             "artifact.bin",
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, testTime, 1),
		SourceID:        ids.NewAt(ids.KindBackupSource, testTime, 2),
		RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, testTime, 3),
		SourceFormat:    SourceFormatPostgresCustom,
		Encryption:      EncryptionNone,
		Evidence: Evidence{
			SourceSizeBytes: 4,
			SourceSHA256:    digest,
			StoredSizeBytes: 5,
			StoredSHA256:    digest,
		},
	}
	if kind, ok := errs.KindOf(artifact.Validate()); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("Validate() kind = %v, %t", kind, ok)
	}
}

// Rationale: a provider's literal VersionId "null" is immutable evidence and
// must remain distinct from the ETag fallback used only when VersionId is absent.
func TestDiscriminatorPreservesLiteralNullVersion(t *testing.T) {
	version := Discriminator{Kind: DiscriminatorVersionID, Value: "null"}
	etag := Discriminator{Kind: DiscriminatorETag, Value: "null"}
	if err := version.Validate(); err != nil {
		t.Fatalf("version Validate() error = %v", err)
	}
	if version == etag {
		t.Fatal("literal null VersionId was conflated with ETag")
	}
}

var testTime = mustTestTime()

func mustTestTime() time.Time {
	return time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
}
