package backupobject

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: S3 keys are durable authority, so non-canonical paths and keys
// beyond the provider's byte limit must be rejected before any provider call.
func TestArtifactRejectsNonCanonicalObjectKeys(t *testing.T) {
	digest := sha256.Sum256(nil)
	base := Artifact{
		Key:             "artifact.bin",
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, testTime, 1),
		SourceID:        ids.NewAt(ids.KindBackupSource, testTime, 2),
		RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, testTime, 3),
		SourceFormat:    SourceFormatEnvironmentConfig,
		Encryption:      EncryptionNone,
		Evidence: Evidence{
			SourceSHA256: digest,
			StoredSHA256: digest,
		},
	}
	keys := []string{
		string([]byte{0xff}),
		"nul\x00key",
		`back\slash`,
		"/leading",
		"trailing/",
		"empty//segment",
		"dot/./segment",
		"dot/../segment",
		strings.Repeat("a", 1025),
	}
	for _, key := range keys {
		artifact := base
		artifact.Key = key
		err := artifact.Validate()
		if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
			t.Errorf("Validate(%q) kind = %v, %t; error = %v", key, kind, ok, err)
		}
	}
}

// Rationale: zero is the absence boundary for age key eras; accepting it
// would make encrypted recovery evidence refer to no usable key generation.
func TestArtifactRequiresPositiveAgeKeyEra(t *testing.T) {
	digest := sha256.Sum256(nil)
	era := uint64(0)
	artifact := Artifact{
		Key:             "artifact.bin",
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, testTime, 1),
		SourceID:        ids.NewAt(ids.KindBackupSource, testTime, 2),
		RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, testTime, 3),
		SourceFormat:    SourceFormatEnvironmentConfig,
		Encryption:      EncryptionAge,
		KeyEra:          &era,
		Evidence: Evidence{
			SourceSHA256: digest,
			StoredSHA256: digest,
		},
	}
	if kind, ok := errs.KindOf(artifact.Validate()); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("zero KeyEra validation kind = %v, %t", kind, ok)
	}
	era = 1
	if err := artifact.Validate(); err != nil {
		t.Fatalf("KeyEra 1 Validate() error = %v", err)
	}
}

// Rationale: VersionId presence is identity even when a provider emits an
// empty value, while an empty ETag has no identity information at all.
func TestDiscriminatorAllowsPresentEmptyVersionOnly(t *testing.T) {
	version := Discriminator{Kind: DiscriminatorVersionID, Value: ""}
	if err := version.Validate(); err != nil {
		t.Fatalf("empty present VersionId Validate() error = %v", err)
	}
	etag := Discriminator{Kind: DiscriminatorETag, Value: ""}
	if kind, ok := errs.KindOf(etag.Validate()); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("empty ETag validation kind = %v, %t", kind, ok)
	}
}

// Rationale: discriminator values cross durable and HTTP boundaries, so their
// limits are UTF-8 byte limits with distinct VersionId and ETag emptiness.
func TestDiscriminatorUTF8ByteBoundaries(t *testing.T) {
	cases := []struct {
		name          string
		discriminator Discriminator
		valid         bool
	}{
		{"empty version", Discriminator{Kind: DiscriminatorVersionID}, true},
		{"1024-byte version", Discriminator{Kind: DiscriminatorVersionID, Value: strings.Repeat("v", 1024)}, true},
		{"1025-byte version", Discriminator{Kind: DiscriminatorVersionID, Value: strings.Repeat("v", 1025)}, false},
		{"invalid UTF-8 version", Discriminator{Kind: DiscriminatorVersionID, Value: string([]byte{0xff})}, false},
		{"one-byte ETag", Discriminator{Kind: DiscriminatorETag, Value: "e"}, true},
		{"1024-byte ETag", Discriminator{Kind: DiscriminatorETag, Value: strings.Repeat("e", 1024)}, true},
		{"empty ETag", Discriminator{Kind: DiscriminatorETag}, false},
		{"1025-byte ETag", Discriminator{Kind: DiscriminatorETag, Value: strings.Repeat("e", 1025)}, false},
		{"invalid UTF-8 ETag", Discriminator{Kind: DiscriminatorETag, Value: string([]byte{0xff})}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := test.discriminator.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid {
				if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
					t.Fatalf("Validate() error = %v, kind = %v, %t", err, kind, ok)
				}
			}
		})
	}
}
