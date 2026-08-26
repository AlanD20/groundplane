package etcd

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestNewVolumeRecordRequiresStableOwnershipSlugAndSafeComposeKey(t *testing.T) {
	// Rationale: the mutable slug and immutable direct-child key are separate
	// identities with separate accepted grammars.
	t.Parallel()
	environmentID := ids.New(ids.KindEnvironment)
	volumeID := ids.New(ids.KindVolume)
	for _, key := range []string{"app-data", "cache.v2", "API_2"} {
		if _, err := NewVolumeRecord(environmentID, volumeID, "application-data", key); err != nil {
			t.Fatalf("NewVolumeRecord(key %q) error = %v", key, err)
		}
	}
	for _, key := range []string{"", ".", "..", "data/other", "data other"} {
		_, err := NewVolumeRecord(environmentID, volumeID, "application-data", key)
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("NewVolumeRecord(key %q) error = %v, want validation.failed", key, err)
		}
	}
	for _, volumeSlug := range []string{"", "Upper", "two--hyphens", "-leading", "trailing-"} {
		_, err := NewVolumeRecord(environmentID, volumeID, volumeSlug, "app-data")
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("NewVolumeRecord(slug %q) error = %v, want validation.failed", volumeSlug, err)
		}
	}
}
