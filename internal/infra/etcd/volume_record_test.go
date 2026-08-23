package etcd

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestNewVolumeRecordRequiresStableOwnershipAndSafeComposeKey(t *testing.T) {
	// Rationale: a durable Volume name becomes one direct child of the private
	// Environment directory, so a valid Compose key must not collapse to dot.
	t.Parallel()
	environmentID := ids.New(ids.KindEnvironment)
	volumeID := ids.New(ids.KindVolume)
	for _, name := range []string{"app-data", "cache.v2", "API_2"} {
		if _, err := NewVolumeRecord(environmentID, volumeID, name); err != nil {
			t.Fatalf("NewVolumeRecord(%q) error = %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "data/other", "data other"} {
		_, err := NewVolumeRecord(environmentID, volumeID, name)
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("NewVolumeRecord(%q) error = %v, want validation.failed", name, err)
		}
	}
}
