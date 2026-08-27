package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: the public Recovery Point paging seam carries only a stable id;
// inverted ULID bodies and environment-index prefixes remain repository-private.
func TestRecoveryPointContinuationKeyRoundTripsToStableID(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC), 1)
	pointID := ids.NewAt(ids.KindRecoveryPoint, time.Date(2026, 8, 27, 12, 1, 0, 0, time.UTC), 2)
	key, err := backupRecoveryPointEnvironmentIndexKey(environmentID, pointID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := backupRecoveryPointIDFromEnvironmentIndexKey(environmentID, key)
	if err != nil || got != pointID {
		t.Fatalf("stable continuation = %q, %v; want %q", got, err, pointID)
	}
	if _, err := backupRecoveryPointIDFromEnvironmentIndexKey(
		environmentID,
		backupRecoveryPointEnvironmentPrefix+environmentID+"/not-an-inverted-ulid",
	); err == nil {
		t.Fatal("continuation decoder accepted a storage-layout-shaped invalid boundary")
	}
}
