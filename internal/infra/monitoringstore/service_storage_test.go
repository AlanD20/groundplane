package monitoringstore

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
)

// Independent Volume extrema do not establish the extrema of their sum. Keep
// the actual aligned Service storage total through rollup and reopen instead.
func TestServiceStorageHistoryRetainsAlignedTotalExtrema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage-history.sqlite")
	store := openTestStore(t, path, 0)
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for index, sizes := range [][2]uint64{{10, 2}, {2, 10}} {
		measurement := testMeasurement(
			"service",
			monitoring.OwnerWorkload,
			at.Add(time.Duration(index)*time.Minute),
			nil,
			"",
			nil,
		)
		total := sizes[0] + sizes[1]
		measurement.Point.PersistentVolumeUsedBytes = &total
		measurement.Volumes = []monitoring.VolumeUsage{
			{VolumeID: "one", UsedBytes: &sizes[0]},
			{VolumeID: "two", UsedBytes: &sizes[1]},
		}
		if err := store.Append(t.Context(), measurement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Maintain(t.Context(), at.Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, path, 0)
	defer store.Close()
	values, err := store.ReadService(t.Context(), "service", at, at.Add(5*time.Minute), monitoring.CoarseResolution)
	if err != nil || len(values) != 1 {
		t.Fatalf("storage history: %#v, %v", values, err)
	}
	value := values[0].Summaries.PersistentVolumeUsedBytes
	if value.Average == nil || value.Minimum == nil || value.Maximum == nil || *value.Average != 12 ||
		*value.Minimum != 12 ||
		*value.Maximum != 12 ||
		value.Valid != 2 {
		t.Fatalf("aligned storage total was reconstructed from independent extrema: %#v", value)
	}
}
