package monitoringstore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestMaintenanceRecoversWritesAtPageCeilingWithoutExpiringRecentHistory(t *testing.T) {
	ctx := t.Context()
	store, err := Open(ctx, Options{
		Path:                 filepath.Join(t.TempDir(), "full.sqlite"),
		MaximumDatabaseBytes: 128 << 10,
		WALTargetBytes:       32 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 11, 23, 1, 0, 0, 0, time.UTC)
	memory, used := uint64(8000), uint64(4000)
	recent := now.Add(-time.Minute)
	appendAt := func(at time.Time) error {
		return store.AppendHost(ctx, monitoring.HostMeasurement{
			HostID: "host", Available: true,
			Point: monitoring.MetricPoint{At: at, MemoryTotalBytes: &memory, MemoryUsedBytes: &used},
		})
	}
	if err := appendAt(recent); err != nil {
		t.Fatal(err)
	}
	oldest := now.Add(-9 * time.Hour)
	for index := range 1024 {
		err = appendAt(oldest.Add(time.Duration(index) * monitoring.SampleInterval))
		if err != nil {
			break
		}
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_FULL {
		t.Fatalf("expected actual page exhaustion, got %v", err)
	}
	result, err := store.Maintain(ctx, now)
	if err != nil || result.ExpiredRows == 0 {
		t.Fatalf("maintenance failed at the real page ceiling: %+v, %v", result, err)
	}
	if err := appendAt(now); err != nil {
		t.Fatalf("append after maintenance: %v", err)
	}
	rows, err := store.ReadHost(ctx, "host", recent, now.Add(monitoring.SampleInterval), monitoring.SampleInterval)
	if err != nil || len(rows) != 2 || !rows[0].Point.At.Equal(recent) || !rows[1].Point.At.Equal(now) ||
		rows[0].Point.MemoryTotalBytes == nil || *rows[0].Point.MemoryTotalBytes != memory {
		t.Fatalf("recent and new Host history: %+v, %v", rows, err)
	}
}
