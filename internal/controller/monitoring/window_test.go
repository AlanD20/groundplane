package monitoring

import (
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
)

// A broad range must not return a chart larger than requested or silently
// include an open bucket whose average will change under an inspection.
func TestHistoryWindowsBoundPointsAndCloseSourceBucket(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 3, 57, 0, time.UTC)
	for _, selected := range []string{"1h", "6h", "24h", "7d"} {
		window, err := HistoryWindow(now, selected, 240)
		if err != nil {
			t.Fatal(err)
		}
		width := time.Duration(window.Public.BucketSeconds) * time.Second
		count := (window.Public.End.Sub(window.Public.Start) + width - 1) / width
		if count > 240 || window.Public.End.After(now) ||
			!window.Public.End.Equal(window.Public.End.Truncate(window.SourceResolution)) {
			t.Fatalf("unbounded or open history window: %+v", window)
		}
	}
	if _, err := HistoryWindow(now, "1h", 721); err == nil {
		t.Fatal("unbounded chart point request accepted")
	}
}

// Unequal sample counts need weighted means; averaging averages loses data.
// A retained unknown extremum must stay unavailable even beside known buckets.
func TestCombineMetricPreservesStatisticsAndMissingEvidence(t *testing.T) {
	ptr := func(value float64) *float64 { return &value }
	result, err := CombineMetric([]metrics.MetricSummary{
		{Average: ptr(2), Minimum: ptr(1), Maximum: ptr(3), Valid: 3, Expected: 4},
		{Average: ptr(6), Minimum: ptr(6), Maximum: ptr(6), Valid: 1, Expected: 4},
		{Expected: 4},
	})
	if err != nil || result.Average == nil || *result.Average != 3 || *result.Minimum != 1 || *result.Maximum != 6 ||
		result.Valid != 4 ||
		result.Expected != 12 {
		t.Fatalf("bucket statistics changed: %+v %v", result, err)
	}
	result, err = CombineMetric(
		[]metrics.MetricSummary{
			{Average: ptr(4), Valid: 1, Expected: 1},
			{Average: ptr(6), Minimum: ptr(6), Maximum: ptr(6), Valid: 1, Expected: 1},
		},
	)
	if err != nil || *result.Average != 5 || result.Minimum != nil || result.Maximum != nil {
		t.Fatal("unknown extrema were invented from averages")
	}
}
