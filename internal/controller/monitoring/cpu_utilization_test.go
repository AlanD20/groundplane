package monitoring

import (
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/api"
)

func TestWorkloadHistoryGroupsPairedCPUUtilization(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	window := Window{
		Public:           api.MonitoringWindow{Start: start, End: start.Add(30 * time.Second), BucketSeconds: 30},
		SourceResolution: metrics.SampleInterval, GroupSize: 2,
	}
	row := func(offset time.Duration, usage, limit float64) historyRow {
		begin := start.Add(offset)
		return historyRow{
			Bucket: metrics.Bucket{Start: begin, End: begin.Add(metrics.SampleInterval)},
			Summaries: metrics.MetricSummaries{
				CPUCores:      metricSummaryFixture(usage, usage, usage, 1),
				CPULimitCores: metricSummaryFixture(limit, limit, limit, 1),
			},
		}
	}
	// The limit changed between samples. The average of 50% and 25% is 37.5%,
	// even though dividing the bucket-wide aggregate usage by the aggregate limit yields 30%.
	buckets, err := groupHistory(
		window,
		[]historyRow{row(0, 0.25, 0.5), row(15*time.Second, 0.5, 2)},
		serviceHistoryFields,
	)
	if err != nil {
		t.Fatal(err)
	}
	ratio := buckets[0].Values.CPUUtilizationRatio
	if ratio == nil || ratio.Average == nil || ratio.Minimum == nil || ratio.Maximum == nil ||
		*ratio.Average != 0.375 || *ratio.Minimum != 0.25 || *ratio.Maximum != 0.5 ||
		ratio.ValidSamples != 2 || ratio.ExpectedSamples != 2 {
		t.Fatalf("grouped utilization = %#v", ratio)
	}
}

func TestWorkloadCPUUtilizationRequiresKnownLimitAcrossRetainedSlots(t *testing.T) {
	t.Parallel()
	usage := metricSummaryFixture(0.75, 0.5, 1, 2)
	usage.Expected = 4
	for _, test := range []struct {
		name  string
		limit metrics.MetricSummary
		valid bool
	}{
		{"constant limit", metricSummaryFixture(2, 2, 2, 4), true},
		{"missing limit at some slots", metricSummaryFixture(2, 2, 2, 2), false},
		{"changing limit", metricSummaryFixture(2, 1, 3, 4), false},
		{"zero limit", metricSummaryFixture(0, 0, 0, 4), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ratio := workloadCPUUtilization(usage, test.limit, 4)
			if !test.valid {
				if ratio.Average != nil || ratio.Valid != 0 {
					t.Fatalf("unpaired utilization = %#v", ratio)
				}
				return
			}
			if ratio.Average == nil || ratio.Minimum == nil || ratio.Maximum == nil ||
				*ratio.Average != 0.375 || *ratio.Minimum != 0.25 || *ratio.Maximum != 0.5 ||
				ratio.Valid != 2 || ratio.Expected != 4 {
				t.Fatalf("retained utilization = %#v", ratio)
			}
		})
	}
}

func TestWorkloadLivePublishesPercentOnlyWithPositiveLimit(t *testing.T) {
	t.Parallel()
	now := time.Now()
	summaries := metrics.MetricSummaries{
		CPUCores:      metricSummaryFixture(0.125, 0.125, 0.125, 1),
		CPULimitCores: metricSummaryFixture(0.25, 0.25, 0.25, 1),
	}
	point := metrics.MetricPoint{At: now}
	coverage := metrics.Coverage{Eligible: 1, Sampled: 1}
	live := livePoint(point, summaries, coverage, true, now)
	if ratio := live.Values.CPUUtilizationRatio; ratio == nil || ratio.Average == nil || *ratio.Average != 0.5 {
		t.Fatalf("live utilization = %#v", ratio)
	}
	summaries.CPULimitCores = metrics.MetricSummary{}
	live = livePoint(point, summaries, coverage, true, now)
	if live.Values.CPUUtilizationRatio != nil || live.Values.CPUCores == nil {
		t.Fatal("missing limit must withhold utilization while preserving CPU usage")
	}
}
