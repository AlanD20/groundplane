package monitoring

import (
	"context"
	"errors"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type historyFixture struct {
	hosts      []metrics.HostMeasurement
	services   []metrics.ServiceMeasurement
	aggregates []metrics.AggregatePoint
	err        error
}

func (fixture *historyFixture) ReadHost(
	context.Context,
	string,
	time.Time,
	time.Time,
	time.Duration,
) ([]metrics.HostMeasurement, error) {
	return fixture.hosts, fixture.err
}

func (fixture *historyFixture) ReadService(
	context.Context,
	string,
	time.Time,
	time.Time,
	time.Duration,
) ([]metrics.ServiceMeasurement, error) {
	return fixture.services, fixture.err
}

func (fixture *historyFixture) ReadWorkloadAggregate(
	context.Context,
	monitoringstore.AggregateScope,
	time.Time,
	time.Time,
	time.Duration,
) ([]metrics.AggregatePoint, error) {
	return fixture.aggregates, fixture.err
}

// Grouping must retain exact half-open timestamps, weight averages by valid
// samples and materialize partial and wholly missing buckets as null evidence.
func TestReadHostHistoryPreservesGapsAndStatistics(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	window := Window{
		Public: api.MonitoringWindow{
			Range: "1h", Start: start, End: start.Add(90 * time.Second), BucketSeconds: 30,
		},
		SourceResolution: metrics.SampleInterval,
		GroupSize:        2,
	}
	fixture := &historyFixture{hosts: []metrics.HostMeasurement{
		hostHistoryMeasurement("local", start, metricSummaryFixture(2, 1, 3, 1)),
		hostHistoryMeasurement("local", start.Add(15*time.Second), metricSummaryFixture(6, 6, 7, 1)),
		hostHistoryMeasurement("local", start.Add(45*time.Second), metricSummaryFixture(10, 9, 11, 1)),
	}}
	buckets, err := ReadHostHistory(context.Background(), fixture, "local", window)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 3 || !buckets[0].Start.Equal(start) ||
		!buckets[0].End.Equal(start.Add(30*time.Second)) ||
		!buckets[2].End.Equal(window.Public.End) {
		t.Fatalf("history timestamps = %#v", buckets)
	}
	first := buckets[0].Values.CPUCores
	if first == nil || first.Average == nil || *first.Average != 4 || *first.Minimum != 1 || *first.Maximum != 7 ||
		first.ValidSamples != 2 || first.ExpectedSamples != 2 {
		t.Fatalf("grouped CPU summary = %#v", first)
	}
	partial := buckets[1].Values.CPUCores
	if partial == nil || partial.Average == nil || *partial.Average != 10 ||
		partial.ValidSamples != 1 || partial.ExpectedSamples != 2 {
		t.Fatalf("partial CPU summary = %#v", partial)
	}
	missing := buckets[2].Values.CPUCores
	if missing == nil || missing.Average != nil || missing.Minimum != nil || missing.Maximum != nil ||
		missing.ValidSamples != 0 || missing.ExpectedSamples != 2 {
		t.Fatalf("missing CPU summary = %#v", missing)
	}
}

func TestReadHistoryDistinguishesUnavailableAndRejectsChangedOwner(t *testing.T) {
	t.Parallel()
	window, err := HistoryWindow(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC), "1h", 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadHostHistory(context.Background(), nil, "local", window); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) {
		t.Fatalf("nil history error = %v", err)
	}
	fixture := &historyFixture{hosts: []metrics.HostMeasurement{
		{
			HostID: "other",
			Bucket: metrics.Bucket{Start: window.Public.Start, End: window.Public.Start.Add(window.SourceResolution)},
		},
	}}
	if _, err := ReadHostHistory(context.Background(), fixture, "local", window); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) {
		t.Fatalf("changed owner error = %v", err)
	}
}

func TestHistoryUsesStorageCadenceWithinMinuteBuckets(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	window := Window{
		Public: api.MonitoringWindow{
			Range: "6h", Start: start, End: start.Add(2 * time.Minute), BucketSeconds: 60,
		},
		SourceResolution: time.Minute,
		GroupSize:        1,
	}
	cpu := metricSummaryFixture(2, 1, 3, 4)
	cpu.Expected = 4
	storage := metricSummaryFixture(1024, 1024, 1024, 1)
	storage.Expected = 4 // Existing store rows use the source tier's 15-second expectation.
	buckets, err := groupHistory(window, []historyRow{{
		Bucket: metrics.Bucket{Start: start, End: start.Add(time.Minute)},
		Summaries: metrics.MetricSummaries{
			CPUCores: cpu, WritableLayerBytes: storage, PersistentVolumeUsedBytes: storage,
		},
	}}, serviceHistoryFields)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 || buckets[0].Values.CPUCores.ExpectedSamples != 4 ||
		buckets[0].Values.WritableLayerBytes.ExpectedSamples != 1 ||
		buckets[0].Values.PersistentVolumeUsedBytes.ExpectedSamples != 1 ||
		buckets[1].Values.CPUCores.ExpectedSamples != 4 ||
		buckets[1].Values.WritableLayerBytes.ExpectedSamples != 1 ||
		buckets[1].Values.PersistentVolumeUsedBytes.ExpectedSamples != 1 {
		t.Fatalf("mixed-cadence buckets = %#v", buckets)
	}
}

func hostHistoryMeasurement(
	hostID string,
	at time.Time,
	summary metrics.MetricSummary,
) metrics.HostMeasurement {
	return metrics.HostMeasurement{
		HostID: hostID, Point: metrics.MetricPoint{At: at},
		Bucket: metrics.Bucket{Start: at, End: at.Add(metrics.SampleInterval)},
		Summaries: metrics.MetricSummaries{
			CPUCores: summary,
		},
		Available: true,
	}
}

func metricSummaryFixture(average, minimum, maximum float64, valid uint32) metrics.MetricSummary {
	return metrics.MetricSummary{
		Average: &average, Minimum: &minimum, Maximum: &maximum, Valid: valid, Expected: 1,
	}
}
